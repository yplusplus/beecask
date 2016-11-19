package beecask

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yplusplus/ylog"
)

var (
	ErrInvalid        = fmt.Errorf("Operation is invalid")
	ErrDataCorruption = fmt.Errorf("Data corruption")
	ErrDataNotExist   = fmt.Errorf("Data not exist")
)

type Beecask struct {
	options       *options
	dirPath       string
	minDataFileId uint64
	maxDataFileId uint64
	keydir        *KeyDir
	activeKeydir  *KeyDir // active-file key dir, use to generate hint-file
	activeFile    *ActiveFile
	wg            sync.WaitGroup
	rwMutex       sync.RWMutex // RWMutex for keydir and activeFile
	dataFileCache *DataFileCache
	isMerging     int32 // atomic
}

func NewBeecask(options options, dirPath string) (*Beecask, error) {
	bc := &Beecask{
		options:       &options,
		dirPath:       dirPath,
		keydir:        NewKeyDir(),
		minDataFileId: 0,
		maxDataFileId: 0,
		activeFile:    nil,
		dataFileCache: NewDataFileCache(options.MaxOpenFiles),
		isMerging:     0,
	}

	err := bc.scan()
	if err != nil {
		ylog.Error(err)
		return nil, err
	}

	return bc, nil
}

func (bc *Beecask) Get(key string) ([]byte, error) {
	bc.rwMutex.RLock()
	kdItem := bc.keydir.Get(key)
	if kdItem == nil || (kdItem.flag&RECORD_FLAG_BIT_DELETE) > 0 {
		// Record not exist or has been deleted
		bc.rwMutex.RUnlock()
		return nil, ErrDataNotExist
	}

	var reader interface {
		ReadRecordAt(int64) (*Record, error)
	}
	if kdItem.fileId == bc.activeFile.fileId {
		ylog.Debug("data on active file.")
		// Data on active file
		reader = bc.activeFile
		defer bc.rwMutex.RUnlock()
	} else {
		ylog.Debug("data on data file.")
		// Data on data file
		path := getDataFilePath(bc.dirPath, kdItem.fileId)
		entry, err := bc.dataFileCache.Ref(path, kdItem.fileId)
		bc.rwMutex.RUnlock()
		if err != nil {
			ylog.Errorf("Ref datafile[%d] failed, err=%s", kdItem.fileId, err)
			return nil, err
		}
		defer bc.dataFileCache.Unref(entry)
		reader = entry.df
	}

	r, err := reader.ReadRecordAt(int64(kdItem.valuePos))
	if err != nil {
		ylog.Errorf("Read record at datafile[%d] @ [%d] failed, err=%s", kdItem.fileId, kdItem.valuePos, err)
		return nil, err
	}

	// check data valid
	if string(r.key) != key {
		ylog.Errorf("Record[%s] is not expected %s in datafile[%d] @ [%d]",
			string(r.key), key, kdItem.fileId, kdItem.valuePos)
		return nil, ErrDataCorruption
	}

	// check data expiration
	ylog.Debugf("Record[%s] expiration[%d]", key, r.expiration)
	if r.expiration > 0 && r.expiration <= time.Now().Unix() {
		return nil, ErrDataNotExist
	}

	return r.value, nil
}

// Set sets a record(key, value) without expiration
func (bc *Beecask) Set(key string, value []byte) error {
	return bc.SetWithExpiration(key, value, 0)
}

// SetWithExpiration sets a record(key, value) with expiration
func (bc *Beecask) SetWithExpiration(key string, value []byte, expiration int64) error {
	return bc.set(key, value, false, expiration)
}

func (bc *Beecask) Delete(key string) error {
	return bc.set(key, nil, true, 0)
}

func (bc *Beecask) Keys() []string {
	bc.rwMutex.RLock()
	defer bc.rwMutex.RUnlock()
	return bc.keydir.Keys()
}

func (bc *Beecask) Merge() {
	bc.merge()
}

func (bc *Beecask) Sync() error {
	bc.rwMutex.Lock()
	defer bc.rwMutex.Unlock()
	return bc.activeFile.Sync()
}

func (bc *Beecask) Close() {
	bc.rwMutex.Lock()
	defer bc.rwMutex.Unlock()

	bc.activeFile.Close()
	bc.dataFileCache.Close()
	bc.wg.Wait()
}

func (bc *Beecask) scan() error {
	err := os.MkdirAll(bc.dirPath, 0755)
	if err != nil {
		ylog.Error(err)
		return err
	}

	filenames, err := ReadDir(bc.dirPath)
	if err != nil {
		ylog.Error(err)
		return err
	}

	for _, name := range filenames {
		// only scan data file
		if !strings.HasSuffix(name, ".data") {
			continue
		}

		intFileId, err := strconv.Atoi(strings.TrimSuffix(name, ".data"))
		if err != nil {
			ylog.Error(err)
			return err
		}
		fileId := uint64(intFileId)

		err = bc.restore(fileId)
		if err != nil {
			ylog.Error(err)
			return err
		}

		if bc.minDataFileId == 0 || bc.minDataFileId > fileId {
			bc.minDataFileId = fileId
		}
		if bc.maxDataFileId == 0 || bc.maxDataFileId < fileId {
			bc.maxDataFileId = fileId
		}
	}

	// open active file
	if bc.maxDataFileId == 0 {
		bc.minDataFileId++
		bc.maxDataFileId++
	}
	fileId := bc.maxDataFileId

	// Evict datafile from cache if exist to prevent opening active-file twice
	bc.dataFileCache.Evict(fileId)

	path := getDataFilePath(bc.dirPath, fileId)
	bc.activeFile, err = NewActiveFile(path, fileId, bc.options.WriteBufferSize)
	if err != nil {
		ylog.Error(err)
		return err
	}
	bc.activeKeydir = NewKeyDir()
	return nil
}

func (bc *Beecask) restore(fileId uint64) (err error) {
	// try to restore data from hint file
	hintfilename := getHintFilePath(bc.dirPath, fileId)
	_, err = os.Stat(hintfilename)
	if err == nil || os.IsExist(err) {
		// restore from hint file
		err = bc.restoreFromHintFile(fileId)
		if err == nil {
			ylog.Infof("restore from hintfile[%d] succ.", fileId)
			return
		}
		ylog.Errorf("restore from hintfile[%d] failed, err=%s.", fileId, err)
	}

	// restore from data file
	err = bc.restoreFromDataFile(fileId)
	if err != nil {
		ylog.Errorf("restore from datafile[%d] failed, err=%s.", fileId, err)
		return
	}
	ylog.Infof("restore from datafile[%d] succ.", fileId)
	return
}

func (bc *Beecask) restoreFromHintFile(fileId uint64) error {
	path := getHintFilePath(bc.dirPath, fileId)
	rhf, err := NewReadableHintFile(path)
	if err != nil {
		return err
	}
	defer rhf.Close()
	item := &KDItem{}
	err = rhf.ForEachItem(func(hitem *HintItem) error {
		key := string(hitem.key)
		kdItem := bc.keydir.Get(key)

		// fileter old data
		//if kdItem == nil || absInt64(kdItem.version) < absInt64(hitem.version) {
		if kdItem == nil || fileId > kdItem.fileId || (fileId == kdItem.fileId && hitem.valuePos > kdItem.valuePos) {
			item.fileId = fileId
			item.valueSize = hitem.valueSize
			item.valuePos = hitem.valuePos
			bc.keydir.Set(key, item)
		}
		return nil
	})
	if err != nil {
		ylog.Error(err)
	}
	return err
}

func (bc *Beecask) restoreFromDataFile(fileId uint64) error {
	path := getDataFilePath(bc.dirPath, fileId)
	entry, err := bc.dataFileCache.Ref(path, fileId)
	if err != nil {
		ylog.Errorf("Ref datafile[%d] failed, err=%s.", fileId, err)
		return err
	}
	defer bc.dataFileCache.Unref(entry)
	item := &KDItem{}
	err = entry.df.ForEachRecord(func(r *Record, fileId uint64, offset int64) error {
		key := string(r.key)
		kdItem := bc.keydir.Get(key)

		// filter old data
		if kdItem == nil || fileId > kdItem.fileId || (fileId == kdItem.fileId && uint32(offset) > kdItem.valuePos) {
			item.fileId = entry.df.fileId
			item.valuePos = uint32(offset)
			item.valueSize = r.valueSize
			item.flag = r.flag
			bc.keydir.Set(key, item)
		}
		return nil
	})
	if err != nil {
		ylog.Error(err)
	}
	return err
}

func (bc *Beecask) set(key string, value []byte, delete bool, expiration int64) error {
	// TODO: Check key and value size
	bc.rwMutex.Lock()
	defer bc.rwMutex.Unlock()

	item := &Record{
		crc:        0,
		flag:       0,
		expiration: expiration,
		keySize:    uint32(len(key)),
		valueSize:  uint32(len(value)),
		key:        []byte(key),
		value:      value,
	}

	if delete {
		item.flag |= RECORD_FLAG_BIT_DELETE
	}

	return bc.setRecord(item)
}

// setRecord requires bc.rwMutex held
func (bc *Beecask) setRecord(r *Record) (err error) {
	// rotate active file
	if bc.activeFile.Size()+r.Size() >= bc.options.MaxFileSize {
		bc.rotateActiveFile()
	}

	// write record to active file
	offset, err := bc.activeFile.WriteRecord(r)
	if err != nil {
		ylog.Fatalf("Write record to activefile failed, err=%s", err)
	}

	// update key dir
	kdItem := &KDItem{
		fileId:    bc.activeFile.FileId(),
		valuePos:  uint32(offset),
		valueSize: r.valueSize,
		flag:      r.flag,
	}

	key := string(r.key)
	bc.keydir.Set(key, kdItem)
	bc.activeKeydir.Set(key, kdItem)

	return nil
}

// rotateActiveFile requires bc.rwMutex held
func (bc *Beecask) rotateActiveFile() {
	bc.wg.Add(1)

	// generate hint file in another goroutine
	go bc.generateHintFile(bc.activeKeydir, bc.activeFile.FileId())

	bc.activeFile.Close()
	bc.activeFile = nil

	bc.maxDataFileId++
	fileId := bc.maxDataFileId

	bc.activeKeydir = NewKeyDir()
	path := getDataFilePath(bc.dirPath, fileId)
	var err error
	bc.activeFile, err = NewActiveFile(path, fileId, bc.options.WriteBufferSize)
	if err != nil {
		ylog.Fatalf("New activefile[%d] failed, err=%s", fileId, err)
	}

	ylog.Infof("Rotato to new activefile[%d]", fileId)
}

func (bc *Beecask) generateHintFile(keydir *KeyDir, fileId uint64) {
	defer bc.wg.Done()

	path := getHintFilePath(bc.dirPath, fileId)
	whf, err := NewWritableHintFile(path)
	if err != nil {
		ylog.Errorf("New writable hint-file[%d] failed, err=%s", fileId, err)
		return
	}
	defer whf.Close()

	// TODO more effient
	item := &HintItem{}
	for k, v := range keydir.dict {
		item.flag = v.flag
		item.keySize = uint32(len(k))
		item.valueSize = v.valueSize
		item.valuePos = v.valuePos
		item.key = []byte(k)
		buff := item.Encode()
		if err = whf.Append(buff); err != nil {
			ylog.Errorf("Append data to hintfile[%d] failed, err = %s", fileId, err)
			return
		}
	}
}

func (bc *Beecask) merge() {
	// make sure only one merge running
	if !atomic.CompareAndSwapInt32(&bc.isMerging, 0, 1) {
		ylog.Info("There is a merge process running.")
		return
	}
	defer atomic.CompareAndSwapInt32(&bc.isMerging, 1, 0)
	ylog.Trace("Involke to merge()")

	bc.rwMutex.Lock()
	end := bc.activeFile.fileId
	bc.rwMutex.Unlock()

	for begin := &bc.minDataFileId; *begin < end; *begin++ {
		err := bc.mergeDataFile(*begin)
		if err != nil {
			ylog.Errorf("Merge datafile[%d] failed, err=%s", *begin, err)
			return
		}
	}
}

// mergeDataFile requires bc.rwMutex held
func (bc *Beecask) mergeDataFile(fileId uint64) error {
	path := getDataFilePath(bc.dirPath, fileId)
	entry, err := bc.dataFileCache.Ref(path, fileId)
	if err != nil {
		ylog.Errorf("Ref datafile[%d] failed, err=%s", fileId, err)
		return err
	}
	defer bc.dataFileCache.Unref(entry)

	begin := time.Now()
	err = entry.df.ForEachRecord(func(r *Record, fileId uint64, offset int64) error {
		bc.rwMutex.Lock()
		defer bc.rwMutex.Unlock()
		key := string(r.key)
		kdItem := bc.keydir.Get(key)
		var err error
		if kdItem != nil && fileId == kdItem.fileId && uint32(offset) == kdItem.valuePos {
			// deleted or expired
			if (r.flag&RECORD_FLAG_BIT_DELETE) > 0 || (r.expiration > 0 && r.expiration <= begin.Unix()) {
				bc.keydir.Delete(key)
				return nil
			}
			if err = bc.setRecord(r); err != nil {
				ylog.Errorf("Set Record[key%s] failed, err=%s", key, err)
			}
		}
		return err
	})

	if err != nil {
		ylog.Errorf("Merge datafile[%d] failed, err=%s", fileId, err)
		return err
	}
	end := time.Now()

	// Remove data file and hint file
	os.Remove(path)
	os.Remove(getHintFilePath(bc.dirPath, fileId))

	ylog.Tracef("Merge datafile[%d](filesize:%d) succ in %fs.", fileId, entry.df.fileId, end.Sub(begin).Seconds())
	return nil
}
