package beecask

import (
	"container/list"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/yplusplus/ylog"
)

const (
	defaultCacheCapacity = 512
)

type RecordFn func(*Record, uint64, int64) error

type DataFile struct {
	file   RandomAccessFile
	fileId uint64
}

func NewDataFile(path string, fileId uint64) (*DataFile, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		ylog.Error(err)
		return nil, err
	}
	file, err := NewMmapFile(f)
	if err != nil {
		ylog.Error(err)
		f.Close()
		return nil, err
	}

	ylog.Tracef("Open datafile[%d]", fileId)
	return &DataFile{file: file, fileId: fileId}, nil
}

// ReadRecordAt reads a record from specific offset
func (df *DataFile) ReadRecordAt(offset int64) (*Record, error) {
	buff, err := df.file.ReadAt(offset, DATA_ITEM_HEADER_SIZE)
	if err != nil {
		ylog.Warn(err)
		return nil, err
	}

	// calculate crc
	r := &Record{
		crc:        binary.LittleEndian.Uint32(buff[0:4]),
		flag:       binary.LittleEndian.Uint32(buff[4:8]),
		expiration: int64(binary.LittleEndian.Uint64(buff[8:16])),
		keySize:    binary.LittleEndian.Uint32(buff[16:20]),
		valueSize:  binary.LittleEndian.Uint32(buff[20:24]),
		key:        nil,
		value:      nil,
	}

	offset += DATA_ITEM_HEADER_SIZE
	r.key, err = df.file.ReadAt(offset, int64(r.keySize))
	if err != nil {
		ylog.Warn(err)
		return nil, err
	}

	offset += int64(r.keySize)
	r.value, err = df.file.ReadAt(offset, int64(r.valueSize))
	if err != nil {
		ylog.Warn(err)
		return nil, err
	}

	crc := crc32.ChecksumIEEE(buff[4:])
	crc = crc32.Update(crc, crc32.IEEETable, r.key)
	crc = crc32.Update(crc, crc32.IEEETable, r.value)
	if crc != r.crc {
		ylog.Errorf("check crc32 failed")
		return nil, ErrDataCorruption
	}

	return r, nil
}

// ForEachRecord runs fn on each record until encounters error
func (df *DataFile) ForEachRecord(fn RecordFn) error {
	var offset int64 = 0
	for {
		r, err := df.ReadRecordAt(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			ylog.Warn(err)
			return err
		}
		err = fn(r, df.fileId, offset)
		if err != nil {
			return err
		}
		offset += r.Size()
	}
	return nil
}

func (df *DataFile) Size() int64 {
	return df.file.Size()
}

func (df *DataFile) Close() error {
	ylog.Tracef("Close datafile[%d]", df.fileId)
	return df.file.Close()
}

type CacheEntry struct {
	df       *DataFile
	refCount int32 // Contains references from external and cache.list
	inList   bool  // indicating entry is in list or not
}

// DataFileCache is a LRU cache which caches data files
type DataFileCache struct {
	l        *list.List
	hash     map[uint64]*list.Element
	capacity int
	mu       sync.Mutex
}

func NewDataFileCache(capacity int) *DataFileCache {
	if capacity <= 0 {
		capacity = defaultCacheCapacity
	}
	return &DataFileCache{
		l:        list.New(),
		hash:     make(map[uint64]*list.Element, capacity),
		capacity: capacity,
	}
}

func (cache *DataFileCache) Ref(path string, fileId uint64) (*CacheEntry, error) {
	cache.mu.Lock()
	defer cache.mu.Unlock()

	var entry *CacheEntry
	ele, ok := cache.hash[fileId]
	if !ok {
		ylog.Tracef("Datafile[%d] not in cache, create a cache entry associated with it.", fileId)
		// Create a new cache entry when not in cache
		df, err := NewDataFile(path, fileId)
		if err != nil {
			ylog.Errorf("New datafile[%s] failed, err = %s", path, err)
			return nil, err
		}
		entry = &CacheEntry{
			df:       df,
			refCount: 1,
			inList:   true,
		}
		ele = cache.l.PushFront(entry)
		cache.hash[fileId] = ele
	} else {
		ylog.Tracef("Datafile[%d] in cache", fileId)
	}

	entry = ele.Value.(*CacheEntry)
	entry.refCount++
	if entry.inList {
		cache.l.MoveToFront(ele)
	}

	for cache.l.Len() > cache.capacity {
		e := cache.l.Remove(cache.l.Back()).(*CacheEntry)
		e.inList = false
		cache.unref(e)
	}

	return entry, nil
}

// unref requires cache.mu held
func (cache *DataFileCache) unref(entry *CacheEntry) {
	entry.refCount--
	if entry.refCount == 0 && !entry.inList {
		// No reference and not in cache
		// Close the associated file
		delete(cache.hash, entry.df.fileId)
		entry.df.Close()
	}
}

func (cache *DataFileCache) Unref(entry *CacheEntry) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.unref(entry)
}

func (cache *DataFileCache) Evict(fileId uint64) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	ele, ok := cache.hash[fileId]
	if ok {
		entry := ele.Value.(*CacheEntry)
		if entry.inList {
			cache.l.Remove(ele)
			entry.inList = false
			cache.unref(entry)
		}
	}
}

func (cache *DataFileCache) Close() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	for cache.l.Len() > 0 {
		entry := cache.l.Remove(cache.l.Back()).(*CacheEntry)
		entry.inList = false
		cache.unref(entry)
	}
}
