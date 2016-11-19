package beecask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/yplusplus/ylog"
)

var (
	ErrReadHintItem = fmt.Errorf("read hint item failed.")
)

const (
	HINT_ITEM_HEADER_SIZE = 24
)

type HintItem struct {
	flag       uint32
	expiration int64
	keySize    uint32
	valueSize  uint32
	valuePos   uint32
	key        []byte
}

func (item *HintItem) Encode() []byte {
	buff := make([]byte, HINT_ITEM_HEADER_SIZE+int(item.keySize))
	binary.LittleEndian.PutUint32(buff[0:4], item.flag)
	binary.LittleEndian.PutUint64(buff[4:12], uint64(item.expiration))
	binary.LittleEndian.PutUint32(buff[12:16], item.keySize)
	binary.LittleEndian.PutUint32(buff[16:20], item.valueSize)
	binary.LittleEndian.PutUint32(buff[20:24], item.valuePos)
	copy(buff[HINT_ITEM_HEADER_SIZE:], item.key)
	return buff
}

type ReadableHintFile struct {
	file RandomAccessFile
}

func NewReadableHintFile(path string) (*ReadableHintFile, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	file, err := NewMmapFile(f)
	if err != nil {
		return nil, err
	}

	return &ReadableHintFile{file: file}, nil
}

func (rhf *ReadableHintFile) ReadItem() (*HintItem, error) {
	return nil, nil
}

func (rhf *ReadableHintFile) readItemAt(offset int64) (*HintItem, error) {
	buff, err := rhf.file.ReadAt(offset, HINT_ITEM_HEADER_SIZE)
	if err != nil {
		ylog.Warn(err)
		return nil, err
	}

	item := &HintItem{
		flag:       binary.LittleEndian.Uint32(buff[0:4]),
		expiration: int64(binary.LittleEndian.Uint64(buff[4:12])),
		keySize:    binary.LittleEndian.Uint32(buff[12:16]),
		valueSize:  binary.LittleEndian.Uint32(buff[16:20]),
		valuePos:   binary.LittleEndian.Uint32(buff[20:24]),
		key:        nil,
	}

	offset += HINT_ITEM_HEADER_SIZE
	item.key, err = rhf.file.ReadAt(offset, int64(item.keySize))
	if err != nil {
		// may return io.EOF
		ylog.Warn(err)
		return nil, err
	}

	return item, nil
}

func (rhf *ReadableHintFile) ForEachItem(fn func(item *HintItem) error) error {
	var offset int64 = 0
	for {
		item, err := rhf.readItemAt(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			ylog.Warn(err)
			return err
		}
		err = fn(item)
		if err != nil {
			return err
		}
		offset += int64(HINT_ITEM_HEADER_SIZE) + int64(item.keySize)
	}
	return nil
}

func (rhf *ReadableHintFile) Close() error {
	return rhf.file.Close()
}

type WritableHintFile struct {
	file *os.File
	wbuf *bufio.Writer
}

func NewWritableHintFile(path string) (*WritableHintFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &WritableHintFile{file: f, wbuf: bufio.NewWriter(f)}, nil
}

func (whf *WritableHintFile) Append(buff []byte) error {
	_, err := whf.wbuf.Write(buff)
	if err != nil {
		ylog.Warn(err)
	}
	return err
}

func (whf *WritableHintFile) Close() error {
	whf.wbuf.Flush()
	return whf.file.Close()
}
