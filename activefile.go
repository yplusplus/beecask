package beecask

import (
	"encoding/binary"
	"hash/crc32"
	"os"

	"github.com/yplusplus/ylog"
)

type ActiveFile struct {
	*FileWithBuffer
	fileId uint64
}

func NewActiveFile(path string, fileId uint64, wbufSize int) (*ActiveFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		ylog.Error(err)
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		ylog.Error(err)
		f.Close()
		return nil, err
	}
	ylog.Tracef("New activefile[%d] succ", fileId)
	return &ActiveFile{
		FileWithBuffer: NewFileWithBuffer(f, stat.Size(), wbufSize),
		fileId:         fileId,
	}, nil
}

// ReadRecordAt reads a record from specific offset
func (af *ActiveFile) ReadRecordAt(offset int64) (*Record, error) {
	data, err := af.ReadAt(offset, DATA_ITEM_HEADER_SIZE)
	if err != nil {
		// may return io.EOF
		return nil, err
	}

	r := Record{
		crc:        binary.LittleEndian.Uint32(data[0:4]),
		flag:       binary.LittleEndian.Uint32(data[4:8]),
		expiration: int64(binary.LittleEndian.Uint64(data[8:16])),
		keySize:    binary.LittleEndian.Uint32(data[16:20]),
		valueSize:  binary.LittleEndian.Uint32(data[20:24]),
		key:        nil,
		value:      nil,
	}

	offset += DATA_ITEM_HEADER_SIZE
	r.key, err = af.ReadAt(offset, int64(r.keySize))
	if err != nil {
		// may return io.EOF
		return nil, err
	}

	offset += int64(r.keySize)
	r.value, err = af.ReadAt(offset, int64(r.valueSize))
	if err != nil {
		// may return io.EOF
		return nil, err
	}

	// check crc
	crc := crc32.ChecksumIEEE(data[4:])
	crc = crc32.Update(crc, crc32.IEEETable, r.key)
	crc = crc32.Update(crc, crc32.IEEETable, r.value)
	if crc != r.crc {
		return nil, ErrDataCorruption
	}

	return &r, nil
}

func (af *ActiveFile) WriteRecord(r *Record) (int64, error) {
	if r.keySize != uint32(len(r.key)) || r.valueSize != uint32(len(r.value)) {
		ylog.Errorf("r.keySize[%d] len(r.key)[%d] r.valueSize[%d] len(r.value)[%d]", r.keySize, len(r.key), r.valueSize, len(r.value))
		return -1, ErrInvalid
	}

	header := make([]byte, DATA_ITEM_HEADER_SIZE)
	binary.LittleEndian.PutUint32(header[4:8], r.flag)
	binary.LittleEndian.PutUint64(header[8:16], uint64(r.expiration))
	binary.LittleEndian.PutUint32(header[16:20], r.keySize)
	binary.LittleEndian.PutUint32(header[20:24], r.valueSize)

	// calculate crc32
	r.crc = crc32.ChecksumIEEE(header[4:])
	r.crc = crc32.Update(r.crc, crc32.IEEETable, r.key)
	r.crc = crc32.Update(r.crc, crc32.IEEETable, r.value)

	binary.LittleEndian.PutUint32(header[0:4], r.crc)

	var err error
	offset := af.Size()
	if _, err = af.Write(header); err != nil {
		return -1, err
	}
	if _, err = af.Write(r.key); err != nil {
		return -1, err
	}
	if _, err = af.Write(r.value); err != nil {
		return -1, err
	}
	return offset, err
}

func (af *ActiveFile) FileId() uint64 {
	return af.fileId
}
