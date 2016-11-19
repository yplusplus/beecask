package beecask

import (
	"github.com/yplusplus/ylog"
	"io"
	"os"
	"syscall"
)

type RandomAccessFile interface {
	ReadAt(offset, len int64) ([]byte, error)
	Size() int64
	Close() error
}

type MmapFile struct {
	mmapedRegion []byte
	f            *os.File
}

func NewMmapFile(f *os.File) (*MmapFile, error) {
	stat, err := f.Stat()
	if err != nil {
		ylog.Errorf("Stat failed, err = %s.", err)
		return nil, err
	}

	var region []byte = nil
	if int(stat.Size()) > 0 {
		region, err = syscall.Mmap(int(f.Fd()), 0, int(stat.Size()), syscall.PROT_READ, syscall.MAP_PRIVATE)
		if err != nil {
			ylog.Errorf("syscall.Mmap failed, err = %s.", err)
			return nil, err
		}
	}

	return &MmapFile{mmapedRegion: region, f: f}, nil
}

func (file *MmapFile) ReadAt(offset, len int64) ([]byte, error) {
	if offset > file.Size() {
		ylog.Errorf("Expect to read from offset[%d], but file size is %d.", offset, file.Size())
		return nil, ErrInvalid
	}

	if offset+len > file.Size() {
		return file.mmapedRegion[offset:], io.EOF
	}

	return file.mmapedRegion[offset : offset+len], nil
}

func (file *MmapFile) Size() int64 {
	return int64(len(file.mmapedRegion))
}

func (file *MmapFile) Close() error {
	if file.mmapedRegion != nil {
		syscall.Munmap(file.mmapedRegion)
	}
	return file.f.Close()
}

type FileWithBuffer struct {
	f    *os.File
	size int64
	wbuf []byte
	n    int
}

func NewFileWithBuffer(f *os.File, size int64, wbufSize int) *FileWithBuffer {
	return &FileWithBuffer{
		f:    f,
		size: size,
		wbuf: make([]byte, wbufSize),
		n:    0,
	}
}

func (file *FileWithBuffer) ReadAt(offset, size int64) ([]byte, error) {
	if offset > file.size {
		return nil, ErrInvalid
	}
	data := make([]byte, size)
	fsize := file.size - int64(file.n)
	// data all from buffer
	if offset >= fsize {
		n := copy(data, file.wbuf[offset-fsize:file.n])
		var err error
		if int64(n) < size {
			err = io.EOF
		}
		return data[:n], err
	}

	// data all from file.f
	if offset+size <= fsize {
		n, err := file.f.ReadAt(data, offset)
		return data[:n], err
	}

	// data from both file.f and buffer
	n, err := file.f.ReadAt(data[:fsize-offset], offset)
	if err != nil {
		return data[:n], err
	}
	n += copy(data[fsize-offset:], file.wbuf[:file.n])
	if int64(n) < size {
		err = io.EOF
	}
	return data[:n], err
}

func (file *FileWithBuffer) Write(data []byte) (nn int, err error) {
	for len(data) > len(file.wbuf)-file.n && err == nil {
		var n int
		if file.n == 0 {
			// Large write, empty buffer
			// Write directly to avoid copy
			n, err = file.f.Write(data)
		} else {
			n = copy(file.wbuf[file.n:], data)
			file.n += n
			err = file.Flush()
		}
		nn += n
		data = data[n:]
	}
	if err != nil {
		ylog.Error(err)
		file.size += int64(nn)
		return
	}
	n := copy(file.wbuf[file.n:], data)
	file.n += n
	nn += n
	file.size += int64(nn)
	return
}

func (file *FileWithBuffer) Flush() error {
	if file.n == 0 {
		return nil
	}
	n, err := file.f.Write(file.wbuf[:file.n])
	if err != nil {
		if n > 0 && n < file.n {
			copy(file.wbuf[:file.n-n], file.wbuf[n:file.n])
		}
		file.n -= n
		return err
	}
	file.n = 0
	return nil
}

func (file *FileWithBuffer) Sync() error {
	file.Flush()
	return file.f.Sync()
}

func (file *FileWithBuffer) Size() int64 {
	return file.size
}

func (file *FileWithBuffer) Close() error {
	file.Flush()
	return file.f.Close()
}
