package beecask

type options struct {
	WriteBufferSize int   // active-file write buffer size
	MaxFileSize     int64 // max file size
	MaxOpenFiles    int   // max open files
}

func NewOptions() *options {
	return &options{
		WriteBufferSize: 4 << 20,  // 4M
		MaxFileSize:     32 << 20, // 32M
		MaxOpenFiles:    1000,
	}
}
