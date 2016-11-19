package beecask

const (
	DATA_ITEM_HEADER_SIZE = 24
)

// Record flag
const (
	RECORD_FLAG_BIT_DELETE = 1 << iota
)

type Record struct {
	crc        uint32
	flag       uint32
	expiration int64
	keySize    uint32
	valueSize  uint32
	key        []byte
	value      []byte
}

func (r *Record) Size() int64 {
	return int64(DATA_ITEM_HEADER_SIZE + r.keySize + r.valueSize)
}
