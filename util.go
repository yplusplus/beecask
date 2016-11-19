package beecask

import (
	"fmt"
	"os"
	"path"
)

const (
	DATA_FILE_FORMAT = "%08d.data"
	HINT_FILE_FORMAT = "%08d.hint"
)

func getDataFilePath(dir string, fileId uint64) string {
	return path.Join(dir, fmt.Sprintf(DATA_FILE_FORMAT, fileId))
}

func getHintFilePath(dir string, fileId uint64) string {
	return path.Join(dir, fmt.Sprintf(HINT_FILE_FORMAT, fileId))
}

// absInt64 is a simple abs function for int64
func absInt64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func ReadDir(path string) ([]string, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	return dir.Readdirnames(-1)
}
