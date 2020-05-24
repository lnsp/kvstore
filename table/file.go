package table

import (
	"os"
	"sync"
)

type File struct {
	*os.File
	sync.Mutex
}
