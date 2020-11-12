package block

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/golang/snappy"
)

// FlushSize defines the flush size of a table block. By default 64 KiB.
const FlushSize = 2 << 16

type ReadSeekLocker interface {
	io.ReadSeeker
	sync.Locker
}

func seekCompressedBlock(file ReadSeekLocker, offset int64) ([]byte, int64, bool) {
	file.Lock()
	defer file.Unlock()
	// Seek to offset
	if _, err := file.Seek(offset, os.SEEK_SET); err != nil {
		return nil, 0, false
	}
	var compressedBlockSize int64
	if err := binary.Read(file, binary.LittleEndian, &compressedBlockSize); err != nil {
		return nil, 0, false
	}
	// Read block and decompress it
	compressedBlock := make([]byte, compressedBlockSize)
	if _, err := file.Read(compressedBlock); err != nil {
		return nil, 0, false
	}
	return compressedBlock, 8 + compressedBlockSize, true
}

// Seek searches the file for a block at the given offset.
func Seek(file ReadSeekLocker, offset int64) ([]byte, int64, bool) {
	compressedBlock, size, ok := seekCompressedBlock(file, offset)
	if !ok {
		return nil, 0, false
	}
	block, err := snappy.Decode(nil, compressedBlock)
	if err != nil {
		return nil, 0, false
	}
	return block, size, true
}

// FindRow looks for a row with the given key in the block.
func FindRow(block, key []byte) [][]byte {
	var (
		offset = int64(0)
		size   = int64(len(block))
		scan   = true
		values = make([][]byte, 0, 1)
	)
	for offset < size && scan {
		var (
			keyLen     = binary.LittleEndian.Uint16(block[offset : offset+2])
			valueLen   = binary.LittleEndian.Uint16(block[offset+2 : offset+4])
			keyStart   = offset + 4
			valueStart = keyStart + int64(keyLen)
			valueEnd   = valueStart + int64(valueLen)
		)
		switch bytes.Compare(key, block[keyStart:valueStart]) {
		case 0:
			values = append(values, block[valueStart:valueEnd])
		case 1:
			scan = false
		}
		offset = valueEnd
	}
	return values
}

// NextRow reads the next row from the block and returns the key, value and offset.
func NextRow(block []byte, offset int64) ([]byte, []byte, int64, bool) {
	if offset >= int64(len(block)) {
		return nil, nil, offset, false
	}
	var (
		keyLen     = binary.LittleEndian.Uint16(block[offset : offset+2])
		valueLen   = binary.LittleEndian.Uint16(block[offset+2 : offset+4])
		keyStart   = offset + 4
		valueStart = keyStart + int64(keyLen)
		valueEnd   = valueStart + int64(valueLen)
	)
	return block[keyStart:valueStart], block[valueStart:valueEnd], valueEnd - offset, true
}

// WrittenRowSize returns the size of the row in binary format.
func WrittenRowSize(key, value []byte) int64 {
	return int64(4 + len(key) + len(value))
}

// WriteBlock writes the block in compressed format to the writer.
func WriteBlock(wr io.Writer, block []byte) {
}

// WriteRow writes the row in binary format to the writer.
func WriteRow(wr io.Writer, key, value []byte) error {
	// Write to buffer
	if err := binary.Write(wr, binary.LittleEndian, uint16(len(key))); err != nil {
		return err
	}
	if err := binary.Write(wr, binary.LittleEndian, uint16(len(value))); err != nil {
		return err
	}
	wr.Write(key)
	wr.Write(value)
	return nil
}
