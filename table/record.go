package table

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Metadata stores metadata on a record value.
type Metadata struct {
	Version int64
	Delete  bool
}

// ReadFrom parses the given metadata into the source struct.
func (md *Metadata) ReadFrom(rd io.Reader) error {
	if err := binary.Read(rd, binary.BigEndian, &md.Version); err != nil {
		return err
	}
	if err := binary.Read(rd, binary.LittleEndian, &md.Delete); err != nil {
		return err
	}
	return nil
}

// Bytes returns the binary representation of the record's metadata.
func (md *Metadata) Bytes() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, md.Version)
	binary.Write(buffer, binary.LittleEndian, md.Delete)
	return buffer.Bytes()
}

// Record is an entry in a table.
type Record struct {
	Metadata
	Value []byte
}

// FromBytes reconstructs the record from the given raw bytes.
func (record *Record) FromBytes(data []byte) error {
	buffer := bytes.NewBuffer(data)
	if err := record.Metadata.ReadFrom(buffer); err != nil {
		return err
	}
	record.Value = buffer.Bytes()
	return nil
}

// Bytes returns the binary representation of this object.
func (record *Record) Bytes() []byte {
	buffer := new(bytes.Buffer)
	buffer.Write(record.Metadata.Bytes())
	buffer.Write(record.Value)
	return buffer.Bytes()
}
