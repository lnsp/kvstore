package main

import (
	"fmt"
	"os"
	"valar/godat/pkg/store/table"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	/*
				sstable, err := table.OpenWritable("world")
				if err != nil {
					return err
				}
				sstable.Append([]byte("A"), []byte("Zalue1"))
				sstable.Append([]byte("C"), []byte("Zalue4"))
				sstable.Append([]byte("E"), []byte("Zalue4"))
				sstable.Flush()
				sstable.Append([]byte("G"), []byte("ZALI"))
				sstable.Append([]byte("Y"), []byte("HIER"))
				sstable.Close()
		sstable1, _ := table.Open("hello")
		sstable2, _ := table.Open("world")
		table.Merge("out", sstable1, sstable2)
	*/
	sstable, err := table.Open("out")
	if err != nil {
		return err
	}
	iterator := sstable.Iterate()
	for key, value, ok := iterator(); ok; key, value, ok = iterator() {
		fmt.Println(string(key), string(value))
	}
	return nil
}
