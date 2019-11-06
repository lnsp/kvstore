package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"valar/godat/pkg/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	db, err := store.New("local")
	if err != nil {
		return err
	}
	defer db.Close()
	fmt.Print("> ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		args := strings.Split(line, " ")
		switch strings.ToLower(args[0]) {
		case "put":
			key, value := []byte(args[1]), []byte(args[2])
			db.Put(key, &store.Record{
				Time:  time.Now().UTC().Unix(),
				Value: value,
			})
		case "get":
			key := []byte(args[1])
			records := db.Get(key)
			for _, r := range records {
				fmt.Println(r)
			}
		case "flush":
			if err := db.Flush(); err != nil {
				fmt.Println("error:", err)
			}
		case "exit":
			return nil
		}
		fmt.Print("> ")
	}
	return nil
}
