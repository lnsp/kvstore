package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"valar/godat/pkg/store"
)

func main() {
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

type task struct {
	action     int
	key, value []byte
}

func (t task) String() string {
	if t.action < 900 {
		return "put"
	} else {
		return "get"
	}
}

func run() error {
	db, err := store.New("local")
	if err != nil {
		return err
	}
	defer db.Close()
	wq := func(q <-chan task) {
		for task := range q {
			if task.action < 1 {
				db.Put(task.key, &store.Record{Time: rand.Int63n(1024), Value: task.value})
			} else {
				db.Get(task.key)
			}
		}
	}
	tasks := make(chan task)
	for i := 0; i < 8; i++ {
		go wq(tasks)
	}
	// fuzzy testing
	delta := time.Now()
	for i := 0; ; i++ {
		key := make([]byte, 8)
		rand.Read(key)
		value := make([]byte, 8)
		rand.Read(value)
		task := task{0, key, value}
		tasks <- task
		if i%10000 == 0 {
			fmt.Println(i, task, time.Since(delta), db.MemSize())
			delta = time.Now()
		}
	}
	/*
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
				for i := 0; i < 10; i++ {
					go func(i int) {
						records := db.Get(key)
						for _, r := range records {
							fmt.Println(i, r)
						}
					}(i)
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
	*/
	return nil
}
