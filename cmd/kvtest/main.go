package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	store "github.com/lnsp/kvstore"
	"github.com/sirupsen/logrus"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
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
	logrus.SetLevel(logrus.DebugLevel)
	// Notify on kill
	cancel := make(chan os.Signal, 1)
	stop := make(chan bool)
	signal.Notify(cancel, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	// Open local database
	db, err := store.New("local")
	if err != nil {
		return err
	}
	wq := func(tasks <-chan task) {
		for {
			select {
			case task := <-tasks:
				if task.action < 1 {
					db.Put(task.key, &store.Record{Time: rand.Int63n(1024), Value: task.value})
				} else {
					db.Get(task.key)
				}
			case <-stop:
				return
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
		select {
		case <-cancel:
			close(stop)
			return db.Close()
		default:
		}
		key := make([]byte, 8)
		rand.Read(key)
		value := make([]byte, 8)
		rand.Read(value)
		task := task{0, key, value}
		tasks <- task
		if i%100000 == 0 {
			fmt.Println(i, task, time.Since(delta), db.MemSize())
			delta = time.Now()
		}
	}
}
