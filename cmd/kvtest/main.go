package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	store "github.com/lnsp/kvstore"
	"github.com/lnsp/kvstore/table"
	"github.com/sirupsen/logrus"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type task struct {
	action     float32
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
	signal.Notify(cancel, syscall.SIGINT, syscall.SIGTERM)
	// Open local database
	db, err := store.New("store/")
	if err != nil {
		return err
	}
	var _, opWrCounter int32
	start := time.Now()
	wq := func(tasks <-chan task) {
		for {
			select {
			default:
				key := make([]byte, 2)
				rand.Read(key)
				value := make([]byte, 8)
				rand.Read(value)
				db.Put(key, &table.Record{Metadata: table.Metadata{Version: rand.Int63n(1024)}, Value: value})
				atomic.AddInt32(&opWrCounter, 1)
			case <-stop:
				return
			}
		}
	}
	tasks := make(chan task)
	for i := 0; i < runtime.NumCPU(); i++ {
		go wq(tasks)
	}
	defer func() {
		fmt.Println("total writes:", opWrCounter)
		fmt.Println("writes per second:", opWrCounter/int32(time.Since(start).Seconds()))
	}()
	// fuzzy testing
	for {
		select {
		case <-cancel:
			close(stop)
			return db.Close()
		}
	}
}
