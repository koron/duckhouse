// Package pidfile provides PID recorded files
package pidfile

import (
	"os"
	"strconv"
	"sync"
)

var (
	mu    sync.Mutex
	names []string
)

func Write(name string) error {
	err := writePID(name)
	if err != nil {
		return err
	}
	mu.Lock()
	names = append(names, name)
	mu.Unlock()
	return nil
}

func writePID(name string) error {
	f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}
	_, err = f.WriteString(strconv.Itoa(os.Getpid()))
	if err != nil {
		return err
	}
	return f.Close()
}

// Close removes all PID files.
func Close() {
	mu.Lock()
	for _, name := range names {
		os.Remove(name)
	}
	names = nil
	mu.Unlock()
}
