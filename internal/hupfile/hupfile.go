// Package hupfile provides files with the same name reopened after a SIGHUP.
package hupfile

import (
	"io/fs"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
)

type File struct {
	name string

	mu     sync.Mutex
	closed bool
	file   *os.File
}

func New(name string) (*File, error) {
	f := &File{
		name: name,
	}
	if err := f.openFile(); err != nil {
		return nil, err
	}
	registerFile(f)
	return f, nil
}

func (f *File) openFile() error {
	if f.file != nil {
		return nil
	}
	newFile, err := os.OpenFile(f.name, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	f.file = newFile
	return nil
}

func (f *File) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return 0, fs.ErrClosed
	}
	if err := f.openFile(); err != nil {
		return 0, nil
	}
	return f.file.Write(b)
}

func (f *File) Close() error {
	f.mu.Lock()
	f.closed = true
	err := f.closeFile()
	f.mu.Unlock()
	unregisterFile(f)
	return err
}

func (f *File) Reopen() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return fs.ErrClosed
	}
	return f.closeFile()
}

func (f *File) closeFile() error {
	if f.file == nil {
		return nil
	}
	errSync := f.file.Sync()
	errClose := f.file.Close()
	f.file = nil
	if errClose != nil {
		return errClose
	}
	if errSync != nil {
		return errSync
	}
	return nil
}

var (
	mu    sync.Mutex
	files []*File = make([]*File, 0, 4)
	sig   chan os.Signal
)

func registerFile(f *File) {
	mu.Lock()
	files = append(files, f)
	if len(files) > 0 {
		monitorStart()
	}
	mu.Unlock()
}

func unregisterFile(f *File) {
	mu.Lock()
	x := slices.Index(files, f)
	if x == -1 {
		return
	}
	last := len(files) - 1
	files[x] = files[last]
	files[last] = nil
	files = files[:last]
	if len(files) == 0 {
		monitorStop()
	}
	mu.Unlock()
}

func monitorStart() {
	if sig != nil {
		return
	}
	sig = make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGHUP)
	go monitorMain(sig)
}

func monitorStop() {
	if sig == nil {
		return
	}
	signal.Stop(sig)
	close(sig)
	sig = nil
}

func monitorMain(sig <-chan os.Signal) {
	for {
		s, ok := <-sig
		if !ok {
			return
		}
		switch s {
		case syscall.SIGHUP:
			mu.Lock()
			for _, f := range files {
				f.Reopen()
			}
			mu.Unlock()
		}
	}
}
