package main

import (
	"embed"
	"io/fs"
	"os"
)

//go:embed _ui
var uiRawFS embed.FS

func getUIFS(override string) (fs.FS, error) {
	if override != "" {
		return os.DirFS(override), nil
	}
	return fs.Sub(uiRawFS, "_ui")
}
