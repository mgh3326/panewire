package main

import (
	"os"

	"github.com/mgh3326/panewire"
)

// version is set by release builds with: -ldflags "-X main.version=<tag>".
var version = "panewire-dev"

func main() { os.Exit(panewire.MainWithVersion(os.Args[1:], version)) }
