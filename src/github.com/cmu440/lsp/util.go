package lsp

import (
	"fmt"
)

const (
	log   = true
	fatal = true
)

// Ref: https://twin.sh/articles/35/how-to-add-colors-to-your-console-terminal-output-in-go
var Reset = "\033[0m"
var Red = "\033[31m"
var Green = "\033[32m"
var Yellow = "\033[33m"
var Blue = "\033[34m"
var Purple = "\033[35m"
var Cyan = "\033[36m"
var Gray = "\033[37m"
var White = "\033[97m"

func clientImplLog(s string) {
	if log {
		fmt.Printf("[client_impl] %s\n", s)
	}
}

func clientImplFatal(s string) {
	if fatal {
		fmt.Printf("%s[client_impl] FATAL: %s\n%s", Red, s, Reset)
	}
}

func serverImplLog(s string) {
	if log {
		fmt.Printf("%s[server_impl]: %s\n%s", Green, s, Reset)
	}
}

func serverImplFatal(s string) {
	if log {
		fmt.Printf("%s[server_impl] FATAL: %s\n%s", Red, s, Reset)
	}
}
