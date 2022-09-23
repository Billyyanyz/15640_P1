package lsp

import (
	"fmt"
)

const verbose = true
func clientImplLog(s string) {
	if verbose {
		fmt.Printf("[client_impl] %s\n", s)
	}
}

func serverImplLog(s string) {
	if verbose {
		fmt.Printf("[server_impl] %s\n", s)
	}
}
