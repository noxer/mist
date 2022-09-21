package main

import (
	"fmt"
	"os"

	"github.com/noxer/mist/splitter"
)

func main() {
	if err := splitter.Process(os.Args[1], os.Args[2], splitter.NewLineSplitter(1_000_000)); err != nil {
		fmt.Printf("Error: %s\n", err)
	}
}
