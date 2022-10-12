package main

import (
	"flag"
	"fmt"

	"github.com/noxer/mist/splitter"
)

const (
	KiB = 1024
	MiB = 1024 * KiB
)

func main() {
	archive := flag.String("archive", "mattermost_import.zip", "Import archive to split")
	prefix := flag.String("prefix", "mattermost_split_", "Prefix of the output file names, the segment number will be appended")
	lines := flag.Uint64("lines", 100_000, "The maximum number of lines per segment")
	size := flag.Uint64("size", 512*MiB, "The maximum number of bytes in attachments per segment")
	flag.Parse()

	var splitters []splitter.Splitter
	if *lines != 0 {
		splitters = append(splitters, splitter.NewLineSplitter(*lines))
	}
	if *size != 0 {
		splitters = append(splitters, splitter.NewSizeSplitter(*size))
	}

	err := splitter.Process(*archive, *prefix, func(old, neu *splitter.State) bool {
		for _, sp := range splitters {
			if sp(old, neu) {
				return true
			}
		}

		return false
	})

	if err != nil {
		fmt.Printf("Error: %s\n", err)
	}
}
