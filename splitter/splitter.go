package splitter

import (
	"archive/zip"
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

type Splitter func(old, neu *State) bool

type State struct {
	TotalLines uint64
	PartLines  uint64
	TotalSize  uint64
	PartSize   uint64
}

func NewLineSplitter(lines uint64) Splitter {
	return func(old, neu *State) bool {
		return neu.PartLines > lines
	}
}

func NewSizeSplitter(size uint64) Splitter {
	return func(old, neu *State) bool {
		return neu.PartSize > size
	}
}

func Process(sourceName, destinationPrefix string, splitter Splitter) error {
	if splitter == nil {
		splitter = func(old, neu *State) bool { return false }
	}

	// open and index the input archive
	stat, err := os.Stat(sourceName)
	if err != nil {
		return err
	}

	fmt.Printf("Input file size: %d bytes\n", stat.Size())

	f, err := os.Open(sourceName)
	if err != nil {
		return err
	}
	defer f.Close()

	zr, err := zip.NewReader(f, stat.Size())
	if err != nil {
		return err
	}

	files := make(map[string]*zip.File)
	var jsonl *zip.File
	for _, file := range zr.File {
		if strings.HasSuffix(file.Name, ".jsonl") {
			jsonl = file
		} else {
			files[file.Name] = file
		}
	}

	fmt.Printf("Number of files in the archive: %d\n", len(zr.File))

	// open the jsonl file
	jr, err := jsonl.Open()
	if err != nil {
		return err
	}
	defer jr.Close()

	s := bufio.NewScanner(jr)
	s.Buffer(make([]byte, 64*1024*1024), 64*1024*1024)

	var (
		state        State
		partNumber   uint64
		partWriter   io.WriteCloser
		partZip      *zip.Writer
		jsonlBuffer  *bytes.Buffer
		jsonlEncoder *json.Encoder
	)

	for s.Scan() {
		var line LineImportData
		err := json.Unmarshal(s.Bytes(), &line)
		if err != nil {
			return err
		}

		nextState := state
		nextState.TotalLines++
		nextState.PartLines++

		var (
			attachments []string
			size        uint64
		)

		switch line.Type {
		case "post":
			attachments, size = processAttachmentInfos(files, line.Post.Attachments)
		case "direct_post":
			attachments, size = processAttachmentInfos(files, line.DirectPost.Attachments)
		}

		nextState.TotalSize += size
		nextState.PartSize += size

		if splitter(&state, &nextState) && partWriter != nil {
			fmt.Printf("Processed %d lines, currently in part %d\n", state.TotalLines, partNumber)
			fmt.Printf("Writing part %d... ", partNumber)

			jsonlOutput, err := partZip.Create("import.jsonl")
			if err != nil {
				return err
			}
			if _, err = jsonlBuffer.WriteTo(jsonlOutput); err != nil {
				return err
			}
			partZip.Close()
			partZip = nil

			partWriter.Close()
			partWriter = nil

			nextState.PartLines = 1
			nextState.PartSize = size

			fmt.Println("OK")
		}

		if partWriter == nil {
			partNumber++
			name := fmt.Sprintf("%s%03d.zip", destinationPrefix, partNumber)
			partWriter, err = os.Create(name)
			if err != nil {
				return err
			}
			partZip = zip.NewWriter(partWriter)
			jsonlBuffer = bytes.NewBuffer([]byte("{\"type\":\"version\",\"version\":1}\n"))
			jsonlEncoder = json.NewEncoder(jsonlBuffer)
		}

		if err = jsonlEncoder.Encode(line); err != nil {
			return err
		}
		if err = processAttachments(files, attachments, partZip); err != nil {
			return err
		}

		state = nextState

		if state.TotalLines%1023 == 0 {
			fmt.Printf("Processed %d lines, currently in part %d\r", state.TotalLines, partNumber)
		}
	}

	fmt.Printf("Processed %d lines, currently in part %d\r", state.TotalLines, partNumber)
	fmt.Printf("\nWriting final part... ")

	jsonlOutput, err := partZip.Create("import.jsonl")
	if err != nil {
		return err
	}
	if _, err = jsonlBuffer.WriteTo(jsonlOutput); err != nil {
		return err
	}
	partZip.Close()
	partWriter.Close()

	fmt.Println("OK")

	return nil
}

func processAttachmentInfos(files map[string]*zip.File, attachments *[]AttachmentImportData) ([]string, uint64) {
	if attachments == nil {
		return nil, 0
	}

	totalSize := uint64(0)
	fileNames := make([]string, 0, len(*attachments))

	for _, attachment := range *attachments {
		fileNames = append(fileNames, *attachment.Path)

		file, ok := files["data/"+*attachment.Path]
		if !ok {
			fmt.Printf("Warning: Missing file data/%s assumed as 0 bytes\n", *attachment.Path)
			continue
		}

		totalSize += file.UncompressedSize64
	}

	return fileNames, totalSize
}

func processAttachments(files map[string]*zip.File, attachments []string, into *zip.Writer) error {
	for _, attachment := range attachments {
		w, err := into.Create("data/" + attachment)
		if err != nil {
			return err
		}

		if file, ok := files["data/"+attachment]; ok {
			r, err := file.Open()
			if err != nil {
				return err
			}

			if _, err = io.Copy(w, r); err != nil {
				return err
			}
		}
	}

	return nil
}
