package processing

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
)

type metaCorrelate struct {
	Workers int `json:"workers"`
}

func loadPersist(path string, d any) error {
	if !strings.HasSuffix(path, ".gz") {
		path += ".gz"
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gr.Close()
	return json.NewDecoder(gr).Decode(&d)
}

func dumpPersist(path string, d any) error {
	if !strings.HasSuffix(path, ".gz") {
		path += ".gz"
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gw := gzip.NewWriter(f)
	defer gw.Close()
	return json.NewEncoder(gw).Encode(d)
}

func dumpNotExists(path string) bool {
	_, err := os.Stat(path)
	return errors.Is(err, fs.ErrNotExist)
}

func correlateDumpFmt(dir string, id int) string {
	return path.Join(dir, fmt.Sprintf("correlate-cmd-%d.json", id))
}
