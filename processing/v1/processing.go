package processing

import (
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type EncodedEntry struct {
	Key   string
	Entry []byte
}
