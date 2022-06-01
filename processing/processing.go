package processing

import (
	jsoniter "github.com/json-iterator/go"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type EncodedEnrichment struct {
	Key   string
	Entry []byte
}
