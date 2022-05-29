package processing

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/markuskont/datamodels"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Enrichment struct {
	Key   string
	Entry datamodels.Map
}

type Processor interface {
	Process(datamodels.Map) (Entries, error)
}

type EntriesCheker interface {
	CheckEntries(Entries) error
}
