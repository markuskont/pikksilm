package processing

import "github.com/markuskont/datamodels"

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
