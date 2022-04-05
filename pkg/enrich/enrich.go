package enrich

import (
	"github.com/markuskont/pikksilm/pkg/models"
)

type Enrichment struct {
	Key   string
	Entry models.Entry
}

type Processor interface {
	Process(models.Entry) error
}

type EntriesCheker interface {
	CheckEntries(Entries) error
}
