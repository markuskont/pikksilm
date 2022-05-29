package processing

type Enrichment struct {
	Key   string
	Entry Entry
}

type Processor interface {
	Process(Entry) (Entries, error)
}

type EntriesCheker interface {
	CheckEntries(Entries) error
}
