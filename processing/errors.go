package processing

import (
	"fmt"
)

type ErrInvalidEvent struct {
	Key string
	Raw Entry
}

func (e ErrInvalidEvent) Error() string {
	return fmt.Sprintf("invalid key - %s", e.Key)
}
