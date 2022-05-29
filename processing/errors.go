package processing

import (
	"fmt"

	"github.com/markuskont/datamodels"
)

type ErrInvalidEvent struct {
	Key string
	Raw datamodels.Map
}

func (e ErrInvalidEvent) Error() string {
	return fmt.Sprintf("invalid key - %s", e.Key)
}
