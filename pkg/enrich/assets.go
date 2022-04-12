package enrich

import (
	"fmt"
	"net"
	"os"

	"github.com/markuskont/pikksilm/pkg/models"
)

type Asset struct {
	OrgAbbrev string `json:"org_abbrev,omitempty"`
	Subnet    string `json:"subnet,omitempty"`
	Name      string `json:"name,omitempty"`
	Addr      string `json:"addr,omitempty"`
	Green     bool   `json:"green,omitempty"`
	OrgSub    string `json:"org_sub,omitempty"`
}

type Assets struct {
	Values map[string]Asset
}

func NewAssets(path string) (*Assets, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var obj []Asset
	if err := models.Decoder.NewDecoder(f).Decode(&obj); err != nil {
		return nil, err
	}
	vals := make(map[string]Asset)
	for _, item := range obj {
		addr := net.ParseIP(item.Addr)
		if addr == nil {
			return nil, fmt.Errorf("%s is not ip", item.Addr)
		}
		vals[addr.String()] = item
	}
	return &Assets{
		Values: vals,
	}, nil
}
