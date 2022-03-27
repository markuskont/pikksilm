package models

import (
	"errors"
	"fmt"
	"net"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/satta/gommunityid"
)

var Decoder = jsoniter.ConfigCompatibleWithStandardLibrary

const ArgTimeFormat = "2006-01-02 15:04:05"

// Entry is a nested map with helper methods for recursive lookups
type Entry map[string]any

// Get is a helper for doing recursive lookups into nested maps (nested JSON). Key argument is a
// slice of strings
func (d Entry) Get(key ...string) (any, bool) {
	if len(key) == 0 {
		return nil, false
	}
	if val, ok := d[key[0]]; ok {
		switch res := val.(type) {
		case map[string]any:
			// key has only one item, user wants the map itselt, not subelement
			if len(key) == 1 {
				return res, ok
			}
			// recurse with key remainder
			return Entry(res).Get(key[1:]...)
		default:
			return val, ok
		}
	}
	return nil, false
}

// GetString wraps Get to cast item to string.
func (d Entry) GetString(key ...string) (string, bool) {
	val, ok := d.Get(key...)
	if !ok {
		return "", false
	}
	str, ok := val.(string)
	if !ok {
		return "", false
	}
	return str, true
}

func (d Entry) GetTimestamp(key ...string) (time.Time, bool, error) {
	val, ok := d.GetString(key...)
	if !ok {
		return time.Time{}, false, nil
	}
	ts, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return time.Time{}, false, err
	}
	return ts, true, nil
}

// GetNumber retrieves JSON numeric value which are by spec floating points
func (d Entry) GetNumber(key ...string) (float64, bool) {
	val, ok := d.Get(key...)
	if !ok {
		return -1, false
	}
	n, ok := val.(float64)
	if !ok {
		return -1, false
	}
	return n, true
}

// NetworkEntry is simplified event_id 3 entry that can be kept in memory with lower overhead.
// It is used to generate community ID
type NetworkEntry struct {
	SrcIP    net.IP `json:"src_ip,omitempty"`
	DestIP   net.IP `json:"dest_ip,omitempty"`
	SrcPort  uint16 `json:"src_port,omitempty"`
	DestPort uint16 `json:"dest_port,omitempty"`
	Proto    string `json:"proto,omitempty"`
	GUID     string `json:"guid,omitempty"`
}

func (n NetworkEntry) CommunityID(cid gommunityid.CommunityID) (string, error) {
	var ft gommunityid.FlowTuple
	switch n.Proto {
	case "tcp":
		ft = gommunityid.MakeFlowTupleTCP(n.SrcIP, n.DestIP, n.SrcPort, n.DestPort)
	case "udp":
		ft = gommunityid.MakeFlowTupleUDP(n.SrcIP, n.DestIP, n.SrcPort, n.DestPort)
	case "icmp":
		ft = gommunityid.MakeFlowTupleICMP(n.SrcIP, n.DestIP, n.SrcPort, n.DestPort)
	default:
		return "", errors.New("unsupported protocol " + n.Proto)
	}
	return cid.CalcBase64(ft), nil
}

func ExtractNetworkEntry(e Entry, guid string) (*NetworkEntry, error) {
	proto, ok := e.GetString("network", "transport")
	if !ok {
		return nil, errors.New("missing transport")
	}

	srcPort, ok := e.GetNumber("source", "port")
	if !ok {
		return nil, errors.New("missing source port")
	}
	if srcPort < 0 {
		return nil, fmt.Errorf("invalid port %v", srcPort)
	}

	destPort, ok := e.GetNumber("destination", "port")
	if !ok {
		return nil, errors.New("missing destination port")
	}
	if destPort < 0 {
		return nil, fmt.Errorf("invalid port %v", destPort)
	}

	srcIP, ok := e.GetString("source", "ip")
	if !ok {
		return nil, errors.New("missing source IP")
	}
	parsedSrcIP := net.ParseIP(srcIP)
	if parsedSrcIP == nil {
		return nil, fmt.Errorf("invalid IP %v", parsedSrcIP)
	}

	destIP, ok := e.GetString("destination", "ip")
	if !ok {
		return nil, errors.New("missing destination IP")
	}
	parsedDestIP := net.ParseIP(destIP)
	if parsedDestIP == nil {
		return nil, fmt.Errorf("invalid IP %v", parsedDestIP)
	}

	if guid == "" {
		guid, ok = e.GetString("process", "entity_id")
		if !ok {
			return nil, errors.New("entity id missing")
		}
	}

	return &NetworkEntry{
		Proto:    proto,
		SrcIP:    parsedSrcIP,
		SrcPort:  uint16(srcPort),
		DestPort: uint16(destPort),
		DestIP:   parsedDestIP,
		GUID:     guid,
	}, nil
}
