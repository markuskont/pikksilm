package processing

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/markuskont/datamodels"
	"github.com/satta/gommunityid"
)

// networkEntry is simplified event_id 3 entry that can be kept in memory with lower overhead.
// It is used to generate community ID
type networkEntry struct {
	SrcIP    net.IP `json:"src_ip,omitempty"`
	DestIP   net.IP `json:"dest_ip,omitempty"`
	SrcPort  uint16 `json:"src_port,omitempty"`
	DestPort uint16 `json:"dest_port,omitempty"`
	Proto    string `json:"proto,omitempty"`
	GUID     string `json:"guid,omitempty"`
}

func (n networkEntry) communityID(cid gommunityid.CommunityID) (string, error) {
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

func extractNetworkEntryBase(e datamodels.Map, guid string) (*networkEntry, error) {
	proto, ok := e.GetString("winlog", "event_data", "Protocol")
	if !ok {
		return nil, errors.New("missing transport")
	}

	srcPort, ok := e.GetString("winlog", "event_data", "SourcePort")
	if !ok {
		return nil, errors.New("missing source port")
	}
	parsedSrcPort, err := strconv.Atoi(srcPort)
	if err != nil {
		return nil, fmt.Errorf("invalid source port %s", srcPort)
	}

	destPort, ok := e.GetString("winlog", "event_data", "DestinationPort")
	if !ok {
		return nil, errors.New("missing destination port")
	}
	parsedDestPort, err := strconv.Atoi(destPort)
	if err != nil {
		return nil, fmt.Errorf("invalid destination port %s", srcPort)
	}

	srcIP, ok := e.GetString("winlog", "event_data", "SourceIp")
	if !ok {
		return nil, errors.New("missing source IP")
	}
	parsedSrcIP := net.ParseIP(srcIP)
	if parsedSrcIP == nil {
		return nil, fmt.Errorf("invalid IP %v", parsedSrcIP)
	}

	destIP, ok := e.GetString("winlog", "event_data", "DestinationIp")
	if !ok {
		return nil, errors.New("missing destination IP")
	}
	parsedDestIP := net.ParseIP(destIP)
	if parsedDestIP == nil {
		return nil, fmt.Errorf("invalid IP %v", parsedDestIP)
	}

	if guid == "" {
		guid, ok = e.GetString("winlog", "event_data", "ProcessGuid")
		if !ok {
			guid, ok = e.GetString("winlog", "event_data", "SourceProcessGUID")
			if !ok {
				return nil, fmt.Errorf("missing GUID")
			}
		}
	}

	return &networkEntry{
		Proto:    proto,
		SrcIP:    parsedSrcIP,
		SrcPort:  uint16(parsedSrcPort),
		DestPort: uint16(parsedDestPort),
		DestIP:   parsedDestIP,
		GUID:     guid,
	}, nil
}

func extractNetworkEntryECS(e datamodels.Map, guid string) (*networkEntry, error) {
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

	return &networkEntry{
		Proto:    proto,
		SrcIP:    parsedSrcIP,
		SrcPort:  uint16(srcPort),
		DestPort: uint16(destPort),
		DestIP:   parsedDestIP,
		GUID:     guid,
	}, nil
}

type ConfigRedisInstance struct {
	Host     string
	Database int
	Key      string
	Batch    int64
	Password string
}

type wiseEntry struct {
	ProcessName string `json:"processname"`
	UserName    string `json:"username"`
	HostName    string `json:"hostname"`
	ProcessPid  int    `json:"processpid"`
	HostIP      string `json:"hostip"`
	HostMAC     string `json:"hostmac"`
}
