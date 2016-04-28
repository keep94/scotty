package snmpsource

import (
	"errors"
	"fmt"
	"github.com/Symantec/scotty/metrics"
	"github.com/Symantec/scotty/sources"
	"github.com/Symantec/tricorder/go/tricorder/units"
	"github.com/cviecco/snmpfetcher"
)

type connectorType string

func newConnector(community string) sources.Connector {
	return connectorType(community)
}

func (c connectorType) Connect(host string, port int) (sources.Poller, error) {
	if port < 0 || port >= (1<<16) {
		return nil, errors.New(
			fmt.Sprintf("Port out of range: %d", port))
	}
	data, err := snmpfetcher.FetchSimpleDataSNMP2c(
		host, uint16(port), string(c))
	if err != nil {
		return nil, err
	}
	return pollerType(data), nil
}

func (c connectorType) Name() string {
	return "snmp"
}

type valueType struct {
	Path  string
	Value uint64
}

type valueListType []valueType

func (l valueListType) Len() int {
	return len(l)
}

func (l valueListType) Index(i int, value *metrics.Value) {
	*value = metrics.Value{
		Path:  l[i].Path,
		Unit:  units.None,
		Value: l[i].Value,
	}
}

type pollerType map[string]snmpfetcher.SNMPDatum

func (p pollerType) Poll() (metrics.List, error) {
	ifaceData, err := snmpfetcher.GetIfaceData(p)
	if err != nil {
		return nil, err
	}
	var results valueListType
	for first, secondAndValue := range ifaceData {
		for second, value := range secondAndValue {
			results = append(
				results,
				valueType{
					Path: fmt.Sprintf(
						"/%s/%s", first, second),
					Value: value,
				},
			)
		}
	}
	return results, nil
}

func (p pollerType) Close() error {
	return nil
}
