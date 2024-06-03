package publisher

import (
	"github.com/platinasystems/pcc-models/v2/bare_metal/avro"
	"sync"
)

type ProvisionerQueue struct {
	PLogs  []avro.Log
	PMutex sync.RWMutex
	Limit  int
}

var LogQueue ProvisionerQueue

func (p *ProvisionerQueue) SetLimit(limit int) {
	p.PMutex.Lock()
	defer p.PMutex.Unlock()
	p.Limit = limit
}

func (p *ProvisionerQueue) Enqueue(l avro.Log) {
	p.PMutex.Lock()
	defer p.PMutex.Unlock()
	p.PLogs = append(p.PLogs, l)
	if len(p.PLogs) == p.Limit {
		_ = (&Publisher{}).SendLogs(p.PLogs)
		p.PLogs = nil
	}
	return
}

func (p *ProvisionerQueue) SendAndFlush() {
	p.PMutex.Lock()
	defer p.PMutex.Unlock()
	if len(p.PLogs) > 0 {
		_ = (&Publisher{}).SendLogs(p.PLogs)
		p.PLogs = nil
	}
	return
}
