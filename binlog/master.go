package binlog

import (
	"bytes"
	"github.com/BurntSushi/toml"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/ioutil2"
	"os"
	"path"
	"sync"
	"time"
)

type master struct {
	sync.RWMutex

	GtIdSet string `toml:"gtid_set"`

	filePath     string
	lastSaveTime time.Time
}

func loadMasterInfo(dataDir string) (*master, error) {
	var m master
	if len(dataDir) == 0 {
		return &m, nil
	}
	m.filePath = path.Join(dataDir, ".master.info")
	m.lastSaveTime = time.Now()
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}
	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return &m, nil
	}
	defer f.Close()
	_, err = toml.DecodeReader(f, &m)
	return &m, errors.Trace(err)
}

func (m *master) Save(gtidSet string) error {
	m.Lock()
	defer m.Unlock()

	if len(m.filePath) == 0 {
		return nil
	}
	n := time.Now()
	if n.Sub(m.lastSaveTime) < time.Second {
		return nil
	}

	m.lastSaveTime = n
	m.GtIdSet = gtidSet
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	e.Encode(m)
	var err error
	if err = ioutil2.WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", m.filePath, err)
	}
	return errors.Trace(err)
}

func (m *master) GtidSet() string {
	m.RLock()
	defer m.RUnlock()

	return m.GtIdSet
}

func (m *master) Close() error {
	return m.Save(m.GtIdSet)
}
