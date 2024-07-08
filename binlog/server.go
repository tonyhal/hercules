package binlog

import (
	"context"
	"fmt"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
	"path/filepath"
	"reflect"
	"regexp"
	"sync"
)

var _ transport.Server = (*Server)(nil)

type config struct {
	host    string
	user    string
	passwd  string
	charset string
	db      string
	port    int64
}

type Server struct {
	canal   *canal.Canal
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	syncCh  chan interface{}
	err     error
	handler map[string]*handler
	master  *master
	conf    *config
}

type handler struct {
	f interface{}
}

type ServerOption func(*Server)

func WithConfig(host, user, passwd, charset, db string, port int64) ServerOption {
	return func(s *Server) {
		s.conf = &config{host: host, user: user, passwd: passwd, charset: charset, db: db, port: port}
	}
}

func (s *Server) init(opts ...ServerOption) {
	for _, o := range opts {
		o(s)
	}
}

func NewServer(opts ...ServerOption) *Server {
	srv := new(Server)
	srv.syncCh = make(chan interface{}, 1024*8)
	srv.handler = make(map[string]*handler)
	srv.ctx, srv.cancel = context.WithCancel(context.Background())
	srv.init(opts...)
	return srv
}

func (s *Server) Name() string {
	return "binlog"
}

func (s *Server) Run() error {
	s.wg.Add(1)
	go s.syncLoop()

	// 从指定的 GTID 开始同步
	gtidSet, err := mysql.ParseGTIDSet("mysql", s.master.GtidSet())
	if err != nil {
		return errors.Trace(err)
	}
	if err := s.canal.StartFromGTID(gtidSet); err != nil {
		return errors.Trace(err)
	}

	log.Infof("successfully connected to %s.", s.Name())
	return nil
}

func (s *Server) syncLoop() {
	defer s.wg.Done()

	for {
		select {
		case ch := <-s.syncCh:
			switch v := ch.(type) {
			case gtidSetSaver:
				if err := s.master.Save(v.GtidSet); err != nil {
					log.Errorf("save sync position %v err %v, close sync.\n", v.GtidSet, err)
				}
			case bulkRequest:
				go func(v bulkRequest) {
					// 处理分表
					table := regexp.MustCompile(`_\d{6}$`).ReplaceAllString(v.Table, "")
					if h, ok := s.handler[table]; ok {
						reflect.ValueOf(h.f).Call([]reflect.Value{reflect.ValueOf(v.Record), reflect.ValueOf(v.Action), reflect.ValueOf(v.Table)})
					}
				}(v)
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Server) Start(ctx context.Context) (err error) {
	if s.err != nil {
		return s.err
	}
	s.ctx = ctx

	// 加载binlog文件同步点
	filePath, _ := filepath.Abs("./")
	if s.master, err = loadMasterInfo(filePath); err != nil {
		return errors.Trace(err)
	}

	tables := []string{}
	for k, _ := range s.handler {
		tables = append(tables, k)
	}

	cfg := canal.NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:%d", s.conf.host, s.conf.port)
	cfg.User = s.conf.user
	cfg.Password = s.conf.passwd
	cfg.Charset = s.conf.charset
	//cfg.Dump.Databases = []string{s.conf.db}
	cfg.Dump.TableDB = s.conf.db
	cfg.Dump.Tables = tables
	cfg.Logger = newLogger(log.LevelError)
	// 是否无限重试
	cfg.MaxReconnectAttempts = -1

	if s.canal, s.err = canal.NewCanal(cfg); s.err != nil {
		log.Errorf("failed opening connection to binlog: %v", s.err)
		return errors.Trace(err)
	}
	s.canal.SetEventHandler(&event{s})
	// 启动
	return s.Run()
}

func (s *Server) Stop(_ context.Context) error {
	defer log.Infof("[%s] server stopping\n", s.Name())

	s.cancel()
	s.canal.Close()
	s.master.Close()
	s.wg.Wait()

	return nil
}

func (s *Server) Register(t string, f interface{}) (err error) {
	if reflect.TypeOf(f).Kind() == reflect.Func {
		s.handler[t] = &handler{f: f}
	} else {
		err = fmt.Errorf("must be function")
	}
	return
}
