package crontab

import (
	"context"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/robfig/cron/v3"
	"sync"
)

var (
	_ transport.Server = (*Server)(nil)
)

type Handle func()

type Crontab struct {
	Spec   string
	Handle Handle
}

type Server struct {
	sync.RWMutex
	baseCtx  context.Context
	err      error
	cron     *cron.Cron
	crontabs []Crontab
}

// ServerOption is an HTTP server option.
type ServerOption func(*Server)

func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		baseCtx: context.Background(),
	}
	srv.cron = cron.New(cron.WithSeconds())
	srv.init(opts...)
	return srv
}

func (s *Server) init(opts ...ServerOption) {
	for _, o := range opts {
		o(s)
	}
}

func (s *Server) Name() string {
	return "crontab"
}

func (s *Server) Start(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}

	s.baseCtx = ctx
	for _, crontab := range s.crontabs {
		if _, err := s.cron.AddFunc(crontab.Spec, crontab.Handle); err != nil {
			return err
		}
	}
	s.cron.Start()
	log.Infof("[%s] server stopping.", s.Name())
	return nil
}

func (s *Server) Stop(_ context.Context) error {
	s.cron.Stop()
	log.Infof("[%s] server closed.", s.Name())
	return nil
}

func (s *Server) RegisterCrontab(spec string, handle Handle) error {
	s.Lock()
	defer s.Unlock()

	s.crontabs = append(s.crontabs, Crontab{spec, handle})
	return nil
}
