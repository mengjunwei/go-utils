package rpc

import (
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/pkg/errors"

	"github.com/mengjunwei/go-utils/log"
	"github.com/mengjunwei/go-utils/prometheus-rpc/gen-go/metrics"
)

type MetricsRpcServer struct {
	addr   string
	server *thrift.TSimpleServer
}

func NewMetricsRpcServer(addr string) *MetricsRpcServer {
	s := &MetricsRpcServer{addr: addr}
	return s
}

func (s *MetricsRpcServer) Run(handler MetricsTransferHandler) error {
	transport, err := thrift.NewTServerSocket(s.addr)
	if err != nil {
		return errors.Wrapf(err, "thrift.NewTServerSocket")
	}

	processor := metrics.NewMetricsTransferProcessor(&handler)

	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTCompactProtocolFactory()
	s.server = thrift.NewTSimpleServer4(
		processor,
		transport,
		transportFactory,
		protocolFactory,
	)

	log.InfoF("MetricsRpcServer addr:%s", s.addr)
	if err := s.server.Serve(); err != nil {
		return errors.Wrapf(err, "s.server.Serve")
	}

	return nil
}

func (s *MetricsRpcServer) Stop() error {
	if s.server != nil {
		_ = s.server.Stop()
		s.server = nil
		log.Info("MetricsRpcServer Stop")
	}
	return nil
}
