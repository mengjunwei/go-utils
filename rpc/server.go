package rpc

import (
	"github.com/apache/thrift/lib/go/thrift"

	"github.com/pkg/errors"

	"github.com/mengjunwei/go-utils/logger"
	"github.com/mengjunwei/go-utils/rpc/gen-go/metrics"
)

var (
	logInstance logger.Logger
)

func init() {
	logInstance = logger.NewNonLogger()
}

func SetLogger(logger logger.Logger) {
	logInstance = logger
}

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
		return errors.Wrapf(err, "NewTServerSocket")
	}

	//handler := &MetricsTransferHandler{processor: f}
	processor := metrics.NewMetricsTransferProcessor(&handler)

	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTCompactProtocolFactory()
	s.server = thrift.NewTSimpleServer4(
		processor,
		transport,
		transportFactory,
		protocolFactory,
	)

	logInstance.Info("MetricsRpcServer Serve:%s", s.addr)
	if err := s.server.Serve(); err != nil {
		return errors.Wrapf(err, "server.Serve()")
	}

	return nil
}

func (s *MetricsRpcServer) Stop() error {
	if s.server != nil {
		s.server.Stop()
		s.server = nil

		logInstance.Info("MetricsRpcServer Stop")
	}

	return nil
}
