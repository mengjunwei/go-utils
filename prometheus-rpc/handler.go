package rpc

import (
	"context"

	"github.com/mengjunwei/go-utils/prometheus-rpc/gen-go/metrics"
)

type MetricsTransferHandler struct {
	Processor               func(ms *metrics.Metrics) error
	ProcessorWithDatasource func(ms *metrics.Metrics, ds string) error
}

func (h *MetricsTransferHandler) Transfer(ctx context.Context, ms *metrics.Metrics) (r int32, err error) {
	return int32(len(ms.List)), h.Processor(ms)
}

func (h *MetricsTransferHandler) TransferWithDatasource(ctx context.Context, ms *metrics.Metrics, ds string) (r int32, err error) {
	return int32(len(ms.List)), h.ProcessorWithDatasource(ms, ds)
}
