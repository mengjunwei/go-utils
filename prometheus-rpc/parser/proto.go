package parser

import (
	"io"

	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	dto "github.com/prometheus/client_model/go"

	"github.com/mengjunwei/go-utils/prometheus-rpc/gen-go/metrics"
)

func ParseProtobuf(in io.Reader, groupLabels map[string]string) (*metrics.Metrics, error) {
	metricFamilies := map[string]*dto.MetricFamily{}
	for {
		mf := &dto.MetricFamily{}
		if _, err := pbutil.ReadDelimited(in, mf); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		metricFamilies[mf.GetName()] = mf
	}
	return metricFamiliesFormat(metricFamilies, groupLabels)
}
