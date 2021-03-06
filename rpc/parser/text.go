package parser

import (
	"io"

	"github.com/pkg/errors"

	"github.com/mengjunwei/go-utils/container/list"
	"github.com/mengjunwei/go-utils/rpc/gen-go/metrics"
	"github.com/mengjunwei/go-utils/rpc/parser/expfmt"
	dto "github.com/prometheus/client_model/go"
)

var parserPool *TextParserPool

func init() {
	pools := list.NewSafeListLimited(10240)
	parserPool = &TextParserPool{pools: pools}
}

type TextParserPool struct {
	pools *list.SafeListLimited
}

func (tpp *TextParserPool) getTextParser() interface{} {
	p := tpp.pools.PopBack()
	if p != nil {
		return p
	}

	//TODO new TextParser
	return &expfmt.TextParser{}
}

func (tpp *TextParserPool) putTextParser(p interface{}) {
	tpp.pools.PushFront(p)
}

func ParseText(in io.Reader, groupLabels map[string]string) (*metrics.Metrics, error) {
	p := parserPool.getTextParser().(*expfmt.TextParser)
	metricFamilies, err := p.TextToMetricFamilies(in)
	parserPool.putTextParser(p)

	if err != nil {
		return nil, errors.Wrapf(err, "TextParserExpfmt.TextToMetrics() error")
	}
	return metricFamiliesForamt(metricFamilies, groupLabels)
}

func ParseTextToMetricFamily(in io.Reader) (map[string]*dto.MetricFamily, error) {
	p := parserPool.getTextParser().(*expfmt.TextParser)
	metricFamilies, err := p.TextToMetricFamilies(in)
	parserPool.putTextParser(p)

	if err != nil {
		return nil, errors.Wrapf(err, "TextParserExpfmt.TextToMetrics() error")
	}
	return metricFamilies, nil
}
