// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package translator // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/datadogreceiver/internal/translator"

import (
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/exp/metrics/identity"
)

type MetricsTranslator struct {
	sync.RWMutex
	buildInfo  component.BuildInfo
	lastTs     map[uint64]pcommon.Timestamp
	stringPool *StringPool
}

func NewMetricsTranslator(buildInfo component.BuildInfo) *MetricsTranslator {
	return &MetricsTranslator{
		buildInfo:  buildInfo,
		lastTs:     make(map[uint64]pcommon.Timestamp),
		stringPool: newStringPool(),
	}
}

func (mt *MetricsTranslator) streamHasTimestamp(stream identity.Stream) (pcommon.Timestamp, bool) {
	mt.RLock()
	defer mt.RUnlock()
	ts, ok := mt.lastTs[stream.Hash().Sum64()]
	return ts, ok
}

func (mt *MetricsTranslator) updateLastTsForStream(stream identity.Stream, ts pcommon.Timestamp) {
	mt.Lock()
	defer mt.Unlock()
	// Store the hash instead of the stream itself to keep the memory footprint small
	// (the `Stream` contains a lot of data we never use).
	// WARNING: `lastTs` will grow unbounded since we never delete from the map.
	// With enough metric variance (cardinality), otelcol will eventually run out of memory.
	mt.lastTs[stream.Hash().Sum64()] = ts
}
