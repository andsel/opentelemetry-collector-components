// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logstashexporter

import (
	"context"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type logstashExporter struct {
	//TODO define the fields that define the status of the exporter instance
	logger *zap.Logger
}

func newLogstashExporter(
	cfg *Config,
	set exporter.Settings,
	logger *zap.Logger,
) *logstashExporter {
	return &logstashExporter{
		logger: logger,
		//TODO initialize the struct
	}
}

// Context is mandatory by signature of consumer.ConsumeLogsFunc
func (e *logstashExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		//resource := rl.Resource()
		ills := rl.ScopeLogs()
		for j := 0; j < ills.Len(); j++ {
			ill := ills.At(j)
			//scope := ill.Scope()
			logs := ill.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				e.logger.Info(logs.At(k).Body().AsString())
			}
		}
	}

	return nil
}
