// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lumberjackexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/syslogexporter"

import (
	"context"
	"fmt"

	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/beat"
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/metadata"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
)

// NewFactory returns a new factory for the syslog exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	cfg := defaultConfig()
	return &cfg
}

func createLogsExporter(
	ctx context.Context,
	settings exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {

	lje, err := newLumberjackExporter(cfg.(*Config), beat.Info{}, NewNilObserver(), settings)
	if err != nil {
		return nil, fmt.Errorf("failed to create the logs exporter: %w", err)
	}

	return exporterhelper.NewLogsExporter(ctx, settings, cfg,
		lje.pushLogs,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithStart(lje.Start),
		exporterhelper.WithShutdown(lje.Shutdown),
	)
}
