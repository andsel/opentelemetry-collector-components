// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package lumberjackexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/syslogexporter"

import (
	"context"
	"fmt"
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/metadata"

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
	params exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	config := cfg.(*Config)
	exp, err := newLogsExporter(ctx, params, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create the logs exporter: %w", err)
	}
	return exp, nil
}
