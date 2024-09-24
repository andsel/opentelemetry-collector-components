// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml

package logstashexporter

import (
	"context"
	"errors"
	"github.com/elastic/opentelemetry-collector-components/exporter/logstashexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"
)

var componentType = component.MustNewType("logstash")

// NewFactory creates a factory for Elastic Logstash exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		componentType,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Verbosity: configtelemetry.LevelBasic,
	}
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	config component.Config,
) (exporter.Logs, error) {
	cfg := config.(*Config)
	exporterLogger := createLogger(cfg, set.TelemetrySettings.Logger)
	exporter := newLogstashExporter(cfg, set, exporterLogger)

	return exporterhelper.NewLogsExporter(
		ctx,
		set,
		cfg,
		exporter.pushLogsData,
	)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	//cfg := config.(*Config)
	return nil, errors.New("not yet implemented")
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	config component.Config,
) (exporter.Traces, error) {
	//cfg := config.(*Config)
	return nil, errors.New("not yet implemented")
}

func createLogger(cfg *Config, logger *zap.Logger) *zap.Logger {
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	// Do not prefix the output with log level (`info`)
	encoderConfig.LevelKey = ""
	// Do not prefix the output with current timestamp.
	encoderConfig.TimeKey = ""
	zapConfig := zap.Config{
		Level:         zap.NewAtomicLevelAt(zap.InfoLevel),
		DisableCaller: true,
		//Sampling: &zap.SamplingConfig{
		//	Initial:    exporterConfig.SamplingInitial,
		//	Thereafter: exporterConfig.SamplingThereafter,
		//},
		Encoding:      "console",
		EncoderConfig: encoderConfig,
		// Send exporter's output to stdout. This should be made configurable.
		OutputPaths: []string{"stdout"},
	}
	return zap.Must(zapConfig.Build())
}
