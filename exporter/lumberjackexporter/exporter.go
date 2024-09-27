package lumberjackexporter

import (
	"context"
	"errors"
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/beat"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
)

func newLogsExporter(
	ctx context.Context,
	params exporter.Settings,
	cfg *Config,
) (exporter.Logs, error) {
	clients, err := makeLogstash(beat.Info{}, NewNilObserver(), cfg, params.Logger)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewLogsExporter(
		ctx,
		params,
		cfg,
		func(ctx context.Context, ld plog.Logs) error {
			//TODO: Testing purpose (not even close to be "done")
			var errs error
			for _, client := range clients {
				err := client.Publish(ctx, ld)
				if err != nil {
					errs = errors.Join(errs, err)
				}
			}
			return errs
		},
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: cfg.Timeout}),
	)
}
