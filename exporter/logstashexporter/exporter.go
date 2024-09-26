package logstashexporter

import (
	"context"
	"errors"
	"github.com/elastic/opentelemetry-collector-components/exporter/logstashexporter/internal/beat"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type clientsPool struct {
	logger  *zap.Logger
	cfg     *Config
	clients []NetworkClient
}

func (p clientsPool) createNewClient() error {
	clients, err := makeLogstash(beat.Info{}, NewNilObserver(), p.cfg, p.logger)
	if err != nil {
		return err
	}
	p.clients = clients
	return nil
}

func (p clientsPool) sendLogs(ctx context.Context, ld plog.Logs) error {
	//TODO: Testing purpose (not even close to be "done")
	//if len(clients) == 1 {
	//	// using single client
	//	client := clients[0]
	//	err := client.Publish(ctx, ld)
	//	if err != nil {
	//		if !cfg.LoadBalance {
	//			if len(cfg.Hosts) > 1 {
	//				// no load balancing and more hosts are configured then retry with another random client
	//				params.Logger.Warn("Create a new client picking a random host")
	//				clients, err = makeLogstash(beat.Info{}, NewNilObserver(), cfg, params.Logger)
	//				if err != nil {
	//					return err
	//				}
	//				// TODO call the Publish again with new client
	//			} else {
	//				return err
	//			}
	//		} else {
	//			// maybe some bad config, if we are means just one host is configured with load balancing enabled
	//			return err
	//		}
	//	}
	//	return nil
	//} else {
	var errs error
	for _, client := range p.clients {
		err := client.Publish(ctx, ld)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
	//}
}

func newLogsExporter(
	ctx context.Context,
	params exporter.Settings,
	cfg *Config,
) (exporter.Logs, error) {
	ljClients := &clientsPool{
		logger: params.Logger,
		cfg:    cfg,
	}
	err := ljClients.createNewClient()

	if err != nil {
		return nil, err
	}

	return exporterhelper.NewLogsExporter(
		ctx,
		params,
		cfg,
		ljClients.sendLogs,
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: cfg.Timeout}),
	)
}
