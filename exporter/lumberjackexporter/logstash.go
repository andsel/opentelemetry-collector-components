// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package lumberjackexporter

import (
	"context"

	"github.com/elastic/elastic-agent-libs/transport"
	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/beat"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const (
	minWindowSize             int = 1
	defaultStartMaxWindowSize int = 10
	defaultPort                   = 5044
)

type lumberjackExporter struct {
	config          *Config
	transportConfig *transport.Config
	beatInfo        beat.Info
	observer        Observer
	// Each worker is a goroutine that will read batches from workerChan and
	// send them to the output.
	workers    []outputWorker
	workerChan chan plog.Logs
	logger     *zap.Logger
	settings   component.TelemetrySettings
}

func newLumberjackExporter(cfg *Config, beatInfo beat.Info, observer Observer, settings exporter.Settings) (*lumberjackExporter, error) {
	logger := settings.Logger

	tlsCommonConfig, err := cfg.TLS.ToTLSCommonConfig()
	if err != nil {
		logger.Error("Error creating TLS config", zap.Error(err))
		return nil, err
	}

	tls, err := tlscommon.LoadTLSConfig(tlsCommonConfig)
	if err != nil {
		return nil, err
	}

	transportConfig := transport.Config{
		Timeout: cfg.Timeout,
		Proxy: &transport.ProxyConfig{
			URL:          cfg.URL,
			LocalResolve: cfg.LocalResolve,
		},
		TLS:   tls,
		Stats: observer,
	}

	return &lumberjackExporter{
		config:          cfg,
		transportConfig: &transportConfig,
		beatInfo:        beatInfo,
		observer:        observer,
		logger:          settings.Logger,
		settings:        settings.TelemetrySettings,
	}, nil
}

func (e *lumberjackExporter) Start(ctx context.Context, host component.Host) error {
	if e.config.LoadBalance {
		clients := make([]NetworkClient, 0, len(e.config.Hosts)*e.config.Workers)
		for _, host := range e.config.Hosts {
			for j := 0; j < e.config.Workers; j++ {
				var client NetworkClient

				conn, err := transport.NewClient(*e.transportConfig, "tcp", host, defaultPort)
				if err != nil {
					return err
				}

				// TODO: Async client / Load balancer, etc
				//if lsConfig.Pipelining > 0 {
				//	client, err = newAsyncClient(beat, conn, observer, lsConfig)
				//} else {
				client, err = newSyncClient(e.beatInfo, conn, e.observer, e.config, e.logger)
				//}
				if err != nil {
					return err
				}

				//client = outputs.WithBackoff(client, lsConfig.Backoff.Init, lsConfig.Backoff.Max)
				clients = append(clients, client)
			}
		}

		workerChan := make(chan plog.Logs)
		workers := make([]outputWorker, len(clients))
		for i, client := range clients {
			workers[i] = makeClientWorker(workerChan, client, e.logger, nil)
		}

		e.workerChan = workerChan
		e.workers = workers
	} else {
		//TODO FailoverClient
	}

	return nil
}

func (e *lumberjackExporter) Shutdown(ctx context.Context) error {
	close(e.workerChan)

	// Signal the output workers to close.
	for _, out := range e.workers {
		if err := out.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (e *lumberjackExporter) pushLogs(ctx context.Context, logs plog.Logs) error {
	e.workerChan <- logs
	return nil
}
