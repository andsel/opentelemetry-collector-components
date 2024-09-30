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
	"errors"
	"fmt"
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal"
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/beat"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer/consumererror"
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

	backOffConfig := configretry.NewDefaultBackOffConfig()
	backOffConfig.Enabled = cfg.MaxRetries != 0
	// TODO: handle other values
	if cfg.MaxRetries == -1 {
		backOffConfig.MaxElapsedTime = 0 // unlimited
	}
	if cfg.Backoff.Init > 0 {
		backOffConfig.InitialInterval = cfg.Backoff.Init
	}
	if cfg.Backoff.Max > 0 {
		backOffConfig.MaxInterval = cfg.Backoff.Max
	}

	return exporterhelper.NewLogsExporter(
		ctx,
		params,
		cfg,
		pushLogsDataConsumer(clients),
		exporterhelper.WithTimeout(exporterhelper.TimeoutSettings{Timeout: cfg.Timeout}),
		exporterhelper.WithRetry(backOffConfig),
	)
}

func pushLogsDataConsumer(clients []NetworkClient) func(ctx context.Context, ld plog.Logs) error {
	return func(ctx context.Context, ld plog.Logs) error {
		// TODO: replace by the loadbalancer impl
		var errs []error
		for _, client := range clients {
			err := client.Connect()
			if err != nil {
				errs = append(errs, fmt.Errorf("error connecting client %s", client))
				continue
			}
			return client.Publish(ctx, internal.NewLogRecordBatch(ld))
		}
		return consumererror.NewLogs(errors.Join(errs...), ld)
	}
}
