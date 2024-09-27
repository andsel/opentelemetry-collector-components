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
	"github.com/elastic/elastic-agent-libs/transport"
	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/beat"
	"go.uber.org/zap"
)

const (
	minWindowSize             int = 1
	defaultStartMaxWindowSize int = 10
	defaultPort                   = 5044
)

func makeLogstash(beat beat.Info, observer Observer, lsConfig *Config, log *zap.Logger) ([]NetworkClient, error) {
	tlsCommonConfig, err := lsConfig.TLS.ToTLSCommonConfig()
	if err != nil {
		log.Error("Error creating TLS config", zap.Error(err))
		return nil, err
	}

	tls, err := tlscommon.LoadTLSConfig(tlsCommonConfig)
	if err != nil {
		return nil, err
	}

	transp := transport.Config{
		Timeout: lsConfig.Timeout,
		Proxy: &transport.ProxyConfig{
			URL:          lsConfig.URL,
			LocalResolve: lsConfig.LocalResolve,
		},
		TLS:   tls,
		Stats: observer,
	}

	clients := make([]NetworkClient, len(lsConfig.Hosts))
	for i, host := range lsConfig.Hosts {
		var client NetworkClient

		conn, err := transport.NewClient(transp, "tcp", host, defaultPort)
		if err != nil {
			return nil, err
		}

		// TODO: Async client / Load balancer, etc
		//if lsConfig.Pipelining > 0 {
		//	client, err = newAsyncClient(beat, conn, observer, lsConfig)
		//} else {
		client, err = newSyncClient(beat, conn, observer, lsConfig, log)
		//}
		if err != nil {
			return nil, err
		}

		//client = outputs.WithBackoff(client, lsConfig.Backoff.Init, lsConfig.Backoff.Max)
		clients[i] = client
	}

	return clients, nil
}
