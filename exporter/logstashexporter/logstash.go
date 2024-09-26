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

package logstashexporter

import (
	"github.com/elastic/elastic-agent-libs/transport"
	"github.com/elastic/elastic-agent-libs/transport/tlscommon"
	"github.com/elastic/opentelemetry-collector-components/exporter/logstashexporter/internal/beat"
	"go.uber.org/zap"
	"math/rand"
)

const (
	minWindowSize             int = 1
	defaultStartMaxWindowSize int = 10
	defaultPort                   = 5044
)

func makeLogstash(beat beat.Info, observer Observer, lsConfig *Config, log *zap.Logger) ([]NetworkClient, error) {
	tls, err := tlscommon.LoadTLSConfig(lsConfig.TLS)
	if err != nil {
		return nil, err
	}

	transp := transport.Config{
		Timeout: lsConfig.Timeout,
		Proxy:   &lsConfig.Proxy,
		TLS:     tls,
		Stats:   observer,
	}
	clients := make([]NetworkClient, len(lsConfig.Hosts))

	// Assume lsConfig.LoadBalance is false, pick one randomly
	if !lsConfig.LoadBalance {
		host := selectHost(lsConfig.Hosts)

		client, err := createLumberjackClient(host, beat, transp, observer, lsConfig, log)
		if err != nil {
			return nil, err
		}
		clients[0] = client
	} else {
		// TODO create one client for each host
	}
	return clients, nil
}

// select the first host if contains only one or picks one randomly
func selectHost(hosts []string) string {
	if len(hosts) == 1 {
		return hosts[0]
	}

	// pick one randomly
	randIndex := rand.Intn(len(hosts))
	return hosts[randIndex]
}

func createLumberjackClient(
	host string,
	beat beat.Info,
	transp transport.Config,
	observer Observer,
	lsConfig *Config,
	log *zap.Logger,
) (NetworkClient, error) {
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
	return client, nil
}
