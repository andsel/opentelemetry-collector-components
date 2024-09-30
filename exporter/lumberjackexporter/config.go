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
	"errors"
	"time"

	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/elasticagentlib"
	"go.opentelemetry.io/collector/component"
)

type Config struct {
	Hosts            []string                   `mapstructure:"hosts"`
	Index            string                     `mapstructure:"index"`
	Workers          int                        `mapstructure:"workers"`
	LoadBalance      bool                       `mapstructure:"loadbalance"`
	BulkMaxSize      int                        `mapstructure:"bulk_max_size"`
	SlowStart        bool                       `mapstructure:"slow_start"`
	Timeout          time.Duration              `mapstructure:"timeout"`
	TTL              time.Duration              `mapstructure:"ttl"               validate:"min=0"`
	Pipelining       int                        `mapstructure:"pipelining"        validate:"min=0"`
	CompressionLevel int                        `mapstructure:"compression_level" validate:"min=0, max=9"`
	MaxRetries       int                        `mapstructure:"max_retries"       validate:"min=-1"`
	TLS              *elasticagentlib.TLSConfig `mapstructure:"ssl"`
	ProxyConfig      `mapstructure:",squash"`
	Backoff          Backoff `mapstructure:"backoff"`
	EscapeHTML       bool    `mapstructure:"escape_html"`
}

type Backoff struct {
	Init time.Duration `mapstructure:"init"`
	Max  time.Duration `mapstructure:"max"`
}

// ProxyConfig holds the configuration information required to proxy
// connections through a SOCKS5 proxy server.
type ProxyConfig struct {
	// URL of the SOCKS proxy. Scheme must be socks5. Username and password can be
	// embedded in the URL.
	URL string `mapstructure:"proxy_url"`

	// Resolve names locally instead of on the SOCKS server.
	LocalResolve bool `mapstructure:"proxy_use_local_resolver"`
}

func defaultConfig() Config {
	defaultTLSEnabled := false

	return Config{
		Workers:          2,
		LoadBalance:      true,
		Pipelining:       2,
		BulkMaxSize:      2048,
		SlowStart:        false,
		CompressionLevel: 3,
		Timeout:          30 * time.Second,
		MaxRetries:       3,
		TTL:              0 * time.Second,
		TLS: &elasticagentlib.TLSConfig{
			Enabled:          &defaultTLSEnabled,
			VerificationMode: "full",
			Versions:         []string{"TLSv1.1", "TLSv1.2", "TLSv1.3"},
			Renegotiation:    "never",
		},
		Backoff: Backoff{
			Init: 1 * time.Second,
			Max:  60 * time.Second,
		},
		EscapeHTML: false,
	}
}

var _ component.Config = (*Config)(nil)

func (cfg *Config) Validate() error {
	if cfg.Hosts == nil || len(cfg.Hosts) == 0 {
		return errors.New("hosts must be non-empty")
	}
	return nil
}

// TODO
//func readConfig(cfg *config.C, info beat.Info) (*Config, error) {
//	c := defaultConfig()
//
//	err := cfgwarn.CheckRemoved6xSettings(cfg, "port")
//	if err != nil {
//		return nil, err
//	}
//
//	if err := cfg.Unpack(&c); err != nil {
//		return nil, err
//	}
//
//	if c.Index == "" {
//		c.Index = strings.ToLower(info.IndexPrefix)
//	}
//
//	return &c, nil
//}
