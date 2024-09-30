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
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal"
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/beat"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.uber.org/zap"
	"time"

	"github.com/elastic/elastic-agent-libs/transport"
	v2 "github.com/elastic/go-lumber/client/v2"
)

type syncClient struct {
	log *zap.Logger
	*transport.Client
	client    *v2.SyncClient
	observer  Observer
	win       *window
	ttl       time.Duration
	ttlTicker *time.Ticker
}

func newSyncClient(
	beat beat.Info,
	conn *transport.Client,
	observer Observer,
	config *Config,
	log *zap.Logger,
) (*syncClient, error) {
	c := &syncClient{
		log:      log,
		Client:   conn,
		observer: observer,
		ttl:      config.TTL,
	}

	if config.SlowStart {
		c.win = newWindower(defaultStartMaxWindowSize, config.BulkMaxSize)
	}
	if c.ttl > 0 {
		c.ttlTicker = time.NewTicker(c.ttl)
	}

	var err error
	enc := makeLogstashEventEncoder(log, beat, config.EscapeHTML, config.Index)
	c.client, err = v2.NewSyncClientWithConn(conn,
		v2.JSONEncoder(enc),
		v2.Timeout(config.Timeout),
		v2.CompressionLevel(config.CompressionLevel),
	)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *syncClient) Connect() error {
	c.log.Debug("connect")
	err := c.Client.Connect()
	if err != nil {
		return err
	}

	if c.ttlTicker != nil {
		c.ttlTicker = time.NewTicker(c.ttl)
	}
	return nil
}

func (c *syncClient) Close() error {
	if c.ttlTicker != nil {
		c.ttlTicker.Stop()
	}
	c.log.Debug("close connection")
	return c.Client.Close()
}

func (c *syncClient) reconnect() error {
	if err := c.Client.Close(); err != nil {
		c.log.Sugar().Errorf("error closing connection to logstash host %s: %+v, reconnecting...", c.Host(), err)
	}
	return c.Client.Connect()
}

func (c *syncClient) Publish(_ context.Context, batch *internal.LogRecordBatch) error {
	st := c.observer
	st.NewBatch(batch.Len())
	if batch.Len() == 0 {
		return nil
	}

	defer c.logBatchPermanentErrors(batch)

	for batch.PendingLogsCount() > 0 {
		// check if we need to reconnect
		if c.ttlTicker != nil {
			select {
			case <-c.ttlTicker.C:
				if err := c.reconnect(); err != nil {
					return consumererror.NewLogs(err, batch.RetryPendingLogs())
				}
				// reset window size on reconnect
				if c.win != nil {
					c.win.windowSize = int32(defaultStartMaxWindowSize)
				}
			default:
			}
		}

		begin := time.Now()
		var sent int
		var err error
		if c.win == nil {
			sent, err = c.sendAllEvents(batch)
		} else {
			sent, err = c.publishWindowed(batch)
		}

		took := time.Since(begin)
		st.ReportLatency(took)
		st.AckedEvents(sent)
		c.log.Sugar().Debugf("%v events out of %v sent to host %s, and %v permanent errors.",
			sent, batch.Len(), c.Host(), batch.PermanentErrorsCount())

		if err != nil {
			if c.win != nil {
				c.win.shrinkWindow()
			}
			_ = c.Close()
			c.log.Sugar().Errorf("Failed to publish events caused by: %+v", err)

			pending := batch.PendingLogsCount()
			permanent := batch.PermanentErrorsCount()
			if permanent > 0 {
				st.PermanentErrors(permanent)
				// No more logs to retry or send
				if pending == 0 {
					return consumererror.NewPermanent(batch.JoinPermanentErrors())
				}
			}

			if pending > 0 {
				c.log.Sugar().Infof("Will retry %v pending events", pending)
				st.RetryableErrors(pending)
				return consumererror.NewLogs(err, batch.RetryPendingLogs())
			}

			return err
		}
	}

	// Other permanent errors not related to the client's sending
	if batch.PermanentErrorsCount() > 0 {
		return consumererror.NewPermanent(batch.JoinPermanentErrors())
	}

	return nil
}

func (c *syncClient) logBatchPermanentErrors(batch *internal.LogRecordBatch) {
	permanent := batch.PermanentErrorsCount()
	if permanent > 0 {
		err := batch.JoinPermanentErrors()
		c.log.Sugar().Errorf("%v events permanently dropped due to unrecoverable errors: %+v", permanent, err)
	}
}

func (c *syncClient) publishWindowed(batch *internal.LogRecordBatch) (int, error) {
	batchSize := batch.Len()
	windowSize := c.win.get()

	c.log.Sugar().Debugf("Try to publish %v events to host %s with window size %v",
		batchSize, c.Host(), windowSize)

	n, err := c.sendEvents(batch, windowSize)
	if err != nil {
		return n, err
	}

	c.win.tryGrowWindow(batchSize)
	return n, nil
}

func (c *syncClient) sendAllEvents(batch *internal.LogRecordBatch) (int, error) {
	return c.sendEvents(batch, 0)
}

func (c *syncClient) sendEvents(batch *internal.LogRecordBatch, size int) (int, error) {
	var lumberjackWindow []any
	var lumberjackWindowAck []*internal.LogRecordBatchItem

	pendingLogs := batch.PendingLogs()
	var windowSize int
	if size > 0 && size < len(pendingLogs) {
		windowSize = size
	} else {
		windowSize = len(pendingLogs)
	}

	for _, event := range pendingLogs[:windowSize] {
		beatsEvent, err := beat.FromLogRecord(event.LogRecord)
		if err != nil {
			batch.PermanentError(event, err)
			continue
		}
		lumberjackWindow = append(lumberjackWindow, beatsEvent)
		lumberjackWindowAck = append(lumberjackWindowAck, event)
	}

	if len(lumberjackWindow) == 0 {
		return 0, nil
	}

	// TODO: Move to the load-balancer?
	err := c.Client.Connect()
	if err != nil {
		return 0, err
	}

	n, err := c.client.Send(lumberjackWindow)
	if n > 0 {
		batch.Ack(lumberjackWindowAck[:n]...)
	}

	return n, err
}
