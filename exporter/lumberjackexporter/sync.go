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
	"github.com/elastic/opentelemetry-collector-components/exporter/lumberjackexporter/internal/beat"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"time"

	"github.com/elastic/elastic-agent-libs/transport"
	v2 "github.com/elastic/go-lumber/client/v2"
)

type syncClient struct {
	log *zap.Logger
	*transport.Client
	client   *v2.SyncClient
	observer Observer
	win      *window
	ttl      time.Duration
	ticker   *time.Ticker
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
		c.ticker = time.NewTicker(c.ttl)
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

	if c.ticker != nil {
		c.ticker = time.NewTicker(c.ttl)
	}
	return nil
}

func (c *syncClient) Close() error {
	if c.ticker != nil {
		c.ticker.Stop()
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

func (c *syncClient) Publish(_ context.Context, logs plog.Logs) error {
	var events []plog.LogRecord
	st := c.observer

	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		resourceLogs := logs.ResourceLogs().At(i)
		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			scopeLogs := resourceLogs.ScopeLogs().At(j)
			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				logRecord := scopeLogs.LogRecords().At(k)
				events = append(events, logRecord)
			}
		}
	}

	st.NewBatch(len(events))
	if len(events) == 0 {
		//batch.ACK()
		return nil
	}

	for len(events) > 0 {

		// check if we need to reconnect
		if c.ticker != nil {
			select {
			case <-c.ticker.C:
				if err := c.reconnect(); err != nil {
					//batch.Retry()
					return consumererror.NewLogs(err, logs)
				}

				// reset window size on reconnect
				if c.win != nil {
					c.win.windowSize = int32(defaultStartMaxWindowSize)
				}
			default:
			}
		}

		var (
			n   int
			err error
		)

		begin := time.Now()
		if c.win == nil {
			n, err = c.sendEvents(events)
		} else {
			n, err = c.publishWindowed(events)
		}
		took := time.Since(begin)
		st.ReportLatency(took)
		c.log.Sugar().Debugf("%v events out of %v events sent to logstash host %s. Continue sending",
			n, len(events), c.Host())

		events = events[n:]
		st.AckedEvents(n)
		if err != nil {
			// return batch to pipeline before reporting/counting error
			//batch.RetryEvents(events)
			if c.win != nil {
				c.win.shrinkWindow()
			}
			_ = c.Close()

			c.log.Sugar().Errorf("Failed to publish events caused by: %+v", err)

			rest := len(events)
			if consumererror.IsPermanent(err) {
				st.PermanentErrors(rest)
				return err
			}

			st.RetryableErrors(rest)
			return consumererror.NewLogs(err, logs)
		}

	}

	//batch.ACK()
	return nil
}

func (c *syncClient) publishWindowed(events []plog.LogRecord) (int, error) {
	batchSize := len(events)
	windowSize := c.win.get()
	c.log.Sugar().Debugf("Try to publish %v events to logstash host %s with window size %v",
		batchSize, c.Host(), windowSize)

	// prepare message payload
	if batchSize > windowSize {
		events = events[:windowSize]
	}

	n, err := c.sendEvents(events)
	if err != nil {
		return n, err
	}

	c.win.tryGrowWindow(batchSize)
	return n, nil
}

func (c *syncClient) sendEvents(events []plog.LogRecord) (int, error) {
	window := make([]interface{}, 0, len(events))
	//TODO: needs more tests and definitions regarding non-beats events actions
	for i := range events {
		logRecord := events[i]
		logRecordBody, ok := newLogRecordBody(&logRecord)
		if !ok {
			return 0, consumererror.NewPermanent(errors.New("invalid beats event body"))
		}

		metadata := extractEventMetadata(logRecordBody)
		if !isBeatsEvent(metadata) {
			return 0, consumererror.NewPermanent(errors.New("received a non-beats event"))
		}

		timestamp, ok := extractEventTimestamp(logRecordBody)
		if !ok {
			timestamp = logRecord.ObservedTimestamp().AsTime()
		}

		fields := logRecordBody.AsRaw()
		window = append(window, &beat.Event{Timestamp: timestamp, Meta: metadata, Fields: fields})
	}

	// TODO: Move to the load-balancer?
	err := c.Client.Connect()
	if err != nil {
		return 0, err
	}

	return c.client.Send(window)
}

func isBeatsEvent(metadata map[string]any) bool {
	_, ok := metadata["beat"]
	return ok
}

func newLogRecordBody(logRecord *plog.LogRecord) (*pcommon.Map, bool) {
	cp := pcommon.NewMap()
	if logRecord.Body().Type() != pcommon.ValueTypeMap {
		return nil, false
	}
	logRecord.Body().Map().CopyTo(cp)
	return &cp, true
}

func extractEventTimestamp(logRecordBody *pcommon.Map) (time.Time, bool) {
	timestamp, ok := logRecordBody.Get("@timestamp")
	if !ok {
		return time.Time{}, false
	}
	if timestamp.Type() != pcommon.ValueTypeInt {
		return time.Time{}, false
	}

	result := time.UnixMilli(timestamp.Int())
	logRecordBody.Remove("@timestamp")
	return result, true
}

func extractEventMetadata(logRecordBody *pcommon.Map) map[string]any {
	recordMetadata, hasMetadata := logRecordBody.Get("@metadata")
	if !hasMetadata {
		return nil
	}
	if recordMetadata.Type() != pcommon.ValueTypeMap {
		return nil
	}

	metadataMap := recordMetadata.Map()
	logRecordBody.Remove("@metadata")
	return metadataMap.AsRaw()
}
