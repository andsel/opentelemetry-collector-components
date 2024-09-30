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
	"fmt"

	"go.elastic.co/apm/v2"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// outputWorker instances pass events from the shared workQueue to the outputs.Client
// instances.
type outputWorker interface {
	Close() error
}

type worker struct {
	qu   chan plog.Logs
	done chan struct{}
}

// clientWorker manages output client of type outputs.Client, not supporting reconnect.
type clientWorker struct {
	worker
	client Client
}

// netClientWorker manages reconnectable output clients of type outputs.NetworkClient.
type netClientWorker struct {
	worker
	client NetworkClient

	logger *zap.Logger

	tracer *apm.Tracer
}

func makeClientWorker(qu chan plog.Logs, client Client, logger *zap.Logger, tracer *apm.Tracer) outputWorker {
	w := worker{
		qu:   qu,
		done: make(chan struct{}),
	}

	var c interface {
		outputWorker
		run()
	}

	if nc, ok := client.(NetworkClient); ok {
		c = &netClientWorker{
			worker: w,
			client: nc,
			logger: logger,
			tracer: tracer,
		}
	} else {
		c = &clientWorker{worker: w, client: client}
	}

	go c.run()
	return c
}

func (w *worker) close() {
	close(w.done)
}

func (w *clientWorker) Close() error {
	w.worker.close()
	return w.client.Close()
}

func (w *clientWorker) run() {
	for {
		// We wait for either the worker to be closed or for there to be a batch of
		// events to publish.
		select {

		case <-w.done:
			return

		case batch := <-w.qu:

			if err := w.client.Publish(context.TODO(), batch); err != nil {
				return
			}
		}
	}
}

func (w *netClientWorker) Close() error {
	w.worker.close()
	return w.client.Close()
}

func (w *netClientWorker) run() {
	var (
		connected         = false
		reconnectAttempts = 0
	)

	for {
		// We wait for either the worker to be closed or for there to be a batch of
		// events to publish.
		select {

		case <-w.done:
			return

		case batch, ok := <-w.qu:
			if !ok {
				// close channel gives empty plogs
				return
			}

			// Try to (re)connect so we can publish batch
			if !connected {
				// Return batch to other output workers while we try to (re)connect
				//batch.Cancelled()

				if reconnectAttempts == 0 {
					w.logger.Info(fmt.Sprintf("Connecting to %v", w.client))
				} else {
					w.logger.Info(fmt.Sprintf("Attempting to reconnect to %v with %d reconnect attempt(s)", w.client, reconnectAttempts))
				}

				err := w.client.Connect()
				connected = err == nil
				if connected {
					w.logger.Info(fmt.Sprintf("Connection to %v established", w.client))
					reconnectAttempts = 0
				} else {
					w.logger.Error(fmt.Sprintf("Failed to connect to %v: %v", w.client, err))
					reconnectAttempts++
				}

				//continue
			}

			if err := w.publishBatch(batch); err != nil {
				connected = false
			}
		}
	}
}

func (w *netClientWorker) publishBatch(batch plog.Logs) error {
	ctx := context.Background()
	if w.tracer != nil && w.tracer.Recording() {
		tx := w.tracer.StartTransaction("publish", "output")
		defer tx.End()
		tx.Context.SetLabel("worker", "netclient")
		ctx = apm.ContextWithTransaction(ctx, tx)
	}

	err := w.client.Publish(ctx, batch)
	if err != nil {
		apm.CaptureError(ctx, err).Send()
		w.logger.Error("failed to publish events: ", zap.Error(err))
		// on error return to connect loop
		return err
	}
	return nil
}
