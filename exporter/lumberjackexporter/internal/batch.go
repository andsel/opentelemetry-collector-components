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

package internal

import (
	"errors"
	"fmt"
	"go.opentelemetry.io/collector/pdata/plog"
)

type LogRecordBatch struct {
	plog.Logs
	events          []*LogRecordBatchItem
	sent            map[string]struct{}
	permanentErrors map[string]error
}

type LogRecordBatchItem struct {
	resourceIndex  int
	scopeLogsIndex int
	recordIndex    int
	*plog.LogRecord
}

func (r *LogRecordBatchItem) key() string {
	return newLogRecordBatchItemKey(r.resourceIndex, r.scopeLogsIndex, r.recordIndex)
}

func newLogRecordBatchItemKey(resourceIndex, scopeLogsIndex, recordIndex int) string {
	return fmt.Sprintf("%v%v%v", resourceIndex, scopeLogsIndex, recordIndex)
}

func NewLogRecordBatch(logs plog.Logs) *LogRecordBatch {
	b := &LogRecordBatch{
		Logs:            logs,
		sent:            make(map[string]struct{}),
		permanentErrors: make(map[string]error),
		events:          newLogRecordBatchItems(&logs),
	}
	return b
}

func newLogRecordBatchItems(logs *plog.Logs) []*LogRecordBatchItem {
	var events []*LogRecordBatchItem
	for i := 0; i < logs.ResourceLogs().Len(); i++ {
		resourceLogs := logs.ResourceLogs().At(i)
		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			scopeLogs := resourceLogs.ScopeLogs().At(j)
			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				logRecord := scopeLogs.LogRecords().At(k)
				item := &LogRecordBatchItem{
					resourceIndex:  i,
					scopeLogsIndex: j,
					recordIndex:    k,
					LogRecord:      &logRecord,
				}
				events = append(events, item)
			}
		}
	}
	return events
}

func (b *LogRecordBatch) Len() int {
	return len(b.events)
}

func (b *LogRecordBatch) Ack(events ...*LogRecordBatchItem) {
	for _, event := range events {
		b.sent[event.key()] = struct{}{}
	}
}

func (b *LogRecordBatch) PermanentError(item *LogRecordBatchItem, err error) {
	b.permanentErrors[item.key()] = err
}

func (b *LogRecordBatch) PendingLogs() []*LogRecordBatchItem {
	var events []*LogRecordBatchItem
	for _, event := range b.events {
		_, sent := b.sent[event.key()]
		_, failed := b.permanentErrors[event.key()]
		if !sent && !failed {
			events = append(events, event)
		}
	}
	return events
}

func (b *LogRecordBatch) PendingLogsCount() int {
	return len(b.events) - (len(b.sent) + len(b.permanentErrors))
}

func (b *LogRecordBatch) PermanentErrorsCount() int {
	return len(b.permanentErrors)
}

func (b *LogRecordBatch) RetryPendingLogs() plog.Logs {
	return b.newLogsSub(b.PendingLogs()...)
}

func (b *LogRecordBatch) JoinPermanentErrors() error {
	if len(b.permanentErrors) == 0 {
		return nil
	}

	errs := make([]error, 0, len(b.permanentErrors))
	for _, event := range b.events {
		if err, ok := b.permanentErrors[event.key()]; ok {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (b *LogRecordBatch) newLogsSub(logs ...*LogRecordBatchItem) plog.Logs {
	sub := plog.NewLogs()
	if len(logs) == 0 {
		return sub
	}

	keysInclude := map[string]struct{}{}
	for _, log := range logs {
		keysInclude[log.key()] = struct{}{}
	}

	seenObjects := map[any]int{}
	subResourceLogsSlice := sub.ResourceLogs()
	for i := 0; i < b.Logs.ResourceLogs().Len(); i++ {
		resourceLogs := b.Logs.ResourceLogs().At(i)
		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			scopeLogs := resourceLogs.ScopeLogs().At(j)
			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				key := newLogRecordBatchItemKey(i, j, k)
				_, includeRecord := keysInclude[key]
				if includeRecord {
					var r plog.ResourceLogs
					seenIdx, ok := seenObjects[&resourceLogs]
					if !ok {
						r = subResourceLogsSlice.AppendEmpty()
						resourceLogs.Resource().CopyTo(r.Resource())
						seenObjects[&resourceLogs] = subResourceLogsSlice.Len() - 1
					} else {
						r = sub.ResourceLogs().At(seenIdx)
					}

					var s plog.ScopeLogs
					seenIdx, ok = seenObjects[&scopeLogs]
					if !ok {
						s = r.ScopeLogs().AppendEmpty()
						scopeLogs.Scope().CopyTo(s.Scope())
						seenObjects[&scopeLogs] = r.ScopeLogs().Len() - 1
					} else {
						s = r.ScopeLogs().At(seenIdx)
					}

					appending := s.LogRecords().AppendEmpty()
					scopeLogs.LogRecords().At(k).CopyTo(appending)
				}
			}
		}
	}

	return sub
}
