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

package beat

import (
	"errors"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"time"
)

func FromLogRecord(logRecord *plog.LogRecord) (*Event, error) {
	logRecordBody, ok := newLogRecordBody(logRecord)
	if !ok {
		return nil, consumererror.NewPermanent(errors.New("invalid beats event body"))
	}

	metadata := extractEventMetadata(logRecordBody)
	if !isBeatsEvent(metadata) {
		return nil, consumererror.NewPermanent(errors.New("invalid beats event metadata"))
	}

	timestamp, ok := extractEventTimestamp(logRecordBody)
	if !ok {
		timestamp = logRecord.ObservedTimestamp().AsTime()
	}

	fields := logRecordBody.AsRaw()
	return &Event{Timestamp: timestamp, Meta: metadata, Fields: fields}, nil
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
	timestamp, ok := logRecordBody.Get(TimestampFieldKey)
	if !ok {
		return time.Time{}, false
	}
	if timestamp.Type() != pcommon.ValueTypeInt {
		return time.Time{}, false
	}

	result := time.UnixMilli(timestamp.Int())
	logRecordBody.Remove(TimestampFieldKey)
	return result, true
}

func extractEventMetadata(logRecordBody *pcommon.Map) map[string]any {
	recordMetadata, hasMetadata := logRecordBody.Get(MetadataFieldKey)
	if !hasMetadata {
		return nil
	}
	if recordMetadata.Type() != pcommon.ValueTypeMap {
		return nil
	}

	metadataMap := recordMetadata.Map()
	logRecordBody.Remove(MetadataFieldKey)
	return metadataMap.AsRaw()
}
