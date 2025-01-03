// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package mylogical

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func compare(
	a *assert.Assertions, batch *types.TemporalBatch, table ident.Table, muts []types.Mutation,
) {
	tableBatch, ok := batch.Data.Get(table)
	if !a.Truef(ok, "unknown table %s", table) {
		return
	}
	tableMutations := tableBatch.Data
	if !a.Len(tableMutations, len(muts), "table %s", table) {
		return
	}
	for idx, mut := range tableMutations {
		a.Equal(muts[idx].Before, mut.Before)
		a.Equal(muts[idx].Data, mut.Data)
		a.Equal(muts[idx].Key, mut.Key)
		a.Equal(muts[idx].Time, mut.Time)
		a.NotNil(mut.Meta["mylogical"])
	}
}

// TestOnDataTuple verifies that incoming tuples are added to a batch
func TestOnDataTuple(t *testing.T) {
	r := require.New(t)
	consistentPoint, err := newConsistentPoint(mysql.MariaDBFlavor).parseFrom("1-1-1")
	r.NoError(err)
	schema := ident.MustSchema(ident.Public)
	// Simple KV table
	kvTableID := uint64(0)
	kvTable := ident.NewTable(schema, ident.New("t1"))
	kvCols := []types.ColData{
		{Name: ident.New("k"), Primary: true, Type: "int"},
		{Name: ident.New("v"), Primary: false, Type: "int"},
	}
	// Table with no key
	noKeyID := uint64(1)
	noKeyTable := ident.NewTable(schema, ident.New("t2"))
	noKeyCols := []types.ColData{
		{Name: ident.New("v"), Primary: false, Type: "int"},
	}
	tables := []ident.Table{kvTable, noKeyTable}
	columns := &ident.TableMap[[]types.ColData]{}
	columns.Put(kvTable, kvCols)
	columns.Put(noKeyTable, noKeyCols)
	c := &conn{
		columns: columns,
		relations: map[uint64]ident.Table{
			kvTableID: kvTable,
			noKeyID:   noKeyTable,
		},
		target: schema,
	}
	ts := c.monotonic.External(consistentPoint)
	tests := []struct {
		name      string
		tuple     *replication.RowsEvent
		operation mutationType
		wantMuts  []types.Mutation
		wantErr   string
	}{
		{
			name: "insert",
			tuple: &replication.RowsEvent{
				TableID: kvTableID,
				Rows: [][]any{
					{1, 10},
					{2, 20},
					{3, 30},
				},
			},
			operation: insertMutation,
			wantMuts: []types.Mutation{
				{
					Data: json.RawMessage(`{"k":1,"v":10}`),
					Key:  json.RawMessage(`[1]`),
					Time: ts,
				},
				{
					Data: json.RawMessage(`{"k":2,"v":20}`),
					Key:  json.RawMessage(`[2]`),
					Time: ts,
				},
				{
					Data: json.RawMessage(`{"k":3,"v":30}`),
					Key:  json.RawMessage(`[3]`),
					Time: ts,
				},
			},
		},
		{
			name: "insert no key",
			tuple: &replication.RowsEvent{
				TableID: noKeyID,
				Rows: [][]any{
					{1},
				},
			},
			operation: insertMutation,
			wantMuts: []types.Mutation{
				{
					Data: json.RawMessage(`{"v":1}`),
					Key:  json.RawMessage(`null`),
					Time: ts,
				},
			},
		},
		{
			name: "update",
			tuple: &replication.RowsEvent{
				TableID: kvTableID,
				Rows: [][]any{
					{1, 10},
					{1, 11},
					{2, 20},
					{2, 21},
				},
			},
			operation: updateMutation,
			wantMuts: []types.Mutation{
				{
					Data: json.RawMessage(`{"k":1,"v":11}`),
					Key:  json.RawMessage(`[1]`),
					Time: ts,
				},
				{
					Data: json.RawMessage(`{"k":2,"v":21}`),
					Key:  json.RawMessage(`[2]`),
					Time: ts,
				},
			},
		},
		{
			name: "update no key",
			tuple: &replication.RowsEvent{
				TableID: noKeyID,
				Rows: [][]any{
					{1},
					{1},
				},
			},
			operation: updateMutation,
			wantErr:   "only inserts supported with no key",
		},
		{
			name: "delete",
			tuple: &replication.RowsEvent{
				TableID: kvTableID,
				Rows: [][]any{
					{3, 2},
				},
			},
			operation: deleteMutation,
			wantMuts: []types.Mutation{
				{
					Data:     json.RawMessage(`{"k":3,"v":2}`),
					Deletion: true,
					Key:      json.RawMessage(`[3]`),
					Time:     ts,
				},
			},
		},
		{
			name: "invalid_row_size", // Verification for bug #858
			tuple: &replication.RowsEvent{
				TableID: kvTableID,
				Rows: [][]any{
					{1, 2, 3},
				},
			},
			operation: insertMutation,
			wantErr:   "unexpected number of columns",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			batch := &types.TemporalBatch{
				Time: ts,
			}
			err := c.onDataTuple(batch, tt.tuple, tt.operation)
			if tt.wantErr != "" {
				a.ErrorContains(err, tt.wantErr)
				return
			}
			a.NoError(err)
			compare(a, batch, tables[tt.tuple.TableID], tt.wantMuts)
		})
	}
}

func TestOnRelation(t *testing.T) {
	mySchema := ident.MustSchema(ident.New("my"), ident.Public)
	tests := []struct {
		name         string
		tableEvent   *replication.TableMapEvent
		targetSchema ident.Schema
		wantColumns  []types.ColData
		wantTable    ident.Table
	}{
		{
			name: "same db one pk",
			tableEvent: &replication.TableMapEvent{
				ColumnCount: 2,
				ColumnName:  [][]byte{[]byte("pk1"), []byte("col1")},
				ColumnType:  []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR},
				PrimaryKey:  []uint64{0},
				Schema:      []byte("my"),
				Table:       []byte("table1"),
				TableID:     1,
			},
			targetSchema: mySchema,
			wantColumns: []types.ColData{
				{
					Name:    ident.New("pk1"),
					Primary: true,
					Type:    fmt.Sprintf("%d", mysql.MYSQL_TYPE_STRING),
				},
				{
					Name:    ident.New("col1"),
					Primary: false,
					Type:    fmt.Sprintf("%d", mysql.MYSQL_TYPE_VARCHAR),
				},
			},
			wantTable: ident.NewTable(mySchema, ident.New("table1")),
		},
		{
			name: "same db two pk",
			tableEvent: &replication.TableMapEvent{
				ColumnCount: 3,
				ColumnName:  [][]byte{[]byte("pk1"), []byte("pk2"), []byte("col1")},
				ColumnType: []byte{
					mysql.MYSQL_TYPE_STRING,
					mysql.MYSQL_TYPE_LONG,
					mysql.MYSQL_TYPE_VARCHAR,
				},
				PrimaryKey: []uint64{0, 1},
				Schema:     []byte("my"),
				Table:      []byte("table2"),
				TableID:    2,
			},
			targetSchema: mySchema,
			wantColumns: []types.ColData{
				{
					Name:    ident.New("pk1"),
					Primary: true,
					Type:    fmt.Sprintf("%d", mysql.MYSQL_TYPE_STRING),
				},
				{
					Name:    ident.New("pk2"),
					Primary: true,
					Type:    fmt.Sprintf("%d", mysql.MYSQL_TYPE_LONG),
				},
				{
					Name:    ident.New("col1"),
					Primary: false,
					Type:    fmt.Sprintf("%d", mysql.MYSQL_TYPE_VARCHAR),
				},
			},
			wantTable: ident.NewTable(mySchema, ident.New("table2")),
		},
		{
			name: "different db",
			tableEvent: &replication.TableMapEvent{
				ColumnCount: 2,
				ColumnName:  [][]byte{[]byte("pk1"), []byte("col1")},
				ColumnType:  []byte{mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_VARCHAR},
				PrimaryKey:  []uint64{0},
				Schema:      []byte("different"),
				Table:       []byte("table1"),
				TableID:     1,
			},
			targetSchema: mySchema,
			wantColumns: []types.ColData{
				{
					Name:    ident.New("pk1"),
					Primary: true,
					Type:    fmt.Sprintf("%d", mysql.MYSQL_TYPE_STRING),
				},
				{
					Name:    ident.New("col1"),
					Primary: false,
					Type:    fmt.Sprintf("%d", mysql.MYSQL_TYPE_VARCHAR),
				},
			},
			wantTable: ident.NewTable(mySchema, ident.New("table1")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := assert.New(t)
			r := require.New(t)
			c := &conn{
				columns:   &ident.TableMap[[]types.ColData]{},
				config:    &Config{},
				relations: make(map[uint64]ident.Table),
				target:    tt.targetSchema,
			}
			err := c.onRelation(tt.tableEvent)
			a.NoError(err)
			a.Equal(tt.wantTable.Raw(), c.relations[tt.tableEvent.TableID].Raw())
			cols, ok := c.columns.Get(tt.wantTable)
			a.True(ok)
			r.Equal(len(tt.wantColumns), len(cols))
			for idx, want := range tt.wantColumns {
				r.Equal(want.Ignored, cols[idx].Ignored)
				r.Equal(want.Name, cols[idx].Name)
				r.Equal(want.Primary, cols[idx].Primary)
				r.Equal(want.Type, cols[idx].Type)
			}
		})
	}
}

type mockMemo struct {
	kv sync.Map
}

var _ types.Memo = &mockMemo{}

// Get implements types.Memo.
func (m *mockMemo) Get(ctx context.Context, tx types.StagingQuerier, key string) ([]byte, error) {
	res, ok := m.kv.Load(key)
	if !ok {
		return nil, nil
	}
	return res.([]byte), nil
}

// Put implements types.Memo.
func (m *mockMemo) Put(
	ctx context.Context, tx types.StagingQuerier, key string, value []byte,
) error {
	m.kv.Store(key, value)
	return nil
}

// TestInitialConsistentPoint verifies that we are persisting the correct initial value
func TestInitialConsistentPoint(t *testing.T) {
	tests := []struct {
		config string
		flavor string
		name   string
		stored string
		want   string
	}{
		{
			flavor: mysql.MySQLFlavor,
			name:   "empty",
			want:   "",
		},
		{
			flavor: mysql.MySQLFlavor,
			name:   "stored",
			stored: "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374",
			want:   "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374",
		},
		{
			config: "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-10",
			flavor: mysql.MySQLFlavor,
			name:   "config",
			want:   "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-10",
		},
		{
			config: "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-10",
			flavor: mysql.MySQLFlavor,
			name:   "stored_config",
			stored: "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374",
			want:   "6fa7e6ef-c49a-11ec-950a-0242ac120002:1-5374",
		},
		{
			flavor: mysql.MariaDBFlavor,
			name:   "empty",
			want:   "",
		},
		{
			flavor: mysql.MariaDBFlavor,
			name:   "stored",
			stored: "1-1-100",
			want:   "1-1-100",
		},
		{
			config: "1-1-2",
			flavor: mysql.MariaDBFlavor,
			name:   "config",
			want:   "1-1-2",
		},
		{
			config: "1-1-2",
			flavor: mysql.MariaDBFlavor,
			name:   "stored_config",
			stored: "1-1-100",
			want:   "1-1-100",
		},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("%s_%s", tt.name, tt.flavor)
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stop := stopper.WithContext(ctx)

			a := assert.New(t)
			m := &mockMemo{}
			c := &conn{
				config: &Config{
					InitialGTID: tt.config,
				},
				flavor: tt.flavor,
				memo:   m,
				target: ident.MustSchema(ident.Public),
			}
			key := fmt.Sprintf("mysql-wal-offset-%s", c.target.Raw())
			if tt.stored != "" {
				a.NoError(m.Put(stop, nil, key, []byte(tt.stored)))
			}
			a.NoError(c.persistWALOffset(stop))
			a.Equal(c.monotonic.Last().External().(*consistentPoint).String(), tt.want)
		})
	}
}

func TestIsColumnSignedFromColumnType(t *testing.T) {
	// Define table-driven test cases
	tests := []struct {
		name        string
		columnTypes [][]byte
		idx         int
		expected    bool
	}{
		{
			name:        "signed column type",
			columnTypes: [][]byte{[]byte("bigint"), []byte("int unsigned")},
			idx:         0,
			expected:    true,
		},
		{
			name:        "unsigned column type",
			columnTypes: [][]byte{[]byte("bigint"), []byte("int unsigned")},
			idx:         1,
			expected:    false,
		},
		{
			name:        "empty column type",
			columnTypes: [][]byte{[]byte("")},
			idx:         0,
			expected:    true, // Default to signed for empty type
		},
		{
			name:        "unsigned with extra whitespace",
			columnTypes: [][]byte{[]byte(" int  unsigned ")},
			idx:         0,
			expected:    false,
		},
		{
			name:        "mixed casing in unsigned",
			columnTypes: [][]byte{[]byte("BigInt UNSIGNED")},
			idx:         0,
			expected:    false,
		},
	}

	// Run each test case
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isColumnSignedFromColumnType(tt.columnTypes, tt.idx)
			require.Equal(t, tt.expected, result, "test: %s", tt.name)
		})
	}
}

func TestIsColumnSignedFromBitmap(t *testing.T) {

	tests := []struct {
		name        string
		bitmap      []byte
		columnIndex int
		expected    bool
	}{
		{
			name:        "signed column in first byte",
			bitmap:      []byte{0b01011010}, // First byte: [0=Signed, 1=Unsigned, 0=Signed, 1=Unsigned, ...]
			columnIndex: 0,
			expected:    true, // bit 7 is 0 (signed)
		},
		{
			name:        "unsigned column in first byte",
			bitmap:      []byte{0b01011010}, // First byte: [0, 1, 0, 1, 1, 0, ...]
			columnIndex: 1,
			expected:    false, // bit 6 is 1 (unsigned)
		},
		{
			name:        "signed column in second byte",
			bitmap:      []byte{0b01011010, 0b10110111},
			columnIndex: 9, // second byte, bit 6 (signed)
			expected:    true,
		},
		{
			name:        "unsigned column in second byte",
			bitmap:      []byte{0b01011010, 0b10110111},
			columnIndex: 8, // second byte, bit 7 (unsigned)
			expected:    false,
		},
		{
			name:        "out of bounds index",
			bitmap:      []byte{0b01011010},
			columnIndex: 16,
			expected:    true, // default to signed
		},
		{
			name:        "signed column in multi-byte bitmap",
			bitmap:      []byte{0b01011010, 0b10110111, 0b11000000},
			columnIndex: 18, // third byte, bit 5 (signed)
			expected:    true,
		},
		{
			name:        "unsigned column in multi-byte bitmap",
			bitmap:      []byte{0b01011010, 0b10110111, 0b11000000},
			columnIndex: 17, // third byte, bit 6 (unsigned)
			expected:    false,
		},
	}

	// Run each test case
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isColumnSignedFromBitmap(tt.bitmap, tt.columnIndex)
			require.Equal(t, tt.expected, result, "test: %s", tt.name)
		})
	}
}
