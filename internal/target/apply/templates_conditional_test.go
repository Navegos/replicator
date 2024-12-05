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

package apply

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/applycfg"
	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type conditionalUpsertTestRow struct {
	pk0        int
	pk1        string
	ver0       int
	ver1       int
	hasDefault string // If empty, the upserted row will assume the default column value of 'default_val'.
	ts         time.Time
}

type conditionalUpsertTestCase struct {
	name         string
	cfg          *applycfg.Config
	upsertRows   []conditionalUpsertTestRow
	expectedRows []conditionalUpsertTestRow
}

// Verify that the conditional upsert query template behaves as expected against a running DB instance.
func TestConditionalUpsertQuery(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	tableSchema, cols := getTestingTableSchemaAndColumns(fixture, r, t)

	// Rows with this timestamp are "fresh" enough and will pass the deadline filter.
	freshTimestamp := time.Now().UTC().Round(time.Second)
	// Rows with this timestamp are too old/stale and dropped.
	staleTimestamp := freshTimestamp.Add(2 * -time.Hour)
	// Skip upserting rows if their "ts" column is < now() - deadlineInterval (use the above timestamps).
	deadlineInterval := time.Hour

	// Initial rows to start with in the test table for each test case.
	// NOTE: should be in ascending PK order, since actual rows will be read in this order.
	initialRows := []conditionalUpsertTestRow{
		{pk0: 0, pk1: "A", ver0: 1, ver1: 2, hasDefault: "initial", ts: freshTimestamp},
		{pk0: 1, pk1: "B", ver0: 2, ver1: 1, hasDefault: "initial", ts: freshTimestamp},
	}

	tcs := constructTestCases(initialRows, freshTimestamp, staleTimestamp, deadlineInterval)
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			// Start with a fresh target table for each test case and insert initial rows.
			tbl, err := fixture.CreateTargetTable(ctx, tableSchema)
			r.NoError(err)
			insertInitialRows(r, ctx, fixture, tbl.Name(), initialRows)

			// Instantiate template for the current test case based on global template options
			// to be used for each test case (ex. table col defs), and specific options for the
			// test case (ex. deadline and CAS columns to use).
			queryStr := instantiateConditionalUpsertTemplate(r, cols, tc, tbl.Name(), fixture)

			// Execute the SQL query and substitute in args for '?' placeholders.
			_, err = fixture.TargetPool.ExecContext(ctx, queryStr, upsertRowsToPlaceholderArgs(tc.upsertRows)...)
			r.NoError(err)
			confirmRowsAfterUpsert(ctx, a, r, fixture, tbl.Name(), tc.expectedRows)
		})
	}
}

// Return the table schema and column definitions to be used for each test case, depending
// on the DB product. Causes the test to fail if an untested DB product is detected.
func getTestingTableSchemaAndColumns(
	fixture *base.Fixture, r *require.Assertions, t *testing.T,
) (tableSchema string, cols []types.ColData) {
	switch fixture.TargetPool.Product {
	// DB products to test. Use the appropriate CREATE TABLE SQL statement for each.
	case types.ProductMySQL, types.ProductMariaDB:
		tableSchema = "CREATE TABLE %s (pk0 INT, pk1 VARCHAR(512), ver0 INT, ver1 INT," +
			"has_default VARCHAR(2048) NOT NULL DEFAULT 'default_val', ts TIMESTAMP, PRIMARY KEY (pk0,pk1))"
	case types.ProductCockroachDB, types.ProductPostgreSQL:
		tableSchema = "CREATE TABLE %s (pk0 INT, pk1 VARCHAR(512), ver0 INT, ver1 INT," +
			"has_default VARCHAR(2048) NOT NULL DEFAULT 'default_val', ts TIMESTAMP WITH TIME ZONE, PRIMARY KEY (pk0,pk1))"
	case types.ProductOracle:
		// TODO: skip Oracle tests for now. apply_test.go:TestConditionals() should provide SOME coverage.
		// It is known that ora/upsert.tmpl will break if multiple version columns are used for the CAS
		// inequality list, since (data.ver1, data.ver0) > (current.ver1, current.ver0) is NOT allowed in Oracle (error ORA-01796).
		t.Skip()
	default:
		r.FailNow("Untested product.")
	}

	cols = []types.ColData{
		{
			Name:    ident.New("pk0"),
			Primary: true,
			Type:    "INT",
		},
		{
			Name:    ident.New("pk1"),
			Primary: true,
			Type:    "VARCHAR(512)",
		},
		{
			Name: ident.New("ver0"),
			Type: "INT",
		},
		{
			Name: ident.New("ver1"),
			Type: "INT",
		},
		{
			Name:        ident.New("has_default"),
			Type:        "VARCHAR(2048)",
			DefaultExpr: "'default_val'",
		},
		{
			Name: ident.New("ts"),
			Type: "TIMESTAMP",
		},
	}
	return tableSchema, cols
}

// Construct and return the different tests cases to be used to test conditional upserts.
func constructTestCases(
	initialRows []conditionalUpsertTestRow,
	freshTimestamp time.Time,
	staleTimestamp time.Time,
	deadlineInterval time.Duration,
) []*conditionalUpsertTestCase {
	casColumns := []ident.Ident{ident.New("ver1"), ident.New("ver0")}
	// Define test cases in three categories: CAS (check version columns and swap if greater),
	// deadline (check that stale upsert rows aren't included), and CAS + deadline.
	return []*conditionalUpsertTestCase{
		{
			name: "casBothRowsUpdated",
			cfg: &applycfg.Config{
				// ver1 takes precedence over ver0 for multi-column version comparisons.
				// i.e. Will use this tuple comparison: (data.ver1, data.ver0) > (current.ver1, current.ver0).
				CASColumns: casColumns,
				Exprs: ident.MapOf[string](
					ident.New("ver1"), "1 * $0", // Test that expresssions are working.
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "", ts: freshTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "default_val", ts: freshTimestamp},
			},
		},
		{
			name: "casOneRowUpdated",
			cfg: &applycfg.Config{
				CASColumns: casColumns,
			},
			upsertRows: []conditionalUpsertTestRow{
				// Version columns are older than initial, so don't update.
				{pk0: 0, pk1: "A", ver0: 0, ver1: 2, hasDefault: "updated", ts: freshTimestamp},
				// Version columns are newer, so update.
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "", ts: freshTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				initialRows[0],
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "default_val", ts: freshTimestamp},
			},
		},
		{
			name: "casNoRowsUpdated",
			cfg: &applycfg.Config{
				CASColumns: casColumns,
			},
			upsertRows: []conditionalUpsertTestRow{
				// Version columns are older for both upsert rows.
				{pk0: 0, pk1: "A", ver0: 0, ver1: 2, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 2, ver1: 0, hasDefault: "", ts: freshTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				initialRows[0],
				initialRows[1],
			},
		},
		{
			name: "casDeadlineBothRowsUpdated",
			cfg: &applycfg.Config{
				CASColumns: casColumns,
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "updated", ts: freshTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "updated", ts: freshTimestamp},
			},
		},
		{
			name: "casDeadlineOneRowUpdatedDeadlineFail",
			cfg: &applycfg.Config{
				CASColumns: casColumns,
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "updated", ts: staleTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				initialRows[1], // Second row was stale, so it wasn't upserted.
			},
		},
		{
			name: "casDeadlineOneRowUpdatedOneRowInsertedDeadlineFail",
			cfg: &applycfg.Config{
				CASColumns: casColumns,
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "updated", ts: staleTimestamp},
				{pk0: 2, pk1: "C", ver0: 0, ver1: 0, hasDefault: "inserted", ts: freshTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				initialRows[1], // Second row was stale, so it wasn't upserted.
				{pk0: 2, pk1: "C", ver0: 0, ver1: 0, hasDefault: "inserted", ts: freshTimestamp},
			},
		},
		{
			name: "casDeadlineOneRowUpdatedCASFail",
			cfg: &applycfg.Config{
				CASColumns: casColumns,
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				// Version columns are newer, so update.
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				// Version columns are older, don't update.
				{pk0: 1, pk1: "B", ver0: 1, ver1: 1, hasDefault: "updated", ts: freshTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				initialRows[1],
			},
		},
		{
			name: "casDeadlineOneRowUpdatedOneRowInsertedCASFail",
			cfg: &applycfg.Config{
				CASColumns: casColumns,
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				// Version columns are newer, so update.
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				// Version columns are older, don't update.
				{pk0: 1, pk1: "B", ver0: 1, ver1: 1, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 2, pk1: "C", ver0: 0, ver1: 0, hasDefault: "inserted", ts: freshTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 1, ver1: 3, hasDefault: "updated", ts: freshTimestamp},
				initialRows[1],
				{pk0: 2, pk1: "C", ver0: 0, ver1: 0, hasDefault: "inserted", ts: freshTimestamp},
			},
		},
		{
			name: "casDeadlineNoRowsUpdated",
			cfg: &applycfg.Config{
				CASColumns: casColumns,
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				// Version columns are equal, don't update.
				{pk0: 0, pk1: "A", ver0: 1, ver1: 2, hasDefault: "updated", ts: freshTimestamp},
				// Version columns are older, don't update.
				{pk0: 1, pk1: "B", ver0: 1, ver1: 1, hasDefault: "updated", ts: freshTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				initialRows[0],
				initialRows[1],
			},
		},
		{
			name: "deadlineBothRowsUpdated",
			cfg: &applycfg.Config{
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				// Older version columns than initial rows, but only deadline filter matters here.
				{pk0: 0, pk1: "A", ver0: 0, ver1: 0, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 1, ver1: 1, hasDefault: "updated", ts: freshTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 0, ver1: 0, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 1, ver1: 1, hasDefault: "updated", ts: freshTimestamp},
			},
		},
		{
			name: "deadlineOneRowUpdated",
			cfg: &applycfg.Config{
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 0, ver1: 0, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "updated", ts: staleTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 0, ver1: 0, hasDefault: "updated", ts: freshTimestamp},
				initialRows[1], // Second row was stale, so it didn't upsert.
			},
		},
		{
			name: "deadlineOneRowUpdatedOneRowInserted",
			cfg: &applycfg.Config{
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 0, ver1: 0, hasDefault: "updated", ts: freshTimestamp},
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "updated", ts: staleTimestamp},
				{pk0: 2, pk1: "C", ver0: 0, ver1: 0, hasDefault: "inserted", ts: freshTimestamp},
				{pk0: 3, pk1: "D", ver0: 0, ver1: 0, hasDefault: "inserted", ts: staleTimestamp}, // Stale, shouldn't insert.
			},
			expectedRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 0, ver1: 0, hasDefault: "updated", ts: freshTimestamp},
				initialRows[1], // Second row was stale, so it didn't upsert.
				{pk0: 2, pk1: "C", ver0: 0, ver1: 0, hasDefault: "inserted", ts: freshTimestamp},
			},
		},
		{
			name: "deadlineNoRowsUpdated",
			cfg: &applycfg.Config{
				Deadlines: ident.MapOf[time.Duration](
					ident.New("ts"), deadlineInterval,
				),
			},
			upsertRows: []conditionalUpsertTestRow{
				{pk0: 0, pk1: "A", ver0: 0, ver1: 0, hasDefault: "updated", ts: staleTimestamp},
				{pk0: 1, pk1: "B", ver0: 3, ver1: 1, hasDefault: "updated", ts: staleTimestamp},
			},
			expectedRows: []conditionalUpsertTestRow{
				initialRows[0], // Both rows were stale, so no rows upserted.
				initialRows[1],
			},
		},
	}
}

// Convert rows to upsert for the test case into a slice of placeholder args to substitute into the SQL query.
func upsertRowsToPlaceholderArgs(upsertRows []conditionalUpsertTestRow) []any {
	args := []any{}
	for _, row := range upsertRows {
		hasNonDefaultValue := row.hasDefault != ""
		hasNonDefaultValueBit := "0"
		if hasNonDefaultValue {
			hasNonDefaultValueBit = "1"
		}

		args = append(args, row.pk0, row.pk1, row.ver0, row.ver1, hasNonDefaultValueBit, row.hasDefault, row.ts)
	}
	return args
}

// Insert the initial rows into the newly-created testing table for the current test case.
func insertInitialRows(
	r *require.Assertions,
	ctx *stopper.Context,
	fixture *base.Fixture,
	tableName ident.Table,
	initialRows []conditionalUpsertTestRow,
) {
	insertQuery := "INSERT INTO %s VALUES (%d, '%s', %d, %d, '%s', now())"
	for _, row := range initialRows {
		_, err := fixture.TargetPool.ExecContext(ctx, fmt.Sprintf(insertQuery, tableName,
			row.pk0, row.pk1, row.ver0, row.ver1, row.hasDefault))
		r.NoError(err)
	}
}

// Based on the column definitions for the test schema and the number of rows to conditionally upsert,
// instantiate a conditional upsert SQL statement string with placeholders for the row data to upsert.
// See conditional.tmpl or condtional_compat.tmpl for the template being instantiated.
func instantiateConditionalUpsertTemplate(
	r *require.Assertions,
	cols []types.ColData,
	tc *conditionalUpsertTestCase,
	tableID ident.Table,
	fixture *base.Fixture,
) string {
	cfg := applycfg.NewConfig()
	if tc.cfg != nil {
		cfg.Patch(tc.cfg)
	}

	product := fixture.TargetPool.Product
	version := fixture.TargetPool.Version

	var hint string
	mapping, err := newColumnMapping(cfg, cols, product, version, ident.WithHint(tableID, hint))
	r.NoError(err)

	tmpls, err := newTemplates(mapping)
	r.NoError(err)

	upsertSQL, err := tmpls.upsertExpr(len(tc.upsertRows), applyConditional)
	r.NoError(err)

	return upsertSQL
}

// Check that the resulting rows after executing the conditional upsert SQL statement are
// equal to the expected result rows for the current test case.
func confirmRowsAfterUpsert(
	ctx *stopper.Context,
	a *assert.Assertions,
	r *require.Assertions,
	fixture *base.Fixture,
	tableName ident.Table,
	expectedRows []conditionalUpsertTestRow,
) {
	// Read rows from the target table in ascending PK order.
	rows, err := fixture.TargetPool.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s ORDER BY \"pk0\", \"pk1\"", tableName))
	r.NoError(err)

	i := 0
	for ; rows.Next(); i++ {
		a.Lessf(i, len(expectedRows), "Expected only %d rows in %s after upserting, but found more.",
			len(expectedRows), tableName)

		// Create an empty slice to hold the column values for the currently read row.
		columns, err := rows.Columns()
		r.NoError(err)
		actualRow := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range actualRow {
			valuePtrs[i] = &actualRow[i]
		}

		// Read the column values for the row into 'actualRow'.
		err = rows.Scan(valuePtrs...)
		r.NoError(err)

		// Convert each []uint8 column value to a string, int64 to int,
		// so column value comparisons work.
		for i, val := range actualRow {
			if byteSlice, ok := val.([]uint8); ok {
				actualRow[i] = string(byteSlice)
			} else if int64Bit, ok := val.(int64); ok {
				actualRow[i] = int(int64Bit)
			}
		}

		// Compare column values for the expected result row vs. actual.
		expectedRow := expectedRows[i]
		a.Truef(
			expectedRowEqualsActual(expectedRow, actualRow),
			"Expected row does not match actual row after a conditional upsert:\nexpected: %v,\nactual: %v",
			expectedRow,
			actualRow,
		)
	}
	a.Equalf(i, len(expectedRows), "Expected %d rows in %s after upserting, but found %d",
		len(expectedRows), tableName, len(expectedRows))
}

// Returns true if an expected upsert row for a test case matches the actual row
// read from the target table.
func expectedRowEqualsActual(expectedRow conditionalUpsertTestRow, actualRow []any) bool {
	// Skip ts comparison; we just need to know whether the other columns updated or not.
	return expectedRow.pk0 == actualRow[0] &&
		expectedRow.pk1 == actualRow[1] &&
		expectedRow.ver0 == actualRow[2] &&
		expectedRow.ver1 == actualRow[3] &&
		expectedRow.hasDefault == actualRow[4]
}
