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

package sinktest

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/cockroachdb/replicator/internal/util/stdpool"
)

// ChangefeedStatement is a struct that represents a changefeed statement and it
// allows the caller to specify various configuration options and parameters
// useful for creating changefeeds on CRDB sources.
type ChangefeedStatement struct {
	Diff                   bool
	Host                   string
	KeyInValue             bool
	QueryProjectionColumns []ident.Ident
	SourceVersion          string
	Tables                 []ident.Table
	Target                 ident.Table
	Token                  string
	Webhook                bool
}

// String returns a string representation of the changefeed statement.
func (cfs *ChangefeedStatement) String() string {
	params := make(url.Values)
	var feedURL url.URL
	var pathIdent ident.Identifier
	createStmt := "CREATE CHANGEFEED"

	if len(cfs.QueryProjectionColumns) > 0 {
		pathIdent = cfs.Target
	} else {
		// Creating the comma-separated table string required by the changefeed.
		tablesStr := ""
		for i, table := range cfs.Tables {
			if i > 0 {
				tablesStr += ", "
			}
			tablesStr += table.String()
		}
		pathIdent = cfs.Target.Schema()
		createStmt += fmt.Sprintf(" FOR TABLE %s", tablesStr)
	}

	if cfs.Webhook {
		params.Set("insecure_tls_skip_verify", "true")
		feedURL = url.URL{
			Scheme:   "webhook-https",
			Host:     cfs.Host,
			Path:     ident.Join(pathIdent, ident.Raw, '/'),
			RawQuery: params.Encode(),
		}
		createStmt += " INTO '" + feedURL.String() + "' " +
			" WITH updated," +
			"     resolved='1s'," +
			"     webhook_auth_header='Bearer " + cfs.Token + "'"
	} else {
		params.Set("access_token", cfs.Token)
		feedURL = url.URL{
			Scheme:   "experimental-http",
			Host:     cfs.Host,
			Path:     ident.Join(pathIdent, ident.Raw, '/'),
			RawQuery: params.Encode(),
		}
		createStmt += " INTO '" + feedURL.String() + "' " +
			"WITH updated, resolved='1s'"
	}

	if cfs.Diff {
		createStmt += ", diff"
	}

	if cfs.KeyInValue {
		createStmt += ", key_in_value"
	}

	if ok, err := SupportsMinCheckpointFrequency(cfs.SourceVersion); err == nil && ok {
		createStmt += ", min_checkpoint_frequency='1s'"
	}

	if len(cfs.QueryProjectionColumns) > 0 {
		createStmt += ",envelope='wrapped',format='json'"
		var columns []string
		for _, col := range cfs.QueryProjectionColumns {
			columns = append(columns, col.String())
		}
		createStmt += " AS SELECT " + strings.Join(columns, ", ")
		createStmt += fmt.Sprintf(" FROM %s", cfs.Tables[0].String())
	}

	return createStmt
}

// SupportsMinCheckpointFrequency checks if a certain version of CRDB supports the
// min_checkpoint_frequency option.
func SupportsMinCheckpointFrequency(version string) (bool, error) {
	return stdpool.CockroachMinVersion(version, "v22.1")
}
