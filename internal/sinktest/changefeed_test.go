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
	"testing"

	"github.com/cockroachdb/replicator/internal/util/ident"
	"github.com/stretchr/testify/require"
)

// Note that the `sourceVersion` must contain the platform in the semver regex
// (i.e. "CockroachDB CCL v24.2.1 (platform)").
func TestCreateChangefeedStatement(t *testing.T) {
	tests := []struct {
		name string
		stmt ChangefeedStatement
		want string
	}{
		{
			name: "basic no changefeed configs",
			stmt: ChangefeedStatement{
				Host:          "localHost:8080",
				SourceVersion: "CockroachDB CCL v24.2.1 (platform)",
				Target: ident.NewTable(ident.MustSchema(ident.New("target"),
					ident.New("public")), ident.New("tbl1")),
				Token: "my_token",
				Tables: []ident.Table{ident.NewTable(ident.MustSchema(ident.New("source"),
					ident.New("public")), ident.New("tbl1"))},
			},
			want: `CREATE CHANGEFEED FOR TABLE "source"."public"."tbl1" INTO ` +
				`'experimental-http://localHost:8080/target/public?access_token=my_token' ` +
				`WITH updated, resolved='1s', min_checkpoint_frequency='1s'`,
		},
		{
			name: "basic webhook",
			stmt: ChangefeedStatement{
				Host:          "localHost:8080",
				SourceVersion: "CockroachDB CCL v24.2.1 (platform)",
				Target: ident.NewTable(ident.MustSchema(ident.New("target"), ident.New("public")),
					ident.New("tbl1")),
				Token: "my_token",
				Tables: []ident.Table{
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")),
						ident.New("tbl1")),
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")),
						ident.New("tbl2")),
				},
				Webhook: true,
			},
			want: `CREATE CHANGEFEED FOR TABLE "source"."public"."tbl1", "source"."public"."tbl2" ` +
				`INTO 'webhook-https://localHost:8080/target/public?insecure_tls_skip_verify=true'  ` +
				`WITH updated,     resolved='1s',     webhook_auth_header='Bearer my_token', ` +
				`min_checkpoint_frequency='1s'`,
		},
		{
			name: "webhook and diff and key_in_value enabled",
			stmt: ChangefeedStatement{
				Diff:          true,
				Host:          "localHost:8080",
				KeyInValue:    true,
				SourceVersion: "CockroachDB CCL v24.2.1 (platform)",
				Target: ident.NewTable(ident.MustSchema(ident.New("target"), ident.New("public")),
					ident.New("tbl1")),
				Token: "my_token",
				Tables: []ident.Table{
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")),
						ident.New("tbl1")),
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")),
						ident.New("tbl2")),
				},
				Webhook: true,
			},
			want: `CREATE CHANGEFEED FOR TABLE "source"."public"."tbl1", "source"."public"."tbl2" ` +
				`INTO 'webhook-https://localHost:8080/target/public?insecure_tls_skip_verify=true'  ` +
				`WITH updated,     resolved='1s',     webhook_auth_header='Bearer my_token', diff, key_in_value, ` +
				`min_checkpoint_frequency='1s'`,
		},
		{
			name: "basic webhook CDC queries",
			stmt: ChangefeedStatement{
				Host:                   "localHost:8080",
				QueryProjectionColumns: []ident.Ident{ident.New("pk"), ident.New("val")},
				SourceVersion:          "CockroachDB CCL v24.2.1 (platform)",
				Target: ident.NewTable(ident.MustSchema(ident.New("target"), ident.New("public")),
					ident.New("tbl1")),
				Token: "my_token",
				Tables: []ident.Table{
					ident.NewTable(ident.MustSchema(ident.New("source"), ident.New("public")),
						ident.New("tbl1")),
				},
				Webhook: true,
			},
			want: `CREATE CHANGEFEED INTO ` +
				`'webhook-https://localHost:8080/target/public/tbl1?insecure_tls_skip_verify=true'  ` +
				`WITH updated,     resolved='1s',     webhook_auth_header='Bearer my_token', ` +
				`min_checkpoint_frequency='1s',envelope='wrapped',format='json' AS SELECT "pk", "val" ` +
				`FROM "source"."public"."tbl1"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.stmt.String()
			require.Equal(t, tt.want, got)
		})
	}
}
