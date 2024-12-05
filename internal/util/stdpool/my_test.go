// Copyright 2023 The Cockroach Authors
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

// Package stdpool creates standardized database connection pools.
package stdpool

import (
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetConnOptions verifies that connection
// options are correctly extracted from the URL and
// the TLS options.
func TestGetConnOptions(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)
	// CA Pool
	caPem, err := os.ReadFile("./testdata/ca.crt")
	r.NoError(err)
	pool, err := x509.SystemCertPool()
	r.NoError(err)
	ok := pool.AppendCertsFromPEM(caPem)
	r.True(ok)
	// Client certificate
	certPem, err := os.ReadFile("./testdata/test.crt")
	r.NoError(err)
	keyPem, err := os.ReadFile("./testdata/test.key")
	r.NoError(err)
	cert, err := tls.X509KeyPair(certPem, keyPem)
	r.NoError(err)
	tests := []struct {
		name          string
		option        *tls.Config
		tlsConfigName string
		url           string
		wantConn      string
		wantErr       error
	}{
		{
			name:     "insecure",
			url:      "mysql://test:test@localhost:3306/mysql",
			wantConn: "test:test@tcp(localhost:3306)/mysql?sql_mode=ansi",
		},
		{
			name: "skip-verify",
			option: &tls.Config{
				InsecureSkipVerify: true,
			},
			tlsConfigName: tlsConfigNames.newName("mysql_driver"),
			url:           "mysql://test:test@localhost:3306/mysql",
			wantConn:      "test:test@tcp(localhost:3306)/mysql?sql_mode=ansi&tls=",
		},
		{
			name: "full",
			option: &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      pool,
			},
			tlsConfigName: tlsConfigNames.newName("mysql_driver"),
			url:           "mysql://test:test@localhost:3306/mysql",
			wantConn:      "test:test@tcp(localhost:3306)/mysql?sql_mode=ansi&tls=",
		},
	}
	t.Parallel()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := url.Parse(tt.url)
			r.NoError(err)
			gotc, err := getConnString(parsed, tt.tlsConfigName, tt.option)
			if tt.wantErr != nil {
				a.Equal(tt.wantErr, err)
				return
			}
			r.NoError(err)
			a.Equal(tt.wantConn+tt.tlsConfigName, gotc)
		})
	}
}

func TestMySQLSemVer(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    string
		wantErr bool
	}{
		{
			name:    "Expected VERSION() returned value format",
			version: "8.0.13-standard",
			want:    "v8.0.13",
			wantErr: false,
		},
		{
			name:    "Numbers in suffix",
			version: "8.0.13-8572346.5123",
			want:    "v8.0.13",
			wantErr: false,
		},
		{
			name:    "Has prefix",
			version: "MySQL 8.0.13-log",
			want:    "v8.0.13",
			wantErr: false,
		},
		{
			name:    "Just semantic version",
			version: "8.0.13",
			want:    "v8.0.13",
			wantErr: false,
		},
		{
			name:    "semantic version with 'v' prefix",
			version: "v8.0.13",
			want:    "v8.0.13",
			wantErr: false,
		},
		{
			name:    "Only two version numbers",
			version: "8.0.-51234",
			want:    "",
			wantErr: true,
		},
		{
			name:    "Only one version number",
			version: "8..-51234",
			want:    "",
			wantErr: true,
		},
		{
			name:    "Characters in version numbers",
			version: "8.a1.2-51234",
			want:    "",
			wantErr: true,
		},
		{
			name:    "Empty",
			version: "",
			want:    "",
			wantErr: true,
		},
		{
			name:    "MariaDB",
			version: "10.4.7-MariaDB-debug",
			want:    "v10.4.7",
			wantErr: false,
		},
		{
			name:    "MariaDB double version",
			version: "10.2.1-MariaDB-10.2.1+foo~bar",
			want:    "v10.2.1",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MySQLSemVer(tt.version)
			hasError := err != nil
			if hasError != tt.wantErr {
				t.Errorf("MySQLSemVer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MySQLSemVer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMySQLMinVersion(t *testing.T) {
	type args struct {
		version    string
		minVersion string
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name:    "Higher patch version than min",
			args:    args{version: "8.0.23-log", minVersion: "8.0.3-standard"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "Lower patch version than min",
			args:    args{version: "8.0.3-standard", minVersion: "8.0.23-log"},
			want:    false,
			wantErr: false,
		},
		{
			name:    "Higher major and minor version than min",
			args:    args{version: "8.0.23-log", minVersion: "5.7.23-standard"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "Lower major and minor version than min",
			args:    args{version: "5.7.23-standard", minVersion: "8.0.23-log"},
			want:    false,
			wantErr: false,
		},
		{
			name:    "Higher major version than min double digit",
			args:    args{version: "10.0.23-log", minVersion: "5.7.23-standard"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "Same version, different build",
			args:    args{version: "8.0.23-log", minVersion: "8.0.23-standard"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "Same version, same build",
			args:    args{version: "8.0.23-log", minVersion: "8.0.23-log"},
			want:    true,
			wantErr: false,
		},
		{
			name:    "Invalid version",
			args:    args{version: "8.f0.c23-log", minVersion: "8.0.23-log"},
			want:    false,
			wantErr: true,
		},
		{
			name:    "Invalid minVersion",
			args:    args{version: "8.0.23-log", minVersion: "8.f0.c23-log"},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MySQLMinVersion(tt.args.version, tt.args.minVersion)
			hasError := err != nil
			if hasError != tt.wantErr {
				t.Errorf("MySQLMinVersion() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MySQLMinVersion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMySQLVersionSupportsCTEs(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    bool
		wantErr bool
	}{
		{
			name:    "Does support CTEs",
			version: "8.0.1",
			want:    true,
			wantErr: false,
		},
		{
			name:    "Does support CTEs higher minor",
			version: "8.0.15",
			want:    true,
			wantErr: false,
		},
		{
			name:    "Does support CTEs higher major",
			version: "8.1.13",
			want:    true,
			wantErr: false,
		},
		{
			name:    "Does not support CTEs",
			version: "8.0.0",
			want:    false,
			wantErr: false,
		},
		{
			name:    "Does not support CTEs 5.7",
			version: "5.7.2",
			want:    false,
			wantErr: false,
		},
		{
			name:    "Invalid version",
			version: "5.7g.2a",
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MySQLVersionSupportsCTEs(tt.version)
			hasError := err != nil
			if hasError != tt.wantErr {
				t.Errorf("MySQLSupportsCTEs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MySQLSupportsCTEs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMySQLVersionSupportsDefaultExpressions(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    bool
		wantErr bool
	}{
		{
			name:    "Does support default expressions",
			version: "8.0.13",
			want:    true,
			wantErr: false,
		},
		{
			name:    "Does support default expressions higher minor",
			version: "8.0.15",
			want:    true,
			wantErr: false,
		},
		{
			name:    "Does not support default expressions",
			version: "8.0.3",
			want:    false,
			wantErr: false,
		},
		{
			name:    "Does not support CTEs 5.7",
			version: "5.7.2",
			want:    false,
			wantErr: false,
		},
		{
			name:    "Invalid version",
			version: "5.7g.2a",
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MySQLVersionSupportsExpressionsForDefaultColVals(tt.version)
			hasError := err != nil
			if hasError != tt.wantErr {
				t.Errorf("MySQLSupportsDefaultExpressions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MySQLSupportsDefaultExpressions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMySQLVersionDefaultCollationIsLatin1(t *testing.T) {
	tests := []struct {
		name    string
		version string
		want    bool
		wantErr bool
	}{
		{
			name:    "Newer version has utf8mb4_0900_ai_ci as default",
			version: "8.0.1-standard",
			want:    false,
			wantErr: false,
		},
		{
			name:    "Older version has latin1_swedish_ci as default",
			version: "5.7.44-standard",
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MySQLVersionDefaultCollationIsLatin1(tt.version)
			hasError := err != nil
			if hasError != tt.wantErr {
				t.Errorf("MySQLVersionDefaultCollationIsLatin1() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MySQLVersionDefaultCollationIsLatin1() = %v, want %v", got, tt.want)
			}
		})
	}
}
