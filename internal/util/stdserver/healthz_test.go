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

package stdserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/stretchr/testify/require"
)

func TestHealthzTimeout(t *testing.T) {
	r := require.New(t)

	fixture, err := base.NewFixture(t)
	r.NoError(err)
	ctx := fixture.Context

	cfg := &Config{
		HealthCheckTimeout: -1, // Cancel the context immediately.
	}

	mux := Mux(
		cfg,
		http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}),
		fixture.StagingPool,
		fixture.TargetPool,
	)

	req := httptest.NewRequestWithContext(ctx, "GET", healthCheckPath, nil /* body */)

	handler, _ := mux.Handler(req)
	r.NotNil(handler)

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	r.Equal(http.StatusRequestTimeout, w.Code)
	r.Contains(w.Body.String(), "timed out")

	// Break the staging pool and expect a closed-pool error.
	fixture.StagingPool.Close()
	// Allow context to function.
	cfg.HealthCheckTimeout = defaultHealthCheckTimeout

	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	r.Equal(http.StatusInternalServerError, w.Code)
	r.Contains(w.Body.String(), "closed pool")
}
