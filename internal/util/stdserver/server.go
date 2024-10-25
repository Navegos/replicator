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

// Package stdserver contains a generic HTTP server that
// can be used by sources that receive http requests.
package stdserver

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/cockroachdb/replicator/internal/util/stdlogical"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

const healthCheckPath = "/_/healthz"

// errHealthCheckTimeout is a causal error used by [Mux].
var errHealthCheckTimeout = errors.New("health check timed out")

// A Server receives incoming messages and
// applies them to a target cluster.
type Server struct {
	auth     types.Authenticator
	diags    *diag.Diagnostics
	listener net.Listener
	mux      *http.ServeMux
}

var (
	_ stdlogical.HasAuthenticator = (*Server)(nil)
	_ stdlogical.HasDiagnostics   = (*Server)(nil)
	_ stdlogical.HasServeMux      = (*Server)(nil)
)

// GetAuthenticator implements [stdlogical.HasAuthenticator].
func (s *Server) GetAuthenticator() types.Authenticator {
	return s.auth
}

// GetDiagnostics implements [stdlogical.HasDiagnostics].
func (s *Server) GetDiagnostics() *diag.Diagnostics {
	return s.diags
}

// GetListener returns the network listener for the HTTP server.
func (s *Server) GetListener() net.Listener {
	return s.listener
}

// GetServeMux implements [stdlogical.HasServeMux].
func (s *Server) GetServeMux() *http.ServeMux {
	return s.mux
}

// New constructs the top-level network server.
// The server will execute the server on a background
// goroutine and will gracefully drain the server when the cancel
// function is called.
func New(
	ctx *stopper.Context,
	auth types.Authenticator,
	diags *diag.Diagnostics,
	listener net.Listener,
	mux *http.ServeMux,
	tlsConfig *tls.Config,
) *Server {
	srv := &http.Server{
		Handler:   h2c.NewHandler(mux, &http2.Server{}),
		TLSConfig: tlsConfig,
	}

	ctx.Go(func(ctx *stopper.Context) error {
		var err error
		if srv.TLSConfig != nil {
			err = srv.ServeTLS(listener, "", "")
		} else {
			err = srv.Serve(listener)
		}
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return errors.Wrap(err, "unable to serve requests")
	})
	ctx.Go(func(ctx *stopper.Context) error {
		<-ctx.Stopping()
		if err := srv.Shutdown(ctx); err != nil {
			log.WithError(err).Error("did not shut down cleanly")
		} else {
			log.Info("Server shutdown complete")
		}
		return nil
	})

	return &Server{auth, diags, listener, mux}
}

// Mux constructs the http.ServeMux that routes requests.
func Mux(
	cfg *Config, handler http.Handler, stagingPool *types.StagingPool, targetPool *types.TargetPool,
) *http.ServeMux {
	mux := &http.ServeMux{}
	mux.HandleFunc(healthCheckPath, func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeoutCause(context.Background(),
			cfg.HealthCheckTimeout, errHealthCheckTimeout)
		defer cancel()

		setError := func(err error, name string) {
			if errors.Is(context.Cause(ctx), errHealthCheckTimeout) {
				msg := fmt.Sprintf("health check for %s pool timed out", name)
				log.Warn(msg)
				http.Error(w, msg, http.StatusRequestTimeout)
				return
			}
			msg := fmt.Sprintf("health check fail for %s pool: %v", name, err)
			log.WithError(err).Warn(msg)
			http.Error(w, msg, http.StatusInternalServerError)
		}

		if err := stagingPool.Ping(ctx); err != nil {
			setError(err, "staging")
			return
		}
		if err := targetPool.PingContext(ctx); err != nil {
			setError(err, "target")
			return
		}
		http.Error(w, "OK", http.StatusOK)
	})
	mux.Handle("/", logWrapper(handler))
	return mux
}
