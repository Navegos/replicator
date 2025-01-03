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

// Package all contains a test rig for all services.
package all

import (
	"context"
	"time"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/sinktest"
	"github.com/cockroachdb/replicator/internal/sinktest/base"
	"github.com/cockroachdb/replicator/internal/staging"
	"github.com/cockroachdb/replicator/internal/staging/stage"
	"github.com/cockroachdb/replicator/internal/target"
	"github.com/cockroachdb/replicator/internal/target/dlq"
	"github.com/cockroachdb/replicator/internal/target/schemawatch"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/cockroachdb/replicator/internal/util/diag"
	"github.com/google/wire"
)

// RefreshDelay is a named type for the schema watch refresh delay configuration.
type RefreshDelay time.Duration

// TestSet contains providers to create a self-contained Fixture.
var TestSet = wire.NewSet(
	base.TestSet,
	staging.Set,
	target.Set,

	ProvideDLQConfig,
	ProvideSchemaWatchConfig,
	ProvideStageConfig,
	ProvideWatcher,

	wire.Struct(new(Fixture), "*"),
)

// TestSetBase creates a Fixture from a [base.Fixture].
var TestSetBase = wire.NewSet(
	wire.FieldsOf(new(*base.Fixture),
		"Context", "SourcePool", "SourceSchema",
		"StagingPool", "StagingDB",
		"TargetCache", "TargetPool", "TargetSchema"),
	diag.New,
	staging.Set,
	target.Set,

	ProvideDLQConfig,
	ProvideSchemaWatchConfig,
	ProvideStageConfig,
	ProvideWatcher,

	wire.Bind(new(context.Context), new(*stopper.Context)),
	wire.Struct(new(Fixture), "*"),
)

// ProvideDLQConfig emits a default configuration.
func ProvideDLQConfig() (*dlq.Config, error) {
	cfg := &dlq.Config{}
	return cfg, cfg.Preflight()
}

// ProvideStageConfig emits a default configuration.
func ProvideStageConfig() (*stage.Config, error) {
	cfg := &stage.Config{}
	return cfg, cfg.Preflight()
}

// ProvideWatcher is called by Wire to construct a Watcher
// bound to the testing database.
func ProvideWatcher(target sinktest.TargetSchema, watchers types.Watchers) (types.Watcher, error) {
	return watchers.Get(target.Schema())
}

// ProvideSchemaWatchConfig is called by Wire to construct the SchemaWatch
// configuration.
func ProvideSchemaWatchConfig(refreshDelay RefreshDelay) (*schemawatch.Config, error) {
	cfg := &schemawatch.Config{
		RefreshDelay: time.Duration(refreshDelay),
	}
	return cfg, cfg.Preflight()
}
