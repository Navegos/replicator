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

// Package script provides an API shim that integrates the userscript
// into the [sequencer.Sequencer] API.
package script

import (
	"github.com/cockroachdb/cdc-sink/internal/script"
	"github.com/cockroachdb/cdc-sink/internal/sequencer"
	"github.com/cockroachdb/cdc-sink/internal/types"
	"github.com/cockroachdb/cdc-sink/internal/util/notify"
	"github.com/cockroachdb/cdc-sink/internal/util/stopper"
)

// Sequencer injects the userscript shim into a [sequencer.Sequencer]
// stack.
type Sequencer struct {
	loader   *script.Loader
	watchers types.Watchers
}

var _ sequencer.Shim = (*Sequencer)(nil)

// Wrap implements [sequencer.Shim].
func (s *Sequencer) Wrap(
	_ *stopper.Context, delegate sequencer.Sequencer,
) (sequencer.Sequencer, error) {
	return &wrapper{s, delegate}, nil
}

type wrapper struct {
	*Sequencer
	delegate sequencer.Sequencer
}

var _ sequencer.Sequencer = (*wrapper)(nil)

// Start injects a userscript shim into the Sequencer stack.
func (w *wrapper) Start(
	ctx *stopper.Context, opts *sequencer.StartOptions,
) (types.MultiAcceptor, *notify.Var[sequencer.Stat], error) {
	// Loader is nil if no userscript has been configured.
	if w.loader == nil {
		return w.delegate.Start(ctx, opts)
	}

	schema, err := opts.Group.Schema()
	if err != nil {
		return nil, nil, err
	}

	scr, err := w.loader.Bind(ctx, schema, w.watchers)
	if err != nil {
		return nil, nil, err
	}

	// Only inject if the source or any tables have a configuration.
	_, inject := scr.Sources.Get(opts.Group.Name)
	if !inject {
		for _, tbl := range opts.Group.Tables {
			_, inject = scr.Targets.Get(tbl)
			if inject {
				break
			}
		}
	}
	if inject {
		opts = opts.Copy()
		opts.Delegate = types.OrderedAcceptorFrom(&acceptor{
			delegate:   opts.Delegate,
			group:      opts.Group,
			userScript: scr,
			watchers:   w.watchers,
		}, w.watchers)
	}
	return w.delegate.Start(ctx, opts)
}

// Unwrap is an informal protocol to return the delegate.
func (w *wrapper) Unwrap() sequencer.Sequencer {
	return w.delegate
}