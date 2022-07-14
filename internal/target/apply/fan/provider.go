// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fan

import "github.com/google/wire"

// Set is used by Wire.
var Set = wire.NewSet(
	wire.Struct(new(Fans), "*"),
)