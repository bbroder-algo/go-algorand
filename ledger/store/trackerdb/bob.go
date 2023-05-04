// Copyright (C) 2019-2023 Algorand, Inc.
// This file is part of go-algorand
//
// go-algorand is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// go-algorand is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with go-algorand.  If not, see <https://www.gnu.org/licenses/>.

package trackerdb

import (
	"github.com/algorand/go-algorand/crypto/bobtrie"
)

// TrieMemoryConfig is the memory configuration setup used for the merkle trie.
var BobTrieMemoryConfig = bobtrie.MemoryConfig{
	NodesCountPerPage:         BobCommitterNodesPerPage,
	CachedNodesCount:          BobTrieCachedNodesCount,
	PageFillFactor:            0.95,
	MaxChildrenPagesThreshold: 64,
}

// MerkleCommitterNodesPerPage controls how many nodes will be stored in a single page
// value was calibrated using BenchmarkCalibrateNodesPerPage
var BobCommitterNodesPerPage = int64(116)

// TrieCachedNodesCount defines how many balances trie nodes we would like to keep around in memory.
// value was calibrated using BenchmarkCalibrateCacheNodeSize
var BobTrieCachedNodesCount = 9000
