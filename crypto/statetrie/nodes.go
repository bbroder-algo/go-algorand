// Copyright (C) 2018-2023 Algorand, Inc.
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

package statetrie

import (
	"fmt"
	"github.com/algorand/go-algorand/crypto"
	"github.com/cockroachdb/pebble"
	"runtime/debug"
)

// Trie nodes

type dbKey []byte

type dbnode interface {
	getDBKey() dbKey         // nibble key
	getKey() nibbles         // the key of the node in the trie
	getHash() *crypto.Digest // the hash of the node, if it has been hashed
}

type node interface {
	dbnode

	descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error)
	descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error)
	descendHash() error
	descendHashWithCommit(b *pebble.Batch) error
	serialize() ([]byte, error)
	evict(func(node) bool)
	lambda(func(node))
}

// Node serializers / deserializers
//
// prefix: 0 == root.
//
//	 1 == extension, half.   2 == extension, full
//		3 == leaf, half.        4 == leaf, full
//		5 == branch

func deserializeNode(nbytes []byte, key nibbles) (node, error) {
	if len(nbytes) == 0 {
		debug.PrintStack()
		return nil, fmt.Errorf("empty node")
	}
	switch nbytes[0] {
	case 1, 2:
		return deserializeExtensionNode(nbytes, key)
	case 3, 4:
		return deserializeLeafNode(nbytes, key)
	case 5:
		return deserializeBranchNode(nbytes, key)
	default:
		return nil, fmt.Errorf("unknown node type")
	}
}

func makeDBKey(key nibbles, hash *crypto.Digest) dbKey {
	stats.makedbkey++
	return dbKey(key)
}
