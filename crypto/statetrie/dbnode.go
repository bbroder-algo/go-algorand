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
	"errors"
	"github.com/algorand/go-algorand/crypto"
	"github.com/cockroachdb/pebble"
)

type DBNode struct {
	key  nibbles
	hash *crypto.Digest
}

func makeDBNode(hash *crypto.Digest, key nibbles) *DBNode {
	stats.makedbnodes++
	dbn := &DBNode{hash: hash, key: make(nibbles, len(key))}
	copy(dbn.key, key)
	return dbn
}

func (dbn *DBNode) lambda(l func(node)) {
	l(dbn)
}

func (dbn *DBNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	// this upgrades the node to a regular trie node by reading it in from the DB
	n, err := mt.getNode(dbn)
	if err != nil {
		return nil, err
	}
	return n.descendAdd(mt, pathKey, remainingKey, valueHash)
}

func (dbn *DBNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	n, err := mt.getNode(dbn)
	if err != nil {
		return nil, false, err
	}
	return n.descendDelete(mt, pathKey, remainingKey)
}
func (dbn *DBNode) evict(eviction func(node) bool) {
	return
}

func (dbn *DBNode) serialize() ([]byte, error) {
	return nil, errors.New("DBNode cannot be serialized")
}
func (dbn *DBNode) descendHashWithCommit(b *pebble.Batch) error {
	return nil // DBNodes are immutably hashed and already committed
}
func (dbn *DBNode) descendHash() error {
	return nil // DBNodes are immutably hashed
}
func (dbn *DBNode) getKey() nibbles {
	return dbn.key
}
func (dbn *DBNode) getHash() *crypto.Digest {
	return dbn.hash
}
func (dbn *DBNode) getDBKey() dbKey {
	if dbn.hash == nil || dbn.key == nil {
		return nil
	}
	return makeDBKey(dbn.key, dbn.hash)
}
