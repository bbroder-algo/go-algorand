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

package statetrie

import (
	"github.com/algorand/go-algorand/crypto"
)

type backing interface {
	batchStart()
	batchEnd()
	get(key nibbles) node
	set(key nibbles, value []byte) error
	del(key nibbles) error
	close() error
}

type backingNode struct {
	key  nibbles
	hash *crypto.Digest
}

func makeBackingNode(hash *crypto.Digest, key nibbles) *backingNode {
	stats.makedbnodes++
	ba := &backingNode{hash: hash, key: make(nibbles, len(key))}
	copy(ba.key, key)
	return ba
}
func (ba *backingNode) add(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	return mt.store.get(pathKey).add(mt, pathKey, remainingKey, valueHash)
}
func (ba *backingNode) delete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	return mt.store.get(pathKey).delete(mt, pathKey, remainingKey)
}
func (ba *backingNode) hashingCommit(store backing) error {
	return nil
}
func (ba *backingNode) hashing() error {
	return nil
}
func (ba *backingNode) evict(eviction func(node) bool) {}
func (ba *backingNode) preload(store backing) node {
	return store.get(ba.key)
}
func (ba *backingNode) lambda(l func(node)) {
	l(ba)
}
func (ba *backingNode) getKey() nibbles {
	return ba.key
}
func (ba *backingNode) getHash() *crypto.Digest {
	return ba.hash
}

func (ba *backingNode) merge(mt *Trie) {
	panic("backingNode cannot be merged")
}
func (ba *backingNode) child() node {
	panic("backingNode cannot have children ")
}
func (ba *backingNode) serialize() ([]byte, error) {
	panic("backingNode cannot be serialized")
}

type memoryBackstore struct {
	db map[string][]byte
}

func makeMemoryBackstore() *memoryBackstore {
	return &memoryBackstore{db: make(map[string][]byte)}
}
func (mb *memoryBackstore) get(key nibbles) node {
	if v, ok := mb.db[string(key)]; ok {
		return deserializeNode(v, key)
	}
	return nil
}
func (mb *memoryBackstore) set(key nibbles, value []byte) error {
	mb.db[string(key)] = value
	return nil
}
func (mb *memoryBackstore) del(key nibbles) error {
	delete(mb.db, string(key))
	return nil
}
func (mb *memoryBackstore) batchStart() {}
func (mb *memoryBackstore) batchEnd()   {}
func (mb *memoryBackstore) close() error {
	mb.db = make(map[string][]byte)
	return nil
}
