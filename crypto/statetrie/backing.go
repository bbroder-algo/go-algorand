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
)

type backing interface {
	batchStart()
	batchEnd()
	get(key nibbles) (node, error)
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
	n, err := mt.store.get(pathKey)
	if err != nil {
		return nil, err
	}
	return n.add(mt, pathKey, remainingKey, valueHash)
}
func (ba *backingNode) delete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	n, err := mt.store.get(pathKey)
	if err != nil {
		return nil, false, err
	}
	return n.delete(mt, pathKey, remainingKey)
}
func (ba *backingNode) hashingCommit(store backing) error {
	return nil
}
func (ba *backingNode) hashing() error {
	return nil
}
func (ba *backingNode) evict(eviction func(node) bool) {
	return
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
func (ba *backingNode) copy() node {
	panic("backingNode cannot be copied")
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
func (mb *memoryBackstore) get(key nibbles) (node, error) {
	n, err := deserializeNode(mb.db[string(key)], key)
	if err != nil {
		fmt.Printf("\ndbKey panic: key %x\n", key)
		panic(err)
	}
	return n, nil
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
