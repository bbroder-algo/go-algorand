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
	"github.com/algorand/go-algorand/crypto/statetrie/nibbles"
	"sync"
)

type backing interface {
	batchStart()
	batchEnd()
	get(key nibbles.Nibbles) node
	set(key nibbles.Nibbles, value []byte) error
	del(key nibbles.Nibbles) error
	isTrusted() bool
	close() error
}

type backingNode struct {
	key  nibbles.Nibbles
	hash crypto.Digest
}

var backingNodePool = sync.Pool{
	New: func() interface{} {
		return &backingNode{
			key: make(nibbles.Nibbles, 0),
		}
	},
}

func makeBackingNode(hash crypto.Digest, key nibbles.Nibbles) *backingNode {
	stats.makebanodes++
	ba := backingNodePool.Get().(*backingNode)
	ba.hash = hash
	ba.key = append(ba.key[:0], key...)
	return ba
}
func (ba *backingNode) setHash(hash crypto.Digest) {
	ba.hash = hash
}
func (ba *backingNode) get(store backing) node {
	n := store.get(ba.key)
	if !store.isTrusted() {
		if !verify(n, ba.hash) {
			panic("hash mismatch")
		}
	}

	n.setHash(ba.hash)
	return n
}
func (ba *backingNode) add(mt *Trie, pathKey nibbles.Nibbles, remainingKey nibbles.Nibbles, valueHash crypto.Digest) (node, error) {
	n, err := ba.get(mt.store).add(mt, pathKey, remainingKey, valueHash)
	if err != nil {
		return nil, err
	}
	backingNodePool.Put(ba)
	return n, nil
}
func (ba *backingNode) delete(mt *Trie, pathKey nibbles.Nibbles, remainingKey nibbles.Nibbles) (node, bool, error) {
	n, found, err := ba.get(mt.store).delete(mt, pathKey, remainingKey)
	if err != nil {
		return nil, false, err
	}
	if found {
		backingNodePool.Put(ba)
	}

	return n, found, nil
}
func (ba *backingNode) raise(mt *Trie, prefix nibbles.Nibbles, key nibbles.Nibbles) node {
	n := ba.get(mt.store).raise(mt, prefix, key)
	backingNodePool.Put(ba)
	return n
}
func (ba *backingNode) hashingCommit(store backing, e Eviction) error {
	return nil
}
func (ba *backingNode) hashing() error {
	return nil
}
func (ba *backingNode) preload(store backing, length int) node {
	if len(ba.key) <= length {
		n := ba.get(store).preload(store, length)
		backingNodePool.Put(ba)
		return n
	}
	return ba
}
func (ba *backingNode) lambda(l func(node), store backing) {
	if store != nil {
		ba.get(store).lambda(l, store)
		return
	}
	l(ba)
}
func (ba *backingNode) getKey() nibbles.Nibbles {
	return ba.key
}

//	func (ba *backingNode) getHash() crypto.Digest {
//		return ba.hash
func (ba *backingNode) getHash() *crypto.Digest {
	return &ba.hash
}
func (ba *backingNode) merge(mt *Trie) {
	panic("backingNode cannot be merged")
}
func (ba *backingNode) child() node {
	return makeBackingNode(ba.hash, ba.key)
}
func (ba *backingNode) serialize() ([]byte, error) {
	panic("backingNode cannot be serialized")
}

type memoryBackstore struct {
	db map[string][]byte
}

func (mb *memoryBackstore) isTrusted() bool {
	return true
}

func makeMemoryBackstore() *memoryBackstore {
	return &memoryBackstore{db: make(map[string][]byte)}
}
func (mb *memoryBackstore) get(key nibbles.Nibbles) node {
	if v, ok := mb.db[string(key)]; ok {
		return deserializeNode(v, key)
	}
	return nil
}
func (mb *memoryBackstore) set(key nibbles.Nibbles, value []byte) error {
	mb.db[string(key)] = value
	return nil
}
func (mb *memoryBackstore) del(key nibbles.Nibbles) error {
	delete(mb.db, string(key))
	return nil
}
func (mb *memoryBackstore) batchStart() {}
func (mb *memoryBackstore) batchEnd()   {}
func (mb *memoryBackstore) close() error {
	mb.db = make(map[string][]byte)
	return nil
}

type nullBackstore struct {
}

func makeNullBackstore() *nullBackstore {
	return &nullBackstore{}
}
func (nb *nullBackstore) get(key nibbles.Nibbles) node {
	return nil
}
func (nb *nullBackstore) set(key nibbles.Nibbles, value []byte) error {
	return nil
}
func (nb *nullBackstore) del(key nibbles.Nibbles) error {
	return nil
}
func (nb *nullBackstore) batchStart() {}
func (nb *nullBackstore) batchEnd()   {}
func (nb *nullBackstore) close() error {
	return nil
}
func (nb *nullBackstore) isTrusted() bool {
	return true
}
