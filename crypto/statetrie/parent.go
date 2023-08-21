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
	"github.com/algorand/go-algorand/crypto"
)

type parent struct {
	p node
}

func makeParent(p node) *parent {
	stats.makepanodes++
	pa := &parent{p: p}
	return pa
}

func (pa *parent) add(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	return pa.p.child().add(mt, pathKey, remainingKey, valueHash)
}
func (pa *parent) delete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	return pa.p.child().delete(mt, pathKey, remainingKey)
}
func (pa *parent) hashingCommit(store backing) error {
	return pa.p.hashingCommit(store)
}
func (pa *parent) hashing() error {
	return pa.p.hashing()
}
func (pa *parent) evict(eviction func(node) bool) {}
func (pa *parent) preload(store backing) node {
	return pa.p
}
func (pa *parent) lambda(l func(node)) {
	l(pa)
}
func (pa *parent) getKey() nibbles {
	return pa.p.getKey()
}
func (pa *parent) getHash() *crypto.Digest {
	return pa.p.getHash()
}

func (pa *parent) merge(mt *Trie) {
	panic("parent cannot be merged")
}
func (pa *parent) child() node {
	panic("parent cannot be copied")
}
func (pa *parent) serialize() ([]byte, error) {
	panic("parent cannot be serialized")
}
