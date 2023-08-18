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

package bobtrie2

import (
	"errors"
	"fmt"
	"github.com/algorand/go-algorand/crypto"
	"github.com/cockroachdb/pebble"
)

type RootNode struct {
	child node
	key   nibbles
	hash  *crypto.Digest
}

func makeRootNode(child node) *RootNode {
	stats.makeroots++
	return &RootNode{child: child, key: nibbles{}}
}
func (rn *RootNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	var err error
	if rn.child == nil {
		// Root node with a blank crypto digest in the child.  Make a leaf node.
		rn.child = makeLeafNode(remainingKey, valueHash, pathKey)
		mt.addNode(rn.child)
		stats.newrootnode++
		rn.hash = nil
		return rn, nil
	}
	replacementChild, err := rn.child.descendAdd(mt, pathKey, remainingKey, valueHash)
	if err != nil {
		return nil, err
	}
	rn.child = replacementChild
	stats.newrootnode++
	rn.hash = nil
	return rn, nil
}
func (rn *RootNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	replacementChild, found, err := rn.child.descendDelete(mt, pathKey, remainingKey)
	if err != nil {
		return nil, false, err
	}
	if found {
		mt.addNode(replacementChild)
		return makeRootNode(replacementChild), true, nil
	}
	return nil, false, err
}

func (rn *RootNode) descendHashWithCommit(b *pebble.Batch) error {
	if rn.hash == nil {
		if rn.child != nil && rn.child.getHash() == nil {
			err := rn.child.descendHashWithCommit(b)
			if err != nil {
				return err
			}
		}
		bytes, err := rn.serialize()
		if err == nil {
			stats.cryptohashes++
			rn.hash = new(crypto.Digest)
			*rn.hash = crypto.Hash(bytes)
		}
		options := &pebble.WriteOptions{}
		stats.dbsets++
		if debugTrie {
			fmt.Printf("db.set rn key %x dbkey %x\n", rn.getKey(), rn.getDBKey())
		}
		if b != nil {
			return b.Set(rn.getDBKey(), bytes, options)
		}
	}
	return nil
}
func (rn *RootNode) descendHash() error {
	return rn.descendHashWithCommit(nil)
}
func deserializeRootNode(data []byte) (*RootNode, error) {
	if data[0] != 0 {
		return nil, errors.New("invalid prefix for root node")
	}

	ch := new(crypto.Digest)
	copy(ch[:], data[1:33])
	var child node
	if *ch != (crypto.Digest{}) {
		child = makeDBNode(ch, []byte{0x01})
	}
	return makeRootNode(child), nil
}
func (rn *RootNode) serialize() ([]byte, error) {
	data := make([]byte, 33)
	data[0] = 0
	if rn.child != nil {
		copy(data[1:33], rn.child.getHash()[:])
	}
	return data, nil
}
func (rn *RootNode) getKey() nibbles {
	return rn.key
}
func (rn *RootNode) getHash() *crypto.Digest {
	return rn.hash
}
func (rn *RootNode) getDBKey() dbKey {
	if rn.hash == nil || rn.key == nil {
		return nil
	}
	return makeDBKey(rn.key, rn.hash)
}
