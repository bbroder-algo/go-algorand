// Copyright (C) 2017-2023 Algorand, Inc.

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
	"fmt"
	"github.com/algorand/go-algorand/crypto"
	"github.com/cockroachdb/pebble"
)

type BranchNode struct {
	children  [16]node
	valueHash crypto.Digest
	key       nibbles
	hash      *crypto.Digest
}

func makeBranchNode(children [16]node, valueHash crypto.Digest, key nibbles) *BranchNode {
	stats.makebranches++
	bn := &BranchNode{children: children, valueHash: valueHash, key: make(nibbles, len(key))}
	copy(bn.key, key)
	return bn
}
func (bn *BranchNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	if len(remainingKey) == 0 {
		// If we're here, then set the value hash in this node, overwriting the old one.
		bn.valueHash = valueHash
		// transition BN.1
		bn.hash = nil
		return bn, nil
	}

	// Otherwise, shift out the first nibble and check the children for it.
	shifted := shiftNibbles(remainingKey, 1)
	slot := remainingKey[0]
	if bn.children[slot] == nil {
		// nil children are available.
		lnKey := pathKey[:]
		lnKey = append(lnKey, slot)

		// transition BN.2
		bn.children[slot] = makeLeafNode(shifted, valueHash, lnKey)
		mt.addNode(bn.children[slot])
	} else {
		// Not available.  Descend down the branch.
		replacement, err := bn.children[slot].descendAdd(mt, append(pathKey, remainingKey[0]), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		bn.hash = nil
		// transition BN.3
		bn.children[slot] = replacement
	}

	return bn, nil
}
func (bn *BranchNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	if len(remainingKey) == 0 {
		if (bn.valueHash == crypto.Digest{}) {
			// valueHash is empty -- key not found.
			return bn, false, nil
		}
		// delete this branch's value hash. reset the value to the empty hash.
		// update the branch node if there are children, or remove it completely.
		bn.valueHash = crypto.Digest{}
		return bn, true, nil
	}
	// descend into the branch node.
	if bn.children[remainingKey[0]] == nil {
		// no child at this index.  key not found.
		return bn, false, nil
	}
	shifted := shiftNibbles(remainingKey, 1)
	lnKey := pathKey[:]
	lnKey = append(lnKey, remainingKey[0])
	replacementChild, found, err := bn.children[remainingKey[0]].descendDelete(mt, lnKey, shifted)
	if found && err == nil {
		bn.children[remainingKey[0]] = replacementChild
	}
	return bn, found, err
}
func (bn *BranchNode) descendHashWithCommit(b *pebble.Batch) error {
	if bn.hash == nil {
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil && bn.children[i].getHash() == nil {
				err := bn.children[i].descendHashWithCommit(b)
				if err != nil {
					return err
				}
			}
		}
		bytes, err := bn.serialize()
		if err == nil {
			stats.cryptohashes++
			bn.hash = new(crypto.Digest)
			*bn.hash = crypto.Hash(bytes)
		}
		options := &pebble.WriteOptions{}
		stats.dbsets++
		if debugTrie {
			fmt.Printf("db.set bn key %x dbkey %v\n", bn.getKey(), bn)
		}
		if b != nil {
			return b.Set(bn.getDBKey(), bytes, options)
		}
	}
	return nil
}

func (bn *BranchNode) descendHash() error {
	return bn.descendHashWithCommit(nil)
}
func deserializeBranchNode(data []byte, key nibbles) (*BranchNode, error) {
	if data[0] != 5 {
		return nil, errors.New("invalid prefix for branch node")
	}
	if len(data) < 545 {
		return nil, errors.New("data too short to be a branch node")
	}

	var children [16]node
	for i := 0; i < 16; i++ {
		hash := new(crypto.Digest)
		copy(hash[:], data[1+i*32:33+i*32])
		if *hash != (crypto.Digest{}) {
			chKey := key[:]
			chKey = append(chKey, byte(i))
			children[i] = makeDBNode(hash, chKey)
		}
	}
	value_hash := crypto.Digest(data[513:545])
	return makeBranchNode(children, value_hash, key), nil
}
func (bn *BranchNode) serialize() ([]byte, error) {
	data := make([]byte, 545)
	data[0] = 5

	for i := 0; i < 16; i++ {
		if bn.children[i] != nil {
			copy(data[1+i*32:33+i*32], bn.children[i].getHash()[:])
		}
	}
	copy(data[513:545], bn.valueHash[:])
	return data, nil
}
func (bn *BranchNode) evict(eviction func(node) bool) {
	if eviction(bn) {
		if debugTrie {
			fmt.Printf("evicting branch node %x, (%v)\n", bn.getKey(), bn)
		}
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil && bn.children[i].getHash() != nil {
				bn.children[i] = makeDBNode(bn.children[i].getHash(), bn.children[i].getKey())
				stats.evictions++
			}
		}
	} else {
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil {
				bn.children[i].evict(eviction)
			}
		}
	}
}
func (bn *BranchNode) lambda(l func(node)) {
	l(bn)
	for i := 0; i < 16; i++ {
		if bn.children[i] != nil {
			bn.children[i].lambda(l)
		}
	}
}

func (bn *BranchNode) getKey() nibbles {
	return bn.key
}
func (bn *BranchNode) getHash() *crypto.Digest {
	return bn.hash
}
func (bn *BranchNode) getDBKey() dbKey {
	if bn.hash == nil || bn.key == nil {
		return nil
	}
	return makeDBKey(bn.key, bn.hash)
}
