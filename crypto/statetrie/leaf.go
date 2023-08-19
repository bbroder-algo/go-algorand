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
	"fmt"
	"github.com/algorand/go-algorand/crypto"
	"github.com/cockroachdb/pebble"
)

type LeafNode struct {
	keyEnd    nibbles
	valueHash crypto.Digest
	key       nibbles
	hash      *crypto.Digest
}

func makeLeafNode(keyEnd nibbles, valueHash crypto.Digest, key nibbles) *LeafNode {
	stats.makeleaves++
	ln := &LeafNode{keyEnd: keyEnd, valueHash: valueHash, key: make(nibbles, len(key))}
	copy(ln.key, key)
	return ln
}
func (ln *LeafNode) lambda(l func(node)) {
	l(ln)
}

func (ln *LeafNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	if equalNibbles(ln.keyEnd, remainingKey) {
		// The two keys are the same. Replace the value.
		// transition LN.1
		ln.valueHash = valueHash
		ln.hash = nil
		return ln, nil
	}

	// Calculate the shared nibbles between the leaf node we're on and the key we're inserting.
	shNibbles := sharedNibbles(ln.keyEnd, remainingKey)
	// Shift away the common nibbles from both the keys.
	shiftedLn1 := shiftNibbles(ln.keyEnd, len(shNibbles))
	shiftedLn2 := shiftNibbles(remainingKey, len(shNibbles))

	// Make a branch node.
	var children [16]node
	branchHash := crypto.Digest{}

	// If the existing leaf node has no more nibbles, then store it in the branch node's value slot.
	if len(shiftedLn1) == 0 {
		// transition LN.2
		branchHash = ln.valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
		// in the branch node.
		key1 := append(append(pathKey, shNibbles...), shiftedLn1[0])
		ln1 := makeLeafNode(shiftNibbles(shiftedLn1, 1), ln.valueHash, key1)
		mt.addNode(ln1)
		// transition LN.3
		children[shiftedLn1[0]] = ln1
	}

	// Similarly, for our new insertion, if it has no more nibbles, store it in the branch node's value slot.
	if len(shiftedLn2) == 0 {
		if len(shiftedLn1) == 0 {
			// They can't both be empty, otherwise they would have been caught earlier in the equalNibbles check.
			return nil, fmt.Errorf("both keys are the same but somehow wasn't caught earlier")
		}
		// transition LN.4
		branchHash = valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
		// in the branch node.
		key2 := pathKey[:]
		key2 = append(key2, shNibbles...)
		key2 = append(key2, shiftedLn2[0])
		ln2 := makeLeafNode(shiftNibbles(shiftedLn2, 1), valueHash, key2)
		mt.addNode(ln2)
		// transition LN.5
		children[shiftedLn2[0]] = ln2
	}
	bn2key := pathKey[:]
	bn2key = append(bn2key, shNibbles...)
	bn2 := makeBranchNode(children, branchHash, bn2key)
	mt.addNode(bn2)

	if len(shNibbles) >= 2 {
		// If there was more than one shared nibble, insert an extension node before the branch node.
		//		mt.addNode(bn2)
		enKey := pathKey[:]
		en := makeExtensionNode(shNibbles, bn2, enKey)
		mt.addNode(en)
		// transition LN.6
		return en, nil
	}
	if len(shNibbles) == 1 {
		// If there is only one shared nibble, we just make a second branch node as opposed to an
		// extension node with only one shared nibble, the chances are high that we'd have to just
		// delete that node and replace it with a full branch node soon anyway.
		//		mt.addNode(bn2)
		var children2 [16]node
		children2[shNibbles[0]] = bn2
		bnKey := pathKey[:]
		bn3 := makeBranchNode(children2, crypto.Digest{}, bnKey)
		mt.addNode(bn3)
		// transition LN.7
		return bn3, nil
	}
	// There are no shared nibbles anymore, so just return the branch node.

	// transition LN.8
	return bn2, nil
}
func (ln *LeafNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	if equalNibbles(ln.keyEnd, remainingKey) {
		// The two keys are the same. Delete the value.
		mt.delNode(ln)
		return nil, true, nil
	}
	return ln, false, nil
}

func (ln *LeafNode) descendHashWithCommit(b *pebble.Batch) error {
	if ln.hash == nil {
		bytes, err := ln.serialize()
		if err == nil {
			stats.cryptohashes++
			ln.hash = new(crypto.Digest)
			*ln.hash = crypto.Hash(bytes)
		}
		options := &pebble.WriteOptions{}
		stats.dbsets++
		if debugTrie {
			fmt.Printf("db.set ln key %x : (%v)\n", ln.getKey(), ln)
		}
		if b != nil {
			return b.Set(ln.getDBKey(), bytes, options)
		}
	}
	return nil
}
func (ln *LeafNode) descendHash() error {
	return ln.descendHashWithCommit(nil)
}
func deserializeLeafNode(data []byte, key nibbles) (*LeafNode, error) {
	if data[0] != 3 && data[0] != 4 {
		return nil, errors.New("invalid prefix for leaf node")
	}
	if len(data) < 33 {
		return nil, errors.New("data too short to be a leaf node")
	}

	keyEnd, err := unpack(data[33:], data[0] == 3)
	if err != nil {
		return nil, err
	}
	lnKey := key[:]
	return makeLeafNode(keyEnd, crypto.Digest(data[1:33]), lnKey), nil
}
func (ln *LeafNode) serialize() ([]byte, error) {
	pack, half, err := ln.keyEnd.pack()
	if err != nil {
		return nil, err
	}
	data := make([]byte, 33+len(pack))
	if half {
		data[0] = 3
	} else {
		data[0] = 4
	}
	copy(data[1:33], ln.valueHash[:])
	copy(data[33:], pack)
	return data, nil
}
func (ln *LeafNode) evict(eviction func(node) bool) {
	return
}
func (ln *LeafNode) getKey() nibbles {
	return ln.key
}
func (ln *LeafNode) getHash() *crypto.Digest {
	return ln.hash
}
func (ln *LeafNode) getDBKey() dbKey {
	if ln.hash == nil || ln.key == nil {
		return nil
	}
	return makeDBKey(ln.key, ln.hash)
}