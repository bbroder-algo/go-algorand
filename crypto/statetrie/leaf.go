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
	"bytes"
	"fmt"
	"github.com/algorand/go-algorand/crypto"
)

type leafNode struct {
	keyEnd    nibbles
	valueHash crypto.Digest
	key       nibbles
	hash      crypto.Digest
}

//
//**Leaf nodes**
//

func makeLeafNode(keyEnd nibbles, valueHash crypto.Digest, key nibbles) *leafNode {
	stats.makeleaves++
	ln := &leafNode{keyEnd: make(nibbles, len(keyEnd)), valueHash: valueHash, key: make(nibbles, len(key))}
	copy(ln.key, key)
	copy(ln.keyEnd, keyEnd)
	return ln
}
func (ln *leafNode) raise(mt *Trie, prefix nibbles, key nibbles) node {
	mt.delNode(ln)
	ke := ln.keyEnd
	ln.keyEnd = make(nibbles, len(prefix)+len(ln.keyEnd))
	copy(ln.keyEnd, prefix)
	copy(ln.keyEnd[len(prefix):], ke)
	ln.key = make(nibbles, len(key))
	copy(ln.key, key)
	ln.hash = crypto.Digest{}
	mt.addNode(ln)
	return ln
}
func (ln *leafNode) lambda(l func(node), store backing) {
	l(ln)
}
func (ln *leafNode) preload(store backing, length int) node {
	return ln
}

func (ln *leafNode) merge(mt *Trie) {}
func (ln *leafNode) child() node {
	return makeLeafNode(ln.keyEnd, ln.valueHash, ln.key)
}
func (ln *leafNode) setHash(hash crypto.Digest) {
	ln.hash = hash
}
func (ln *leafNode) add(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	//* Add operation transitions
	//
	//- LN.ADD.1: Store the new value in the existing leaf node, overwriting it.
	//
	//- LN.ADD.2: Store the existing leaf value in a new branch node value space.
	//
	//- LN.ADD.3: Store the existing leaf value in a new leaf node attached to a new branch node.
	//
	//- LN.ADD.4: Store the new value in the new branch node value space.
	//
	//- LN.ADD.5: Store the new value in a new leaf node attached to the new branch node.
	//
	//- LN.ADD.6: Replace the leaf node with a new extention node in front of the new branch node.
	//
	//- LN.ADD.7: Replace the leaf node with the branch node created earlier.
	//
	//  Codepath 1: LN.ADD.1
	//- LN.ADD.1: Store the new value in the existing leaf node, overwriting it.
	//    1:Same key
	//  {key="A", value="DEF"}
	//  {key="A", value="GHI"}
	//
	//  Codepath 2: LN.ADD.2 then LN.ADD.5 then LN.ADD.6
	//- LN.ADD.2: Store the existing leaf value in a new branch node value space.
	//- LN.ADD.5: Store the new value in a new leaf node attached to the new branch node.
	//- LN.ADD.6: Replace the leaf node with a new extention node in front of the new branch node.
	//    6:Shared bits
	//    2:Existing leaf entirely common bits
	//    5:New key extra bits after common
	//  {key="A", value="DEF"}
	//  {key="AB", value="GHI"}
	//
	//  Codepath 3: LN.ADD.3 then LN.ADD.4 then LN.ADD.6
	//- LN.ADD.3: Store the existing leaf value in a new leaf node attached to a new branch node.
	//- LN.ADD.4: Store the new value in the new branch node value space.
	//- LN.ADD.6: Replace the leaf node with a new extention node in front of the new branch node.
	//    6:Shared bits
	//    3:Existing leaf extra bits after common
	//    4:New key entirely common bits
	//  {key="AB", value="DEF"}
	//  {key="A", value="GHI"}
	//
	//  Codepath 4: LN.ADD.3 then LN.ADD.5 then LN.ADD.6
	//- LN.ADD.3: Store the existing leaf value in a new leaf node attached to a new branch node.
	//- LN.ADD.5: Store the new value in a new leaf node attached to the new branch node.
	//- LN.ADD.6: Replace the leaf node with a new extention node in front of the new branch node.
	//    6:Shared bits
	//    3:Existing leaf extra bits after common
	//    5:New key extra bits after common
	//  {key="AB", value="DEF"}
	//  {key="AC", value="GHI"}
	//
	//  Codepath 5: LN.ADD.2 then LN.ADD.5 then LN.ADD.7
	//- LN.ADD.2: Store the existing leaf value in a new branch node value space.
	//- LN.ADD.5: Store the new value in a new leaf node attached to the new branch node.
	//- LN.ADD.7: Replace the leaf node with the branch node created earlier.
	//    7:No shared bits
	//    2:Existing leaf entirely common bits
	//    5:New key extra bits after common
	//  {key="A", value="DEF"}
	//  {key="B", value="GHI"}
	//  {key="AB", value="JKL"}
	//
	//  Codepath 6: LN.ADD.3 then LN.ADD.4 then LN.ADD.7
	//- LN.ADD.3: Store the existing leaf value in a new leaf node attached to a new branch node.
	//- LN.ADD.4: Store the new value in the new branch node value space.
	//- LN.ADD.7: Replace the leaf node with the branch node created earlier.
	//    7:No shared bits
	//    3:Existing leaf extra bits after common
	//    4:New key entirely common bits
	//  {key="AB", value="DEF"}
	//  {key="B", value="GHI"}
	//  {key="A", value="JKL"}
	//
	//  Codepath 7: LN.ADD.3 then LN.ADD.5 then LN.ADD.7
	//    7:No shared bits
	//    3:Existing leaf extra bits after common
	//    5:New key extra bits after common
	//- LN.ADD.3: Store the existing leaf value in a new leaf node attached to a new branch node.
	//- LN.ADD.5: Store the new value in a new leaf node attached to the new branch node.
	//- LN.ADD.7: Replace the leaf node with the branch node created earlier.
	//  {key="AB", value="DEF"}
	//  {key="CD", value="GHI"}
	//
	if equalNibbles(ln.keyEnd, remainingKey) {
		// The two keys are the same. Replace the value.
		if ln.valueHash == valueHash {
			// The two values are the same.  No change, don't clear the hash.
			return ln, nil
		}
		// LN.ADD.1
		ln.valueHash = valueHash
		ln.hash = crypto.Digest{}
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
		// LN.ADD.2
		branchHash = ln.valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
		// in the branch node.
		key1 := append(append(pathKey, shNibbles...), shiftedLn1[0])
		ln1 := makeLeafNode(shiftNibbles(shiftedLn1, 1), ln.valueHash, key1)
		mt.addNode(ln1)
		// LN.ADD.3
		children[shiftedLn1[0]] = ln1
	}

	// Similarly, for our new insertion, if it has no more nibbles, store it in the
	// branch node's value slot.
	if len(shiftedLn2) == 0 {
		// LN.ADD.4
		branchHash = valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one
		// nibble, and store it in that nibble's slot in the branch node.
		key2 := pathKey[:]
		key2 = append(key2, shNibbles...)
		key2 = append(key2, shiftedLn2[0])
		ln2 := makeLeafNode(shiftNibbles(shiftedLn2, 1), valueHash, key2)
		mt.addNode(ln2)
		// LN.ADD.5
		children[shiftedLn2[0]] = ln2
	}
	bn2key := pathKey[:]
	bn2key = append(bn2key, shNibbles...)
	bn2 := makeBranchNode(children, branchHash, bn2key)
	mt.addNode(bn2)

	if len(shNibbles) >= 1 {
		// If there was more than one shared nibble, insert an extension node before the branch node.
		enKey := pathKey[:]
		en := makeExtensionNode(shNibbles, bn2, enKey)
		mt.addNode(en)
		// LN.ADD.6
		return en, nil
	}
	// LN.ADD.7
	return bn2, nil
}
func (ln *leafNode) delete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	//- LN.DEL.1: Delete this leaf node that matches the Delete key.  Pointers to
	//  this node from a branch or extension node are replaced with nil.  The node is
	//  added to the trie's list of deleted keys for later backstore commit.
	//
	if equalNibbles(ln.keyEnd, remainingKey) {
		// The two keys are the same. Delete the value.
		// transition LN.DEL.1
		mt.delNode(ln)
		return nil, true, nil
	}
	return ln, false, nil
}

func (ln *leafNode) hashingCommit(store backing) error {
	if ln.hash.IsZero() {
		bytes, err := ln.serialize()
		if debugTrie {
			fmt.Printf("leafNode.hashingCommit %x : %x\n", ln.getKey(), bytes)
		}
		if err == nil {
			stats.cryptohashes++
			ln.hash = crypto.Hash(bytes)
		}
		if store != nil {
			stats.dbsets++
			if debugTrie {
				fmt.Printf("db.set ln key %x %v\n", ln.getKey(), ln)
			}
			err := store.set(ln.getKey(), bytes)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (ln *leafNode) hashing() error {
	return ln.hashingCommit(nil)
}
func deserializeLeafNode(data []byte, key nibbles) *leafNode {
	if data[0] != 3 && data[0] != 4 {
		panic("invalid leaf node")
	}
	if len(data) < 1+crypto.DigestSize {
		panic("data too short to be a leaf node")
	}

	keyEnd, err := unpack(data[(1+crypto.DigestSize):], data[0] == 3)
	if err != nil {
		panic(err)
	}
	lnKey := key[:]
	return makeLeafNode(keyEnd, crypto.Digest(data[1:(1+crypto.DigestSize)]), lnKey)
}

var lnbuffer bytes.Buffer

func (ln *leafNode) serialize() ([]byte, error) {
	lnbuffer.Reset()

	prefix := byte(4)
	pack, half, err := ln.keyEnd.pack()
	if err != nil {
		return nil, err
	}
	if half {
		prefix = byte(3)
	}
	lnbuffer.WriteByte(prefix)
	lnbuffer.Write(ln.valueHash[:])
	lnbuffer.Write(pack)
	return lnbuffer.Bytes(), nil
}
func (ln *leafNode) evict(eviction func(node) bool) {}
func (ln *leafNode) getKey() nibbles {
	return ln.key
}

func (ln *leafNode) getHash() *crypto.Digest {
	return &ln.hash
}
