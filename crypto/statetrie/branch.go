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

type branchNode struct {
	children  [16]node
	valueHash crypto.Digest
	key       nibbles
	hash      crypto.Digest
}

func makeBranchNode(children [16]node, valueHash crypto.Digest, key nibbles) *branchNode {
	stats.makebranches++
	bn := &branchNode{children: children, valueHash: valueHash, key: make(nibbles, len(key))}
	copy(bn.key, key)
	return bn
}
func (bn *branchNode) merge(mt *Trie) {
	for i := range bn.children {
		if bn.children[i] != nil {
			if pa, ok := bn.children[i].(*parent); ok {
				bn.children[i] = pa.p
			} else {
				bn.children[i].merge(mt)
			}
		}
	}
}
func (bn *branchNode) child() node {
	var children [16]node
	for i := range bn.children {
		if bn.children[i] != nil {
			children[i] = makeParent(bn.children[i])
		}
	}
	return makeBranchNode(children, bn.valueHash, bn.key)
}

func (bn *branchNode) add(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	//Three operational transitions:
	//
	//- BN.ADD.1: Store the new value in the branch node value slot. This overwrites
	//  the branch node slot value.
	//
	//- BN.ADD.2: Make a new leaf node with the new value, and point an available
	//  branch child slot at it. This stores a new leaf node in a child slot.
	//
	//- BN.ADD.3: This repoints the child node to a new/existing node resulting from
	//  performing the Add operation on the child node.
	if len(remainingKey) == 0 {
		// If we're here, then set the value hash in this node, overwriting the old one.
		if bn.valueHash == valueHash {
			// If it is the same value, do not zero the hash
			return bn, nil
		}

		bn.valueHash = valueHash
		// transition BN.ADD.1
		bn.hash = crypto.Digest{}
		return bn, nil
	}

	// Otherwise, shift out the first nibble and check the children for it.
	shifted := shiftNibbles(remainingKey, 1)
	slot := remainingKey[0]
	if bn.children[slot] == nil {
		// nil children are available.
		lnKey := pathKey[:]
		lnKey = append(lnKey, slot)

		// transition BN.ADD.2
		bn.hash = crypto.Digest{}
		bn.children[slot] = makeLeafNode(shifted, valueHash, lnKey)
		mt.addNode(bn.children[slot])
	} else {
		// Not available.  Descend down the branch.
		replacement, err := bn.children[slot].add(mt, append(pathKey, remainingKey[0]), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		// If the replacement hash is zero, zero the branch node hash
		if replacement.getHash().IsZero() {
			bn.hash = crypto.Digest{}
		}
		// transition BN.ADD.3
		bn.children[slot] = replacement
	}

	return bn, nil
}
func (bn *branchNode) raise(mt *Trie, prefix nibbles, key nibbles) node {
	en := makeExtensionNode(prefix, bn, key)
	mt.addNode(en)
	return en
}
func (bn *branchNode) delete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	//- BN.DEL.1: Copy the empty hash into the value slot, mark the node for rehashing.
	//
	//- BN.DEL.2: Raise up the only child left to replace the branch node.
	//
	//- BN.DEL.3: Delete the childless and valueless branch node.  Add it to the
	//  trie's list of deleted keys for later backstore commit.
	//
	//- BN.DEL.4: Replace the child slot with a new node created by deleting the node
	//  lower in the trie Mark for rehashing.
	//
	//- BN.DEL.5: Replace the branch node with a leaf node valued by the branch node value slot.
	//
	//- BN.DEL.6: Replace the branch node with the only child left raised up a nibble.
	//
	//- BN.DEL.7: Delete the childless and valueless branch node.  Add it to the
	//  trie's list of deleted keys for later backstore commit.
	if len(remainingKey) == 0 {
		if (bn.valueHash == crypto.Digest{}) {
			// valueHash is empty -- key not found.
			return bn, false, nil
		}
		// delete this branch's value hash. reset the value to the empty hash.
		// update the branch node if there are children, or remove it completely.
		bn.hash = crypto.Digest{}
		bn.valueHash = crypto.Digest{}
		var only node
		var onlyIndex int
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil {
				if only != nil {
					// more than one child.  no need to continue.
					// transition BN.DEL.1
					return bn, true, nil
				}
				only = bn.children[i]
				onlyIndex = i
			}
		}
		if only != nil {
			// only one child.  replace this branch with the child.
			// transition BN.DEL.2
			return only.raise(mt, nibbles{byte(onlyIndex)}, bn.key), true, nil
		}
		// no children.  delete this branch.
		// transition BN.DEL.3
		mt.delNode(bn)
		return nil, true, nil
	}

	// if there is no child at this index.  key not found.
	if bn.children[remainingKey[0]] == nil {
		return bn, false, nil
	}

	// descend into the branch node.
	shifted := shiftNibbles(remainingKey, 1)
	lnKey := pathKey[:]
	lnKey = append(lnKey, remainingKey[0])
	replacement, found, err := bn.children[remainingKey[0]].delete(mt, lnKey, shifted)
	if found && err == nil {
		bn.hash = crypto.Digest{}
		bn.children[remainingKey[0]] = replacement

		hasValueHash := !bn.valueHash.IsZero()
		var only node
		var onlyIndex int
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil {
				if (only != nil) || (only == nil && hasValueHash) {
					// more than one child (two children or a child and the value slot).  no need to continue.
					// transition BN.DEL.4
					return bn, true, nil
				}
				only = bn.children[i]
				onlyIndex = i
			}
		}
		if only == nil && hasValueHash {
			// only the value slot. replace this branch with a leaf.
			// transition BN.DEL.5
			ln := makeLeafNode(nibbles{}, bn.valueHash, bn.key)
			mt.addNode(ln)
			return ln, true, nil
		}
		if only != nil {
			// only one child.  replace this branch with the raised child.
			// transition BN.DEL.6
			return only.raise(mt, nibbles{byte(onlyIndex)}, bn.key), true, nil
		}
		// no children.  delete this branch.
		// transition BN.DEL.7
		mt.delNode(bn)
		return nil, true, nil
	}
	// returning either false or an error (or both).
	return nil, found, err
}
func (bn *branchNode) hashingCommit(store backing) error {
	if bn.hash.IsZero() {
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil && bn.children[i].getHash().IsZero() {
				err := bn.children[i].hashingCommit(store)
				if err != nil {
					return err
				}
			}
		}
		bytes, err := bn.serialize()
		if err != nil {
			return err
		}
		stats.cryptohashes++
		bn.hash = crypto.Hash(bytes)

		if store != nil {
			stats.dbsets++
			if debugTrie {
				fmt.Printf("db.set bn key %x %v\n", bn.getKey(), bn)
			}
			err = store.set(bn.getKey(), bytes)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (bn *branchNode) hashing() error {
	return bn.hashingCommit(nil)
}

func deserializeBranchNode(data []byte, key nibbles) *branchNode {
	if data[0] != 5 {
		panic("invalid prefix for branch node")
	}
	if len(data) < 545 {
		panic("data too short to be a branch node")
	}

	var children [16]node
	for i := 0; i < 16; i++ {
		var hash crypto.Digest

		copy(hash[:], data[1+i*32:33+i*32])
		if !hash.IsZero() {
			chKey := key[:]
			chKey = append(chKey, byte(i))
			children[i] = makeBackingNode(hash, chKey)
		}
	}
	var valueHash crypto.Digest
	copy(valueHash[:], data[513:545])
	return makeBranchNode(children, valueHash, key)
}

func (bn *branchNode) setHash(hash crypto.Digest) {
	bn.hash = hash
}

var bnbuffer bytes.Buffer

func (bn *branchNode) serialize() ([]byte, error) {
	bnbuffer.Reset()
	var empty crypto.Digest
	prefix := byte(5)

	bnbuffer.WriteByte(prefix)
	for i := 0; i < 16; i++ {
		if bn.children[i] != nil {
			bnbuffer.Write(bn.children[i].getHash().ToSlice())
		} else {
			bnbuffer.Write(empty[:])
		}
	}
	bnbuffer.Write(bn.valueHash[:])
	return bnbuffer.Bytes(), nil
}

// evict walks through the node tree and replaces nodes (and their subtrees) with backing nodes
// if they meet a user-defined eviction criterion
// The eviction function is provided as an argument and operates on each node to decide whether it should be evicted.
func (bn *branchNode) evict(eviction func(node) bool) {
	// If the current branch node meets the eviction criterion
	if eviction(bn) {
		if debugTrie {
			fmt.Printf("evicting branch node %x, (%v)\n", bn.getKey(), bn)
		}
		for i := 0; i < 16; i++ {
			// Evict children if they meet certain conditions
			ch := bn.children[i]
			// Only evict the child if it's not nil and its hash is not zero
			// Backing nodes need hashes for the hashing function to continue
			// to work properly
			if ch != nil && !ch.getHash().IsZero() {
				bn.children[i] = makeBackingNode(*ch.getHash(), ch.getKey())
				stats.evictions++
			}
		}
	} else {
		// If the current branch node doesn't meet the eviction criterion,
		// recursively attempt to evict its children
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil {
				bn.children[i].evict(eviction)
			}
		}
	}
}
func (bn *branchNode) preload(store backing, length int) node {
	for i := 0; i < 16; i++ {
		if bn.children[i] != nil {
			bn.children[i] = bn.children[i].preload(store, length)
		}
	}
	return bn
}
func (bn *branchNode) lambda(l func(node), store backing) {
	l(bn)
	for i := 0; i < 16; i++ {
		if bn.children[i] != nil {
			bn.children[i].lambda(l, store)
		}
	}
}

func (bn *branchNode) getKey() nibbles {
	return bn.key
}

//	func (bn *branchNode) getHash() crypto.Digest {
//		return bn.hash
func (bn *branchNode) getHash() *crypto.Digest {
	return &bn.hash
}
