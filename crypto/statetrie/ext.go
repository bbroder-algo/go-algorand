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
	"fmt"
	"github.com/algorand/go-algorand/crypto"
)

type extensionNode struct {
	sharedKey nibbles
	next      node
	key       nibbles
	hash      crypto.Digest
}

func makeExtensionNode(sharedKey nibbles, next node, key nibbles) *extensionNode {
	stats.makeextensions++
	en := &extensionNode{sharedKey: make(nibbles, len(sharedKey)), next: next, key: make(nibbles, len(key))}
	copy(en.key, key)
	copy(en.sharedKey, sharedKey)
	return en
}
func (en *extensionNode) add(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	//- EN.ADD.1: Point the existing extension node at a (possibly new or existing) node resulting
	//            from performing the Add operation on the child node.
	//
	//- EN.ADD.2: Create an extension node for the current child and store it in a new branch node child slot.
	//
	//- EN.ADD.3: Store the existing extension node child in a new branch node child slot.
	//
	//- EN.ADD.4: Store the new value in a new leaf node stored in an available child slot of the new branch node.
	//
	//- EN.ADD.5: Store the new value in the value slot of the new branch node.
	//
	//- EN.ADD.6: Modify the existing extension node shared key and point the child at the new branch node.
	//
	//- EN.ADD.7: Replace the extension node with the branch node created earlier.
	//
	//Operation sets (1 + 2x2 + 2x2 = 9 sets) :
	//
	//  * EN.ADD.1
	//
	//  This redirects the extension node to a new/existing node resulting from
	//  performing the Add operation on the extension child.
	//
	//  * EN.ADD.2|EN.ADD.3 then EN.ADD.4|EN.ADD.5 then EN.ADD.6
	//
	//  This stores the current extension node child in either a new branch node
	//  child slot or by creating a new extension node at a new key pointing at the
	//  child, and attaching that to a new branch node.  Either way, the new branch
	//  node also receives a new leaf node with the new value or has its value slot
	//  assigned, and another extension node is created to replace it pointed at the
	//  branch node as its target.
	//
	//  * EN.ADD.2|EN.ADD.3 then EN.ADD.4|EN.ADD.5 then EN.ADD.7
	//
	//  Same as above, only the new branch node replaceds the existing extension node
	//  outright, without the additional extension node.
	//

	// Calculate the shared nibbles between the key we're adding and this extension node.
	shNibbles := sharedNibbles(en.sharedKey, remainingKey)
	if len(shNibbles) == len(en.sharedKey) {
		// The entire extension node is shared.  descend.
		shifted := shiftNibbles(remainingKey, len(shNibbles))
		replacement, err := en.next.add(mt, append(pathKey, shNibbles...), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		if replacement.getHash().IsZero() {
			en.hash = crypto.Digest{}
		}
		// transition EN.ADD.1
		en.next = replacement
		return en, nil
	}

	// we have to upgrade part or all of this extension node into a branch node.
	var children [16]node
	branchHash := crypto.Digest{}
	// what's left of the extension node shared key after removing the shared part gets
	// attached to the new branch node.
	shifted := shiftNibbles(en.sharedKey, len(shNibbles))
	if len(shifted) >= 2 {
		// if there's two or more nibbles left, make another extension node.
		shifted2 := shiftNibbles(shifted, 1)
		enKey := pathKey[:]
		enKey = append(enKey, shNibbles...)
		enKey = append(enKey, shifted[0])
		en2 := makeExtensionNode(shifted2, en.next, enKey)
		mt.addNode(en2)
		// transition EN.ADD.2
		children[shifted[0]] = en2
	} else {
		// if there's only one nibble left, store the child in the branch node.
		// there can't be no nibbles left, or the earlier entire-node-shared case would have been triggered.
		// transition EN.ADD.3
		children[shifted[0]] = en.next
	}

	//what's left of the new remaining key gets put into the branch node bucket corresponding
	//with its first nibble, or into the valueHash if it's now empty.
	shifted = shiftNibbles(remainingKey, len(shNibbles))
	if len(shifted) > 0 {
		shifted3 := shiftNibbles(shifted, 1)
		// we know this slot will be empty because it's the first nibble that differed from the
		// only other occupant in the child arrays, the one that leads to the extension node's child.
		lnKey := pathKey[:]
		lnKey = append(lnKey, shNibbles...)
		lnKey = append(lnKey, shifted[0])
		ln := makeLeafNode(shifted3, valueHash, lnKey)
		mt.addNode(ln)
		// transition EN.ADD.4
		children[shifted[0]] = ln
	} else {
		// if the key is no more, store it in the branch node's value hash slot.
		// transition EN.ADD.5
		branchHash = valueHash
	}

	bnKey := pathKey[:]
	bnKey = append(bnKey, shNibbles...)
	replacement := makeBranchNode(children, branchHash, bnKey)
	mt.addNode(replacement)
	// the shared bits of the extension node get smaller
	if len(shNibbles) > 0 {
		// still some shared key left, store them in an extension node
		// and point in to the new branch node
		en.sharedKey = shNibbles
		en.next = replacement
		en.hash = crypto.Digest{}
		// transition EN.ADD.6
		return en, nil
	}
	// or else there there is no shared key left, and the extension node is destroyed.
	// transition EN.ADD.7
	return replacement, nil
}
func (en *extensionNode) raise(mt *Trie, prefix nibbles, key nibbles) node {
	mt.delNode(en)
	en.sharedKey = make(nibbles, len(prefix)+len(en.sharedKey))
	copy(en.sharedKey, prefix)
	copy(en.sharedKey[len(prefix):], en.sharedKey)
	en.key = make(nibbles, len(key))
	copy(en.key, key)
	mt.addNode(en)
	en.hash = crypto.Digest{}
	return en
}
func (en *extensionNode) delete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	//- EN.DEL.1: The extension node can be deleted because the child was deleted
	//  after finding the key in the lower subtrie.
	//
	//- EN.DEL.2: Raise up the results of the successful deletion operation on the
	//  extension node child to replace the existing node (possibly with another
	//  extension nodes, as branches are raised up the trie by placing extension
	//  nodes in front of them)
	var err error
	if len(remainingKey) == 0 {
		// can't stop on an exension node
		return nil, false, fmt.Errorf("key too short")
	}
	shNibbles := sharedNibbles(remainingKey, en.sharedKey)
	if len(shNibbles) != len(en.sharedKey) {
		return en, false, nil
	}

	shifted := shiftNibbles(remainingKey, len(en.sharedKey))
	enKey := pathKey[:]
	enKey = append(enKey, shNibbles...)
	replacement, found, err := en.next.delete(mt, enKey, shifted)
	if found && err == nil {
		// the key was found below this node and deleted, and there
		// is no replacement
		if replacement == nil {
			// Transition EN.DEL.1
			mt.delNode(en)
			return nil, true, nil
		}

		// the key was found below this node and deleted, and there
		// is a replacement.  Raise it up to replace this node.
		// Transition EN.DEL.2
		return replacement.raise(mt, en.sharedKey, pathKey), true, nil
	}
	return nil, found, err
}
func (en *extensionNode) merge(mt *Trie) {
	if pa, ok := en.next.(*parent); ok {
		en.next = pa.p
	} else {
		en.next.merge(mt)
	}
}
func (en *extensionNode) child() node {
	return makeExtensionNode(en.sharedKey, makeParent(en.next), en.getKey())
}
func (en *extensionNode) setHash(hash crypto.Digest) {
	en.hash = hash
}

func (en *extensionNode) hashingCommit(store backing) error {
	if en.hash.IsZero() {
		if en.next.getHash().IsZero() {
			err := en.next.hashingCommit(store)
			if err != nil {
				return err
			}
		}
		bytes, err := en.serialize()
		if err != nil {
			return err
		}

		stats.cryptohashes++
		en.hash = crypto.Hash(bytes)

		if store != nil {
			stats.dbsets++
			if debugTrie {
				fmt.Printf("db.set en key %x %v\n", en.getKey(), en)
			}
			err = store.set(en.getKey(), bytes)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (en *extensionNode) hashing() error {
	return en.hashingCommit(nil)
}
func deserializeExtensionNode(data []byte, key nibbles) *extensionNode {
	if data[0] != 1 && data[0] != 2 {
		panic("invalid prefix for extension node")
	}

	if len(data) < (1 + crypto.DigestSize) {
		panic("data too short to be an extension node")
	}

	sharedKey, err := unpack(data[(1+crypto.DigestSize):], data[0] == 1)
	if err != nil {
		panic(err)
	}
	if len(sharedKey) == 0 {
		panic("sharedKey can't be empty in an extension node")
	}
	var hash crypto.Digest
	copy(hash[:], data[1:(1+crypto.DigestSize)])
	var child node
	if !hash.IsZero() {
		chKey := key[:]
		chKey = append(chKey, sharedKey...)
		child = makeBackingNode(hash, chKey)
	} else {
		panic("next node hash can't be zero in an extension node")
	}

	return makeExtensionNode(sharedKey, child, key)
}
func (en *extensionNode) serialize() ([]byte, error) {
	pack, half, err := en.sharedKey.pack()
	if err != nil {
		return nil, err
	}
	data := make([]byte, 1+crypto.DigestSize+len(pack))
	if half {
		data[0] = 1
	} else {
		data[0] = 2
	}

	copy(data[1:(1+crypto.DigestSize)], en.next.getHash()[:])
	copy(data[(1+crypto.DigestSize):], pack)
	return data, nil
}
func (en *extensionNode) lambda(l func(node), store backing) {
	l(en)
	en.next.lambda(l, store)
}
func (en *extensionNode) preload(store backing, length int) node {
	en.next = en.next.preload(store, length)
	return en
}

func (en *extensionNode) evict(eviction func(node) bool) {
	if eviction(en) {
		fmt.Printf("evicting ext node %x\n", en.getKey())
		en.next = makeBackingNode(*en.next.getHash(), en.next.getKey())
		stats.evictions++
	} else {
		en.next.evict(eviction)
	}
}
func (en *extensionNode) getKey() nibbles {
	return en.key
}

func (en *extensionNode) getHash() *crypto.Digest {
	return &en.hash
}
