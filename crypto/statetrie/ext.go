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
	hash      *crypto.Digest
}

func makeExtensionNode(sharedKey nibbles, next node, key nibbles) *extensionNode {
	stats.makeextensions++
	en := &extensionNode{sharedKey: sharedKey, next: next, key: make(nibbles, len(key))}
	copy(en.key, key)
	return en
}
func (en *extensionNode) add(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	// Calculate the shared nibbles between the key we're adding and this extension node.
	shNibbles := sharedNibbles(en.sharedKey, remainingKey)
	if len(shNibbles) == len(en.sharedKey) {
		// The entire extension node is shared.  descend.
		shifted := shiftNibbles(remainingKey, len(shNibbles))
		replacement, err := en.next.add(mt, append(pathKey, shNibbles...), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		// transition EN.1
		en.next = replacement
		en.hash = nil
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
		// transition EN.2
		children[shifted[0]] = en2
	} else {
		// if there's only one nibble left, store the child in the branch node.
		// there can't be no nibbles left, or the earlier entire-node-shared case would have been triggered.
		// transition EN.3
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
		// transition EN.4
		children[shifted[0]] = ln
	} else {
		// if the key is no more, store it in the branch node's value hash slot.
		// transition EN.5
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
		en.hash = nil
		// transition EN.6
		return en, nil
	}
	// or else there there is no shared key left, and the extension node is destroyed.
	// transition EN.7
	return replacement, nil
}
func (en *extensionNode) delete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
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
	replacementChild, found, err := en.next.delete(mt, enKey, shifted)
	if found && err == nil {
		// the key was found below this node and deleted,
		// make a new extension node pointing to its replacement.
		en.next = replacementChild
	}
	return en, found, err
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

func (en *extensionNode) hashingCommit(store backing) error {
	if en.hash == nil {
		if en.next.getHash() == nil {
			err := en.next.hashingCommit(store)
			if err != nil {
				return err
			}
		}
		bytes, err := en.serialize()
		if err == nil {
			stats.cryptohashes++
			en.hash = new(crypto.Digest)
			*en.hash = crypto.Hash(bytes)
		}
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

	if len(data) < 33 {
		panic("data too short to be an extension node")
	}

	sharedKey, err := unpack(data[33:], data[0] == 1)
	if err != nil {
		panic(err)
	}
	if len(sharedKey) == 0 {
		panic("sharedKey can't be empty in an extension node")
	}
	hash := new(crypto.Digest)
	copy(hash[:], data[1:33])
	var child node
	if *hash != (crypto.Digest{}) {
		chKey := key[:]
		chKey = append(chKey, sharedKey...)
		child = makeBackingNode(hash, chKey)
	}
	return makeExtensionNode(sharedKey, child, key)
}
func (en *extensionNode) serialize() ([]byte, error) {
	pack, half, err := en.sharedKey.pack()
	if err != nil {
		return nil, err
	}
	data := make([]byte, 33+len(pack))
	if half {
		data[0] = 1
	} else {
		data[0] = 2
	}

	copy(data[1:33], en.next.getHash()[:])
	copy(data[33:], pack)
	return data, nil
}
func (en *extensionNode) lambda(l func(node)) {
	l(en)
	en.next.lambda(l)
}
func (en *extensionNode) preload(store backing) node {
	en.next = en.next.preload(store)
	en.next.preload(store)
	return en
}

func (en *extensionNode) evict(eviction func(node) bool) {
	if eviction(en) {
		fmt.Printf("evicting ext node %x\n", en.getKey())
		en.next = makeBackingNode(en.next.getHash(), en.next.getKey())
		stats.evictions++
	} else {
		en.next.evict(eviction)
	}
}
func (en *extensionNode) getKey() nibbles {
	return en.key
}
func (en *extensionNode) getHash() *crypto.Digest {
	return en.hash
}
