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

type ExtensionNode struct {
	sharedKey nibbles
	child     node
	key       nibbles
	hash      *crypto.Digest
}

func makeExtensionNode(sharedKey nibbles, child node, key nibbles) *ExtensionNode {
	stats.makeextensions++
	en := &ExtensionNode{sharedKey: sharedKey, child: child, key: key}
	return en
}
func (en *ExtensionNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	// Calculate the shared nibbles between the key we're adding and this extension node.
	shNibbles := sharedNibbles(en.sharedKey, remainingKey)
	if len(shNibbles) == len(en.sharedKey) {
		// The entire extension node is shared.  descend.
		shifted := shiftNibbles(remainingKey, len(shNibbles))
		replacement, err := en.child.descendAdd(mt, append(pathKey, shNibbles...), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		en.child = replacement
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
		en2 := makeExtensionNode(shifted2, en.child, append(pathKey, shifted[0]))
		mt.addNode(en2)
		children[shifted[0]] = en2
	} else {
		// if there's only one nibble left, store the child in the branch node.
		// there can't be no nibbles left, or the earlier entire-node-shared case would have been triggered.
		children[shifted[0]] = en.child
	}

	//what's left of the new add remaining key gets put into the branch node bucket corresponding
	//with its first nibble, or into the valueHash if it's now empty.
	shifted = shiftNibbles(remainingKey, len(shNibbles))
	if len(shifted) > 0 {
		shifted3 := shiftNibbles(shifted, 1)
		// we know this slot will be empty because it's the first nibble that differed from the
		// only other occupant in the child arrays, the one that leads to the extension node's child.
		ln := makeLeafNode(shifted3, valueHash, append(append(pathKey, shNibbles...), shifted[0]))
		mt.addNode(ln)
		children[shifted[0]] = ln
	} else {
		// if the key is no more, store it in the branch node's value hash slot.
		branchHash = valueHash
	}

	replacement := makeBranchNode(children, branchHash, append(pathKey, shNibbles...))
	mt.addNode(replacement)
	// the shared bits of the extension node get smaller
	if len(shNibbles) > 0 {
		// still some shared key left, store them in an extension node
		// and point in to the new branch node
		en.sharedKey = shNibbles
		en.child = replacement
		en.hash = nil
		return en, nil
	}
	// or else there there is no shared key left, and the extension node is destroyed.
	mt.delNode(en)
	return replacement, nil
}
func (en *ExtensionNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	var err error
	if len(remainingKey) == 0 {
		// can't stop on an exension node
		return nil, false, fmt.Errorf("key too short")
	}
	shNibbles := sharedNibbles(remainingKey, en.sharedKey)
	if len(shNibbles) == len(en.sharedKey) {
		shifted := shiftNibbles(remainingKey, len(en.sharedKey))
		replacementChild, found, err := en.child.descendDelete(mt, append(pathKey, shNibbles...), shifted)
		if err != nil {
			return nil, false, err
		}
		if found && replacementChild != nil {
			// the key was found below this node and deleted,
			// make a new extension node pointing to its replacement.
			mt.addNode(replacementChild)
			return makeExtensionNode(en.sharedKey, replacementChild, pathKey), true, nil
		}
		// returns empty digest if there's nothing left.
		return nil, found, err
	}
	// didn't match the entire extension node.
	return nil, false, err
}

func (en *ExtensionNode) descendHashWithCommit(b *pebble.Batch) error {
	if en.hash == nil {
		if en.child != nil && en.child.getHash() == nil {
			err := en.child.descendHashWithCommit(b)
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
		options := &pebble.WriteOptions{}
		stats.dbsets++
		if debugTrie {
			fmt.Printf("db.set en key %x dbkey %x\n", en.getKey(), en.getDBKey())
		}
		if b != nil {
			return b.Set(en.getDBKey(), bytes, options)
		}
	}
	return nil
}

func (en *ExtensionNode) descendHash() error {
	return en.descendHashWithCommit(nil)
}
func deserializeExtensionNode(data []byte, key nibbles) (*ExtensionNode, error) {
	if data[0] != 1 && data[0] != 2 {
		return nil, errors.New("invalid prefix for extension node")
	}

	if len(data) < 33 {
		return nil, errors.New("data too short to be an extension node")
	}

	sharedKey, err := unpack(data[33:], data[0] == 1)
	if err != nil {
		return nil, err
	}
	if len(sharedKey) == 0 {
		return nil, errors.New("sharedKey can't be empty in an extension node")
	}
	hash := new(crypto.Digest)
	copy(hash[:], data[1:33])
	var child node
	if *hash != (crypto.Digest{}) {
		child = makeDBNode(hash, append(key, sharedKey...))
	}
	return makeExtensionNode(sharedKey, child, key), nil
}
func (en *ExtensionNode) serialize() ([]byte, error) {
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

	if en.child != nil {
		copy(data[1:33], en.child.getHash()[:])
	}
	copy(data[33:], pack)
	return data, nil
}
func (en *ExtensionNode) getKey() nibbles {
	return en.key
}
func (en *ExtensionNode) getHash() *crypto.Digest {
	return en.hash
}
func (en *ExtensionNode) getDBKey() dbKey {
	if en.hash == nil || en.key == nil {
		return nil
	}
	return makeDBKey(en.key, en.hash)
}
