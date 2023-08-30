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
	"runtime/debug"
)

// Trie nodes

type node interface {
	child() node
	getKey() nibbles         // the key of the node in the trie
	getHash() *crypto.Digest // the hash of the node, if it has been hashed
	//	getHash() crypto.Digest // the hash of the node, if it has been hashed
	add(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error)
	delete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error)
	raise(mt *Trie, prefix nibbles, key nibbles) node
	hashing() error
	hashingCommit(store backing, e Eviction) error
	merge(mt *Trie)
	serialize() ([]byte, error)
	preload(store backing, length int) node
	lambda(func(node), backing)
	setHash(hash crypto.Digest)
}

// First byte of a committed node indicates the type of node.
//
//  1 == extension, half nibble
//  2 == extension, full
//  3 == leaf, half nibble
//  4 == leaf, full
//  5 == branch
//

func deserializeNode(nbytes []byte, key nibbles) node {
	if len(nbytes) == 0 {
		debug.PrintStack()
		panic("deserializeNode: zero length node")
	}
	switch nbytes[0] {
	case 1, 2:
		return deserializeExtensionNode(nbytes, key)
	case 3, 4:
		return deserializeLeafNode(nbytes, key)
	case 5:
		return deserializeBranchNode(nbytes, key)
	default:
		panic(fmt.Sprintf("deserializeNode: invalid node type %d", nbytes[0]))
	}
}

func verify(n node, hash crypto.Digest) bool {
	bytes, err := n.serialize()
	if err != nil {
		panic(err)
	}
	verify := hash == crypto.Hash(bytes)
	if !verify {
		panic(fmt.Sprintf("verify: hash mismatch %s != %s", hash, crypto.Hash(bytes)))
	}
	fmt.Printf("verify: hash match %s == %s\n", hash, hash)
	return verify
}
