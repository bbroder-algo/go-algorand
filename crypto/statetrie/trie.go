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
	"errors"
	"fmt"
	"github.com/algorand/go-algorand/crypto"
	"runtime"
)

const (
	// MaxKeyLength is the maximum key length in bytes that can be added to the trie
	MaxKeyLength = 65535
)

var debugTrie = false

// Trie is a hashable 16-way radix tree
type Trie struct {
	parent *Trie
	root   node
	dels   map[string]bool
	store  backing
}

// MakeTrie creates a merkle trie
func MakeTrie(store backing) *Trie {
	mt := &Trie{store: store}
	mt.dels = make(map[string]bool)
	if store == nil {
		mt.store = makeMemoryBackstore()
	}
	mt.root = mt.store.get(nibbles{})
	return mt
}

// Add adds the given key/value pair to the trie.
func (mt *Trie) Add(key nibbles, value []byte) (err error) {
	if len(key) == 0 {
		return errors.New("empty key not allowed")
	}

	if len(key) > MaxKeyLength {
		return errors.New("key too long")
	}
	if debugTrie {
		fmt.Printf("Add: %x : %v\n", key, crypto.Hash(value))
	}

	if mt.root == nil {
		stats.cryptohashes++
		stats.newrootnode++
		mt.root = makeLeafNode(key, crypto.Hash(value), nibbles{})
		mt.addNode(mt.root)
		return nil
	}

	stats.cryptohashes++
	replacement, err := mt.root.add(mt, nibbles{}, key, crypto.Hash(value))
	if err != nil {
		return err
	}
	stats.newrootnode++
	mt.root = replacement
	return nil
}

// Delete deletes the given key from the trie, if such element exists.
// if no such element exists, return false
func (mt *Trie) Delete(key nibbles) (bool, error) {
	var err error
	if len(key) == 0 {
		return false, errors.New("empty key not allowed")
	}
	if len(key) > MaxKeyLength {
		return false, errors.New("key too long")
	}
	if mt.root == nil {
		return false, nil
	}

	replacement, found, err := mt.root.delete(mt, nibbles{}, key)
	if err == nil && found {
		mt.root = replacement
	}
	return found, err
}

// Hash provides the root hash for this trie
func (mt *Trie) Hash() crypto.Digest {
	if mt.root == nil {
		return crypto.Digest{}
	}
	if mt.root.getHash() != nil {
		return *(mt.root.getHash())
	}

	err := mt.root.hashing()
	if err != nil {
		panic(err)
	}

	return *(mt.root.getHash())
}

// Child creates a child trie, which can be merged back into the parent
func (mt *Trie) Child() *Trie {
	ch := &Trie{parent: mt, store: mt.store}
	ch.dels = make(map[string]bool)
	if mt.root != nil {
		ch.root = makeParent(mt.root)
	}
	return ch
}

// Merge merges the child trie back into the parent
func (mt *Trie) Merge() {
	if mt.parent == nil {
		return
	}
	if mt.root != nil {
		mt.root.merge(mt)
		mt.parent.root = mt.root
		mt.root = makeParent(mt.root)
		for k, v := range mt.dels {
			mt.parent.dels[k] = v
		}
	}
	mt.dels = make(map[string]bool)
}

// Commit commits the trie to the backing store
func (mt *Trie) Commit() error {
	mt.store.batchStart()
	if mt.root != nil {
		err := mt.root.hashingCommit(mt.store)
		if err != nil {
			return err
		}
	}
	for k := range mt.dels {
		err := mt.store.del([]byte(k))
		if err != nil {
			return err
		}
	}
	mt.store.batchEnd()
	mt.dels = make(map[string]bool)
	return nil
}

// delNode marks a node for deletion
func (mt *Trie) delNode(n node) {
	stats.delnode++
	dbKey := n.getKey()
	if dbKey != nil {
		if debugTrie {
			fmt.Printf("delNode %T (%x) : (%v)\n", n, dbKey, n)
		}
		mt.dels[string(dbKey)] = true
	}
}

// addNode marks a node as added
func (mt *Trie) addNode(n node) {
	stats.addnode++
	key := string(n.getKey())
	if debugTrie {
		_, _, lineno, _ := runtime.Caller(1)
		fmt.Printf("addNode %T (%x) : (%v) caller %d\n", n, key, n, lineno)
	}
	// this key is no longer to be deleted, as it will be overwritten.
	delete(mt.dels, key)
}
