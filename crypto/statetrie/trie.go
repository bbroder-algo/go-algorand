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
	"runtime"
)

const (
	// Maximum key length in bytes that can be added to the trie
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
	mt.root = store.get(nibbles{})
	return mt
}

// Trie Add adds the given key/value pair to the trie.
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

// Provide the root hash for this trie
func (mt *Trie) RootHash() (crypto.Digest, error) {
	if mt.root == nil {
		return crypto.Digest{}, nil
	}
	if mt.root.getHash() != nil {
		return *(mt.root.getHash()), nil
	}

	err := mt.root.hashing()
	if err != nil {
		return crypto.Digest{}, err
	}

	return *(mt.root.getHash()), nil
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
	_, _, lineno, _ := runtime.Caller(1)
	if debugTrie {
		fmt.Printf("addNode %T (%x) : (%v) caller %d\n", n, key, n, lineno)
	}
	// this key is no longer to be deleted, as it will be overwritten.
	delete(mt.dels, key)
}

// countNodes returns a string with the number of nodes in the trie but
// does not follow parent nodes or load backing nodes
func (mt *Trie) countNodes() string {
	if mt.root == nil {
		return "Empty trie"
	}
	var nc struct {
		branches int
		leaves   int
		exts     int
		parents  int
		backings int
	}

	count := func() func(n node) {
		innerCount := func(n node) {
			switch n.(type) {
			case *branchNode:
				nc.branches++
			case *leafNode:
				nc.leaves++
			case *extensionNode:
				nc.exts++
			case *parent:
				nc.parents++
			case *backingNode:
				nc.backings++
			}
		}
		return innerCount
	}()
	mt.root.lambda(count)

	var nmem struct {
		branches int
		leaves   int
		exts     int
		parents  int
		backings int
	}

	mem := func() func(n node) {
		innerCount := func(n node) {
			switch v := n.(type) {
			//estimates
			case *branchNode:
				nmem.branches += 16*16 + 32 + 24 + len(v.key) + 8 + 32
			case *leafNode:
				nmem.leaves += 24 + len(v.key) + 24 + len(v.keyEnd) + 32 + 8 + 32
			case *extensionNode:
				nmem.exts += 24 + len(v.key) + 24 + len(v.sharedKey) + 8 + 32
			case *parent:
				nmem.parents += 8
			case *backingNode:
				nmem.backings += len(v.key) + 8 + 32
			}
		}
		return innerCount
	}()
	mt.root.lambda(mem)

	return fmt.Sprintf("[nodes: total %d (branches: %d, leaves: %d, exts: %d, parents: %d, backings: %d), mem: total %d (branches: %d, leaves: %d, exts: %d, parents: %d, backings: %d)]",
		nc.branches+nc.leaves+nc.exts+nc.parents,
		nc.branches, nc.leaves, nc.exts, nc.parents, nc.backings,
		nmem.branches+nmem.leaves+nmem.exts+nmem.parents,
		nmem.branches, nmem.leaves, nmem.exts, nmem.parents, nmem.backings)

}

// Make a dot graph of the trie
func (mt *Trie) DotGraph(keysAdded [][]byte, valuesAdded [][]byte) string {
	var keys string
	for i := 0; i < len(keysAdded); i++ {
		keys += fmt.Sprintf("%x = %x\\n", keysAdded[i], valuesAdded[i])
	}
	fmt.Printf("root: %v\n", mt.root)
	return fmt.Sprintf("digraph trie { key [shape=box, label=\"key/value inserted:\\n%s\"];\n %s }\n", keys, mt.dotGraph(mt.root, nibbles{}))
}

// dot graph generation helper
func (mt *Trie) dotGraph(n node, path nibbles) string {

	switch tn := n.(type) {
	case *backingNode:
		return mt.dotGraph(mt.store.get(path), path)
	case *parent:
		return mt.dotGraph(tn.p, path)
	case *leafNode:
		ln := tn
		return fmt.Sprintf("n%p [label=\"leaf\\nkeyEnd:%x\\nvalueHash:%s\" shape=box];\n", tn, ln.keyEnd, ln.valueHash)
	case *extensionNode:
		en := tn
		return fmt.Sprintf("n%p [label=\"extension\\nshKey:%x\" shape=box];\n", tn, en.sharedKey) +
			fmt.Sprintf("n%p -> n%p;\n", en, en.next) +
			mt.dotGraph(en.next, append(path, en.sharedKey...))
	case *branchNode:
		bn := tn
		var indexesFilled string
		indexesFilled = "--"
		for i, ch := range bn.children {
			if ch != nil {
				indexesFilled += fmt.Sprintf("%x ", i)
			}
		}
		indexesFilled += "--"

		s := fmt.Sprintf("n%p [label=\"branch\\nindexesFilled:%s\\nvalueHash:%s\" shape=box];\n", tn, indexesFilled, bn.valueHash)
		for _, child := range bn.children {
			if child != nil {
				s += fmt.Sprintf("n%p -> n%p;\n", tn, child)
			}
		}
		for childrenIndex, ch := range bn.children {
			if ch != nil {
				s += mt.dotGraph(ch, append(path, byte(childrenIndex)))
			}
		}
		return s
	default:
		return ""
	}
}
