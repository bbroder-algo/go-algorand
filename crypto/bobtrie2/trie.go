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
	"bytes"
	"errors"
	"fmt"
	"github.com/algorand/go-algorand/crypto"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"io"
	"runtime/debug"
)

var debugTrie = false

// Trie nodes
type nodehash struct {
	hash *crypto.Digest
	path *nibbles
}

func (nh *nodehash) key(path nibbles) ([]byte, error) {
	if nh.hash == nil {
		return nil, errors.New("nil hash")
	}
	return nh.hash.ToSlice(), nil

	//	if nh.path == nil {
	//        panic(fmt.Sprintf("nil path: %s", debug.Stack()))
	////		return []byte{}, errors.New("trying to get the database key of a node without a path")
	//	}

	var buf bytes.Buffer

	p, half, err := path.pack()
	if err != nil {
		return nil, err
	}

	if half {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	buf.Write(p)
	buf.Write(nh.hash.ToSlice())

	return buf.Bytes(), nil
}

func (nh *nodehash) setHash(bytes []byte) {
	stats.cryptohashes++
	nh.hash = new(crypto.Digest)
	*nh.hash = crypto.Hash(bytes)
}

type node interface {
	descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (*nodehash, error)
	descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (*nodehash, bool, error)
	hash(mt *Trie, pathKey nibbles, nh *nodehash) ([]byte, error)
	set(mt *Trie, fullpath nibbles) *nodehash
}

type RootNode struct {
	child *nodehash
}
type LeafNode struct {
	keyEnd    nibbles
	valueHash crypto.Digest
}

type ExtensionNode struct {
	sharedKey nibbles
	child     *nodehash
}

type BranchNode struct {
	children  [16]*nodehash
	valueHash crypto.Digest
}

func makeRootNode(child *nodehash) *RootNode {
	return &RootNode{child: child}
}
func makeLeafNode(keyEnd nibbles, valueHash crypto.Digest) *LeafNode {
	return &LeafNode{keyEnd: keyEnd, valueHash: valueHash}
}
func makeExtensionNode(sharedKey nibbles, child *nodehash) *ExtensionNode {
	return &ExtensionNode{sharedKey: sharedKey, child: child}
}
func makeBranchNode(children [16]*nodehash, valueHash crypto.Digest) *BranchNode {
	return &BranchNode{children: children, valueHash: valueHash}
}

type Trie struct {
	db     *pebble.DB
	root   *nodehash
	parent *Trie
	sets   map[*nodehash]node
	gets   map[*nodehash]node
	dels   map[*nodehash]nibbles
}

// MakeTrie creates a merkle trie
func MakeTrie() (*Trie, error) {
	var db *pebble.DB
	var err error
	if false {
		fmt.Printf("mem db")
		db, err = pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	} else {
		dbdir := "/tmp/blahdb/"
		fmt.Printf("disk db: %s\n", dbdir)
		db, err = pebble.Open(dbdir, &pebble.Options{})
	}
	if err != nil {
		return nil, err
	}
	mt := &Trie{db: db}
	mt.ClearPending()

	return mt, nil
}

func (mt *Trie) child() *Trie {
	ch := &Trie{root: mt.root, parent: mt}
	ch.ClearPending()
	return ch
}

const (
	MaxKeyLength = 65535
)

type triestats struct {
	extensionNodes int
	branchNodes    int
	dbgets         int
	leafnodesets   int
	branchnodesets int
	extnodesets    int
	rootnodesets   int
	dbdeletes      int
	cryptohashes   int
}

var stats triestats

func (s triestats) String() string {
	return fmt.Sprintf("extensionNodes: %d, branchNodes: %d, dbgets: %d, leafnodesets: %d, branchnodesets: %d, extnodesets: %d, rootnodesets: %d, dbdeletes: %d, cryptohashes: %d",
		s.extensionNodes, s.branchNodes, s.dbgets, s.leafnodesets, s.branchnodesets, s.extnodesets, s.rootnodesets, s.dbdeletes, s.cryptohashes)
}

// nibbles are 4-bit values stored in an 8-bit byte
type nibbles []byte

// Pack/unpack compact the 8-bit nibbles into 4 high bits and 4 low bits.
// half indicates if the last byte of the returned array is a full byte or
// only the high 4 bits are included.
func unpack(data []byte, half bool) (nibbles, error) {
	var ns nibbles
	if half {
		ns = make([]byte, len(data)*2-1)
	} else {
		ns = make([]byte, len(data)*2)
	}

	half = false
	j := 0
	for i := 0; i < len(ns); i++ {
		half = !half
		if half {
			ns[i] = data[j] >> 4
		} else {
			ns[i] = data[j] & 15
			j++
		}
	}
	return ns, nil
}
func (ns *nibbles) pack() ([]byte, bool, error) {
	var data []byte
	half := false
	j := 0
	for i := 0; i < len(*ns); i++ {
		if (*ns)[i] > 15 {
			return nil, false, errors.New("nibbles can't contain values greater than 15")
		}
		half = !half
		if half {
			data = append(data, (*ns)[i]<<4)
		} else {
			data[j] = data[j] | (*ns)[i]
			j++
		}
	}

	return data, half, nil
}

// nibble utilities
func equalNibbles(a nibbles, b nibbles) bool {
	return bytes.Equal(a, b)
}

func shiftNibbles(a nibbles, numNibbles int) nibbles {
	if numNibbles <= 0 {
		return a
	}
	if numNibbles > len(a) {
		return nibbles{}
	}

	return a[numNibbles:]
}

func sharedNibbles(arr1 nibbles, arr2 nibbles) nibbles {
	minLength := len(arr1)
	if len(arr2) < minLength {
		minLength = len(arr2)
	}
	shared := nibbles{}
	for i := 0; i < minLength; i++ {
		if arr1[i] == arr2[i] {
			shared = append(shared, arr1[i])
		} else {
			break
		}
	}
	return shared
}

// Provide the root hash for this trie
func (mt *Trie) RootHash() (crypto.Digest, error) {
	if mt.root == nil {
		return crypto.Digest{}, nil
	}
	err := mt.descendHash(mt.root, nibbles{})
	if err != nil {
		return crypto.Digest{}, err
	}

	return *(mt.root.hash), nil
}

func (mt *Trie) ClearPending() {
	mt.sets = make(map[*nodehash]node)
	mt.gets = make(map[*nodehash]node)
	mt.dels = make(map[*nodehash]nibbles)
	// mt.pendingRoot = nil
}
func (mt *Trie) merge() error {
	if mt.parent == nil {
		return nil
	}
	mt.parent.root = mt.root
	for k, v := range mt.sets {
		mt.parent.sets[k] = v
	}
	for k, v := range mt.gets {
		mt.parent.gets[k] = v
	}
	for k, v := range mt.dels {
		mt.parent.dels[k] = v
	}
	return nil
}

func (mt *Trie) Commit() error {
	if mt.parent != nil {
		mt.merge()
		return mt.parent.Commit()
	}

	b := mt.db.NewBatch()
	options := &pebble.WriteOptions{}
	for nh, n := range mt.sets {
		//		err := mt.db.Set(k[:], v, pebble.Sync)
		if nh.path == nil {
			return errors.New("nil path")
		}
		v, err := n.hash(mt, *(nh.path), nh)
		if err != nil {
			return err
		}

		key, err := nh.key(*nh.path)
		if err != nil {
			return err
		}

		err = b.Set(key, v, options)
		if err != nil {
			return err
		}

	}
	for nh, path := range mt.dels {
		key, err := nh.key(path)
		err = b.Delete(key, options)
		if err != nil {
			return err
		}
	}
	//err := b.SyncWait()
	err := mt.db.Apply(b, options)
	if err != nil {
		return err
	}
	err = b.Close()
	if err != nil {
		return err
	}

	mt.ClearPending()

	return nil
}

// Trie Add adds the given key/value pair to the trie.
func (mt *Trie) Add(key nibbles, value []byte) (err error) {
	if debugTrie {
		fmt.Printf("Add %v %v\n", key, value)
	}

	if len(key) == 0 {
		return errors.New("empty key not allowed")
	}

	if len(key) > MaxKeyLength {
		return errors.New("key too long")
	}

	if mt.root == nil {
		mt.root = makeRootNode(nil).set(mt, nil)
	}

	stats.cryptohashes++
	nh, err := mt.descendAdd(mt.root, []byte{}, key, crypto.Hash(value))
	if err == nil {
		mt.root = nh
	}
	return err
}

// Delete deletes the given key from the trie, if such element exists.
// if no such element exists, return false
func (mt *Trie) Delete(key nibbles) (bool, error) {
	var err error
	if len(key) == 0 {
		return false, errors.New("empty key not allowed")
	}
	if mt.root == nil {
		return false, nil
	}

	nh, found, err := mt.descendDelete(mt.root, []byte{}, key)
	if err == nil && found {
		mt.root = nh
	}
	return found, err
}

// Trie node get/set/delete operations
func (mt *Trie) get(nh *nodehash, pathKey nibbles) (node, error) {
	if debugTrie {
		fmt.Printf("get %v %v\n", nh, pathKey)
	}
	if nh == nil {
		return nil, nil
	}

	var n node
	var ok bool
	var err error
	if n, ok = mt.sets[nh]; !ok {
		if _, ok = mt.dels[nh]; ok {
			return nil, errors.New(fmt.Sprintf("nodehash %v deleted", nh))
		}
		if n, ok = mt.gets[nh]; !ok {
			if mt.parent != nil {
				return mt.parent.get(nh, pathKey)
			}
			if nh.hash == nil {
				return nil, errors.New(fmt.Sprintf("db lookup of nodehash %v with empty hash", nh))
			}

			stats.dbgets++
			var key []byte
			key, err = nh.key(pathKey)
			if err != nil {
				return nil, err
			}

			var closer io.Closer
			var dbbytes []byte
			dbbytes, closer, err = mt.db.Get(key)
			if err != nil {
				fmt.Printf("key: %v\n", key)
				fmt.Printf("pathKey: %v\n", pathKey)
				fmt.Printf("nh.hash %v\n", nh.hash)
				return nil, err
			}
			defer closer.Close()
			n, err = deserializeNode(dbbytes)
			if err != nil {
				return nil, err
			}
			mt.gets[nh] = n
		}
	}
	return n, nil
}
func (mt *Trie) del(nh *nodehash, pathKey nibbles) {
	if debugTrie {
		fmt.Printf("del %v\n", nh)
	}
	if nh == nil {
		return
	}
	delete(mt.gets, nh)
	if _, ok := mt.sets[nh]; ok {
		delete(mt.sets, nh)
	} else {
		mt.dels[nh] = pathKey
	}
}
func (mt *Trie) set(nh *nodehash, n node) {
	if debugTrie {
		fmt.Printf("set %v %v\n", nh, n)
	}
	if nh == nil {
		return
	}

	mt.sets[nh] = n
	delete(mt.dels, nh)
	delete(mt.gets, nh)
}

// Trie descendAdd descends down the trie, adding the valueHash to the node at the end of the key.
// It returns the hash of the replacement node, or an error.
func (mt *Trie) descendAdd(nh *nodehash, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (*nodehash, error) {
	if debugTrie {
		fmt.Printf("descendAdd %v %v %v %v\n", nh, pathKey, remainingKey, valueHash)
	}

	n, err := mt.get(nh, pathKey)
	if err != nil {
		return nil, err
	}
	newnh, err := n.descendAdd(mt, pathKey, remainingKey, valueHash)
	if err == nil && newnh != nh {
		//		mt.db.Delete([]byte(node.ToSlice()), pebble.NoSync)
		mt.del(nh, pathKey)
	}
	return newnh, err
}

// Trie descendDelete descends down the trie, deleting the valueHash to the node at the end of the key.
func (mt *Trie) descendDelete(nh *nodehash, pathKey nibbles, remainingKey nibbles) (*nodehash, bool, error) {
	n, err := mt.get(nh, pathKey)
	if err != nil {
		return nil, false, err
	}

	newnh, found, err := n.descendDelete(mt, pathKey, remainingKey)
	if found && newnh != nh && err == nil {
		//		mt.db.Delete([]byte(node.ToSlice()), pebble.NoSync)
		mt.del(nh, pathKey)
	}
	return newnh, found, err
}

func (mt *Trie) descendHash(nh *nodehash, pathKey nibbles) error {
	if nh == nil {
		return nil
	}
	if nh.hash == nil {
		n, err := mt.get(nh, pathKey)
		if err != nil {
			return err
		}
		_, err = n.hash(mt, pathKey, nh)
		if err != nil {
			return err
		}
	}
	return nil
}

// Node methods for adding a new key-value to the trie
func (rn *RootNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (*nodehash, error) {
	if debugTrie {
		fmt.Printf("RootNode descendAdd %v %v %v %v\n", rn.child, pathKey, remainingKey, valueHash)
	}

	var err error
	var nh *nodehash
	if rn.child == nil {
		// Root node with a blank crypto digest in the child.  Make a leaf node.
		nh = makeLeafNode(remainingKey, valueHash).set(mt, remainingKey)
	} else {
		nh, err = mt.descendAdd(rn.child, pathKey, remainingKey, valueHash)
		if err != nil {
			return nil, err
		}
	}
	nh = makeRootNode(nh).set(mt, nil)
	return nh, nil
}

func (bn *BranchNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (*nodehash, error) {
	if debugTrie {
		fmt.Printf("BranchNode descendAdd %v %v %v %v\n", bn.children, pathKey, remainingKey, valueHash)
	}
	if len(remainingKey) == 0 {
		// If we're here, then set the value hash in this node, overwriting the old one.
		return makeBranchNode(bn.children, valueHash).set(mt, pathKey), nil
	}

	// Otherwise, shift out the first nibble and check the children for it.
	shifted := shiftNibbles(remainingKey, 1)
	if bn.children[remainingKey[0]] == nil {
		// nil children are available.
		bn.children[remainingKey[0]] = makeLeafNode(shifted, valueHash).set(mt, append(pathKey, remainingKey[0]))
	} else {
		// Not available.  Descend down the branch.
		nh, err := mt.descendAdd(bn.children[remainingKey[0]], append(pathKey, remainingKey[0]), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		bn.children[remainingKey[0]] = nh
	}

	return makeBranchNode(bn.children, bn.valueHash).set(mt, pathKey), nil
}

func (ln *LeafNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (*nodehash, error) {
	if debugTrie {
		fmt.Printf("LeafNode descendAdd %x %x %x %x\n", ln.keyEnd, pathKey, remainingKey, valueHash)
	}
	if equalNibbles(ln.keyEnd, remainingKey) {
		// The two keys are the same. Replace the value.
		return makeLeafNode(remainingKey, valueHash).set(mt, append(pathKey, remainingKey...)), nil
	}

	// Calculate the shared nibbles between the leaf node we're on and the key we're inserting.
	shNibbles := sharedNibbles(ln.keyEnd, remainingKey)
	// Shift away the common nibbles from both the keys.
	shiftedLn1 := shiftNibbles(ln.keyEnd, len(shNibbles))
	shiftedLn2 := shiftNibbles(remainingKey, len(shNibbles))

	// Make a branch node.
	var children [16]*nodehash
	branchHash := crypto.Digest{}

	// If the existing leaf node has no more nibbles, then store it in the branch node's value slot.
	if len(shiftedLn1) == 0 {
		branchHash = ln.valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
		// in the branch node.
		children[shiftedLn1[0]] = makeLeafNode(shiftNibbles(shiftedLn1, 1), ln.valueHash).set(mt, append(pathKey, shiftedLn1[0]))
	}

	// Similarly, for our new insertion, if it has no more nibbles, store it in the branch node's value slot.
	if len(shiftedLn2) == 0 {
		if len(shiftedLn1) == 0 {
			// They can't both be empty, otherwise they would have been caught earlier in the equalNibbles check.
			return nil, fmt.Errorf("both keys are the same but somehow wasn't caught earlier")
		}
		branchHash = valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
		// in the branch node.
		children[shiftedLn2[0]] = makeLeafNode(shiftNibbles(shiftedLn2, 1), valueHash).set(mt, append(pathKey, shiftedLn2[0]))
	}
	nh := makeBranchNode(children, branchHash).set(mt, pathKey)
	if len(shNibbles) >= 2 {
		// If there was more than one shared nibble, insert an extension node before the branch node.
		return makeExtensionNode(shNibbles, nh).set(mt, pathKey), nil
	}
	if len(shNibbles) == 1 {
		// If there is only one shared nibble, we just make a second branch node as opposed to an
		// extension node with only one shared nibble, the chances are high that we'd have to just
		// delete that node and replace it with a full branch node soon anyway.
		var children2 [16]*nodehash
		children2[shNibbles[0]] = nh
		return makeBranchNode(children2, crypto.Digest{}).set(mt, append(pathKey, shNibbles[0])), nil
	}
	// There are no shared nibbles anymore, so just return the branch node.
	return nh, nil
}

func (en *ExtensionNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (*nodehash, error) {
	if debugTrie {
		fmt.Printf("ExtensionNode descendAdd %x %x %x %x\n", en.sharedKey, pathKey, remainingKey, valueHash)
	}
	// Calculate the shared nibbles between the key we're adding and this extension node.
	shNibbles := sharedNibbles(en.sharedKey, remainingKey)
	if len(shNibbles) == len(en.sharedKey) {
		// The entire extension node is shared.  descend.
		shifted := shiftNibbles(remainingKey, len(shNibbles))
		nh, err := mt.descendAdd(en.child, append(pathKey, shNibbles...), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		return makeExtensionNode(shNibbles, nh).set(mt, pathKey), nil
	}

	// we have to upgrade part or all of this extension node into a branch node.
	var children [16]*nodehash
	branchHash := crypto.Digest{}
	// what's left of the extension node shared key after removing the shared part gets
	// attached to the new branch node.
	shifted := shiftNibbles(en.sharedKey, len(shNibbles))
	if len(shifted) >= 2 {
		// if there's two or more nibbles left, make another extension node.
		shifted2 := shiftNibbles(shifted, 1)
		children[shifted[0]] = makeExtensionNode(shifted2, en.child).set(mt, nil)
	} else {
		// if there's only one nibble left, store the child in the branch node.
		// there can't be no nibbles left, or the earlier entire-node-shared case would have been triggered.
		children[shifted[0]] = en.child
	}

	//what's left of the new add remaining key gets put into the branch node bucket corresponding
	//with its first nibble, or into the valueHash if it's now empty.
	shifted = shiftNibbles(remainingKey, len(shNibbles))
	if len(shifted) > 0 {
		shifted2 := shiftNibbles(shifted, 1)
		// we know this slot will be empty because it's the first nibble that differed from the
		// only other occupant in the child arrays, the one that leads to the extension node's child.
		children[shifted[0]] = makeLeafNode(shifted2, valueHash).set(mt, append(append(pathKey, shNibbles...), shifted[0]))
	} else {
		// if the key is no more, store it in the branch node's value hash slot.
		branchHash = valueHash
	}
	nh := makeBranchNode(children, branchHash).set(mt, append(pathKey, shNibbles...))

	// the shared bits of the extension node get smaller
	if len(shNibbles) > 0 {
		// still some shared key left, store them in an extension node
		// and point in to the new branch node
		return makeExtensionNode(shNibbles, nh).set(mt, nil), nil
	}
	// or else there there is no shared key left, and the extension node is destroyed.
	return nh, nil
}

// Node methods for deleting a key from the trie
func (rn *RootNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (*nodehash, bool, error) {
	nh, found, err := mt.descendDelete(rn.child, pathKey, remainingKey)
	if err == nil && found {
		return makeRootNode(nh).set(mt, nil), true, nil
	}
	return nil, false, err
}

func (ln *LeafNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (*nodehash, bool, error) {
	return nil, equalNibbles(remainingKey, ln.keyEnd), nil
}

func (en *ExtensionNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (*nodehash, bool, error) {
	var err error
	if len(remainingKey) == 0 {
		// can't stop on an exension node
		return nil, false, fmt.Errorf("key too short")
	}
	shNibbles := sharedNibbles(remainingKey, en.sharedKey)
	if len(shNibbles) == len(en.sharedKey) {
		shifted := shiftNibbles(remainingKey, len(en.sharedKey))
		nh, found, err := mt.descendDelete(en.child, append(pathKey, shNibbles...), shifted)
		if err == nil && found && nh != nil {
			// the key was found below this node and deleted,
			// make a new extension node pointing to its replacement.
			nh = makeExtensionNode(en.sharedKey, nh).set(mt, nil)
		}
		// returns empty digest if there's nothing left.
		return nh, found, err
	}
	// didn't match the entire extension node.
	return nil, false, err
}

func (bn *BranchNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (*nodehash, bool, error) {
	if len(remainingKey) == 0 {
		if (bn.valueHash == crypto.Digest{}) {
			// valueHash is empty -- key not found.
			return nil, false, nil
		}
		// delete this branch's value hash. reset the value to the empty hash.
		// update the branch node if there are children, or remove it completely.
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil {
				return makeBranchNode(bn.children, crypto.Digest{}).set(mt, pathKey), true, nil
			}
		}
		// no children, so just return a nil *nodehash
		return nil, true, nil
	}
	// descend into the branch node.
	if bn.children[remainingKey[0]] == nil {
		// no child at this index.  key not found.
		return nil, false, nil
	}
	shifted := shiftNibbles(remainingKey, 1)
	nh, found, err := mt.descendDelete(bn.children[remainingKey[0]], append(pathKey, remainingKey[0]), shifted)
	if err == nil && found {
		bn.children[remainingKey[0]] = nh
		return makeBranchNode(bn.children, bn.valueHash).set(mt, pathKey), true, nil
	}
	return nil, false, err
}

func (rn *RootNode) hash(mt *Trie, pathKey nibbles, nh *nodehash) ([]byte, error) {
	err := mt.descendHash(rn.child, pathKey)
	if err != nil {
		return nil, err
	}
	bytes, err := serializeRootNode(rn)
	if err == nil {
		nh.setHash(bytes)
	}
	return bytes, err
}
func (ln *LeafNode) hash(mt *Trie, pathKey nibbles, nh *nodehash) ([]byte, error) {
	bytes, err := serializeLeafNode(ln)
	if err == nil {
		nh.setHash(bytes)
	}
	return bytes, err
}
func (en *ExtensionNode) hash(mt *Trie, pathKey nibbles, nh *nodehash) ([]byte, error) {
	err := mt.descendHash(en.child, append(pathKey, en.sharedKey...))
	if err != nil {
		return nil, err
	}
	bytes, err := serializeExtensionNode(en)
	if err == nil {
		nh.setHash(bytes)
	}
	return bytes, err
}
func (bn *BranchNode) hash(mt *Trie, pathKey nibbles, nh *nodehash) ([]byte, error) {
	for i := 0; i < 16; i++ {
		err := mt.descendHash(bn.children[i], append(pathKey, byte(i)))
		if err != nil {
			return nil, err
		}
	}

	bytes, err := serializeBranchNode(bn)
	if err == nil {
		nh.setHash(bytes)
	}
	return bytes, err
}

func (rn *RootNode) set(mt *Trie, fullpath nibbles) *nodehash {
	stats.rootnodesets++
	nh := &nodehash{}
	path := make(nibbles, len(fullpath))
	copy(path, fullpath)
	nh.path = &path
	mt.set(nh, rn)
	return nh
}
func (ln *LeafNode) set(mt *Trie, fullpath nibbles) *nodehash {
	stats.leafnodesets++
	nh := &nodehash{}
	path := make(nibbles, len(fullpath))
	copy(path, fullpath)
	nh.path = &path
	mt.set(nh, ln)
	return nh
}
func (en *ExtensionNode) set(mt *Trie, fullpath nibbles) *nodehash {
	stats.extnodesets++
	nh := &nodehash{}
	path := make(nibbles, len(fullpath))
	copy(path, fullpath)
	nh.path = &path
	mt.set(nh, en)
	return nh
}
func (bn *BranchNode) set(mt *Trie, fullpath nibbles) *nodehash {
	stats.branchnodesets++
	nh := &nodehash{}
	path := make(nibbles, len(fullpath))
	copy(path, fullpath)
	nh.path = &path
	mt.set(nh, bn)
	return nh
}

// Node serializers / deserializers
//
// prefix: 0 == root.  1 == extension, half.   2 == extension, full
//
//	3 == leaf, half.        4 == leaf, full
//	5 == branch
func deserializeRootNode(data []byte) (*RootNode, error) {
	if data[0] != 0 {
		return nil, errors.New("invalid prefix for root node")
	}
	hash := new(crypto.Digest)
	copy(hash[:], data[1:33])
	if *hash == (crypto.Digest{}) {
		return makeRootNode(nil), nil
	}
	return makeRootNode(&nodehash{hash: hash}), nil
}

func deserializeExtensionNode(data []byte) (*ExtensionNode, error) {
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
	if *hash == (crypto.Digest{}) {
		return nil, errors.New("child hash can't be empty in an extension node")
	}
	return makeExtensionNode(sharedKey, &nodehash{hash: hash}), nil
}
func deserializeBranchNode(data []byte) (*BranchNode, error) {
	if data[0] != 5 {
		return nil, errors.New("invalid prefix for branch node")
	}
	if len(data) < 545 {
		return nil, errors.New("data too short to be a branch node")
	}

	var children [16]*nodehash
	for i := 0; i < 16; i++ {
		hash := new(crypto.Digest)
		copy(hash[:], data[1+i*32:33+i*32])
		if *hash != (crypto.Digest{}) {
			children[i] = &nodehash{hash: hash}
		}
	}
	return makeBranchNode(children, crypto.Digest(data[513:545])), nil
}
func deserializeLeafNode(data []byte) (*LeafNode, error) {
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
	return makeLeafNode(keyEnd, crypto.Digest(data[1:33])), nil
}
func deserializeNode(nbytes []byte) (node, error) {
	if len(nbytes) == 0 {
		debug.PrintStack()
		return nil, fmt.Errorf("empty node")
	}
	switch nbytes[0] {
	case 0:
		return deserializeRootNode(nbytes)
	case 1, 2:
		return deserializeExtensionNode(nbytes)
	case 3, 4:
		return deserializeLeafNode(nbytes)
	case 5:
		return deserializeBranchNode(nbytes)
	default:
		return nil, fmt.Errorf("unknown node type")
	}
}

func serializeRootNode(rn *RootNode) ([]byte, error) {
	var childHash crypto.Digest
	childHash = crypto.Digest{}
	if rn.child != nil {
		childHash = *(rn.child.hash)
	}

	data := make([]byte, 33)
	data[0] = 0
	copy(data[1:33], childHash[:])
	return data, nil
}
func serializeExtensionNode(en *ExtensionNode) ([]byte, error) {
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

	childHash := crypto.Digest{}
	if en.child != nil {
		childHash = *(en.child.hash)
	}

	copy(data[1:33], childHash[:])
	copy(data[33:], pack)
	return data, nil
}
func serializeBranchNode(bn *BranchNode) ([]byte, error) {
	data := make([]byte, 545)
	data[0] = 5

	for i := 0; i < 16; i++ {
		var childHash crypto.Digest

		if bn.children[i] == nil {
			childHash = crypto.Digest{}
		} else {
			childHash = *(bn.children[i].hash)
		}
		copy(data[1+i*32:33+i*32], childHash[:])
	}
	copy(data[513:545], bn.valueHash[:])
	return data, nil
}
func serializeLeafNode(ln *LeafNode) ([]byte, error) {
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

// Make a dot graph of the trie
func (mt *Trie) DotGraph(keysAdded [][]byte, valuesAdded [][]byte) string {
	var keys string
	for i := 0; i < len(keysAdded); i++ {
		keys += fmt.Sprintf("%x = %x\\n", keysAdded[i], valuesAdded[i])
	}
	fmt.Printf("root: %v\n", mt.root)
	return fmt.Sprintf("digraph trie { key [shape=box, label=\"key/value inserted:\\n%s\"];\n %s }\n", keys, mt.dotGraph(mt.root, nibbles{}))
}
func (mt *Trie) dotGraph(nh *nodehash, path nibbles) string {
	var err error
	node, err := mt.get(nh, path)
	//	nbytes, closer, err := mt.db.Get([]byte(hash.ToSlice()))
	//	defer closer.Close()
	if err != nil {
		return ""
	}

	switch n := node.(type) {
	case *RootNode:
		return fmt.Sprintf("n%p [label=\"root\" shape=box];\n", nh) +
			fmt.Sprintf("n%p -> n%p;\n", nh, n.child) +
			mt.dotGraph(n.child, path)
	case *LeafNode:
		ln := n
		return fmt.Sprintf("n%p [label=\"leaf\\nkeyEnd:%x\\nvalueHash:%s\" shape=box];\n", nh, ln.keyEnd, ln.valueHash)
	case *ExtensionNode:
		en := n
		return fmt.Sprintf("n%p [label=\"extension\\nshKey:%x\" shape=box];\n", nh, en.sharedKey) +
			fmt.Sprintf("n%p -> n%p;\n", nh, n.child) +
			mt.dotGraph(n.child, append(path, en.sharedKey...))
	case *BranchNode:
		bn := n
		var indexesFilled string
		indexesFilled = "--"
		for i, child := range bn.children {
			if child != nil {
				indexesFilled += fmt.Sprintf("%x ", i)
			}
		}
		indexesFilled += "--"

		s := fmt.Sprintf("n%p [label=\"branch\\nindexesFilled:%s\\nvalueHash:%s\" shape=box];\n", nh, indexesFilled, bn.valueHash)
		for _, child := range n.children {
			if child != nil {
				s += fmt.Sprintf("n%p -> n%p;\n", nh, child)
			}
		}
		for childrenIndex, child := range n.children {
			if child != nil {
				s += mt.dotGraph(child, append(path, byte(childrenIndex)))
			}
		}
		return s
	default:
		return ""
	}
}
