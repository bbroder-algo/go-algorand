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
	"runtime/debug"
)

var debugTrie = false

// Trie nodes

type dbKey []byte

type dbnode interface {
	getDBKey() dbKey         // 0x00 || key || nodehash, 0x01 || key || nodehash (0x00 indicates a full path, 0x01 indicates a half path)
	getKey() *nibbles        // the key of the node in the trie
	getHash() *crypto.Digest // the hash of the node, if it has been hashed
}

type node interface {
	dbnode

	descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error)
	descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error)
	descendHash() error
	descendHashCommit(b *pebble.Batch) error
	serialize() ([]byte, error)
}
type DBNode struct {
	key  *nibbles
	hash *crypto.Digest
}

func (dbn *DBNode) serialize() ([]byte, error) {
	return nil, errors.New("DBNode cannot be serialized")
}

func (dbn *DBNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	dbn.key = &pathKey
	n, err := mt.getNode(dbn)
	if err != nil {
		return nil, err
	}
	return n.descendAdd(mt, pathKey, remainingKey, valueHash)
}
func (dbn *DBNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	dbn.key = &pathKey
	n, err := mt.getNode(dbn)
	if err != nil {
		return nil, false, err
	}
	return n.descendDelete(mt, pathKey, remainingKey)
}
func (dbn *DBNode) descendHashCommit(b *pebble.Batch) error {
	return nil // DBNodes are immutably hashed
}
func (dbn *DBNode) descendHash() error {
	return nil // DBNodes are immutably hashed
}
func (dbn *DBNode) getKey() *nibbles {
	return dbn.key
}
func (dbn *DBNode) getHash() *crypto.Digest {
	return dbn.hash
}
func (dbn *DBNode) getDBKey() dbKey {
	if dbn.hash == nil || dbn.key == nil {
		return nil
	}
	return makeDBKey(dbn.key, dbn.hash)
}
func (rn *RootNode) getKey() *nibbles {
	return rn.key
}
func (ln *LeafNode) getKey() *nibbles {
	return ln.key
}
func (en *ExtensionNode) getKey() *nibbles {
	return en.key
}
func (bn *BranchNode) getKey() *nibbles {
	return bn.key
}
func (rn *RootNode) getHash() *crypto.Digest {
	return rn.hash
}
func (ln *LeafNode) getHash() *crypto.Digest {
	return ln.hash
}
func (en *ExtensionNode) getHash() *crypto.Digest {
	return en.hash
}
func (bn *BranchNode) getHash() *crypto.Digest {
	return bn.hash
}

func makeDBKey(key *nibbles, hash *crypto.Digest) dbKey {
	stats.makedbkey++
	var buf bytes.Buffer

	p, half, err := key.pack()
	if err != nil {
		return nil
	}

	if half {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	buf.Write(p)
	buf.Write(hash.ToSlice())

	return buf.Bytes()
}
func (rn *RootNode) getDBKey() dbKey {
	// Key: 0x00 || path || nodehash
	// Key: 0x01 || path || nodehash
	// 0x00 indicates a full path, 0x01 indicates a half path
	// path is the nibble path to the node
	// nodehash is the hash of the node
	// The key is used for k/v storage unique to the path and node's hash

	if rn.hash == nil || rn.key == nil {
		return nil
	}
	return makeDBKey(rn.key, rn.hash)
}
func (ln *LeafNode) getDBKey() dbKey {
	if ln.hash == nil || ln.key == nil {
		return nil
	}
	return makeDBKey(ln.key, ln.hash)
}
func (en *ExtensionNode) getDBKey() dbKey {
	if en.hash == nil || en.key == nil {
		return nil
	}
	return makeDBKey(en.key, en.hash)
}
func (bn *BranchNode) getDBKey() dbKey {
	if bn.hash == nil || bn.key == nil {
		return nil
	}
	return makeDBKey(bn.key, bn.hash)
}

type RootNode struct {
	child node
	key   *nibbles
	hash  *crypto.Digest
}
type LeafNode struct {
	keyEnd    nibbles
	valueHash crypto.Digest
	key       *nibbles
	hash      *crypto.Digest
}
type ExtensionNode struct {
	sharedKey nibbles
	child     node
	key       *nibbles
	hash      *crypto.Digest
}
type BranchNode struct {
	children  [16]node
	valueHash crypto.Digest
	key       *nibbles
	hash      *crypto.Digest
}

func makeDBNode(hash *crypto.Digest) *DBNode {
	stats.makedbnodes++
	return &DBNode{key: nil, hash: hash}
}
func makeRootNode(child node) *RootNode {
	stats.makeroots++
	return &RootNode{child: child, key: &nibbles{}, hash: nil}
}
func makeLeafNode(keyEnd nibbles, valueHash crypto.Digest, key nibbles) *LeafNode {
	stats.makeleaves++
	return &LeafNode{keyEnd: keyEnd, valueHash: valueHash, key: &key}
}
func makeExtensionNode(sharedKey nibbles, child node, key nibbles) *ExtensionNode {
	stats.makeextensions++
	return &ExtensionNode{sharedKey: sharedKey, child: child, key: &key, hash: nil}
}
func makeBranchNode(children [16]node, valueHash crypto.Digest, key nibbles) *BranchNode {
	stats.makebranches++
	return &BranchNode{children: children, valueHash: valueHash, key: &key, hash: nil}
}

type Trie struct {
	db     *pebble.DB
	parent *Trie
	root   node
	sets   map[string]node
	dels   map[string]node
	gets   map[string]node
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
	dbgets         int
	dbdeletes      int
	cryptohashes   int
	makeroots      int
	makeleaves     int
	makeextensions int
	makebranches   int
	makedbnodes    int
	makedbkey      int
	newrootnode    int
}

var stats triestats

func (s triestats) String() string {
	return fmt.Sprintf("dbgets: %d, dbdeletes: %d, cryptohashes: %d, makeroots: %d, makeleaves: %d, makeextensions: %d, makebranches: %d, makedbnodes: %d, makedbkey: %d, newrootnode: %d",
		s.dbgets, s.dbdeletes, s.cryptohashes, s.makeroots, s.makeleaves, s.makeextensions, s.makebranches, s.makedbnodes, s.makedbkey, s.newrootnode)
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
	err := mt.root.descendHash()
	if err != nil {
		return crypto.Digest{}, err
	}

	return *(mt.root.getHash()), nil
}

func (mt *Trie) ClearPending() {
	mt.sets = make(map[string]node)
	mt.dels = make(map[string]node)
	mt.gets = make(map[string]node)
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
	mt.root.descendHash()
	//    mt.ClearPending()
	return nil

	if mt.parent != nil {
		mt.merge()
		return mt.parent.Commit()
	}

	b := mt.db.NewBatch()
	if mt.root != nil {
		//		err := mt.descendHashCommit(mt.root, b)
		//		if err != nil {
		//			return err
		//		}
	}
	for k := range mt.dels {
		options := &pebble.WriteOptions{}
		err := b.Delete([]byte(k), options)
		if err != nil {
			return err
		}
	}
	//err := b.SyncWait()
	options := &pebble.WriteOptions{}
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
		stats.newrootnode++
		mt.root = makeRootNode(nil)
		mt.addNode(mt.root)
	}

	stats.cryptohashes++
	replacement, err := mt.descendAdd(mt.root, []byte{0xcb}, key, crypto.Hash(value))
	if err == nil {
		stats.newrootnode++
		mt.root = replacement
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
	if len(key) > MaxKeyLength {
		return false, errors.New("key too long")
	}
	if mt.root == nil {
		return false, nil
	}

	replacement, found, err := mt.descendDelete(mt.root, []byte{0xcb}, key)
	if err == nil && found {
		mt.root = replacement
	}
	return found, err
}

// Trie node get/set/delete operations
func (mt *Trie) getNode(dbn dbnode) (node, error) {
	key := string(*dbn.getKey())
	if n, ok := mt.sets[key]; ok {
		return n, nil // return a new node that matches on key
	}
	dbKey := dbn.getDBKey()
	if dbKey != nil {
		if _, ok := mt.dels[string(dbKey)]; ok {
			return nil, nil // return nil if node is deleted
		}
		if n, ok := mt.gets[string(dbKey)]; ok {
			return n, nil // return a cached database node
		}

		stats.dbgets++

		dbbytes, closer, err := mt.db.Get(dbKey)
		if err != nil {
			panic(err)
		}
		defer closer.Close()
		trieNode, err := deserializeNode(dbbytes, *dbn.getKey())
		if err != nil {
			panic(err)
		}

		mt.gets[string(dbKey)] = trieNode
		return trieNode, nil
	}
	return nil, nil
}
func (mt *Trie) delNode(n node) {
	//	delete(mt.sets, string(*n.getKey()))
	//	dbKey := n.getDBKey()
	//	if dbKey != nil {
	//		delete(mt.gets, string(dbKey))
	//		mt.dels[string(dbKey)] = n
	//	}
	if _, ok := mt.sets[string(*n.getKey())]; ok {
		delete(mt.sets, string(*n.getKey()))
	} else {
		//        panic("wtf")
		dbKey := n.getDBKey()
		if dbKey != nil {
			delete(mt.gets, string(dbKey))
			mt.dels[string(dbKey)] = n
		}
	}
}
func (mt *Trie) addNode(n node) {
	// this is a new node that does not have a hash set but does have a trie key.
	key := string(*n.getKey())
	mt.sets[key] = n
}

// Trie descendAdd descends down the trie, adding the valueHash to the node at the end of the key.
// It returns the hash of the replacement node, or an error.
func (mt *Trie) descendAdd(n node, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	replacement, err := n.descendAdd(mt, pathKey, remainingKey, valueHash)
	if err == nil {
		mt.delNode(n)
		mt.addNode(replacement)
	}
	return replacement, err
}

// Trie descendDelete descends down the trie, deleting the valueHash to the node at the end of the key.
func (mt *Trie) descendDelete(n node, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	replacement, found, err := n.descendDelete(mt, pathKey, remainingKey)
	if found && err == nil {
		mt.delNode(n)
		mt.addNode(replacement)
	}
	return replacement, found, err
}

// Node methods for adding a new key-value to the trie
func (rn *RootNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	var err error
	if rn.child == nil {
		// Root node with a blank crypto digest in the child.  Make a leaf node.
		n := makeLeafNode(remainingKey, valueHash, pathKey)
		mt.addNode(n)
		return makeRootNode(n), nil
	}
	replacementChild, err := mt.descendAdd(rn.child, pathKey, remainingKey, valueHash)
	if err != nil {
		return nil, err
	}
	mt.addNode(replacementChild)
	return makeRootNode(replacementChild), nil
}

func (bn *BranchNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	if len(remainingKey) == 0 {
		// If we're here, then set the value hash in this node, overwriting the old one.
		return makeBranchNode(bn.children, valueHash, pathKey), nil
	}

	// Otherwise, shift out the first nibble and check the children for it.
	shifted := shiftNibbles(remainingKey, 1)
	if bn.children[remainingKey[0]] == nil {
		// nil children are available.
		bn.children[remainingKey[0]] = makeLeafNode(shifted, valueHash, append(pathKey, remainingKey[0]))
	} else {
		// Not available.  Descend down the branch.
		replacement, err := mt.descendAdd(bn.children[remainingKey[0]], append(pathKey, remainingKey[0]), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		bn.children[remainingKey[0]] = replacement
	}
	mt.addNode(bn.children[remainingKey[0]])

	return makeBranchNode(bn.children, bn.valueHash, pathKey), nil
}

func (ln *LeafNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	if equalNibbles(ln.keyEnd, remainingKey) {
		// The two keys are the same. Replace the value.
		return makeLeafNode(remainingKey, valueHash, pathKey), nil
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
		branchHash = ln.valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
		// in the branch node.
		ln := makeLeafNode(shiftNibbles(shiftedLn1, 1), ln.valueHash, append(append(pathKey, shNibbles...), shiftedLn1[0]))
		mt.addNode(ln)
		children[shiftedLn1[0]] = ln
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
		ln := makeLeafNode(shiftNibbles(shiftedLn2, 1), valueHash, append(append(pathKey, shNibbles...), shiftedLn2[0]))
		mt.addNode(ln)
		children[shiftedLn2[0]] = ln
	}
	bn2 := makeBranchNode(children, branchHash, append(pathKey, shNibbles...))
	if len(shNibbles) >= 2 {
		// If there was more than one shared nibble, insert an extension node before the branch node.
		mt.addNode(bn2)
		return makeExtensionNode(shNibbles, bn2, pathKey), nil
	}
	if len(shNibbles) == 1 {
		// If there is only one shared nibble, we just make a second branch node as opposed to an
		// extension node with only one shared nibble, the chances are high that we'd have to just
		// delete that node and replace it with a full branch node soon anyway.
		mt.addNode(bn2)
		var children2 [16]node
		children2[shNibbles[0]] = bn2
		return makeBranchNode(children2, crypto.Digest{}, pathKey), nil
	}
	// There are no shared nibbles anymore, so just return the branch node.
	return bn2, nil
}

func (en *ExtensionNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	// Calculate the shared nibbles between the key we're adding and this extension node.
	shNibbles := sharedNibbles(en.sharedKey, remainingKey)
	if len(shNibbles) == len(en.sharedKey) {
		// The entire extension node is shared.  descend.
		shifted := shiftNibbles(remainingKey, len(shNibbles))
		replacement, err := mt.descendAdd(en.child, append(pathKey, shNibbles...), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		mt.addNode(replacement)
		return makeExtensionNode(shNibbles, replacement, pathKey), nil
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
		en2 := makeExtensionNode(shifted2, en.child, append(pathKey, shNibbles...))
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
		shifted2 := shiftNibbles(shifted, 1)
		// we know this slot will be empty because it's the first nibble that differed from the
		// only other occupant in the child arrays, the one that leads to the extension node's child.
		ln := makeLeafNode(shifted2, valueHash, append(append(pathKey, shNibbles...), shifted[0]))
		mt.addNode(ln)
		children[shifted[0]] = ln
	} else {
		// if the key is no more, store it in the branch node's value hash slot.
		branchHash = valueHash
	}

	replacement := makeBranchNode(children, branchHash, append(pathKey, shNibbles...))
	// the shared bits of the extension node get smaller
	if len(shNibbles) > 0 {
		// still some shared key left, store them in an extension node
		// and point in to the new branch node
		mt.addNode(replacement)
		return makeExtensionNode(shNibbles, replacement, pathKey), nil
	}
	// or else there there is no shared key left, and the extension node is destroyed.
	return replacement, nil
}

// Node methods for deleting a key from the trie
func (rn *RootNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	replacementChild, found, err := mt.descendDelete(rn.child, pathKey, remainingKey)
	if err != nil {
		return nil, false, err
	}
	if found {
		mt.addNode(replacementChild)
		return makeRootNode(replacementChild), true, nil
	}
	return nil, false, err
}

func (ln *LeafNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	return nil, equalNibbles(remainingKey, ln.keyEnd), nil
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
		replacementChild, found, err := mt.descendDelete(en.child, append(pathKey, shNibbles...), shifted)
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

func (bn *BranchNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	if len(remainingKey) == 0 {
		if (bn.valueHash == crypto.Digest{}) {
			// valueHash is empty -- key not found.
			return nil, false, nil
		}
		// delete this branch's value hash. reset the value to the empty hash.
		// update the branch node if there are children, or remove it completely.
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil {
				return makeBranchNode(bn.children, crypto.Digest{}, pathKey), true, nil
			}
		}
		// no children, so just return a nil node
		return nil, true, nil
	}
	// descend into the branch node.
	if bn.children[remainingKey[0]] == nil {
		// no child at this index.  key not found.
		return nil, false, nil
	}
	shifted := shiftNibbles(remainingKey, 1)
	replacementChild, found, err := mt.descendDelete(bn.children[remainingKey[0]], append(pathKey, remainingKey[0]), shifted)
	if found && err == nil {
		bn.children[remainingKey[0]] = replacementChild
		mt.addNode(replacementChild)
		return makeBranchNode(bn.children, bn.valueHash, pathKey), true, nil
	}
	return nil, false, err
}

// //////////////
func (rn *RootNode) descendHashCommit(b *pebble.Batch) error {
	if rn.hash == nil {
		if rn.child != nil && rn.child.getHash() == nil {
			err := rn.child.descendHashCommit(b)
			if err != nil {
				return err
			}
		}
		bytes, err := rn.serialize()
		if err == nil {
			stats.cryptohashes++
			rn.hash = new(crypto.Digest)
			*rn.hash = crypto.Hash(bytes)
		}
		options := &pebble.WriteOptions{}
		return b.Set(rn.getDBKey(), bytes, options)
	}
	return nil
}
func (ln *LeafNode) descendHashCommit(b *pebble.Batch) error {
	if ln.hash == nil {
		bytes, err := ln.serialize()
		if err == nil {
			stats.cryptohashes++
			ln.hash = new(crypto.Digest)
			*ln.hash = crypto.Hash(bytes)
		}
		options := &pebble.WriteOptions{}
		return b.Set(ln.getDBKey(), bytes, options)
	}
	return nil
}
func (en *ExtensionNode) descendHashCommit(b *pebble.Batch) error {
	if en.hash == nil {
		if en.child != nil && en.child.getHash() == nil {
			err := en.child.descendHashCommit(b)
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
		return b.Set(en.getDBKey(), bytes, options)
	}
	return nil
}
func (bn *BranchNode) descendHashCommit(b *pebble.Batch) error {
	if bn.hash == nil {
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil && bn.children[i].getHash() == nil {
				err := bn.children[i].descendHashCommit(b)
				if err != nil {
					return err
				}
			}
		}
		bytes, err := bn.serialize()
		if err == nil {
			stats.cryptohashes++
			bn.hash = new(crypto.Digest)
			*bn.hash = crypto.Hash(bytes)
		}
		options := &pebble.WriteOptions{}
		return b.Set(bn.getDBKey(), bytes, options)
	}
	return nil
}

// /////////////////
func (rn *RootNode) descendHash() error {
	if rn.hash == nil {
		if rn.child != nil && rn.child.getHash() == nil {
			err := rn.child.descendHash()
			if err != nil {
				return err
			}
		}
		bytes, err := rn.serialize()
		if err == nil {
			stats.cryptohashes++
			rn.hash = new(crypto.Digest)
			*rn.hash = crypto.Hash(bytes)
		}
		return err
	}
	return nil
}
func (ln *LeafNode) descendHash() error {
	if ln.hash == nil {
		bytes, err := ln.serialize()
		if err == nil {
			stats.cryptohashes++
			ln.hash = new(crypto.Digest)
			*ln.hash = crypto.Hash(bytes)
		}
		return err
	}
	return nil
}
func (en *ExtensionNode) descendHash() error {
	if en.hash == nil {
		if en.child != nil && en.child.getHash() == nil {
			err := en.child.descendHash()
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
		return err
	}
	return nil
}
func (bn *BranchNode) descendHash() error {
	if bn.hash == nil {
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil && bn.children[i].getHash() == nil {
				err := bn.children[i].descendHash()
				if err != nil {
					return err
				}
			}
		}
		bytes, err := bn.serialize()
		if err == nil {
			stats.cryptohashes++
			bn.hash = new(crypto.Digest)
			*bn.hash = crypto.Hash(bytes)
		}
		return err
	}
	return nil
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

	ch := new(crypto.Digest)
	copy(ch[:], data[1:33])
	var child node
	if *ch != (crypto.Digest{}) {
		child = makeDBNode(ch)
	}
	return makeRootNode(child), nil
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
		child = makeDBNode(hash)
	}
	return makeExtensionNode(sharedKey, child, key), nil
}
func deserializeBranchNode(data []byte, key nibbles) (*BranchNode, error) {
	if data[0] != 5 {
		return nil, errors.New("invalid prefix for branch node")
	}
	if len(data) < 545 {
		return nil, errors.New("data too short to be a branch node")
	}

	var children [16]node
	for i := 0; i < 16; i++ {
		hash := new(crypto.Digest)
		copy(hash[:], data[1+i*32:33+i*32])
		if *hash != (crypto.Digest{}) {
			children[i] = makeDBNode(hash)
		}
	}
	value_hash := crypto.Digest(data[513:545])
	return makeBranchNode(children, value_hash, key), nil
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
	return makeLeafNode(keyEnd, crypto.Digest(data[1:33]), key), nil
}
func deserializeNode(nbytes []byte, key nibbles) (node, error) {
	if len(nbytes) == 0 {
		debug.PrintStack()
		return nil, fmt.Errorf("empty node")
	}
	switch nbytes[0] {
	case 0:
		return deserializeRootNode(nbytes)
	case 1, 2:
		return deserializeExtensionNode(nbytes, key)
	case 3, 4:
		return deserializeLeafNode(nbytes, key)
	case 5:
		return deserializeBranchNode(nbytes, key)
	default:
		return nil, fmt.Errorf("unknown node type")
	}
}

func (rn *RootNode) serialize() ([]byte, error) {
	data := make([]byte, 33)
	data[0] = 0
	if rn.child != nil {
		copy(data[1:33], rn.child.getHash()[:])
	}
	return data, nil
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
func (bn *BranchNode) serialize() ([]byte, error) {
	data := make([]byte, 545)
	data[0] = 5

	for i := 0; i < 16; i++ {
		if bn.children[i] != nil {
			copy(data[1+i*32:33+i*32], bn.children[i].getHash()[:])
		}
	}
	copy(data[513:545], bn.valueHash[:])
	return data, nil
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

// Make a dot graph of the trie
func (mt *Trie) DotGraph(keysAdded [][]byte, valuesAdded [][]byte) string {
	var keys string
	for i := 0; i < len(keysAdded); i++ {
		keys += fmt.Sprintf("%x = %x\\n", keysAdded[i], valuesAdded[i])
	}
	fmt.Printf("root: %v\n", mt.root)
	return fmt.Sprintf("digraph trie { key [shape=box, label=\"key/value inserted:\\n%s\"];\n %s }\n", keys, mt.dotGraph(mt.root, nibbles{}))
}
func (mt *Trie) dotGraph(n node, path nibbles) string {

	switch tn := n.(type) {
	case *DBNode:
		n2, _ := mt.getNode(tn)
		return mt.dotGraph(n2, path)
	case *RootNode:
		return fmt.Sprintf("n%p [label=\"root\" shape=box];\n", tn) +
			fmt.Sprintf("n%p -> n%p;\n", tn, tn.child) +
			mt.dotGraph(tn.child, path)
	case *LeafNode:
		ln := tn
		return fmt.Sprintf("n%p [label=\"leaf\\nkeyEnd:%x\\nvalueHash:%s\" shape=box];\n", tn, ln.keyEnd, ln.valueHash)
	case *ExtensionNode:
		en := tn
		return fmt.Sprintf("n%p [label=\"extension\\nshKey:%x\" shape=box];\n", tn, en.sharedKey) +
			fmt.Sprintf("n%p -> n%p;\n", en, en.child) +
			mt.dotGraph(en.child, append(path, en.sharedKey...))
	case *BranchNode:
		bn := tn
		var indexesFilled string
		indexesFilled = "--"
		for i, child := range bn.children {
			if child != nil {
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
		for childrenIndex, child := range bn.children {
			if child != nil {
				s += mt.dotGraph(child, append(path, byte(childrenIndex)))
			}
		}
		return s
	default:
		return ""
	}
}
