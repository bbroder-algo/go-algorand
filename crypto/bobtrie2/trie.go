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
	"os"
	"runtime"
	"runtime/debug"
)

var debugTrie = false

// Trie nodes

type dbKey []byte

type dbnode interface {
	getDBKey() dbKey         // nibble key
	getKey() *nibbles        // the key of the node in the trie
	getHash() *crypto.Digest // the hash of the node, if it has been hashed
}

type node interface {
	dbnode

	descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error)
	descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error)
	descendHash() error
	descendHashWithCommit(b *pebble.Batch) error
	serialize() ([]byte, error)
}
type DBNode struct {
	key  *nibbles
	hash *crypto.Digest
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

func makeDBNode(hash *crypto.Digest, key nibbles) *DBNode {
	stats.makedbnodes++
	dbn := &DBNode{hash: hash}
	copyKey := make(nibbles, len(key))
	copy(copyKey, key)
	dbn.key = &copyKey
	return dbn
}
func makeRootNode(child node) *RootNode {
	stats.makeroots++
	return &RootNode{child: child, key: &nibbles{}}
}
func makeLeafNode(keyEnd nibbles, valueHash crypto.Digest, key nibbles) *LeafNode {
	stats.makeleaves++
	ln := &LeafNode{keyEnd: keyEnd, valueHash: valueHash}
	copyKey := make(nibbles, len(key))
	copy(copyKey, key)
	ln.key = &copyKey
	return ln
}
func makeExtensionNode(sharedKey nibbles, child node, key nibbles) *ExtensionNode {
	stats.makeextensions++
	en := &ExtensionNode{sharedKey: sharedKey, child: child}
	copyKey := make(nibbles, len(key))
	copy(copyKey, key)
	en.key = &copyKey
	return en
}
func makeBranchNode(children [16]node, valueHash crypto.Digest, key nibbles) *BranchNode {
	stats.makebranches++
	bn := &BranchNode{children: children, valueHash: valueHash}
	copyKey := make(nibbles, len(key))
	copy(copyKey, key)
	bn.key = &copyKey
	return bn
}

type Trie struct {
	db     *pebble.DB
	parent *Trie
	root   node
	dels   map[string]node
}

// MakeTrie creates a merkle trie
func MakeTrie() (*Trie, error) {
	var db *pebble.DB
	var err error
	if true {
		fmt.Println("mem db")
		db, err = pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	} else {
		dbdir := "/Users/bobbroderick/algorand/go-algorand/crypto/bobtrie2/pebbledb"
		if _, err := os.Stat(dbdir); os.IsNotExist(err) {
			fmt.Printf("creating dbdir: %s\n", dbdir)
			os.Mkdir(dbdir, 0755)
		}

		fmt.Printf("disk db: %s\n", dbdir)
		db, err = pebble.Open(dbdir, &pebble.Options{})
	}
	if err != nil {
		return nil, err
	}
	mt := &Trie{db: db}
	mt.dels = make(map[string]node)

	return mt, nil
}

func (mt *Trie) child() *Trie {
	ch := &Trie{root: mt.root, parent: mt}
	ch.dels = make(map[string]node)
	return ch
}

const (
	MaxKeyLength = 65535
)

type triestats struct {
	dbsets         int
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
	addnode        int
	delnode        int
	getnode        int
}

var stats triestats

func (s triestats) String() string {
	return fmt.Sprintf("dbsets: %d, dbgets: %d, dbdeletes: %d, cryptohashes: %d, makeroots: %d, makeleaves: %d, makeextensions: %d, makebranches: %d, makedbnodes: %d, makedbkey: %d, newrootnode: %d, addnode: %d, delnode: %d, getnode: %d",
		s.dbsets, s.dbgets, s.dbdeletes, s.cryptohashes, s.makeroots, s.makeleaves, s.makeextensions, s.makebranches, s.makedbnodes, s.makedbkey, s.newrootnode, s.addnode, s.delnode, s.getnode)
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
func (ns *nibbles) serialize() ([]byte, error) {
	data, half, err := ns.pack()
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if half {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}
	buf.Write(data)
	return buf.Bytes(), nil
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
	if mt.root.getHash() != nil {
		return *(mt.root.getHash()), nil
	}

	err := mt.root.descendHash()
	if err != nil {
		return crypto.Digest{}, err
	}

	return *(mt.root.getHash()), nil
}

func (mt *Trie) merge() error {
	if mt.parent == nil {
		return nil
	}
	mt.parent.root = mt.root
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
	if mt.root != nil {
		err := mt.root.descendHashWithCommit(b)
		if err != nil {
			return err
		}
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

	mt.dels = make(map[string]node)
	//	mt.root = makeDBNode(mt.root.getHash(), nibbles{})
	//    mt.root = nil
	return nil
}

// Trie Add adds the given key/value pair to the trie.
func (mt *Trie) Add(key nibbles, value []byte) (err error) {
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
	_, err = mt.root.descendAdd(mt, []byte{0x01}, key, crypto.Hash(value))

	if err != nil {
		return err
	}

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

	replacement, found, err := mt.descendDelete(mt.root, []byte{0x01}, key)
	if err == nil && found {
		mt.root = replacement
	}
	return found, err
}

// Trie node get/set/delete operations
func (mt *Trie) getNode(dbn dbnode) (node, error) {
	stats.getnode++
	dbKey := dbn.getDBKey()
	if dbKey != nil {
		if _, ok := mt.dels[string(dbKey)]; ok {
			return nil, nil // return nil if node is scheduled for deletion
		}

		stats.dbgets++

		if debugTrie {
			fmt.Printf("getNode dbKey %x\n", dbKey)
		}
		dbbytes, closer, err := mt.db.Get(dbKey)
		if err != nil {
			fmt.Printf("\ndbKey panic: dbkey %x\n", dbKey)
			panic(err)
		}
		defer closer.Close()

		// this node may have dbnodes in it, but it is not a dbnode.
		trieNode, err := deserializeNode(dbbytes, *dbn.getKey())
		if err != nil {
			panic(err)
		}
		return trieNode, nil
	}
	return nil, nil
}
func (mt *Trie) delNode(n node) {
	stats.delnode++
	dbKey := n.getDBKey()
	if dbKey != nil {
		// note this key for deletion on commit
		mt.dels[string(dbKey)] = n
	}
}
func (mt *Trie) addNode(n node) {
	stats.addnode++
	key := string(*n.getKey())
	_, _, lineno, _ := runtime.Caller(1)
	if debugTrie {
		fmt.Printf("addNode %T (%x) : (%v) caller %d\n", n, key, n, lineno)
	}
	// this key is no longer to be deleted, as it will be overwritten.
	delete(mt.dels, key)
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

//rules to descendAdd:
//if hash is set to nil, the node will be hashed and committed on commit
//if in the transformation no new node can be set to pathKey then the itself node must be deleted

// Node methods for adding a new key-value to the trie
func (rn *RootNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	var err error
	if rn.child == nil {
		// Root node with a blank crypto digest in the child.  Make a leaf node.
		rn.child = makeLeafNode(remainingKey, valueHash, pathKey)
		mt.addNode(rn.child)
		stats.newrootnode++
		rn.hash = nil
		return rn, nil
	}
	replacementChild, err := rn.child.descendAdd(mt, pathKey, remainingKey, valueHash)
	if err != nil {
		return nil, err
	}
	rn.child = replacementChild
	stats.newrootnode++
	rn.hash = nil
	return rn, nil
}

func (bn *BranchNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	if len(remainingKey) == 0 {
		// If we're here, then set the value hash in this node, overwriting the old one.
		bn.valueHash = valueHash
		bn.hash = nil
		return bn, nil
	}

	// Otherwise, shift out the first nibble and check the children for it.
	shifted := shiftNibbles(remainingKey, 1)
	if bn.children[remainingKey[0]] == nil {
		// nil children are available.
		bn.children[remainingKey[0]] = makeLeafNode(shifted, valueHash, append(pathKey, remainingKey[0]))
		mt.addNode(bn.children[remainingKey[0]])
	} else {
		// Not available.  Descend down the branch.
		replacement, err := bn.children[remainingKey[0]].descendAdd(mt, append(pathKey, remainingKey[0]), shifted, valueHash)
		if err != nil {
			return nil, err
		}
		bn.hash = nil
		bn.children[remainingKey[0]] = replacement
	}

	return bn, nil
}

func (ln *LeafNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	if equalNibbles(ln.keyEnd, remainingKey) {
		// The two keys are the same. Replace the value.
		ln.valueHash = valueHash
		ln.hash = nil
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
		branchHash = ln.valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
		// in the branch node.
		ln1 := makeLeafNode(shiftNibbles(shiftedLn1, 1), ln.valueHash, append(append(pathKey, shNibbles...), shiftedLn1[0]))
		mt.addNode(ln1)
		children[shiftedLn1[0]] = ln1
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
		ln2 := makeLeafNode(shiftNibbles(shiftedLn2, 1), valueHash, append(append(pathKey, shNibbles...), shiftedLn2[0]))
		mt.addNode(ln2)
		children[shiftedLn2[0]] = ln2
	}
	bn2 := makeBranchNode(children, branchHash, append(pathKey, shNibbles...))
	mt.addNode(bn2)

	if len(shNibbles) >= 2 {
		// If there was more than one shared nibble, insert an extension node before the branch node.
		mt.addNode(bn2)
		en := makeExtensionNode(shNibbles, bn2, pathKey)
		mt.addNode(en)
		return en, nil
	}
	if len(shNibbles) == 1 {
		// If there is only one shared nibble, we just make a second branch node as opposed to an
		// extension node with only one shared nibble, the chances are high that we'd have to just
		// delete that node and replace it with a full branch node soon anyway.
		mt.addNode(bn2)
		var children2 [16]node
		children2[shNibbles[0]] = bn2
		bn3 := makeBranchNode(children2, crypto.Digest{}, pathKey)
		mt.addNode(bn3)
		return bn3, nil
	}
	// There are no shared nibbles anymore, so just return the branch node.
	mt.delNode(ln)
	return bn2, nil
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

func (rn *RootNode) descendHashWithCommit(b *pebble.Batch) error {
	if rn.hash == nil {
		if rn.child != nil && rn.child.getHash() == nil {
			err := rn.child.descendHashWithCommit(b)
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
		stats.dbsets++
		if debugTrie {
			fmt.Printf("db.set rn key %x dbkey %x\n", rn.getKey(), rn.getDBKey())
		}
		if b != nil {
			return b.Set(rn.getDBKey(), bytes, options)
		}
	}
	return nil
}
func (ln *LeafNode) descendHashWithCommit(b *pebble.Batch) error {
	if ln.hash == nil {
		bytes, err := ln.serialize()
		if err == nil {
			stats.cryptohashes++
			ln.hash = new(crypto.Digest)
			*ln.hash = crypto.Hash(bytes)
		}
		options := &pebble.WriteOptions{}
		stats.dbsets++
		if debugTrie {
			fmt.Printf("db.set ln key %x dbkey %x\n", ln.getKey(), ln.getDBKey())
		}
		if b != nil {
			return b.Set(ln.getDBKey(), bytes, options)
		}
	}
	return nil
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
func (bn *BranchNode) descendHashWithCommit(b *pebble.Batch) error {
	if bn.hash == nil {
		for i := 0; i < 16; i++ {
			if bn.children[i] != nil && bn.children[i].getHash() == nil {
				err := bn.children[i].descendHashWithCommit(b)
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
		stats.dbsets++
		if debugTrie {
			fmt.Printf("db.set bn key %x dbkey %x\n", bn.getKey(), bn.getDBKey())
		}
		if b != nil {
			return b.Set(bn.getDBKey(), bytes, options)
		}
	}
	return nil
}

func (rn *RootNode) descendHash() error {
	return rn.descendHashWithCommit(nil)
}
func (ln *LeafNode) descendHash() error {
	return ln.descendHashWithCommit(nil)
}
func (en *ExtensionNode) descendHash() error {
	return en.descendHashWithCommit(nil)
}
func (bn *BranchNode) descendHash() error {
	return bn.descendHashWithCommit(nil)
}

// Node serializers / deserializers
//
// prefix: 0 == root.
//
//	 1 == extension, half.   2 == extension, full
//		3 == leaf, half.        4 == leaf, full
//		5 == branch
func deserializeRootNode(data []byte) (*RootNode, error) {
	if data[0] != 0 {
		return nil, errors.New("invalid prefix for root node")
	}

	ch := new(crypto.Digest)
	copy(ch[:], data[1:33])
	var child node
	if *ch != (crypto.Digest{}) {
		child = makeDBNode(ch, []byte{0x01})
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
		child = makeDBNode(hash, append(key, sharedKey...))
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
			children[i] = makeDBNode(hash, append(key, byte(i)))
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

func (dbn *DBNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (node, error) {
	// this upgrades the node to a regular trie node by reading it in from the DB
	n, err := mt.getNode(dbn)
	if err != nil {
		return nil, err
	}
	replacement, err := n.descendAdd(mt, pathKey, remainingKey, valueHash)
	if err != nil {
		return nil, err
	}
	// this replacement will replace this DBNode for gc.
	return replacement, nil
}

func (dbn *DBNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (node, bool, error) {
	n, err := mt.getNode(dbn)
	if err != nil {
		return nil, false, err
	}
	return n.descendDelete(mt, pathKey, remainingKey)
}
func makeDBKey(key *nibbles, hash *crypto.Digest) dbKey {
	stats.makedbkey++
	return dbKey(*key)
}

func (dbn *DBNode) serialize() ([]byte, error) {
	return nil, errors.New("DBNode cannot be serialized")
}
func (dbn *DBNode) descendHashWithCommit(b *pebble.Batch) error {
	return nil // DBNodes are immutably hashed and already committed
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
func (rn *RootNode) getDBKey() dbKey {
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
