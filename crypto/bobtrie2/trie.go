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
	//    "io"
	"github.com/algorand/go-algorand/crypto"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"runtime/debug"
)

var debugTrie = false

type Trie struct {
	db          *pebble.DB
	rootHash    *crypto.Digest
	pendingRoot *crypto.Digest
	sets        map[crypto.Digest][]byte
	gets        map[crypto.Digest][]byte
	dels        map[crypto.Digest]bool
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

// Trie nodes

type node interface {
	descendAdd(mt *Trie, fullKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (crypto.Digest, error)
	descendDelete(mt *Trie, fullKey nibbles, remainingKey nibbles) (crypto.Digest, bool, error)
}

// Data can only live in leaf and branch nodes.
type RootNode struct {
	child crypto.Digest
}

type LeafNode struct {
	keyEnd    nibbles
	valueHash crypto.Digest
}

type ExtensionNode struct {
	sharedKey nibbles
	child     crypto.Digest
}

type BranchNode struct {
	children  [16]crypto.Digest
	valueHash crypto.Digest
}

// MakeTrie creates a merkle trie
func MakeTrie() (*Trie, error) {
	db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
	//	db, err := pebble.Open("/tmp/blahdb/", &pebble.Options{})
	if err != nil {
		return nil, err
	}
	mt := &Trie{db: db, rootHash: nil}
	mt.ClearPending()

	return mt, nil
}

// Provide the root hash for this trie
func (mt *Trie) RootHash() *crypto.Digest {
	if mt.pendingRoot != nil {
		return mt.pendingRoot
	}
	return mt.rootHash
}

func (mt *Trie) ClearPending() {
	mt.sets = make(map[crypto.Digest][]byte)
	mt.gets = make(map[crypto.Digest][]byte)
	mt.dels = make(map[crypto.Digest]bool)
	mt.pendingRoot = nil
}

func (mt *Trie) CommitPending() error {
	//	b := mt.db.NewBatch()
	//	options := &pebble.WriteOptions{Sync: true}
	//	for k, v := range mt.sets {
	//		err := mt.db.Set(k[:], v, pebble.Sync)
	//err := b.Set(k[:], v, options)
	//		if err != nil {
	//			return err
	//		}
	//
	//	}
	//	for k := range mt.dels {
	//		err := mt.db.Delete(k[:], pebble.Sync)
	//		err := b.Delete(k[:], options)
	//		if err != nil {
	//			return err
	//		}
	//	}
	//    err := b.SyncWait()
	//	err := mt.db.Apply(b, options)
	//	if err != nil {
	//		return err
	//	}
	//	err = b.Close()
	//	if err != nil {
	//		return err
	//	}

	//	if mt.pendingRoot != nil {
	//		mt.rootHash = mt.pendingRoot
	//	}
	//	mt.ClearPending()

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

	var hash crypto.Digest
	if mt.rootHash == nil && mt.pendingRoot == nil {
		hash, err = mt.storeNewRootNode(crypto.Digest{})
		if err != nil {
			return err
		}
		mt.pendingRoot = &hash
	}

	var rootHash crypto.Digest
	if mt.pendingRoot != nil {
		rootHash = *mt.pendingRoot
	} else {
		rootHash = *mt.rootHash
	}

	stats.cryptohashes++
	hash, err = mt.descendAdd(rootHash, []byte{}, key, crypto.Hash(value))
	if err == nil {
		mt.pendingRoot = &hash
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
	if mt.rootHash == nil && mt.pendingRoot == nil {
		return false, nil
	}

	var rootHash crypto.Digest
	if mt.pendingRoot != nil {
		rootHash = *mt.pendingRoot
	} else {
		rootHash = *mt.rootHash
	}
	hash, found, err := mt.descendDelete(rootHash, []byte{}, key)
	if err == nil && found {
		mt.pendingRoot = &hash
	}
	return found, err
}

// backing store operations
func (mt *Trie) storeNewLeafNode(key nibbles, keyEnd nibbles, valueHash crypto.Digest) (crypto.Digest, error) {
	ln := &LeafNode{keyEnd: keyEnd, valueHash: valueHash}
	data, err := serializeLeafNode(ln)
	if err != nil {
		return crypto.Digest{}, err
	}
	stats.cryptohashes++
	hash := crypto.Hash(append(key, data...))
	stats.leafnodesets++
	//	err = mt.db.Set([]byte(hash.ToSlice()), data, pebble.NoSync)
	mt.set(hash, data)
	return hash, err
}

func (mt *Trie) storeNewRootNode(child crypto.Digest) (crypto.Digest, error) {
	rn := &RootNode{child: child}
	data, err := serializeRootNode(rn)
	if err != nil {
		return crypto.Digest{}, err
	}
	stats.cryptohashes++
	hash := crypto.Hash(data)
	stats.rootnodesets++
	//	err = mt.db.Set([]byte(hash.ToSlice()), data, pebble.NoSync)
	mt.set(hash, data)
	return hash, err
}
func (mt *Trie) storeNewExtensionNode(sharedKey nibbles, child crypto.Digest) (crypto.Digest, error) {
	en := &ExtensionNode{sharedKey: sharedKey, child: child}
	data, err := serializeExtensionNode(en)
	if err != nil {
		return crypto.Digest{}, err
	}
	stats.cryptohashes++
	hash := crypto.Hash(data)
	stats.extnodesets++
	//	err = mt.db.Set([]byte(hash.ToSlice()), data, pebble.NoSync)
	mt.set(hash, data)
	return hash, err
}
func (mt *Trie) storeNewBranchNode(key nibbles, children [16]crypto.Digest, valueHash crypto.Digest) (crypto.Digest, error) {
	bn := &BranchNode{children: children, valueHash: valueHash}
	data, err := serializeBranchNode(bn)
	if err != nil {
		return crypto.Digest{}, err
	}
	stats.cryptohashes++
	hash := crypto.Hash(append(key, data...))
	stats.branchnodesets++
	//	err = mt.db.Set([]byte(hash.ToSlice()), data, pebble.NoSync)
	//    mt.sets[hash] = data
	mt.set(hash, data)
	return hash, err
}

func (mt *Trie) get(node crypto.Digest) ([]byte, error) {
	if debugTrie {
		fmt.Printf("get %x\n", node)
	}
	var nbytes []byte
	var ok bool
	if nbytes, ok = mt.sets[node]; !ok {
		if _, ok = mt.dels[node]; ok {
			return nil, errors.New(fmt.Sprintf("node %x deleted", node))
		}

		if nbytes, ok = mt.gets[node]; !ok {
			stats.dbgets++
			nbytes, closer, err := mt.db.Get([]byte(node.ToSlice()))
			if err != nil {
				return nil, err
			}
			mt.gets[node] = make([]byte, len(nbytes))

			copy(mt.gets[node], nbytes)
			closer.Close()
			nbytes, _ = mt.gets[node]
		}
	}
	return nbytes, nil
}
func (mt *Trie) del(node crypto.Digest) {
	if debugTrie {
		fmt.Printf("del %x\n", node)
	}
	delete(mt.gets, node)
	if _, ok := mt.sets[node]; ok {
		delete(mt.sets, node)
	} else {
		mt.dels[node] = true
	}
}
func (mt *Trie) set(node crypto.Digest, data []byte) {
	if debugTrie {
		fmt.Printf("set %x\n", node)
	}
	mt.sets[node] = data
	delete(mt.dels, node)
	delete(mt.gets, node)
}

// Trie descendAdd descends down the trie, adding the valueHash to the node at the end of the key.
// It returns the hash of the replacement node, or an error.
func (mt *Trie) descendAdd(node crypto.Digest, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (crypto.Digest, error) {
	var err error
	if debugTrie {
		fmt.Printf("descendAdd %x %x %x %x\n", node, pathKey, remainingKey, valueHash)
	}

	nbytes, err := mt.get(node)
	if err != nil {
		return crypto.Digest{}, err
	}

	n, err := deserializeNode(nbytes)
	if err != nil {
		return crypto.Digest{}, err
	}
	hash, err := n.descendAdd(mt, pathKey, remainingKey, valueHash)
	if err == nil && node != hash {
		//		mt.db.Delete([]byte(node.ToSlice()), pebble.NoSync)
		mt.del(node)
	}
	return hash, err
}

// Trie descendDelete descends down the trie, deleting the valueHash to the node at the end of the key.
func (mt *Trie) descendDelete(node crypto.Digest, pathKey nibbles, remainingKey nibbles) (crypto.Digest, bool, error) {
	var err error
	//	nbytes, closer, err := mt.db.Get([]byte(node.ToSlice()))
	//	defer closer.Close()
	//	if err != nil {
	//		return crypto.Digest{}, false, err
	//	}

	nbytes, err := mt.get(node)
	if err != nil {
		return crypto.Digest{}, false, err
	}

	n, err := deserializeNode(nbytes)
	if err != nil {
		return crypto.Digest{}, false, err
	}

	hash, found, err := n.descendDelete(mt, pathKey, remainingKey)
	if found && hash != node && err == nil {
		//		mt.db.Delete([]byte(node.ToSlice()), pebble.NoSync)
		mt.del(node)
	}
	return hash, found, err
}

// Node methods for adding a new key-value to the trie
func (rn *RootNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (crypto.Digest, error) {
	if debugTrie {
		fmt.Printf("RootNode descendAdd %x %x %x %x\n", rn.child, pathKey, remainingKey, valueHash)
	}
	var err error
	var hash crypto.Digest
	if rn.child == (crypto.Digest{}) {
		// Root node with a blank crypto digest in the child.  Make a leaf node.
		hash, err = mt.storeNewLeafNode(remainingKey, remainingKey, valueHash)
	} else {
		hash, err = mt.descendAdd(rn.child, pathKey, remainingKey, valueHash)
	}
	if err != nil {
		return crypto.Digest{}, err
	}
	roothash, err := mt.storeNewRootNode(hash)
	if err != nil {
		return crypto.Digest{}, err
	}
	return roothash, nil
}

func (bn *BranchNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (crypto.Digest, error) {
	if debugTrie {
		fmt.Printf("BranchNode descendAdd %x %x %x %x\n", bn.children, pathKey, remainingKey, valueHash)
	}
	if len(remainingKey) == 0 {
		// If we're here, then set the value hash in this node, overwriting the old one.
		return mt.storeNewBranchNode(pathKey, bn.children, valueHash)
	}

	// Otherwise, shift out the first nibble and check the children for it.
	shifted := shiftNibbles(remainingKey, 1)
	if (bn.children[remainingKey[0]] == crypto.Digest{}) {
		// Children with crypto.Digest{} in them are available.
		hash, err := mt.storeNewLeafNode(append(pathKey, remainingKey[0]), shifted, valueHash)
		if err != nil {
			return crypto.Digest{}, err
		}
		bn.children[remainingKey[0]] = hash
	} else {
		// Not available.  Descend down the branch.
		hash, err := mt.descendAdd(bn.children[remainingKey[0]], append(pathKey, remainingKey[0]), shifted, valueHash)
		if err != nil {
			return crypto.Digest{}, err
		}
		bn.children[remainingKey[0]] = hash
	}

	return mt.storeNewBranchNode(pathKey, bn.children, bn.valueHash)
}

func (ln *LeafNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (crypto.Digest, error) {
	if debugTrie {
		fmt.Printf("LeafNode descendAdd %x %x %x %x\n", ln.keyEnd, pathKey, remainingKey, valueHash)
	}
	if equalNibbles(ln.keyEnd, remainingKey) {
		// The two keys are the same. Replace the value.
		return mt.storeNewLeafNode(append(pathKey, remainingKey...), remainingKey, valueHash)
	}

	// Calculate the shared nibbles between the leaf node we're on and the key we're inserting.
	shNibbles := sharedNibbles(ln.keyEnd, remainingKey)
	// Shift away the common nibbles from both the keys.
	shiftedLn1 := shiftNibbles(ln.keyEnd, len(shNibbles))
	shiftedLn2 := shiftNibbles(remainingKey, len(shNibbles))

	// Make a branch node.
	var children [16]crypto.Digest
	branchHash := crypto.Digest{}

	// If the existing leaf node has no more nibbles, then store it in the branch node's value slot.
	if len(shiftedLn1) == 0 {
		branchHash = ln.valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
		// in the branch node.
		hash, err := mt.storeNewLeafNode(append(pathKey, shiftedLn1[0]), shiftNibbles(shiftedLn1, 1), ln.valueHash)
		if err != nil {
			return crypto.Digest{}, err
		}
		children[shiftedLn1[0]] = hash
	}

	// Similarly, for our new insertion, if it has no more nibbles, store it in the branch node's value slot.
	if len(shiftedLn2) == 0 {
		if len(shiftedLn1) == 0 {
			// They can't both be empty, otherwise they would have been caaught earlier in the equalNibbles check.
			return crypto.Digest{}, fmt.Errorf("both keys are the same but somehow wasn't caught earlier")
		}
		branchHash = valueHash
	} else {
		// Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
		// in the branch node.
		hash, err := mt.storeNewLeafNode(append(pathKey, shiftedLn2[0]), shiftNibbles(shiftedLn2, 1), valueHash)
		if err != nil {
			return crypto.Digest{}, err
		}
		children[shiftedLn2[0]] = hash
	}
	hash, err := mt.storeNewBranchNode(pathKey, children, branchHash)
	if err != nil {
		return crypto.Digest{}, err
	}
	if len(shNibbles) >= 2 {
		// If there was more than one shared nibble, insert an extension node before the branch node.
		return mt.storeNewExtensionNode(shNibbles, hash)
	}
	if len(shNibbles) == 1 {
		// If there is only one shared nibble, we just make a second branch node as opposed to an
		// extension node with only one shared nibble, the chances are high that we'd have to just
		// delete that node and replace it with a full branch node soon anyway.
		var children2 [16]crypto.Digest
		children2[shNibbles[0]] = hash
		hash, err = mt.storeNewBranchNode(append(pathKey, shNibbles[0]), children2, crypto.Digest{})
		if err != nil {
			return crypto.Digest{}, err
		}
		// return the second branch node.
		return hash, nil
	}
	// There are no shared nibbles anymore, so just return the branch node.
	return hash, nil
}

func (en *ExtensionNode) descendAdd(mt *Trie, pathKey nibbles, remainingKey nibbles, valueHash crypto.Digest) (crypto.Digest, error) {
	if debugTrie {
		fmt.Printf("ExtensionNode descendAdd %x %x %x %x\n", en.sharedKey, pathKey, remainingKey, valueHash)
	}
	var err error
	// Calculate the shared nibbles between the key we're adding and this extension node.
	shNibbles := sharedNibbles(en.sharedKey, remainingKey)
	if len(shNibbles) == len(en.sharedKey) {
		// The entire extension node is shared.  descend.
		shifted := shiftNibbles(remainingKey, len(shNibbles))
		hash, err := mt.descendAdd(en.child, append(pathKey, shNibbles...), shifted, valueHash)
		if err != nil {
			return crypto.Digest{}, err
		}
		return mt.storeNewExtensionNode(shNibbles, hash)
	}

	// we have to upgrade part or all of this extension node into a branch node.
	var children [16]crypto.Digest
	branchHash := crypto.Digest{}
	// what's left of the extension node shared key after removing the shared part gets
	// attached to the new branch node.
	shifted := shiftNibbles(en.sharedKey, len(shNibbles))
	if len(shifted) >= 2 {
		// if there's two or more nibbles left, make another extension node.
		shifted2 := shiftNibbles(shifted, 1)
		hash, err := mt.storeNewExtensionNode(shifted2, en.child)
		if err != nil {
			return crypto.Digest{}, err
		}
		children[shifted[0]] = hash
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
		hash, err := mt.storeNewLeafNode(append(append(pathKey, shNibbles...), shifted[0]), shifted2, valueHash)
		if err != nil {
			return crypto.Digest{}, err
		}
		// we know this slot will be empty because it's the first nibble that differed from the
		// only other occupant in the child arrays, the one that leads to the extension node's child.
		children[shifted[0]] = hash
	} else {
		// if the key is no more, store it in the branch node's value hash slot.
		branchHash = valueHash
	}
	hash, err := mt.storeNewBranchNode(append(pathKey, shNibbles...), children, branchHash)

	// the shared bits of the extension node get smaller
	if err == nil && len(shNibbles) > 0 {
		// still some shared key left, store them in an extension node
		// and point in to the new branch node
		return mt.storeNewExtensionNode(shNibbles, hash)
	}
	// or else there there is no shared key left, and the extension node is destroyed.
	return hash, err
}

// Node methods for deleting a key from the trie
func (rn *RootNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (crypto.Digest, bool, error) {
	hash, found, err := mt.descendDelete(rn.child, pathKey, remainingKey)
	if err == nil && found {
		roothash, err := mt.storeNewRootNode(hash)
		return roothash, true, err
	}
	return crypto.Digest{}, false, err
}

func (ln *LeafNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (crypto.Digest, bool, error) {
	return crypto.Digest{}, equalNibbles(remainingKey, ln.keyEnd), nil
}
func (en *ExtensionNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (crypto.Digest, bool, error) {
	var err error
	if len(remainingKey) == 0 {
		// can't stop on an exension node
		return crypto.Digest{}, false, fmt.Errorf("key too short")
	}
	shNibbles := sharedNibbles(remainingKey, en.sharedKey)
	if len(shNibbles) == len(en.sharedKey) {
		shifted := shiftNibbles(remainingKey, len(en.sharedKey))
		hash, found, err := mt.descendDelete(en.child, append(pathKey, shNibbles...), shifted)
		if err == nil && found && (hash != crypto.Digest{}) {
			// the key was found below this node and deleted,
			// make a new extension node pointing to its replacement.
			hash, err = mt.storeNewExtensionNode(en.sharedKey, hash)
		}
		// returns empty digest if there's nothing left.
		return hash, found, err
	}
	// didn't match the entire extension node.
	return crypto.Digest{}, false, err
}
func (bn *BranchNode) descendDelete(mt *Trie, pathKey nibbles, remainingKey nibbles) (crypto.Digest, bool, error) {
	if len(remainingKey) == 0 {
		if (bn.valueHash == crypto.Digest{}) {
			// valueHash is empty -- key not found.
			return crypto.Digest{}, false, nil
		}
		// reset the value to the empty hash if not empty.
		// update the branch node if there are children.
		for i := 0; i < 16; i++ {
			if (bn.children[i] != crypto.Digest{}) {
				hash, err := mt.storeNewBranchNode(append(pathKey, byte(i)), bn.children, crypto.Digest{})
				return hash, true, err
			}
		}
		// no children, so just return the empty hash.
		return crypto.Digest{}, true, nil
	}
	// descend into the branch node.
	if bn.children[remainingKey[0]] == (crypto.Digest{}) {
		// no child at this index.  key not found.
		return crypto.Digest{}, false, nil
	}
	shifted := shiftNibbles(remainingKey, 1)
	hash, found, err := mt.descendDelete(bn.children[remainingKey[0]], append(pathKey, remainingKey[0]), shifted)
	if err == nil && found {
		bn.children[remainingKey[0]] = hash
		hash, err = mt.storeNewBranchNode(pathKey, bn.children, bn.valueHash)
		return hash, true, err
	}
	return crypto.Digest{}, false, err
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
	rn := &RootNode{}
	rn.child = crypto.Digest(data[1:33])
	return rn, nil
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
	en := &ExtensionNode{}
	en.child = crypto.Digest(data[1:33])
	en.sharedKey = sharedKey
	if len(en.sharedKey) == 0 {
		return nil, errors.New("sharedKey can't be empty in an extension node")
	}
	return en, nil
}
func deserializeBranchNode(data []byte) (*BranchNode, error) {
	if data[0] != 5 {
		return nil, errors.New("invalid prefix for branch node")
	}
	if len(data) < 545 {
		return nil, errors.New("data too short to be a branch node")
	}

	bn := &BranchNode{}
	for i := 0; i < 16; i++ {
		bn.children[i] = crypto.Digest(data[1+i*32 : 33+i*32])
	}
	bn.valueHash = crypto.Digest(data[513:545])
	return bn, nil
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
	ln := &LeafNode{}
	ln.valueHash = crypto.Digest(data[1:33])
	ln.keyEnd = keyEnd
	return ln, nil
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
	data := make([]byte, 33)
	data[0] = 0
	copy(data[1:33], rn.child[:])
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
	copy(data[1:33], en.child[:])
	copy(data[33:], pack)
	return data, nil
}
func serializeBranchNode(bn *BranchNode) ([]byte, error) {
	data := make([]byte, 545)
	data[0] = 5
	for i := 0; i < 16; i++ {
		copy(data[1+i*32:33+i*32], bn.children[i][:])
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
	fmt.Printf("rootHash: %x\n", mt.rootHash)
	fmt.Printf("pendingRoot: %x\n", mt.pendingRoot)
	rootHash := *(mt.RootHash())

	return fmt.Sprintf("digraph trie { key [shape=box, label=\"key/value inserted:\\n%s\"];\n %s }\n", keys, mt.dotGraph(rootHash))
}
func (mt *Trie) dotGraph(hash crypto.Digest) string {
	var node node
	var err error
	nbytes, err := mt.get(hash)
	//	nbytes, closer, err := mt.db.Get([]byte(hash.ToSlice()))
	//	defer closer.Close()
	if err != nil {
		return ""
	}
	node, err = deserializeNode(nbytes)
	if err != nil {
		return ""
	}

	switch n := node.(type) {
	case *RootNode:
		return fmt.Sprintf("n%s [label=\"root\" shape=box];\n", hash) +
			fmt.Sprintf("n%s -> n%s;\n", hash, n.child) +
			mt.dotGraph(n.child)
	case *LeafNode:
		ln := n
		return fmt.Sprintf("n%s [label=\"leaf\\nkeyEnd:%x\\nvalueHash:%s\" shape=box];\n", hash, ln.keyEnd, ln.valueHash)
	case *ExtensionNode:
		en := n
		return fmt.Sprintf("n%s [label=\"extension\\nshKey:%x\" shape=box];\n", hash, en.sharedKey) +
			fmt.Sprintf("n%s -> n%s;\n", hash, n.child) +
			mt.dotGraph(n.child)
	case *BranchNode:
		bn := n
		var indexesFilled string
		indexesFilled = "--"
		for i, child := range bn.children {
			if child != (crypto.Digest{}) {
				indexesFilled += fmt.Sprintf("%x ", i)
			}
		}
		indexesFilled += "--"

		s := fmt.Sprintf("n%s [label=\"branch\\nindexesFilled:%s\\nvalueHash:%s\" shape=box];\n", hash, indexesFilled, bn.valueHash)
		for _, child := range n.children {
			if child != (crypto.Digest{}) {
				s += fmt.Sprintf("n%s -> n%s;\n", hash, child)
			}
		}
		for _, child := range n.children {
			if child != (crypto.Digest{}) {
				s += mt.dotGraph(child)
			}
		}
		return s
	default:
		return ""
	}
}
