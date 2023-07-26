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

package bobtrie2

import (
    "fmt"
    "errors"
	"github.com/algorand/go-algorand/crypto"
    "github.com/cockroachdb/pebble"
    "github.com/cockroachdb/pebble/vfs"

)


// Trie is a merkle trie intended to efficiently calculate the merkle root of
// unordered elements
type Trie struct {
    db *pebble.DB
    rootHash *crypto.Digest
}

type RootNode struct {
    prefix uint8
    child crypto.Digest
}

type LeafNode struct {
    prefix uint8
    keyEnd []byte
    valueHash crypto.Digest
}

type ExtensionNode struct {
    prefix uint8
    sharedKey []byte
    child crypto.Digest
}

type BranchNode struct {
    prefix uint8
    children [16]crypto.Digest
    valueHash crypto.Digest
}


// MakeTrie creates a merkle trie
func MakeTrie() (*Trie, error) {
    db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
    if err != nil {
        return nil, err
    }
    mt := &Trie{db:db, rootHash:nil}
    return mt, nil
}

func (mt *Trie) RootHash() (*crypto.Digest) {
	return mt.rootHash
}
func DeserializeRootNode(data []byte) (*RootNode, error) {
    rn := &RootNode{}  
    rn.prefix = data[0]
    rn.child = crypto.Digest(data[1:33])
    return rn, nil
}
func DeserializeExtensionNode(data []byte) (*ExtensionNode, error) {
    en := &ExtensionNode{}
    en.prefix = data[0]
    en.sharedKey = data[1:33]
    en.child = crypto.Digest(data[33:65])
    if len (en.sharedKey) == 0 {
        return nil, errors.New("sharedKey can't be empty in an extension node")
    }
    return en, nil
}
func DeserializeBranchNode(data []byte) (*BranchNode, error) {
    bn := &BranchNode{}
    bn.prefix = data[0]
    for i := 0; i < 16; i++ {
        bn.children[i] = crypto.Digest(data[i*32:i*32+32])
    }
    bn.valueHash = crypto.Digest(data[512:544])
    return bn, nil
}
func DeserializeLeafNode(data []byte) (*LeafNode, error) {
    ln := &LeafNode{}
    ln.prefix = data[0]
    ln.keyEnd = data[1:33]
    ln.valueHash = crypto.Digest(data[33:65])
    return ln, nil
}

func SerializeRootNode(rn *RootNode) ([]byte, error) {
    data := make([]byte, 33)
    data[0] = rn.prefix
    copy(data[1:33], rn.child[:])
    return data, nil
}
func SerializeExtensionNode(en *ExtensionNode) ([]byte, error) {
    data := make([]byte, 65)
    data[0] = en.prefix
    copy(data[1:33], en.sharedKey)
    copy(data[33:65], en.child[:])
    return data, nil
}
func SerializeBranchNode(bn *BranchNode) ([]byte, error) {
    data := make([]byte, 545)
    data[0] = bn.prefix
    for i := 0; i < 16; i++ {
        copy(data[i*32:i*32+32], bn.children[i][:])
    }
    copy(data[512:544], bn.valueHash[:])
    return data, nil
}
func SerializeLeafNode(ln *LeafNode) ([]byte, error) {
    data := make([]byte, 65)
    data[0] = ln.prefix
    copy(data[1:33], ln.keyEnd)
    copy(data[33:65], ln.valueHash[:])
    return data, nil
}

func getHigh4Bits(b byte) byte {
    return (b & 0xF0) >> 4
}
func equalNibbles(a []byte, aHalf bool, b []byte, bHalf bool) bool {
    if len(a) == 0 && len(b) == 0 {
        // empty byte arrays are equal.
        return true
    }
    if len(a) != len(b) || aHalf != bHalf {
        // different nibble length arrays are not equal
        return false
    }
    for i := 0; i < len(a) - 1; i++ {
        if a[i] != b[i] {
            // different bytes are not equal before the last byte
            return false
        }
    }
    // check the last byte nibbles
    if aHalf {
        return getHigh4Bits(a[len(a)-1]) == getHigh4Bits(b[len(b)-1])
    }
    return a[len(a)-1] == b[len(b)-1]
}

func shiftNibbles(a []byte, half bool, numNibbles int) ([]byte, bool) {
    if numNibbles == 0 {
        // Special case: we want to shift 0 bytes
        return a, half
    }

    if len(a) == 0 {
        // Special case: the input array is empty
        fmt.Println("shiftNibbles: trying to shift an empty array")
        return []byte{}, false
    }
    fullBytes := numNibbles / 2
    if len(a) < fullBytes {
        // Special case: the input array is smaller than the number of full bytes
        // we want to shift
        fmt.Println("shiftNibbles: trying to shift more bytes than available")
        return []byte{}, false
    }
    var shiftedArr []byte
    shiftedArr = a[fullBytes:]
    numNibbles -= fullBytes * 2
    if numNibbles == 0 {
        // Special case: we want to shift all the bytes
        return shiftedArr, half
    }
    return shiftNibble (shiftedArr, half)

}
func shiftNibble(a []byte, half bool) ([]byte, bool) {
    if len(a) == 0 {
        // Special case: the input array is empty
        fmt.Println("shiftNibbles: trying to shift an empty array")
        return []byte{}, false
    }

	if len(a) == 1 && half {
		// Special case: the input array has only one byte with a single nibble.
		// In this case, the resulting array is empty
		return []byte{}, false
	}

	var shiftedArr []byte
	var leftover byte

	for i, b := range a {
		if i == 0 {
			// for the first index, pop the left nibble and keep the right one
			leftover = b & 0x0F
			continue
		}

		// Check if it's the last byte and it only contains one nibble
		if half && i == len(a)-1 {
			shiftedArr = append(shiftedArr, (leftover<<4)|((b&0xF0)>>4))
			// If the original last byte only had one nibble, the shifted array will have all full bytes
			return shiftedArr, false
		}

		// Otherwise shift the nibbles
		shiftedArr = append(shiftedArr, (leftover<<4)|(b>>4))
		leftover = b & 0x0F
	}

	// If we reach this point, the original last byte had two nibbles, so the shifted array will have a last byte with one nibble
	shiftedArr = append(shiftedArr, leftover<<4)
	return shiftedArr, true
}
func nibbleLen (a []byte, half bool) int {
    if len(a) == 0 {
        return 0
    }
    if half {  
        return len(a)*2 - 1
    }
    return len(a)*2
}
func sharedNibbles(arr1 []byte, arr1LastByteHalf bool, arr2 []byte, arr2LastByteHalf bool) ([]byte, bool) {
	minLength := len(arr1)
	if len(arr2) < minLength {
		minLength = len(arr2)
	}

	lastByteHalf := false
	commonNibbles := []byte{}
	for i := 0; i < minLength; i++ {
		if i == minLength-1 {
			if arr1LastByteHalf || arr2LastByteHalf {
				if (arr1[i] & 0xF0) == (arr2[i] & 0xF0) {
					commonNibbles = append(commonNibbles, arr1[i]&0xF0)
					lastByteHalf = true
				}
			} else {
				if arr1[i] == arr2[i] {
					commonNibbles = append(commonNibbles, arr1[i])
				}
			}
			break
		}

		if arr1[i] == arr2[i] {
			commonNibbles = append(commonNibbles, arr1[i])
		} else {
			if (arr1[i] & 0xF0) == (arr2[i] & 0xF0) {
				commonNibbles = append(commonNibbles, arr1[i]&0xF0)
				lastByteHalf = true
			}
			break
		}
	}

	return commonNibbles, lastByteHalf
}

// prefix: 0 == root.  1 == extension, half.   2 == extension, full
//                     3 == leaf, half.        4 == leaf, full
//                     5 == branch
func (mt *Trie) descendAdd (node crypto.Digest, remainingKey []byte, keyHalf bool, valueHash crypto.Digest) (crypto.Digest, error) {
    var err error

    nbytes, closer, err := mt.db.Get([]byte(node.ToSlice()))
    defer closer.Close()
    if err != nil {
        return crypto.Digest{}, err
    }
    if len(nbytes) == 0 {
        return crypto.Digest{}, fmt.Errorf("node empty error")
    }

    // switch on the type of node
    switch nbytes[0] {
    case 0: // root node
        rn, err := DeserializeRootNode (nbytes)
        if err != nil {
            return crypto.Digest{}, err
        }
        if rn.child == (crypto.Digest{}) {
            // Root node with a blank crypto digest in the child.  Make a leaf node. 
            ln := &LeafNode{keyEnd: remainingKey, valueHash: valueHash}
            if keyHalf {
                ln.prefix = 3
            } else {
                ln.prefix = 4
            }
            lnbytes, err := SerializeLeafNode(ln)
            if err != nil {
                return crypto.Digest{}, err
            }
            lnhash := crypto.Hash(lnbytes)
            err = mt.db.Set([]byte(lnhash.ToSlice()), lnbytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            rn.child = lnhash
        } else {
            hash, err := mt.descendAdd (rn.child, remainingKey, keyHalf, valueHash)
            if err != nil {
                return crypto.Digest{}, err
            }
            rn.child = hash
        }
        rnbytes, err := SerializeRootNode(rn)
        if err != nil {
            return crypto.Digest{}, err
        }
        rnhash := crypto.Hash(rnbytes)
        err = mt.db.Set([]byte(rnhash.ToSlice()), rnbytes, pebble.Sync)
        if err != nil {
            return crypto.Digest{}, err
        }
        mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
        return rnhash, nil
    case 1: // extension node, half
    case 2: // extension node, full
        en, err := DeserializeExtensionNode(nbytes)
        if err != nil {
            return crypto.Digest{}, err
        }
        shNibble, shNibbleHalf := sharedNibbles (en.sharedKey, en.prefix == 1, remainingKey, keyHalf)
        if nibbleLen (shNibble, shNibbleHalf) == nibbleLen (en.sharedKey, en.prefix == 1) {
            // The entire extension node is shared.  descend.
            shifted, half := shiftNibbles(remainingKey, keyHalf, nibbleLen (shNibble, shNibbleHalf))
            hash, err := mt.descendAdd(en.child, shifted, half, valueHash)
            if err != nil {
                return crypto.Digest{}, err
            }
            en.child = hash
            enbytes, err := SerializeExtensionNode(en)
            if err != nil {
                return crypto.Digest{}, err
            }
            enhash := crypto.Hash(enbytes)
            err = mt.db.Set([]byte(enhash.ToSlice()), enbytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
            return enhash, nil
        }
        // extension node becomes a branch node. 
        bn := &BranchNode{}
        bn.prefix = 5

        // what's left of the extension node shared key after removing the shared part gets 
        // attached to the new branch node.
        shifted, half := shiftNibbles(en.sharedKey, en.prefix == 1, nibbleLen (shNibble, shNibbleHalf))
        if nibbleLen (shifted, half) >= 2 {
            // if there's more than two nibbles left, make another extension node.
            shifted2, half2 := shiftNibbles (shifted, half, 1)
            en2 := &ExtensionNode{sharedKey: shifted2}
            if half2 {
                en2.prefix = 1
            } else {
                en2.prefix = 2
            }
            en2.child = en.child
            en2bytes, err := SerializeExtensionNode(en2)
            if err != nil {
                return crypto.Digest{}, err
            }
            en2hash := crypto.Hash(en2bytes)
            err = mt.db.Set([]byte(en2hash.ToSlice()), en2bytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            bn.children[getHigh4Bits(shifted[0])] = en2hash
        } else {
            // if there's only one nibble left, store the child in the branch node.
            // there can't be no nibbles left, or the earlier entire-node-shared case would have been triggered.
            bn.children[getHigh4Bits(shifted[0])] = en.child
        }

        //what's left of the new add remaining key gets put into the branch node bucket corresponding
        //with its first nibble, or into the valueHash if it's now empty.  
        shifted, half = shiftNibbles(remainingKey, keyHalf, nibbleLen (shNibble, shNibbleHalf))
        if nibbleLen (shifted, half) > 0 {
            shifted2, half2 := shiftNibbles (shifted, half, 1)
            ln := &LeafNode{keyEnd: shifted2, valueHash: valueHash}
            if half2 {
                ln.prefix = 3
            } else {
                ln.prefix = 4
            }
            lnbytes, err := SerializeLeafNode(ln)
            if err != nil {
                return crypto.Digest{}, err
            }
            lnhash := crypto.Hash(lnbytes)
            err = mt.db.Set([]byte(lnhash.ToSlice()), lnbytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            // we know this slot will be empty because it's the first nibble that differed from the
            // only other occupant in the child arrays, the one that leads to the extension node's child. 
            bn.children[getHigh4Bits(shifted[0])] = lnhash
        } else {
            // if the key is no more, store it in the branch node's value hash slot.
            bn.valueHash = valueHash
        }
        bnbytes, err := SerializeBranchNode(bn)
        if err != nil {
            return crypto.Digest{}, err
        }
        bnhash := crypto.Hash(bnbytes)
        err = mt.db.Set([]byte(bnhash.ToSlice()), bnbytes, pebble.Sync)
        if err != nil {
            return crypto.Digest{}, err
        }

        // the shared bits of the extension node get smaller
        if nibbleLen (shNibble, shNibbleHalf) > 0 {
            // still some shared key left, store them in an extension node
            // and point in to the new branch node
            en3 := &ExtensionNode{sharedKey: shNibble}
            if shNibbleHalf {
                en3.prefix = 1
            } else {
                en3.prefix = 2
            }
            en3.child = bnhash
            en3bytes, err := SerializeExtensionNode(en3)
            if err != nil {
                return crypto.Digest{}, err
            }
            en3hash := crypto.Hash(en3bytes)
            err = mt.db.Set([]byte(en3hash.ToSlice()), en3bytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
            return en3hash, nil
        }
        // or else there there is no shared key left, and the extension node is destroyed.
        mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
        return bnhash, nil

    case 3: // leaf node, half
    case 4: // leaf node, full
        ln, err := DeserializeLeafNode(nbytes)
        if err != nil {
            return crypto.Digest{}, err
        }

        if equalNibbles(ln.keyEnd, ln.prefix == 3, remainingKey, keyHalf) {
            // The two keys are the same. Replace the value. 
            // Notebook CASE 1
            ln.valueHash = valueHash
            lnbytes, err := SerializeLeafNode(ln)
            if err != nil {
                return crypto.Digest{}, err
            }
            lnhash := crypto.Hash(lnbytes)
            err = mt.db.Set([]byte(lnhash.ToSlice()), lnbytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
            return lnhash, nil
        }
       
        // Calculate the shared nibbles between the leaf node we're on and the key we're inserting. 
        shNibble, shNibbleHalf := sharedNibbles(ln.keyEnd, ln.prefix == 3, remainingKey, keyHalf)
        // Shift away the common nibbles from both the keys.
        shiftedLn1, halfLn1 := shiftNibbles (ln.keyEnd, ln.prefix==3, nibbleLen (shNibble, shNibbleHalf))
        shiftedLn2, halfLn2 := shiftNibbles (remainingKey, keyHalf, nibbleLen (shNibble, shNibbleHalf))

        // Make a branch node. 
        bn := &BranchNode{prefix: 5}

        // If the existing leaf node has no more nibbles, then store it in the branch node's value slot.
        if nibbleLen(shiftedLn1, halfLn1) == 0 {
            bn.valueHash = ln.valueHash
        } else {
            // Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
            // in the branch node.
            shiftedLn3, halfLn3 := shiftNibbles (shiftedLn1, halfLn1, 1)
            ln1 := &LeafNode{keyEnd: shiftedLn3, valueHash: ln.valueHash}
            if halfLn3 {
                ln1.prefix = 3
            } else {
                ln1.prefix = 4
            }
            ln1bytes, err := SerializeLeafNode(ln1)
            if err != nil {
                return crypto.Digest{}, err
            }
            ln1hash := crypto.Hash(ln1bytes)
            err = mt.db.Set([]byte(ln1hash.ToSlice()), ln1bytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            bn.children[getHigh4Bits(shiftedLn1[0])] = ln1hash
        }

        // Similarly, for our new insertion, if it has no more nibbles, store it in the branch node's value slot.
        if nibbleLen(shiftedLn2, halfLn2) == 0 {
            if nibbleLen (shiftedLn1, halfLn1) == 0 {
                // They can't both be empty, otherwise they would have been caaught earlier in the equalNibbles check.
                fmt.Printf("ERROR: both keys are the same but somehow wasn't caught earlier")
                return crypto.Digest{}, fmt.Errorf("both keys are the same but somehow wasn't caught earlier")
            }
            bn.valueHash = valueHash
        } else {
            // Otherwise, make a new leaf node that shifts away one nibble, and store it in that nibble's slot
            // in the branch node.
            shiftedLn3, halfLn3 := shiftNibbles (shiftedLn2, halfLn2, 1)
            ln2 := &LeafNode{keyEnd: shiftedLn3, valueHash: valueHash}
            if halfLn3 {
                ln2.prefix = 3
            } else {
                ln2.prefix = 4
            }
            ln2bytes, err := SerializeLeafNode(ln2)
            if err != nil {
                return crypto.Digest{}, err
            }
            ln2hash := crypto.Hash(ln2bytes)
            err = mt.db.Set([]byte(ln2hash.ToSlice()), ln2bytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            bn.children[getHigh4Bits(shiftedLn2[0])] = ln2hash
        }
        bnbytes, err := SerializeBranchNode(bn)
        if err != nil {
            return crypto.Digest{}, err
        }
        bnhash := crypto.Hash(bnbytes)
        err = mt.db.Set([]byte(bnhash.ToSlice()), bnbytes, pebble.Sync)
        if err != nil {
            return crypto.Digest{}, err
        }
        // Delete the old leaf node. 
        mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)

        if nibbleLen (shNibble, shNibbleHalf) >= 2  {
            // If there was more than one shared nibble, insert an extension node before the branch node.
            en := &ExtensionNode{sharedKey: shNibble, child: bnhash}
            if shNibbleHalf {
                en.prefix = 1
            } else {
                en.prefix = 2
            }
            enbytes, err := SerializeExtensionNode(en)
            if err != nil {
                return crypto.Digest{}, err
            }
            enhash := crypto.Hash(enbytes)
            err = mt.db.Set([]byte(enhash.ToSlice()), enbytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            // Return the extension node. 
            return enhash, nil
        }
        if nibbleLen (shNibble, shNibbleHalf) == 1  {
            // If there is only one shared nibble, we just make a second branch node as opposed to an 
            // extension node with only one shared nibble, the chances are high that we'd have to just
            // delete that node and replace it with a full branch node soon anyway. 
            bn2 := &BranchNode{prefix: 5}
            bn2.children[getHigh4Bits(shNibble[0])] = bnhash
            bn2bytes, err := SerializeBranchNode(bn2)
            if err != nil {
                return crypto.Digest{}, err
            }
            bn2hash := crypto.Hash(bn2bytes)
            err = mt.db.Set([]byte(bn2hash.ToSlice()), bn2bytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            // return the second branch node.
            return bn2hash, nil
        }
        // There are no shared nibbles anymore, so just return the branch node. 
        return bnhash, nil
    case 5:
        bn, err := DeserializeBranchNode(nbytes)
        if err != nil {
            return crypto.Digest{}, err
        }
        if nibbleLen(remainingKey, keyHalf) == 0 {
            // If we're here, then set the value hash in this node, overwriting the old one.
            bn.valueHash = valueHash
            bnbytes, err := SerializeBranchNode(bn)
            if err != nil {
                return crypto.Digest{}, err
            }
            bnhash := crypto.Hash(bnbytes)
            err = mt.db.Set([]byte(bnhash.ToSlice()), bnbytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
            return bnhash, nil
        } 

        // Otherwise, shift out the first nibble and check the children for it.
        var bnhash crypto.Digest
        shifted, half := shiftNibbles(remainingKey, keyHalf, 1)
        index := getHigh4Bits(remainingKey[0])
        if (bn.children[index] == crypto.Digest{}) {
            // Children with crypto.Digest{} in them are available.
            ln := &LeafNode{keyEnd: shifted, valueHash: valueHash}
            if half {
                ln.prefix = 3
            } else {
                ln.prefix = 4
            }
            lnbytes, err := SerializeLeafNode(ln)
            if err != nil {
                return crypto.Digest{}, err
            }
            lnhash := crypto.Hash(lnbytes)
            err = mt.db.Set([]byte(lnhash.ToSlice()), lnbytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, err
            }
            bn.children[index] = lnhash
        } else {
            hash, err := mt.descendAdd(bn.children[index], shifted, half, valueHash)
            if err != nil {
                return crypto.Digest{}, err
            }
            bn.children[index] = hash
        }

        bnbytes, err := SerializeBranchNode(bn)
        if err != nil {
            return crypto.Digest{}, err
        }
        bnhash = crypto.Hash(bnbytes)
        err = mt.db.Set([]byte(bnhash.ToSlice()), bnbytes, pebble.Sync)
        if err != nil {
            return crypto.Digest{}, err
        }
        mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
        return bnhash, nil
    }
    return crypto.Digest{}, fmt.Errorf("unknown node type")
}

func (mt *Trie) Add(key []byte, keyHalf bool, value[]byte) (bool, error) {
    var err error
    if len(key) == 0 {
        return false, errors.New("empty key not allowed")
    }

    if mt.rootHash == nil {
        // Start a new trie. 
        rn := &RootNode{prefix:0}
        rn.prefix = 0
        rn.child = crypto.Digest{}
        rnbytes, err := SerializeRootNode(rn)
        if err != nil {
            return false, err
        }
        rootHash := crypto.Hash(rnbytes)
        err = mt.db.Set([]byte(rootHash.ToSlice()), rnbytes, pebble.Sync)
        if err != nil {
            return false, err
        }
        mt.rootHash = &rootHash
    } 

    hash, err := mt.descendAdd (*mt.rootHash, key, keyHalf, crypto.Hash(value))
    if err != nil {
        return false, err
    }
    mt.rootHash = &hash

	return true, nil
}

func (mt *Trie) descendDelete(node crypto.Digest, key []byte, keyHalf bool) (crypto.Digest, bool, error) {
    var err error
    nbytes, closer, err := mt.db.Get([]byte(node.ToSlice()))
    defer closer.Close()
    if err != nil {
        return crypto.Digest{}, false, err
    }
    if len(nbytes) == 0 {
        return crypto.Digest{}, false, fmt.Errorf("node empty error")
    }

    // switch on the type of node
    switch nbytes[0] {
    case 0: // root node
        rn, err := DeserializeRootNode (nbytes)
        if err != nil {
            return crypto.Digest{}, false, err
        }
        hash, found, err := mt.descendDelete(rn.child, key, keyHalf)
        if err != nil {
            return crypto.Digest{}, false, err
        }
        if found {
            rn.child = hash
            rnbytes, err := SerializeRootNode(rn)
            if err != nil {
                return crypto.Digest{}, false, err
            }
            rnhash := crypto.Hash(rnbytes)
            err = mt.db.Set([]byte(rnhash.ToSlice()), rnbytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, false, err
            }
            mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
            return rnhash, true, nil
        }
        return node, false, nil
    case 1:
    case 2:
        en, err := DeserializeExtensionNode(nbytes)
        if err != nil {
            return crypto.Digest{}, false, err
        }
        if len(key) == 0 {
            // can't stop on an exension node
            return crypto.Digest{}, false, fmt.Errorf("key too short")
        }
        shNibble, shNibbleHalf := sharedNibbles(key, keyHalf, en.sharedKey, en.prefix == 1)
        if nibbleLen (shNibble, shNibbleHalf) == nibbleLen(en.sharedKey, en.prefix == 1) {
            shifted, half := shiftNibbles(key, keyHalf, nibbleLen(en.sharedKey, en.prefix == 1))
            hash, found, err := mt.descendDelete(en.child, shifted, half)
            if err != nil {
                return crypto.Digest{}, false, err
            }

            if found {
                // the key was found below this node and deleted.
                if (hash != crypto.Digest{}) {
                    // child was replaced with a different node.  update the node.
                    en.child = hash
                    enbytes, err := SerializeExtensionNode(en)
                    if err != nil {
                        return crypto.Digest{}, false, err
                    }
                    hash = crypto.Hash(enbytes)
                    err = mt.db.Set([]byte(hash.ToSlice()), enbytes, pebble.Sync)
                    if err != nil {
                        return crypto.Digest{}, false, err
                    }
                }
                mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
                // return either the updated extension hash or the empty hash
                return hash, true, nil
            }
            // didn't find a node to delete below the node.
            return node, false, nil
        }
        // didn't match the entire extension node.
        return node, false, nil
    case 3:
    case 4:
        ln, err := DeserializeLeafNode(nbytes)
        if err != nil {
            return crypto.Digest{}, false, err
        }
        if equalNibbles (key, keyHalf, ln.keyEnd, ln.prefix == 3) {
            // found the leaf node. delete it.
            mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
            // return the empty hash to show that this node was deleted.
            return crypto.Digest{}, true, nil
        }
        // didn't match, node is not here.
        return node, false, nil
    case 5:
        bn, err := DeserializeBranchNode(nbytes)
        if err != nil {
            return crypto.Digest{}, false, err
        }
        if len(key) == 0 {
            if (bn.valueHash == crypto.Digest{}) {
                // valueHash is empty -- key not found.
                return node, false, nil

            }
            // reset the value to the empty hash if not empty.
            bn.valueHash = crypto.Digest{}

            // update the branch node if there are children.
            for i := 0; i < 16; i++ {
                if (bn.children[i] != crypto.Digest{}) {
                    bnbytes, err := SerializeBranchNode(bn)
                    if err != nil {
                        return crypto.Digest{}, false, err
                    }
                    bnhash := crypto.Hash(bnbytes)
                    err = mt.db.Set([]byte(bnhash.ToSlice()), bnbytes, pebble.Sync)
                    if err != nil {
                        return crypto.Digest{}, false, err
                    }
                    return bnhash, true, nil
                }
            }
            // no children either.  delete the branch node
            mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
            return crypto.Digest{}, true, nil
        }
        // descend into the branch node.
        if bn.children[getHigh4Bits(key[0])] == (crypto.Digest{}) {
            // no child at this index.  key not found.
            return node, false, nil
        }
        shifted, half := shiftNibbles(key, keyHalf, 1)
        hash, found, err := mt.descendDelete(bn.children[getHigh4Bits(key[0])], shifted, half)
        if err != nil {
            return crypto.Digest{}, false, err
        }
        if found {
            bn.children[getHigh4Bits(key[0])] = hash
            bnbytes, err := SerializeBranchNode(bn)
            if err != nil {
                return crypto.Digest{}, false, err
            }
            bnhash := crypto.Hash(bnbytes)
            err = mt.db.Set([]byte(bnhash.ToSlice()), bnbytes, pebble.Sync)
            if err != nil {
                return crypto.Digest{}, false, err
            }
            mt.db.Delete([]byte(node.ToSlice()), pebble.Sync)
            return bnhash, true, nil
        }
        return node, false, nil
    }
    return crypto.Digest{}, false, fmt.Errorf("unknown node type")
}

// Delete deletes the given hash to the trie, if such element exists.
// if no such element exists, return false
func (mt *Trie) Delete(key []byte, keyHalf bool) (bool, error) {
    var err error
    if len(key) == 0 {
        return false, errors.New("empty key not allowed")
    }
    if mt.rootHash == nil {
        return false, nil
    }
    hash, found, err := mt.descendDelete(*mt.rootHash, key, keyHalf)
    if err != nil {
        return false, err
    }

    if found {
        mt.rootHash = &hash
    }
	return found, nil
}

// serialize serializes the trie root
func (mt *Trie) serialize() []byte {
//	serializedBuffer := make([]byte, 5*binary.MaxVarintLen64) // allocate the worst-case scenario for the trie header.
//	version := binary.PutUvarint(serializedBuffer[:], merkleTreeVersion)
//	root := binary.PutUvarint(serializedBuffer[version:], uint64(mt.root))
//	next := binary.PutUvarint(serializedBuffer[version+root:], uint64(mt.nextNodeID))
//	elementLength := binary.PutUvarint(serializedBuffer[version+root+next:], uint64(mt.elementLength))
//	pageSizeLength := binary.PutUvarint(serializedBuffer[version+root+next+elementLength:], uint64(mt.cache.nodesPerPage))
//	return serializedBuffer[:version+root+next+elementLength+pageSizeLength]
    return nil
}

// deserialize deserializes the trie root
func (mt *Trie) deserialize(bytes []byte) (int64, error) {
//	version, versionLen := binary.Uvarint(bytes[:])
//	if versionLen <= 0 {
//		return 0, ErrRootPageDecodingFailure
//	}
//	if version != merkleTreeVersion {
//		return 0, ErrRootPageDecodingFailure
//	}
//	root, rootLen := binary.Uvarint(bytes[versionLen:])
//	if rootLen <= 0 {
//		return 0, ErrRootPageDecodingFailure
//	}
//	nextNodeID, nextNodeIDLen := binary.Uvarint(bytes[versionLen+rootLen:])
//	if nextNodeIDLen <= 0 {
//		return 0, ErrRootPageDecodingFailure
//	}
//	elemLength, elemLengthLength := binary.Uvarint(bytes[versionLen+rootLen+nextNodeIDLen:])
//	if elemLengthLength <= 0 {
//		return 0, ErrRootPageDecodingFailure
//	}
//	pageSize, pageSizeLength := binary.Uvarint(bytes[versionLen+rootLen+nextNodeIDLen+elemLengthLength:])
//	if pageSizeLength <= 0 {
//		return 0, ErrRootPageDecodingFailure
//	}
//	mt.root = storedNodeIdentifier(root)
//	mt.nextNodeID = storedNodeIdentifier(nextNodeID)
//	mt.lastCommittedNodeID = storedNodeIdentifier(nextNodeID)
//	mt.elementLength = int(elemLength)
//	return int64(pageSize), nil
    return 0, nil
}
