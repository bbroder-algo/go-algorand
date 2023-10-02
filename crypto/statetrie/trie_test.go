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
	"os"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/crypto/statetrie/nibbles"
	"github.com/algorand/go-algorand/test/partitiontest"
)

func makenode(hash crypto.Digest) node {
	return makeBackingNode(hash, []byte{0x01, 0x02, 0x03, 0x04})
}

func TestNodeSerialization(t *testing.T) {
	ln := &leafNode{}
	ln.keyEnd = []byte("leafendkey")
	for i := range ln.keyEnd {
		ln.keyEnd[i] &= 0x0f
	}
	ln.valueHash = crypto.Hash([]byte("leafvalue"))
	ln.key = nibbles.Nibbles{0x0, 0x1, 0x2, 0x3}
	data, err := ln.serialize()
	require.NoError(t, err)
	expected := []byte{0x4, 0x9a, 0xf2, 0xee, 0x24, 0xf9, 0xd3, 0xde, 0x8d, 0xdb, 0x45, 0x71, 0x82, 0x90, 0xca, 0x38, 0x42, 0xad, 0x8e, 0xcf, 0x81, 0x56, 0x17, 0x16, 0x55, 0x42, 0x73, 0x6, 0xaa, 0xd0, 0x16, 0x87, 0x45, 0xc5, 0x16, 0x5e, 0x4b, 0x59}
	require.Equal(t, expected, data)

	ln.keyEnd = []byte("leafendke")
	for i := range ln.keyEnd {
		ln.keyEnd[i] &= 0x0f
	}
	data, err = ln.serialize()
	require.NoError(t, err)
	expected = []byte{0x3, 0x9a, 0xf2, 0xee, 0x24, 0xf9, 0xd3, 0xde, 0x8d, 0xdb, 0x45, 0x71, 0x82, 0x90, 0xca, 0x38, 0x42, 0xad, 0x8e, 0xcf, 0x81, 0x56, 0x17, 0x16, 0x55, 0x42, 0x73, 0x6, 0xaa, 0xd0, 0x16, 0x87, 0x45, 0xc5, 0x16, 0x5e, 0x4b, 0x50}
	require.Equal(t, expected, data)

	bn := &branchNode{}
	bn.key = nibbles.Nibbles{0x01, 0x02, 0x03}
	bn.children[0] = makenode(crypto.Hash([]byte("branchchild0")))
	bn.children[1] = makenode(crypto.Hash([]byte("branchchild1")))
	bn.children[2] = makenode(crypto.Hash([]byte("branchchild2")))
	bn.children[3] = makenode(crypto.Hash([]byte("branchchild3")))
	bn.children[4] = makenode(crypto.Hash([]byte("branchchild4")))
	bn.children[5] = makenode(crypto.Hash([]byte("branchchild5")))
	bn.children[6] = makenode(crypto.Hash([]byte("branchchild6")))
	bn.children[7] = makenode(crypto.Hash([]byte("branchchild7")))
	bn.children[8] = makenode(crypto.Hash([]byte("branchchild8")))
	bn.children[9] = makenode(crypto.Hash([]byte("branchchild9")))
	bn.children[10] = makenode(crypto.Hash([]byte("branchchild10")))
	bn.children[11] = makenode(crypto.Hash([]byte("branchchild11")))
	bn.children[12] = makenode(crypto.Hash([]byte("branchchild12")))
	bn.children[13] = makenode(crypto.Hash([]byte("branchchild13")))
	bn.children[14] = makenode(crypto.Hash([]byte("branchchild14")))
	bn.children[15] = makenode(crypto.Hash([]byte("branchchild15")))
	bn.valueHash = crypto.Hash([]byte("branchvalue"))
	data, err = bn.serialize()
	require.NoError(t, err)
	expected = []byte{0x5, 0xe8, 0x31, 0x2c, 0x27, 0xec, 0x3d, 0x32, 0x7, 0x48, 0xab, 0x13, 0xed, 0x2f, 0x67, 0x94, 0xb3, 0x34, 0x8f, 0x1e, 0x14, 0xe5, 0xac, 0x87, 0x6e, 0x7, 0x68, 0xd6, 0xf6, 0x92, 0x99, 0x4b, 0xc8, 0x2e, 0x93, 0xde, 0xf1, 0x72, 0xc8, 0x55, 0xbb, 0x7e, 0xd1, 0x1d, 0x38, 0x6, 0xd2, 0x97, 0xd7, 0x2, 0x2, 0x86, 0x93, 0x37, 0x57, 0xce, 0xa4, 0xc5, 0x7e, 0x4c, 0xd4, 0x50, 0x94, 0x2e, 0x75, 0xeb, 0xcd, 0x9b, 0x80, 0xa2, 0xf5, 0xf3, 0x15, 0x4a, 0xf2, 0x62, 0x6, 0x7d, 0x6d, 0xdd, 0xe9, 0x20, 0xe1, 0x1a, 0x95, 0x3b, 0x2b, 0xb9, 0xc1, 0xaf, 0x3e, 0xcb, 0x72, 0x1d, 0x3f, 0xad, 0xe9, 0xa6, 0x30, 0xc6, 0xc5, 0x65, 0xf, 0x86, 0xb2, 0x3a, 0x5b, 0x47, 0xcb, 0x29, 0x31, 0xf7, 0x8a, 0xdf, 0xe0, 0x41, 0x6b, 0x11, 0xc0, 0xd, 0xbc, 0x80, 0xa7, 0x48, 0x97, 0x21, 0xbd, 0xee, 0x6f, 0x36, 0xf4, 0x7b, 0x6d, 0x68, 0xa1, 0x43, 0x31, 0x90, 0xf8, 0x56, 0x69, 0x4c, 0xee, 0x88, 0x76, 0x9c, 0xd1, 0xde, 0xe4, 0xbd, 0x64, 0x7d, 0x18, 0xce, 0xd6, 0xdb, 0xf8, 0x85, 0x84, 0x88, 0x5d, 0x7e, 0xda, 0xe0, 0xf2, 0xa0, 0x6d, 0x24, 0x4f, 0xcf, 0xb, 0x8c, 0x34, 0x57, 0x2a, 0x13, 0x22, 0xd9, 0x8d, 0x79, 0x8, 0xa4, 0x22, 0x91, 0x45, 0x64, 0x7b, 0xf3, 0xad, 0xe8, 0x9b, 0x5f, 0x7c, 0x5c, 0xbd, 0x9, 0xd3, 0xc7, 0x3, 0xe2, 0xef, 0x6b, 0x8, 0x8, 0x98, 0x52, 0xb, 0xd1, 0x6a, 0x5a, 0x18, 0x89, 0x44, 0x4f, 0xf1, 0xb0, 0x37, 0xd9, 0x7f, 0x99, 0x3f, 0x6a, 0x84, 0x46, 0x83, 0x2c, 0x91, 0x58, 0xa8, 0xb3, 0xda, 0xd8, 0x26, 0x2e, 0x8a, 0x4, 0x8f, 0x81, 0xa5, 0xf3, 0xef, 0x46, 0x34, 0x4a, 0x8f, 0x6a, 0x61, 0x2f, 0x3, 0x26, 0x9d, 0xe6, 0x77, 0xee, 0xec, 0xe2, 0xa4, 0x84, 0x38, 0x6b, 0x6e, 0x7e, 0xf0, 0xef, 0xaa, 0x29, 0xa5, 0x13, 0x0, 0xef, 0xff, 0xdf, 0xb5, 0xd7, 0x4e, 0x41, 0x75, 0x4d, 0x2, 0x84, 0x20, 0xe2, 0x18, 0x50, 0x52, 0xae, 0xf4, 0xea, 0xeb, 0x84, 0xb3, 0x91, 0x85, 0xa8, 0xa, 0xba, 0xc9, 0x31, 0x9f, 0x5e, 0x3e, 0xf8, 0xb5, 0xf4, 0x4b, 0xf8, 0xf2, 0xf0, 0x76, 0xa1, 0x6d, 0xec, 0x57, 0x65, 0xbd, 0x2e, 0x78, 0xbe, 0xf4, 0x7c, 0xe4, 0xf2, 0x45, 0xc0, 0xaf, 0x94, 0xb, 0x45, 0x1b, 0xd3, 0xcf, 0x9f, 0x17, 0x7e, 0x1a, 0x52, 0x6d, 0x18, 0xe5, 0x1a, 0x7c, 0xd9, 0x9d, 0xef, 0x8a, 0xe3, 0xe9, 0xe6, 0xf6, 0x76, 0x5e, 0x12, 0xbf, 0xd2, 0xe8, 0xaa, 0x8, 0x88, 0x15, 0x81, 0x99, 0x4e, 0xa3, 0x12, 0x98, 0xc1, 0xb3, 0xde, 0x42, 0x53, 0x2, 0x29, 0x82, 0x87, 0xfe, 0x3d, 0x8, 0xe0, 0xc2, 0x3, 0x70, 0x56, 0xd, 0x9, 0xad, 0xe4, 0x1a, 0xa5, 0xf6, 0x4, 0xdb, 0x63, 0xd0, 0x49, 0x6b, 0x5b, 0xa2, 0x56, 0xb1, 0xd1, 0x4b, 0x56, 0xc3, 0x7e, 0x4b, 0xec, 0xb5, 0xdb, 0xd4, 0xd9, 0xe1, 0x20, 0x99, 0x80, 0x71, 0x9, 0x72, 0x3b, 0xc, 0x8b, 0x56, 0x4, 0x94, 0xe6, 0x4e, 0x35, 0xd, 0x3e, 0x7, 0x8b, 0x86, 0x73, 0x62, 0x5f, 0x61, 0x8d, 0x70, 0x68, 0x86, 0xe8, 0x65, 0xbe, 0x18, 0xa8, 0x4a, 0xac, 0x6d, 0x81, 0x15, 0xde, 0x1b, 0xe1, 0xb3, 0xe8, 0x6a, 0x46, 0xdf, 0xdc, 0xf1, 0x6, 0x3c, 0xa6, 0x1c, 0xc9, 0xcd, 0x12, 0x5e, 0x5f, 0x28, 0xd1, 0x71, 0x6e, 0x9f, 0xc7, 0xdc, 0x77, 0x98, 0x47, 0x7, 0x94, 0x38, 0x4, 0xc4, 0xc4, 0xfe, 0x17, 0x12, 0x1b, 0xcf, 0x96, 0xd8, 0xb1, 0xf2, 0x1e, 0x81, 0xab, 0x15, 0x86, 0x75, 0x5a, 0x39, 0x13, 0xdb, 0xe, 0x1a, 0xd9, 0xa9, 0x70, 0x7d, 0xdd, 0xaf, 0x64, 0x12, 0x27, 0xe5, 0x97, 0xa1, 0x34, 0xb8, 0x1a, 0x61, 0x48, 0x29, 0x61, 0x62, 0xe4, 0x40, 0xba, 0x5, 0x44, 0x24, 0x51, 0xc1, 0x9b, 0x8e, 0x62, 0xf2, 0x1c, 0x6f, 0xd6, 0x8, 0x3, 0xbe, 0x88, 0xf}
	require.Equal(t, expected, data)

	bn.children[7] = nil
	data, err = bn.serialize()
	require.NoError(t, err)
	expected = []byte{0x5, 0xe8, 0x31, 0x2c, 0x27, 0xec, 0x3d, 0x32, 0x7, 0x48, 0xab, 0x13, 0xed, 0x2f, 0x67, 0x94, 0xb3, 0x34, 0x8f, 0x1e, 0x14, 0xe5, 0xac, 0x87, 0x6e, 0x7, 0x68, 0xd6, 0xf6, 0x92, 0x99, 0x4b, 0xc8, 0x2e, 0x93, 0xde, 0xf1, 0x72, 0xc8, 0x55, 0xbb, 0x7e, 0xd1, 0x1d, 0x38, 0x6, 0xd2, 0x97, 0xd7, 0x2, 0x2, 0x86, 0x93, 0x37, 0x57, 0xce, 0xa4, 0xc5, 0x7e, 0x4c, 0xd4, 0x50, 0x94, 0x2e, 0x75, 0xeb, 0xcd, 0x9b, 0x80, 0xa2, 0xf5, 0xf3, 0x15, 0x4a, 0xf2, 0x62, 0x6, 0x7d, 0x6d, 0xdd, 0xe9, 0x20, 0xe1, 0x1a, 0x95, 0x3b, 0x2b, 0xb9, 0xc1, 0xaf, 0x3e, 0xcb, 0x72, 0x1d, 0x3f, 0xad, 0xe9, 0xa6, 0x30, 0xc6, 0xc5, 0x65, 0xf, 0x86, 0xb2, 0x3a, 0x5b, 0x47, 0xcb, 0x29, 0x31, 0xf7, 0x8a, 0xdf, 0xe0, 0x41, 0x6b, 0x11, 0xc0, 0xd, 0xbc, 0x80, 0xa7, 0x48, 0x97, 0x21, 0xbd, 0xee, 0x6f, 0x36, 0xf4, 0x7b, 0x6d, 0x68, 0xa1, 0x43, 0x31, 0x90, 0xf8, 0x56, 0x69, 0x4c, 0xee, 0x88, 0x76, 0x9c, 0xd1, 0xde, 0xe4, 0xbd, 0x64, 0x7d, 0x18, 0xce, 0xd6, 0xdb, 0xf8, 0x85, 0x84, 0x88, 0x5d, 0x7e, 0xda, 0xe0, 0xf2, 0xa0, 0x6d, 0x24, 0x4f, 0xcf, 0xb, 0x8c, 0x34, 0x57, 0x2a, 0x13, 0x22, 0xd9, 0x8d, 0x79, 0x8, 0xa4, 0x22, 0x91, 0x45, 0x64, 0x7b, 0xf3, 0xad, 0xe8, 0x9b, 0x5f, 0x7c, 0x5c, 0xbd, 0x9, 0xd3, 0xc7, 0x3, 0xe2, 0xef, 0x6b, 0x8, 0x8, 0x98, 0x52, 0xb, 0xd1, 0x6a, 0x5a, 0x18, 0x89, 0x44, 0x4f, 0xf1, 0xb0, 0x37, 0xd9, 0x7f, 0x99, 0x3f, 0x6a, 0x84, 0x46, 0x83, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x84, 0x38, 0x6b, 0x6e, 0x7e, 0xf0, 0xef, 0xaa, 0x29, 0xa5, 0x13, 0x0, 0xef, 0xff, 0xdf, 0xb5, 0xd7, 0x4e, 0x41, 0x75, 0x4d, 0x2, 0x84, 0x20, 0xe2, 0x18, 0x50, 0x52, 0xae, 0xf4, 0xea, 0xeb, 0x84, 0xb3, 0x91, 0x85, 0xa8, 0xa, 0xba, 0xc9, 0x31, 0x9f, 0x5e, 0x3e, 0xf8, 0xb5, 0xf4, 0x4b, 0xf8, 0xf2, 0xf0, 0x76, 0xa1, 0x6d, 0xec, 0x57, 0x65, 0xbd, 0x2e, 0x78, 0xbe, 0xf4, 0x7c, 0xe4, 0xf2, 0x45, 0xc0, 0xaf, 0x94, 0xb, 0x45, 0x1b, 0xd3, 0xcf, 0x9f, 0x17, 0x7e, 0x1a, 0x52, 0x6d, 0x18, 0xe5, 0x1a, 0x7c, 0xd9, 0x9d, 0xef, 0x8a, 0xe3, 0xe9, 0xe6, 0xf6, 0x76, 0x5e, 0x12, 0xbf, 0xd2, 0xe8, 0xaa, 0x8, 0x88, 0x15, 0x81, 0x99, 0x4e, 0xa3, 0x12, 0x98, 0xc1, 0xb3, 0xde, 0x42, 0x53, 0x2, 0x29, 0x82, 0x87, 0xfe, 0x3d, 0x8, 0xe0, 0xc2, 0x3, 0x70, 0x56, 0xd, 0x9, 0xad, 0xe4, 0x1a, 0xa5, 0xf6, 0x4, 0xdb, 0x63, 0xd0, 0x49, 0x6b, 0x5b, 0xa2, 0x56, 0xb1, 0xd1, 0x4b, 0x56, 0xc3, 0x7e, 0x4b, 0xec, 0xb5, 0xdb, 0xd4, 0xd9, 0xe1, 0x20, 0x99, 0x80, 0x71, 0x9, 0x72, 0x3b, 0xc, 0x8b, 0x56, 0x4, 0x94, 0xe6, 0x4e, 0x35, 0xd, 0x3e, 0x7, 0x8b, 0x86, 0x73, 0x62, 0x5f, 0x61, 0x8d, 0x70, 0x68, 0x86, 0xe8, 0x65, 0xbe, 0x18, 0xa8, 0x4a, 0xac, 0x6d, 0x81, 0x15, 0xde, 0x1b, 0xe1, 0xb3, 0xe8, 0x6a, 0x46, 0xdf, 0xdc, 0xf1, 0x6, 0x3c, 0xa6, 0x1c, 0xc9, 0xcd, 0x12, 0x5e, 0x5f, 0x28, 0xd1, 0x71, 0x6e, 0x9f, 0xc7, 0xdc, 0x77, 0x98, 0x47, 0x7, 0x94, 0x38, 0x4, 0xc4, 0xc4, 0xfe, 0x17, 0x12, 0x1b, 0xcf, 0x96, 0xd8, 0xb1, 0xf2, 0x1e, 0x81, 0xab, 0x15, 0x86, 0x75, 0x5a, 0x39, 0x13, 0xdb, 0xe, 0x1a, 0xd9, 0xa9, 0x70, 0x7d, 0xdd, 0xaf, 0x64, 0x12, 0x27, 0xe5, 0x97, 0xa1, 0x34, 0xb8, 0x1a, 0x61, 0x48, 0x29, 0x61, 0x62, 0xe4, 0x40, 0xba, 0x5, 0x44, 0x24, 0x51, 0xc1, 0x9b, 0x8e, 0x62, 0xf2, 0x1c, 0x6f, 0xd6, 0x8, 0x3, 0xbe, 0x88, 0xf}

	require.Equal(t, expected, data)

	en := &extensionNode{}
	en.key = nibbles.Nibbles{0x01, 0x02, 0x03}
	en.sharedKey = []byte("extensionkey")
	for i := range en.sharedKey {
		en.sharedKey[i] &= 0x0f
	}
	en.next = makenode(crypto.Hash([]byte("extensionnext")))
	data, err = en.serialize()
	require.NoError(t, err)
	expected = []byte{0x2, 0xea, 0x24, 0x1a, 0x68, 0x6c, 0x5, 0xc8, 0x4, 0xda, 0x0, 0x66, 0x76, 0x8e, 0xb, 0x1d, 0x12, 0x7c, 0x82, 0x7f, 0x5f, 0xc5, 0x81, 0x97, 0x6c, 0x9c, 0xf0, 0xe6, 0xf2, 0x42, 0x33, 0xa, 0xad, 0x58, 0x45, 0xe3, 0x9f, 0xeb, 0x59}
	require.Equal(t, expected, data)
	en2 := deserializeExtensionNode(data, nibbles.Nibbles{0x01, 0x02, 0x03})
	require.NoError(t, err)
	en2data, err := en2.serialize()
	require.NoError(t, err)
	require.Equal(t, en2data, data)
	en.sharedKey = []byte("extensionke") //  [5 8 4 5 14 3 9 15 14 11 5]
	for i := range en.sharedKey {
		en.sharedKey[i] &= 0x0f
	}
	fmt.Println("extensionke: ", en.sharedKey)
	data, err = en.serialize()
	require.NoError(t, err)
	expected = []byte{0x1, 0xea, 0x24, 0x1a, 0x68, 0x6c, 0x5, 0xc8, 0x4, 0xda, 0x0, 0x66, 0x76, 0x8e, 0xb, 0x1d, 0x12, 0x7c, 0x82, 0x7f, 0x5f, 0xc5, 0x81, 0x97, 0x6c, 0x9c, 0xf0, 0xe6, 0xf2, 0x42, 0x33, 0xa, 0xad, 0x58, 0x45, 0xe3, 0x9f, 0xeb, 0x50}

	require.Equal(t, expected, data)
	en3 := deserializeExtensionNode(data, nibbles.Nibbles{0x01, 0x02, 0x03})
	require.NoError(t, err)
	en3data, err := en3.serialize()
	require.NoError(t, err)
	require.Equal(t, en3data, data)
}

func buildDotGraph(t *testing.T, mt *Trie, keys [][]byte, values [][]byte, fn string, title string) {
	dot := dotGraph(mt, keys, values, title)
	file, err := os.Create(fn)
	require.NoError(t, err)
	defer file.Close()
	_, err = file.WriteString(dot)
}

func TestTrieAddSimpleSequenceNoCache(t *testing.T) { 
	partitiontest.PartitionTest(t)
    t.Parallel()

	mt := MakeTrie()
	var k []byte
	var v []byte
	var kk [][]byte
	var vv [][]byte
	k = []byte{0x01, 0x02, 0x03}
	v = []byte{0x04, 0x05, 0x06}
	kk = append(kk, k)
	vv = append(vv, v)

	mt.Add(k, v)
	v = []byte{0x04, 0x05, 0x07}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x0a}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	k = []byte{0x01, 0x02, 0x03, 0x04}
	v = []byte{0x04, 0x05, 0x0b}
	kk = append(kk, k)
	vv = append(vv, v)
	k = []byte{0x01, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	k = []byte{0x01, 0x0d, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, t.TempDir() + "trieSimpleSequence.dot", "Trie Simple Sequence")
}

func TestTrieLeafAddTransitions(t *testing.T) {
	partitiontest.PartitionTest(t)
	t.Parallel()

    var mt *Trie
    var err error

    tempDir := t.TempDir()

	rootHashCP1s0, _ := crypto.DigestFromString(string("EJKKWSBR6ND6FWXTHTKIANNHEAR5JZHNK4DCQIRAUN4K65F4P6KA"))
	rootHashCP1s1, _ := crypto.DigestFromString(string("LTB7VIEISQQD5DN6WBKVD3ZB2BBXEFQVC7AVU7PXFFS6ZEU2ZQIQ"))
	rootHashCP2s0, _ := crypto.DigestFromString(string("FFQBBL5UTISGD3D226DE3MBE57Z4F5J4KI5LWBZD35MHKOG2HWWQ"))
	rootHashCP2s2, _ := crypto.DigestFromString(string("3HXXEYIIWJHMQTGQF6SRREUM3BHYFC5DAIZ3YPRH73P2IAMV32LA"))
	rootHashCP3s0, _ := crypto.DigestFromString(string("FFQBBL5UTISGD3D226DE3MBE57Z4F5J4KI5LWBZD35MHKOG2HWWQ"))
	rootHashCP3s2, _ := crypto.DigestFromString(string("YEWXAECRC4UKLBV6TJJGQTE7Z5BMUWLO27CAIXV4QY5LYBFZCGGA"))
	rootHashCP4s0, _ := crypto.DigestFromString(string("EJKKWSBR6ND6FWXTHTKIANNHEAR5JZHNK4DCQIRAUN4K65F4P6KA"))
	rootHashCP4s2, _ := crypto.DigestFromString(string("VRSSXJ74NVQXRBQOIOIIQHRX5D6WEWBG3LKGKHEOYQPA7TEQ2U6A"))
	rootHashCP5s0, _ := crypto.DigestFromString(string("EJKKWSBR6ND6FWXTHTKIANNHEAR5JZHNK4DCQIRAUN4K65F4P6KA"))
	rootHashCP5s2, _ := crypto.DigestFromString(string("FS3AP7ELN3VXLPY7NZTU7DOX5YOV2E4IBK2PXQIU562EVFZAAP5Q"))
	rootHashCP6s0, _ := crypto.DigestFromString(string("FFQBBL5UTISGD3D226DE3MBE57Z4F5J4KI5LWBZD35MHKOG2HWWQ"))
	rootHashCP6s1, _ := crypto.DigestFromString(string("O5OVEUEVNBYQW4USTIBAQDU2JHNGUWGLNCVQXSS3NPUONVZ2LM6A"))
	rootHashCP6s2, _ := crypto.DigestFromString(string("DHL5EA3QIAQSNKHRUZBUBNYEY52HFHOOSX2CWDQDQCQORXJZRP2Q"))
	rootHashCP7s0, _ := crypto.DigestFromString(string("EJKKWSBR6ND6FWXTHTKIANNHEAR5JZHNK4DCQIRAUN4K65F4P6KA"))
	rootHashCP7s1, _ := crypto.DigestFromString(string("OHSKGDYIVZR34PLQ5YI7E7KYLLA5SFUDNN324YSHTPCLQB2WGXZA"))
	rootHashCP7s2, _ := crypto.DigestFromString(string("LLXHTSABWNRPDEV5Z7JB3OV4R576AZQ57UIODWRCOMN2I6ELMSTQ"))


	keyA := []byte{0x01}
	keyB := []byte{0x02}
	keyAB := []byte{0x01, 0x02}
	keyAC := []byte{0x01, 0x03}
	keyCD := []byte{0x03, 0x04}
	valDEF := []byte{0x04, 0x05, 0x06}
	valGHI := []byte{0x07, 0x08, 0x09}
	valJKL := []byte{0x0a, 0x0b, 0x0c}

	var kk [][]byte
	var vv [][]byte

	mt = MakeTrie()
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyAB, valDEF)
	kk = append(kk, keyAB)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath1.0.dot", "Leaf add: Codepath 7, before")
	require.Equal(t, rootHashCP1s0, mt.Hash())
	mt.Add(keyCD, valGHI)
	kk = append(kk, keyCD)
	vv = append(vv, valGHI)
	require.Equal(t, rootHashCP1s1, mt.Hash())
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath1.2.dot", "Leaf add: Codepath 7, after")

	require.NoError(t, err)

	mt = MakeTrie()
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyA, valDEF)
	kk = append(kk, keyA)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath2.0.dot", "Leaf add: Codepath 2, before")
	require.Equal(t, rootHashCP2s0, mt.Hash())

	mt.Add(keyA, valGHI)
	kk = [][]byte{}
	vv = [][]byte{}
	kk = append(kk, keyA)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath2.2.dot", "Leaf add: Codepath 2, after")
	require.Equal(t, rootHashCP2s2, mt.Hash())

	mt = MakeTrie()
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyA, valDEF)
	kk = append(kk, keyA)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath3.0.dot", "Leaf add: Codepath 3, before")
	require.Equal(t, rootHashCP3s0, mt.Hash())
	mt.Add(keyAB, valGHI)
	kk = append(kk, keyAB)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath3.2.dot", "Leaf add: Codepath 3, after")
	require.Equal(t, rootHashCP3s2, mt.Hash())

	mt = MakeTrie()
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyAB, valDEF)
	kk = append(kk, keyAB)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath4.0.dot", "Leaf add: Codepath 4, before")
	require.Equal(t, rootHashCP4s0, mt.Hash())
	mt.Add(keyA, valGHI)
	kk = append(kk, keyA)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath4.2.dot", "Leaf add: Codepath 4, after")
	require.Equal(t, rootHashCP4s2, mt.Hash())

	mt = MakeTrie()
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyAB, valDEF)
	kk = append(kk, keyAB)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath5.0.dot", "Leaf add: Codepath 5, before")
	require.Equal(t, rootHashCP5s0, mt.Hash())
	mt.Add(keyAC, valGHI)
	kk = append(kk, keyAC)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath5.2.dot", "Leaf add: Codepath 5, after")
	require.Equal(t, rootHashCP5s2, mt.Hash())

	mt = MakeTrie()
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyA, valDEF)
	kk = append(kk, keyA)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath6.0.dot", "Leaf add: Codepath 6, setup")
	require.Equal(t, rootHashCP6s0, mt.Hash())
	mt.Add(keyB, valGHI)
	kk = append(kk, keyB)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath6.1.dot", "Leaf add: Codepath 6, before")
	require.Equal(t, rootHashCP6s1, mt.Hash())
	mt.Add(keyAB, valJKL)
	kk = append(kk, keyAB)
	vv = append(vv, valJKL)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath6.2.dot", "Leaf add: Codepath 6, after")
	require.Equal(t, rootHashCP6s2, mt.Hash())

	mt = MakeTrie()
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyAB, valDEF)
	kk = append(kk, keyAB)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath7.0.dot", "Leaf add: Codepath 7, setup")
	require.Equal(t, rootHashCP7s0, mt.Hash())
	mt.Add(keyB, valGHI)
	kk = append(kk, keyB)
	vv = append(vv, valGHI)
	require.Equal(t, rootHashCP7s1, mt.Hash())
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath7.1.dot", "Leaf add: Codepath 7, before")
	mt.Add(keyA, valJKL)
	kk = append(kk, keyA)
	vv = append(vv, valJKL)
	require.Equal(t, rootHashCP7s2, mt.Hash())
	buildDotGraph(t, mt, kk, vv, tempDir + "codepath7.2.dot", "Leaf add: Codepath 7, after")

}

func TestTrieAddSimpleSequence(t *testing.T) {
	partitiontest.PartitionTest(t)
	t.Parallel()
    tempDir := t.TempDir()

	mt := MakeTrie()
	var k []byte
	var v []byte
	var kk [][]byte
	var vv [][]byte
	k = []byte{0x01, 0x02, 0x03}
	v = []byte{0x03, 0x05, 0x06}
	kk = append(kk, k)
	vv = append(vv, v)

	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, tempDir + "trie0.dot", "Trie Simple")
	fmt.Printf("done with that")

	v = []byte{0x04, 0x05, 0x07}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)

	buildDotGraph(t, mt, kk, vv, tempDir + "trie2.dot", "Trie Simple")
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, tempDir + "trie3.dot", "Trie Simple")

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, tempDir + "trie4.dot", "Trie Simple")

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x0a}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, tempDir + "trie5.dot", "Trie Simple")

	k = []byte{0x01, 0x02, 0x03, 0x04}
	v = []byte{0x04, 0x05, 0x0b}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, tempDir + "trie6.dot", "Trie Simple")

	k = []byte{0x01, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, tempDir + "trie7.dot", "Trie Simple")

	k = []byte{0x01, 0x0d, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, tempDir + "trie8.dot", "Trie Simple")

	//duplicate key and value
	k = []byte{0x01, 0x0d, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, tempDir + "trie9.dot", "Trie Simple")
}

// DotGraph returns a dot graph of the trie
func dotGraph(mt *Trie, keysAdded [][]byte, valuesAdded [][]byte, title string) string {
	var keys string
	for i := 0; i < len(keysAdded); i++ {
		keys += fmt.Sprintf("%x = %x\\n", keysAdded[i], valuesAdded[i])
	}
	fmt.Printf("root: %v\n", mt.root)
	return fmt.Sprintf("digraph trie { key [shape=box, label=\"%s\\nkey/value inserted:\\n%s\"];\n %s }\n", title, keys, dotGraphHelper(mt, mt.root, nibbles.Nibbles{}))
}

// dot graph generation helper
func dotGraphHelper(mt *Trie, n node, path nibbles.Nibbles) string {

	switch tn := n.(type) {
	case *backingNode:
		return "backingnode"
	case *leafNode:
		ln := tn
		return fmt.Sprintf("n%p [label=\"leaf\\nkeyEnd:%x\\nvalueHash:%s\" shape=box];\n", tn, ln.keyEnd, ln.valueHash)
	case *extensionNode:
		en := tn
		return fmt.Sprintf("n%p [label=\"extension\\nshKey:%x\" shape=box];\n", tn, en.sharedKey) +
			fmt.Sprintf("n%p -> n%p;\n", en, en.next) +
			dotGraphHelper(mt, en.next, append(path, en.sharedKey...))
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
				s += dotGraphHelper(mt, ch, append(path, byte(childrenIndex)))
			}
		}
		return s
	default:
		return ""
	}
}
