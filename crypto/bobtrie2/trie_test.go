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
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func verifyNewTrie(t *testing.T, mt *Trie) {
	require.NotNil(t, mt)
	require.NotNil(t, mt.db)
	require.Nil(t, mt.rootHash)
}

func TestMakeTrie(t *testing.T) {
	mt, err := MakeTrie()
	require.NoError(t, err)
	verifyNewTrie(t, mt)

}

func TestTrieAdd(t *testing.T) {
	mt, err := MakeTrie()
	require.NoError(t, err)
	verifyNewTrie(t, mt)

}

func TestNibbleUtilities(t *testing.T) {
	if false {
		fmt.Println("TestNibbleUtilities")
	}
	sampleNibbles := []nibbles{
		{0x0, 0x1, 0x2, 0x3, 0x4},
		{0x4, 0x1, 0x2, 0x3, 0x4},
		{0x0, 0x0, 0x2, 0x3, 0x5},
		{0x0, 0x1, 0x2, 0x3, 0x4, 0x5},
		{},
	}

	sampleNibblesPacked := [][]byte{
		{0x01, 0x23, 0x40},
		{0x41, 0x23, 0x40},
		{0x00, 0x23, 0x50},
		{0x01, 0x23, 0x45},
		{},
	}

	sampleNibblesShifted1 := []nibbles{
		{0x1, 0x2, 0x3, 0x4},
		{0x1, 0x2, 0x3, 0x4},
		{0x0, 0x2, 0x3, 0x5},
		{0x1, 0x2, 0x3, 0x4, 0x5},
		{},
	}

	sampleNibblesShifted2 := []nibbles{
		{0x2, 0x3, 0x4},
		{0x2, 0x3, 0x4},
		{0x2, 0x3, 0x5},
		{0x2, 0x3, 0x4, 0x5},
		{},
	}

	for i, n := range sampleNibbles {
		b, half, err := n.pack()
		require.NoError(t, err)
		if half {
			require.True(t, b[len(b)-1]&0x0f == 0x00)
		}
		require.True(t, bytes.Equal(b, sampleNibblesPacked[i]))

		unp, err := unpack(b, half)
		require.NoError(t, err)
		require.True(t, bytes.Equal(unp, n))

	}
	badNibbles := []nibbles{
		{0x12, 0x02, 0x03, 0x04},
	}
	_, _, err := badNibbles[0].pack()
	require.Error(t, err)

	for i, n := range sampleNibbles {
		require.True(t, bytes.Equal(shiftNibbles(n, 1), sampleNibblesShifted1[i]))
		require.True(t, bytes.Equal(shiftNibbles(n, 2), sampleNibblesShifted2[i]))
	}

}
