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
	"bytes"
	"errors"
)

// nibble utilities
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
