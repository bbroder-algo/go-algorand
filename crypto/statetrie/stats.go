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
	"fmt"
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
	evictions      int
}

var stats triestats

func (s triestats) String() string {
	return fmt.Sprintf("dbsets: %d, dbgets: %d, dbdeletes: %d, cryptohashes: %d, makeroots: %d, makeleaves: %d, makeextensions: %d, makebranches: %d, makedbnodes: %d, makedbkey: %d, newrootnode: %d, addnode: %d, delnode: %d, getnode: %d, evictions: %d",
		s.dbsets, s.dbgets, s.dbdeletes, s.cryptohashes, s.makeroots, s.makeleaves, s.makeextensions, s.makebranches, s.makedbnodes, s.makedbkey, s.newrootnode, s.addnode, s.delnode, s.getnode, s.evictions)
}
