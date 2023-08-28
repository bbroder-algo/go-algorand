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
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"os"
)

type pebbleBackstore struct {
	db *pebble.DB
	b  *pebble.Batch
}

func makePebbleBackstoreVFS() *pebbleBackstore {
	opts := &pebble.Options{FS: vfs.NewMem()}
	db, err := pebble.Open("", opts)
	if err != nil {
		panic(err)
	}
	return &pebbleBackstore{db: db}
}
func makePebbleBackstoreDisk(dbdir string, clear bool) *pebbleBackstore {
	if clear {
		os.RemoveAll(dbdir)
	}
	if _, err := os.Stat(dbdir); os.IsNotExist(err) {
		err := os.MkdirAll(dbdir, 0755)
		if err != nil {
			panic(err)
		}
	}

	opts := &pebble.Options{}
	db, err := pebble.Open(dbdir, opts)
	if err != nil {
		panic(err)
	}
	return &pebbleBackstore{db: db}
}

type untrustedBackstore struct {
	store backing
}

func makeUntrustedBackstore(store backing) *untrustedBackstore {
	return &untrustedBackstore{store: store}
}
func (pub *untrustedBackstore) isTrusted() bool {
	return false
}
func (pub *untrustedBackstore) get(key nibbles) node {
	return pub.store.get(key)
}
func (pub *untrustedBackstore) close() error {
	return pub.store.close()
}
func (pub *untrustedBackstore) set(key nibbles, value []byte) error {
	return pub.store.set(key, value)
}
func (pub *untrustedBackstore) del(key nibbles) error {
	return pub.store.del(key)
}
func (pub *untrustedBackstore) batchStart() {
	pub.store.batchStart()
}
func (pub *untrustedBackstore) batchEnd() {
	pub.store.batchEnd()
}
func (pb *pebbleBackstore) isTrusted() bool {
	return true
}
func (pb *pebbleBackstore) get(key nibbles) node {
	stats.dbgets++

	dbbytes, closer, err := pb.db.Get(key.serialize())
	if err != nil {
		//		fmt.Printf("\npebble err: key %x, error %v\n", key, err)
		return nil
	}
	defer closer.Close()

	n := deserializeNode(dbbytes, key)
	if debugTrie {
		fmt.Printf("pebble.get %T (%x) : (%v)\n", n, key, n)
	}
	return n
}
func (pb *pebbleBackstore) close() error {
	return pb.db.Close()
}
func (pb *pebbleBackstore) set(key nibbles, value []byte) error {
	return pb.db.Set(key.serialize(), value, pebble.NoSync)
}
func (pb *pebbleBackstore) del(key nibbles) error {
	return pb.db.Delete(key.serialize(), pebble.NoSync)
}
func (pb *pebbleBackstore) batchStart() {
	pb.b = pb.db.NewBatch()
}

func (pb *pebbleBackstore) batchEnd() {
	err := pb.db.Apply(pb.b, nil)
	if err != nil {
		panic(err)
	}
	pb.b.Close()
	pb.b = nil
}
