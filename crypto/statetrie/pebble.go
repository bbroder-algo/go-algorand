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

type fileBackstore struct {
	file os.File
}

func makeFileBackstore() *fileBackstore {
	f, err := os.OpenFile("filebackstore", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	return &fileBackstore{file: *f}
}
func (fb *fileBackstore) get(key nibbles) node {
	stats.dbgets++
	return nil
}
func (fb *fileBackstore) close() error {
	return fb.file.Close()
}
func (fb *fileBackstore) set(key nibbles, value []byte) error {
	fb.file.Write(key)
	fb.file.Write(value)
	return nil
}
func (fb *fileBackstore) del(key nibbles) error {
	return nil
}
func (fb *fileBackstore) batchStart() {}
func (fb *fileBackstore) batchEnd()   {}

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
func (pb *pebbleBackstore) get(key nibbles) node {
	stats.dbgets++

	dbbytes, closer, err := pb.db.Get(key)
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
	return pb.db.Set(key, value, pebble.NoSync)
}
func (pb *pebbleBackstore) del(key nibbles) error {
	return pb.db.Delete(key, pebble.NoSync)
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
