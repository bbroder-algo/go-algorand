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
	"fmt"
	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/test/partitiontest"
	"github.com/stretchr/testify/require"
	"math"
	"os"
	//	"runtime"
	"time"
	//    "strconv"
	"math/rand"
	"testing"
)

// var x uint32 = 1234567890
var x uint32 = 1234567891

func pseudoRand() uint32 {
	x ^= x << 13
	x ^= x >> 17
	x ^= x << 5
	return x
}
func reset(mt *Trie) {
	mt.root = nil
	mt.dels = make(map[string]bool)
}

func TestTrieDelete(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())

	mt := MakeTrie(makePebbleBackstoreVFS())
	key1 := []byte{0x08, 0x0e, 0x02, 0x08}
	key2 := []byte{0x08, 0x09, 0x0a, 0x0c}
	key3 := []byte{0x08, 0x09, 0x0a, 0x00}
	key4 := []byte{0x03, 0x0c, 0x04, 0x0c}
	key5 := []byte{0x08, 0x09, 0x03, 0x0c}

	mt.Add(key1, key2)
	fmt.Println("Hash:", mt.Hash(), " K1")
	H1 := mt.Hash()
	mt.Add(key2, key3)
	fmt.Println("Hash:", mt.Hash(), " K1 K2")
	H1H2 := mt.Hash()
	mt.Add(key3, key4)
	fmt.Println("Hash:", mt.Hash(), " K1 K2 K3")
	H1H2H3 := mt.Hash()
	mt.Add(key4, key5)
	fmt.Println("Hash:", mt.Hash(), " K1 K2 K3 K4")
	H1H2H3H4 := mt.Hash()
	//	mt.Add(key5, key1)
	//	fmt.Println("Hash:", mt.Hash(), " K0 K2 k3 K4 K5")
	//	H1H2H3H4H5 := mt.Hash()
	reset(mt)

	mt.Add(key1, key2)
	mt.Add(key2, key3)
	mt.Add(key3, key4)
	mt.Add(key4, key5)
	mt.Delete(key2)
	fmt.Println("Hash:", mt.Hash(), " K1 K2 K3 K4 D2")
	H1H2H3H4D2 := mt.Hash()
	mt.Delete(key1)
	H1H2H3H4D2D1 := mt.Hash()
	fmt.Println("Hash:", mt.Hash(), " K1 K2 K3 K4 D2 D1")
	reset(mt)
	mt.Add(key1, key2)
	mt.Add(key3, key4)
	mt.Add(key4, key5)
	fmt.Println("Hash:", mt.Hash(), " K1 K3 K4")
	require.NotEqual(t, H1H2H3H4, H1H2H3H4D2)

	reset(mt)
	fmt.Println("mt", countNodes(mt))
	fmt.Println("Add key1")
	mt.Add(key1, key2)
	require.Equal(t, H1, mt.Hash())
	fmt.Println("Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt))
	fmt.Println("Making child.. child = mt.Child")
	ch := mt.Child()
	require.Equal(t, H1, mt.Hash())
	fmt.Println("mt", countNodes(mt))
	require.Equal(t, H1, ch.Hash())
	fmt.Println("ch", countNodes(ch))

	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt))

	fmt.Println("Add key2 to Child")
	ch.Add(key2, key3)
	require.Equal(t, H1, mt.Hash())
	require.Equal(t, H1H2, ch.Hash())

	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt))

	fmt.Println("Merge...")
	ch.Merge()
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2, ch.Hash())

	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt))

	fmt.Println("Add key3 to child")
	ch.Add(key3, key4)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3, ch.Hash())

	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt))

	fmt.Println("Add key4 to child")
	ch.Add(key4, key5)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3H4, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt))
	fmt.Println("Del key2 from child")
	ch.Delete(key2)
	fmt.Println("ch", countNodes(ch))
	fmt.Println("mt", countNodes(mt))
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3H4D2, ch.Hash())
	fmt.Println("Merge...")
	ch.Merge()
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	mt.Delete(key1)
	fmt.Println("Parent Hash:", mt.Hash())
	require.Equal(t, H1H2H3H4D2D1, mt.Hash())
}

func TestTrieChildMerge(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())

	mt := MakeTrie(makePebbleBackstoreVFS())
	key1 := []byte{0x08, 0x0e, 0x02, 0x08}
	key2 := []byte{0x08, 0x09, 0x0a, 0x0c}
	key3 := []byte{0x08, 0x09, 0x0a, 0x00}
	key4 := []byte{0x03, 0x0c, 0x04, 0x0c}
	key5 := []byte{0x08, 0x09, 0x03, 0x0c}

	mt.Add(key1, key2)
	fmt.Println("Hash:", mt.Hash(), " K1")
	H1 := mt.Hash()
	mt.Add(key2, key3)
	fmt.Println("Hash:", mt.Hash(), " K1 K2")
	H1H2 := mt.Hash()
	mt.Add(key3, key4)
	fmt.Println("Hash:", mt.Hash(), " K1 K2 K3")
	H1H2H3 := mt.Hash()
	mt.Add(key4, key5)
	fmt.Println("Hash:", mt.Hash(), " K1 K2 K3 K4")
	H1H2H3H4 := mt.Hash()
	mt.Add(key5, key1)
	fmt.Println("Hash:", mt.Hash(), " K1 K2 k3 K4 K5")
	H1H2H3H4H5 := mt.Hash()
	reset(mt)

	fmt.Println("Add key1")
	mt.Add(key1, key2)
	require.Equal(t, H1, mt.Hash())
	fmt.Println("Hash:", mt.Hash())
	fmt.Println("Making child.. child = mt.Child")
	ch := mt.Child()
	require.Equal(t, H1, mt.Hash())
	require.Equal(t, H1, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Add key2 to Child")
	ch.Add(key2, key3)
	require.Equal(t, H1, mt.Hash())
	require.Equal(t, H1H2, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Merge...")
	ch.Merge()
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Add key3 to child")
	ch.Add(key3, key4)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Add key4 to child")
	ch.Add(key4, key5)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3H4, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Add key5 to child")
	ch.Add(key5, key1)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3H4H5, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Merge...")
	ch.Merge()
	require.Equal(t, H1H2H3H4H5, mt.Hash())
	require.Equal(t, H1H2H3H4H5, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())

	fmt.Println("....")
	reset(mt)
	fmt.Println("Add key1")
	mt.Add(key1, key2)
	require.Equal(t, H1, mt.Hash())
	fmt.Println("Hash:", mt.Hash())
	fmt.Println("Making child.. ch = mt.Child")
	ch = mt.Child()
	require.Equal(t, H1, mt.Hash())
	require.Equal(t, H1, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Add key2 to Child")
	ch.Add(key2, key3)
	require.Equal(t, H1, mt.Hash())
	require.Equal(t, H1H2, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Merge...")
	ch.Merge()
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Add key3 to child")
	ch.Add(key3, key4)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Making child2.. child2 = ch.Child")
	ch2 := ch.Child()
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3, ch.Hash())
	require.Equal(t, H1H2H3, ch2.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Child2 Hash:", ch2.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Add key4 to child2")
	ch2.Add(key4, key5)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3, ch.Hash())
	require.Equal(t, H1H2H3H4, ch2.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Child2 Hash:", ch2.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Add key5 to child2")
	ch2.Add(key5, key1)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3, ch.Hash())
	require.Equal(t, H1H2H3H4H5, ch2.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Child2 Hash:", ch2.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Merge child 2...")
	ch2.Merge()
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3H4H5, ch.Hash())
	require.Equal(t, H1H2H3H4H5, ch2.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Child2 Hash:", ch2.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Merge child 1...")
	ch.Merge()
	require.Equal(t, H1H2H3H4H5, mt.Hash())
	require.Equal(t, H1H2H3H4H5, ch.Hash())
	require.Equal(t, H1H2H3H4H5, ch2.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Child2 Hash:", ch2.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	mt.Commit()
	require.Equal(t, H1H2H3H4H5, mt.Hash())
	require.Equal(t, H1H2H3H4H5, ch.Hash())
	require.Equal(t, H1H2H3H4H5, ch2.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("Child2 Hash:", ch2.Hash())
}

func TestTrieSpecial(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()

	mt := MakeTrie(makePebbleBackstoreVFS())
	key1 := []byte{0x08, 0x0e, 0x02, 0x08}
	key2 := []byte{0x0b, 0x09, 0x0a, 0x0c}
	key3 := []byte{0x08, 0x0c, 0x09, 0x00}
	key4 := []byte{0x03, 0x0c, 0x04, 0x0c}
	key5 := []byte{0x07, 0x0f, 0x0b, 0x04}
	mt.Add(key1, key1)
	mt.Add(key2, key2)
	mt.Add(key3, key3)
	mt.Add(key4, key4)
	mt.Add(key5, key5)
	mt.Commit()
}

var accts [][]byte

func addKeyBatches(b *testing.B, mt *Trie, accounts int, totalBatches int, keyLength int, prepopulateCount int, skipCommit bool, batchSize int) {
	if prepopulateCount > accounts {
		panic("prepopulateCount > accounts")
	}

	for m := len(accts); m < accounts; m++ {
		k := make([]byte, keyLength)
		rand.Read(k)
		for j := range k {
			k[j] = k[j] & 0x0f // nibbles only
		}
		accts = append(accts, k)
	}
	// prepopulate the trie
	for m := 0; m < prepopulateCount; m++ {
		mt.Add(accts[m], accts[m])
	}
	mt.Commit()
	b.ResetTimer()
	for m := 0; m < totalBatches; m++ {
		for i := 0; i < batchSize; i++ {
			randk := pseudoRand() % uint32(len(accts))
			randv := pseudoRand() % uint32(len(accts))
			mt.Add(accts[randk], accts[randv])
		}
		if !skipCommit {
			mt.Commit()
		}
	}
	b.StopTimer()
}
func BenchmarkTrieAddFrom4KiB32NoCommit25(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 65_536, b.N, 32, 1*65_536, true, 25_000)
	back.close()
}
func BenchmarkTrieAddFrom64KiB32Disk25(b *testing.B) {
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 65_536, b.N, 32, 1*65_536, false, 25_000)
	back.close()
}
func BenchmarkTrieAddFrom64KiB64InMem25(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 65_536, b.N, 64, 1*65_536, false, 25_000)
	back.close()
}
func BenchmarkTrieAddFrom64KiB32InMem25(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 65_536, b.N, 32, 1*65_536, false, 25_000)
	back.close()
}
func BenchmarkTrieAddFrom64MiB32NoCommit250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*64, b.N, 32, 64*1_048_576, true, 250_000)
	back.close()
}
func skipBenchmarkTrieAddFrom64MiB32Disk250(b *testing.B) {
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*64, b.N, 32, 64*1_048_576, false, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom32MiB32NoCommit250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*32, b.N, 32, 32*1_048_576, true, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom16MiB32NoCommit250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*16, b.N, 32, 16*1_048_576, true, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom16MiB32Disk250(b *testing.B) {
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*16, b.N, 32, 16*1_048_576, false, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom8MiB32NoCommit250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*8, b.N, 32, 8*1_048_576, true, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom8MiB32Disk250(b *testing.B) {
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*8, b.N, 32, 8*1_048_576, false, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom4MiB32NoCommit250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*4, b.N, 32, 4*1_048_576, true, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom4MiB32Disk250(b *testing.B) {
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*4, b.N, 32, 4*1_048_576, false, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom2MiB32NoCommit250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*2, b.N, 32, 2*1_048_576, true, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom2MiB32InMem250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576*2, b.N, 32, 2*1_048_576, false, 250_000)
	back.close()
}
func BenchmarkTrieAddFrom1MiB32InMem250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, 1_048_576, b.N, 32, 1*1_048_576, false, 250_000)
	back.close()
}

func addKeysNoopEvict(mt *Trie, accounts int, totalBatches int, keyLength int, prepopulateCount int, skipCommit bool, batchSize int) {
	var accts [][]byte
	fmt.Println("Prepopulating the trie with ", prepopulateCount, " accounts")
	fmt.Println("mt", (mt))

	acctsNeededForBatch := int(math.Min(float64(accounts), math.Min(float64(batchSize*totalBatches), float64(accounts))))

	accts = make([][]byte, 0, prepopulateCount)

	for m := 0; m < prepopulateCount; m++ {
		k := make([]byte, keyLength) // 32 length keys == crypto.digest.  Trie adds one byte.
		rand.Read(k)
		for j := range k {
			k[j] = k[j] & 0x0f // nibbles only
		}
		mt.Add(k, k) // just add the key as the value
		if m < acctsNeededForBatch {
			accts = append(accts, k)
		}

		if m%(prepopulateCount/10) == (prepopulateCount/10)-1 {
			fmt.Printf("Prepopulated with %d accounts (%4.2f %%)\n", m, float64(m)/float64(prepopulateCount)*100)
		}
	}
	if !skipCommit {
		mt.Commit()
	}
	if len(accts) < acctsNeededForBatch {
		fmt.Println("Generating remaining accounts")
		for m := len(accts); m < acctsNeededForBatch; m++ {
			k := make([]byte, keyLength) // 32 length keys == crypto.digest.  Trie adds one byte.
			rand.Read(k)
			for j := range k {
				k[j] = k[j] & 0x0f // nibbles only
			}
			accts = append(accts, k)
		}
	}

	fmt.Println("mt", (mt))
	fmt.Println("Adding keys (batch size", batchSize, ", accounts", accounts, ", totalBatches", totalBatches, ", total keys:", totalBatches*batchSize, ")")
	for m := 0; m < totalBatches; m++ {

		epochStart := time.Now().Truncate(time.Millisecond)
		for i := 0; i < batchSize; i++ {
			randK := pseudoRand() % uint32(len(accts))
			randV := pseudoRand() % uint32(len(accts))
			mt.Add(accts[randK], accts[randV])
		}
		fmt.Println("Committing", batchSize, "accounts")
		if !skipCommit {
			mt.Commit()
		}

		//        shouldEvict := func(n node) bool {
		//			if _, ok := n.(*branchNode); ok {
		//				bn := n.(*branchNode)
		//				for i := 0; i < 16; i++ {
		//					if _, ok2 := bn.children[i].(*branchNode); ok2 {
		//						return false
		//					}
		//				}
		//				if rand.Intn(10) == 1 {
		//					return false
		//				}
		//			}
		//			return false
		//		}
		//		fmt.Println("Evicting")
		//		mt.root.evict(shouldEvict)

		epochEnd := time.Now().Truncate(time.Millisecond)
		timeConsumed := epochEnd.Sub(epochStart)
		fmt.Println("mt", (mt))
		fmt.Println("time", timeConsumed, "new hash:", mt.root.getHash(), stats.String(), "len(mt.dels):", len(mt.dels))
	}
	fmt.Println("Done", batchSize, ", accounts", accounts, ", totalBatches", totalBatches, ", total keys:", totalBatches*batchSize, ")")
}

func TestTrieBobInMem(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, 1_024, 20, 64, 1_048_576, false, 250_000)
	back.close()
}
func TestTrieAdd10Batches250kIntoPreloadPebbleTest(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	fmt.Println("Preloading")
	mt.root.preload(back)
	addKeysNoopEvict(mt, 250_000, 10, 64, 0, false, 250_000)
	back.close()
}
func TestTrieAdd10Batches250kIntoPebbleTest(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, 250_000, 10, 64, 0, false, 250_000)
	back.close()
}
func TestTrieOriginalAddFrom2MiBInMem(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, 1_048_576*2, 5, 32, 1_048_576*2, false, 250_000)
	back.close()
}
func TestTrieOriginalAddFrom2MiBDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())
	back := makePebbleBackstoreDisk("pebble.test", true)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, 1_048_576*2, 5, 32, 1_048_576*2, false, 250_000)
	back.close()
}
func TestTrieCreate16MiBDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble.test", true)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, 1_048_576*16, 0, 32, 1_048_576*16, false, 250_000)
	back.close()
}

func TestTrieAddFrom1MiBDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, 1_048_576*1, 5, 32, 1_048_576*1, false, 104_857)
	back.close()
}
func TestTrieAddFrom4MiBDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, 1_048_576*4, 5, 32, 1_048_576*4, false, 104_857)
	back.close()
}
func TestTrieAddFrom1MiB(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, 1_048_576*1, 5, 32, 1_048_576*1, false, 104_857)
	back.close()
}
func countDBNodes(store backing, mt *Trie) {
	var nc struct {
		branches int
		leaves   int
		exts     int
	}
	l := func() func(n node) {
		innerCount := func(n node) {
			switch n.(type) {
			case *branchNode:
				nc.branches++
			case *leafNode:
				nc.leaves++
			case *extensionNode:
				nc.exts++
			}
		}
		return innerCount
	}()

	fmt.Println("Preloading tree")
	mt.root.preload(store)
	fmt.Println("Counting nodes")
	mt.root.lambda(l)
	fmt.Println("Branches", nc.branches)
	fmt.Println("Leaves", nc.leaves)
	fmt.Println("Exts", nc.exts)
	fmt.Println("Total", nc.branches+nc.leaves+nc.exts)
}
func TestCountDBNodes(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	//	back := makePebbleBackstoreDisk("pebble.2648983", false)
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	countDBNodes(back, mt)
	back.close()
}

func makebacking(cd crypto.Digest) node {
	return makeBackingNode(&cd, []byte{0x01, 0x02, 0x03, 0x04})
}

func XTestNodeSerialization(t *testing.T) {
	ln := &leafNode{}
	ln.keyEnd = []byte("leafendkey")
	for i := range ln.keyEnd {
		ln.keyEnd[i] &= 0x0f
	}
	ln.valueHash = crypto.Hash([]byte("leafvalue"))
	data, err := ln.serialize()
	require.NoError(t, err)
	expected := []byte{0x4, 0x9a, 0xf2, 0xee, 0x24, 0xf9, 0xd3, 0xde, 0x8d, 0xdb, 0x45, 0x71, 0x82, 0x90, 0xca, 0x38, 0x42, 0xad, 0x8e, 0xcf, 0x81, 0x56, 0x17, 0x16, 0x55, 0x42, 0x73, 0x6, 0xaa, 0xd0, 0x16, 0x87, 0x45, 0xc5, 0x16, 0x5e, 0x4b, 0x59}
	require.Equal(t, expected, data)
	ln2 := deserializeLeafNode(data, nibbles{0x0, 0x1, 0x2, 0x3})
	require.NoError(t, err)
	require.Equal(t, ln, ln2)
	ln.keyEnd = []byte("leafendke")
	for i := range ln.keyEnd {
		ln.keyEnd[i] &= 0x0f
	}
	data, err = ln.serialize()
	require.NoError(t, err)
	expected = []byte{0x3, 0x9a, 0xf2, 0xee, 0x24, 0xf9, 0xd3, 0xde, 0x8d, 0xdb, 0x45, 0x71, 0x82, 0x90, 0xca, 0x38, 0x42, 0xad, 0x8e, 0xcf, 0x81, 0x56, 0x17, 0x16, 0x55, 0x42, 0x73, 0x6, 0xaa, 0xd0, 0x16, 0x87, 0x45, 0xc5, 0x16, 0x5e, 0x4b, 0x50}
	require.Equal(t, expected, data)
	ln3 := deserializeLeafNode(data, nibbles{0x0, 0x1, 0x2, 0x3})
	require.NoError(t, err)
	require.Equal(t, ln, ln3)

	bn := &branchNode{}
	bn.children[0] = makebacking(crypto.Hash([]byte("branchchild0")))
	bn.children[1] = makebacking(crypto.Hash([]byte("branchchild1")))
	bn.children[2] = makebacking(crypto.Hash([]byte("branchchild2")))
	bn.children[3] = makebacking(crypto.Hash([]byte("branchchild3")))
	bn.children[4] = makebacking(crypto.Hash([]byte("branchchild4")))
	bn.children[5] = makebacking(crypto.Hash([]byte("branchchild5")))
	bn.children[6] = makebacking(crypto.Hash([]byte("branchchild6")))
	bn.children[7] = makebacking(crypto.Hash([]byte("branchchild7")))
	bn.children[8] = makebacking(crypto.Hash([]byte("branchchild8")))
	bn.children[9] = makebacking(crypto.Hash([]byte("branchchild9")))
	bn.children[10] = makebacking(crypto.Hash([]byte("branchchild10")))
	bn.children[11] = makebacking(crypto.Hash([]byte("branchchild11")))
	bn.children[12] = makebacking(crypto.Hash([]byte("branchchild12")))
	bn.children[13] = makebacking(crypto.Hash([]byte("branchchild13")))
	bn.children[14] = makebacking(crypto.Hash([]byte("branchchild14")))
	bn.children[15] = makebacking(crypto.Hash([]byte("branchchild15")))
	bn.valueHash = crypto.Hash([]byte("branchvalue"))
	data, err = bn.serialize()
	require.NoError(t, err)
	expected = []byte{0x5, 0xe8, 0x31, 0x2c, 0x27, 0xec, 0x3d, 0x32, 0x7, 0x48, 0xab, 0x13, 0xed, 0x2f, 0x67, 0x94, 0xb3, 0x34, 0x8f, 0x1e, 0x14, 0xe5, 0xac, 0x87, 0x6e, 0x7, 0x68, 0xd6, 0xf6, 0x92, 0x99, 0x4b, 0xc8, 0x2e, 0x93, 0xde, 0xf1, 0x72, 0xc8, 0x55, 0xbb, 0x7e, 0xd1, 0x1d, 0x38, 0x6, 0xd2, 0x97, 0xd7, 0x2, 0x2, 0x86, 0x93, 0x37, 0x57, 0xce, 0xa4, 0xc5, 0x7e, 0x4c, 0xd4, 0x50, 0x94, 0x2e, 0x75, 0xeb, 0xcd, 0x9b, 0x80, 0xa2, 0xf5, 0xf3, 0x15, 0x4a, 0xf2, 0x62, 0x6, 0x7d, 0x6d, 0xdd, 0xe9, 0x20, 0xe1, 0x1a, 0x95, 0x3b, 0x2b, 0xb9, 0xc1, 0xaf, 0x3e, 0xcb, 0x72, 0x1d, 0x3f, 0xad, 0xe9, 0xa6, 0x30, 0xc6, 0xc5, 0x65, 0xf, 0x86, 0xb2, 0x3a, 0x5b, 0x47, 0xcb, 0x29, 0x31, 0xf7, 0x8a, 0xdf, 0xe0, 0x41, 0x6b, 0x11, 0xc0, 0xd, 0xbc, 0x80, 0xa7, 0x48, 0x97, 0x21, 0xbd, 0xee, 0x6f, 0x36, 0xf4, 0x7b, 0x6d, 0x68, 0xa1, 0x43, 0x31, 0x90, 0xf8, 0x56, 0x69, 0x4c, 0xee, 0x88, 0x76, 0x9c, 0xd1, 0xde, 0xe4, 0xbd, 0x64, 0x7d, 0x18, 0xce, 0xd6, 0xdb, 0xf8, 0x85, 0x84, 0x88, 0x5d, 0x7e, 0xda, 0xe0, 0xf2, 0xa0, 0x6d, 0x24, 0x4f, 0xcf, 0xb, 0x8c, 0x34, 0x57, 0x2a, 0x13, 0x22, 0xd9, 0x8d, 0x79, 0x8, 0xa4, 0x22, 0x91, 0x45, 0x64, 0x7b, 0xf3, 0xad, 0xe8, 0x9b, 0x5f, 0x7c, 0x5c, 0xbd, 0x9, 0xd3, 0xc7, 0x3, 0xe2, 0xef, 0x6b, 0x8, 0x8, 0x98, 0x52, 0xb, 0xd1, 0x6a, 0x5a, 0x18, 0x89, 0x44, 0x4f, 0xf1, 0xb0, 0x37, 0xd9, 0x7f, 0x99, 0x3f, 0x6a, 0x84, 0x46, 0x83, 0x2c, 0x91, 0x58, 0xa8, 0xb3, 0xda, 0xd8, 0x26, 0x2e, 0x8a, 0x4, 0x8f, 0x81, 0xa5, 0xf3, 0xef, 0x46, 0x34, 0x4a, 0x8f, 0x6a, 0x61, 0x2f, 0x3, 0x26, 0x9d, 0xe6, 0x77, 0xee, 0xec, 0xe2, 0xa4, 0x84, 0x38, 0x6b, 0x6e, 0x7e, 0xf0, 0xef, 0xaa, 0x29, 0xa5, 0x13, 0x0, 0xef, 0xff, 0xdf, 0xb5, 0xd7, 0x4e, 0x41, 0x75, 0x4d, 0x2, 0x84, 0x20, 0xe2, 0x18, 0x50, 0x52, 0xae, 0xf4, 0xea, 0xeb, 0x84, 0xb3, 0x91, 0x85, 0xa8, 0xa, 0xba, 0xc9, 0x31, 0x9f, 0x5e, 0x3e, 0xf8, 0xb5, 0xf4, 0x4b, 0xf8, 0xf2, 0xf0, 0x76, 0xa1, 0x6d, 0xec, 0x57, 0x65, 0xbd, 0x2e, 0x78, 0xbe, 0xf4, 0x7c, 0xe4, 0xf2, 0x45, 0xc0, 0xaf, 0x94, 0xb, 0x45, 0x1b, 0xd3, 0xcf, 0x9f, 0x17, 0x7e, 0x1a, 0x52, 0x6d, 0x18, 0xe5, 0x1a, 0x7c, 0xd9, 0x9d, 0xef, 0x8a, 0xe3, 0xe9, 0xe6, 0xf6, 0x76, 0x5e, 0x12, 0xbf, 0xd2, 0xe8, 0xaa, 0x8, 0x88, 0x15, 0x81, 0x99, 0x4e, 0xa3, 0x12, 0x98, 0xc1, 0xb3, 0xde, 0x42, 0x53, 0x2, 0x29, 0x82, 0x87, 0xfe, 0x3d, 0x8, 0xe0, 0xc2, 0x3, 0x70, 0x56, 0xd, 0x9, 0xad, 0xe4, 0x1a, 0xa5, 0xf6, 0x4, 0xdb, 0x63, 0xd0, 0x49, 0x6b, 0x5b, 0xa2, 0x56, 0xb1, 0xd1, 0x4b, 0x56, 0xc3, 0x7e, 0x4b, 0xec, 0xb5, 0xdb, 0xd4, 0xd9, 0xe1, 0x20, 0x99, 0x80, 0x71, 0x9, 0x72, 0x3b, 0xc, 0x8b, 0x56, 0x4, 0x94, 0xe6, 0x4e, 0x35, 0xd, 0x3e, 0x7, 0x8b, 0x86, 0x73, 0x62, 0x5f, 0x61, 0x8d, 0x70, 0x68, 0x86, 0xe8, 0x65, 0xbe, 0x18, 0xa8, 0x4a, 0xac, 0x6d, 0x81, 0x15, 0xde, 0x1b, 0xe1, 0xb3, 0xe8, 0x6a, 0x46, 0xdf, 0xdc, 0xf1, 0x6, 0x3c, 0xa6, 0x1c, 0xc9, 0xcd, 0x12, 0x5e, 0x5f, 0x28, 0xd1, 0x71, 0x6e, 0x9f, 0xc7, 0xdc, 0x77, 0x98, 0x47, 0x7, 0x94, 0x38, 0x4, 0xc4, 0xc4, 0xfe, 0x17, 0x12, 0x1b, 0xcf, 0x96, 0xd8, 0xb1, 0xf2, 0x1e, 0x81, 0xab, 0x15, 0x86, 0x75, 0x5a, 0x39, 0x13, 0xdb, 0xe, 0x1a, 0xd9, 0xa9, 0x70, 0x7d, 0xdd, 0xaf, 0x64, 0x12, 0x27, 0xe5, 0x97, 0xa1, 0x34, 0xb8, 0x1a, 0x61, 0x48, 0x29, 0x61, 0x62, 0xe4, 0x40, 0xba, 0x5, 0x44, 0x24, 0x51, 0xc1, 0x9b, 0x8e, 0x62, 0xf2, 0x1c, 0x6f, 0xd6, 0x8, 0x3, 0xbe, 0x88, 0xf}
	require.Equal(t, expected, data)
	bn2 := deserializeBranchNode(data, nibbles{0x01, 0x02, 0x03})
	require.NoError(t, err)
	require.Equal(t, bn, bn2)

	bn.children[7] = nil
	data, err = bn.serialize()
	require.NoError(t, err)
	expected = []byte{0x5, 0xe8, 0x31, 0x2c, 0x27, 0xec, 0x3d, 0x32, 0x7, 0x48, 0xab, 0x13, 0xed, 0x2f, 0x67, 0x94, 0xb3, 0x34, 0x8f, 0x1e, 0x14, 0xe5, 0xac, 0x87, 0x6e, 0x7, 0x68, 0xd6, 0xf6, 0x92, 0x99, 0x4b, 0xc8, 0x2e, 0x93, 0xde, 0xf1, 0x72, 0xc8, 0x55, 0xbb, 0x7e, 0xd1, 0x1d, 0x38, 0x6, 0xd2, 0x97, 0xd7, 0x2, 0x2, 0x86, 0x93, 0x37, 0x57, 0xce, 0xa4, 0xc5, 0x7e, 0x4c, 0xd4, 0x50, 0x94, 0x2e, 0x75, 0xeb, 0xcd, 0x9b, 0x80, 0xa2, 0xf5, 0xf3, 0x15, 0x4a, 0xf2, 0x62, 0x6, 0x7d, 0x6d, 0xdd, 0xe9, 0x20, 0xe1, 0x1a, 0x95, 0x3b, 0x2b, 0xb9, 0xc1, 0xaf, 0x3e, 0xcb, 0x72, 0x1d, 0x3f, 0xad, 0xe9, 0xa6, 0x30, 0xc6, 0xc5, 0x65, 0xf, 0x86, 0xb2, 0x3a, 0x5b, 0x47, 0xcb, 0x29, 0x31, 0xf7, 0x8a, 0xdf, 0xe0, 0x41, 0x6b, 0x11, 0xc0, 0xd, 0xbc, 0x80, 0xa7, 0x48, 0x97, 0x21, 0xbd, 0xee, 0x6f, 0x36, 0xf4, 0x7b, 0x6d, 0x68, 0xa1, 0x43, 0x31, 0x90, 0xf8, 0x56, 0x69, 0x4c, 0xee, 0x88, 0x76, 0x9c, 0xd1, 0xde, 0xe4, 0xbd, 0x64, 0x7d, 0x18, 0xce, 0xd6, 0xdb, 0xf8, 0x85, 0x84, 0x88, 0x5d, 0x7e, 0xda, 0xe0, 0xf2, 0xa0, 0x6d, 0x24, 0x4f, 0xcf, 0xb, 0x8c, 0x34, 0x57, 0x2a, 0x13, 0x22, 0xd9, 0x8d, 0x79, 0x8, 0xa4, 0x22, 0x91, 0x45, 0x64, 0x7b, 0xf3, 0xad, 0xe8, 0x9b, 0x5f, 0x7c, 0x5c, 0xbd, 0x9, 0xd3, 0xc7, 0x3, 0xe2, 0xef, 0x6b, 0x8, 0x8, 0x98, 0x52, 0xb, 0xd1, 0x6a, 0x5a, 0x18, 0x89, 0x44, 0x4f, 0xf1, 0xb0, 0x37, 0xd9, 0x7f, 0x99, 0x3f, 0x6a, 0x84, 0x46, 0x83, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x84, 0x38, 0x6b, 0x6e, 0x7e, 0xf0, 0xef, 0xaa, 0x29, 0xa5, 0x13, 0x0, 0xef, 0xff, 0xdf, 0xb5, 0xd7, 0x4e, 0x41, 0x75, 0x4d, 0x2, 0x84, 0x20, 0xe2, 0x18, 0x50, 0x52, 0xae, 0xf4, 0xea, 0xeb, 0x84, 0xb3, 0x91, 0x85, 0xa8, 0xa, 0xba, 0xc9, 0x31, 0x9f, 0x5e, 0x3e, 0xf8, 0xb5, 0xf4, 0x4b, 0xf8, 0xf2, 0xf0, 0x76, 0xa1, 0x6d, 0xec, 0x57, 0x65, 0xbd, 0x2e, 0x78, 0xbe, 0xf4, 0x7c, 0xe4, 0xf2, 0x45, 0xc0, 0xaf, 0x94, 0xb, 0x45, 0x1b, 0xd3, 0xcf, 0x9f, 0x17, 0x7e, 0x1a, 0x52, 0x6d, 0x18, 0xe5, 0x1a, 0x7c, 0xd9, 0x9d, 0xef, 0x8a, 0xe3, 0xe9, 0xe6, 0xf6, 0x76, 0x5e, 0x12, 0xbf, 0xd2, 0xe8, 0xaa, 0x8, 0x88, 0x15, 0x81, 0x99, 0x4e, 0xa3, 0x12, 0x98, 0xc1, 0xb3, 0xde, 0x42, 0x53, 0x2, 0x29, 0x82, 0x87, 0xfe, 0x3d, 0x8, 0xe0, 0xc2, 0x3, 0x70, 0x56, 0xd, 0x9, 0xad, 0xe4, 0x1a, 0xa5, 0xf6, 0x4, 0xdb, 0x63, 0xd0, 0x49, 0x6b, 0x5b, 0xa2, 0x56, 0xb1, 0xd1, 0x4b, 0x56, 0xc3, 0x7e, 0x4b, 0xec, 0xb5, 0xdb, 0xd4, 0xd9, 0xe1, 0x20, 0x99, 0x80, 0x71, 0x9, 0x72, 0x3b, 0xc, 0x8b, 0x56, 0x4, 0x94, 0xe6, 0x4e, 0x35, 0xd, 0x3e, 0x7, 0x8b, 0x86, 0x73, 0x62, 0x5f, 0x61, 0x8d, 0x70, 0x68, 0x86, 0xe8, 0x65, 0xbe, 0x18, 0xa8, 0x4a, 0xac, 0x6d, 0x81, 0x15, 0xde, 0x1b, 0xe1, 0xb3, 0xe8, 0x6a, 0x46, 0xdf, 0xdc, 0xf1, 0x6, 0x3c, 0xa6, 0x1c, 0xc9, 0xcd, 0x12, 0x5e, 0x5f, 0x28, 0xd1, 0x71, 0x6e, 0x9f, 0xc7, 0xdc, 0x77, 0x98, 0x47, 0x7, 0x94, 0x38, 0x4, 0xc4, 0xc4, 0xfe, 0x17, 0x12, 0x1b, 0xcf, 0x96, 0xd8, 0xb1, 0xf2, 0x1e, 0x81, 0xab, 0x15, 0x86, 0x75, 0x5a, 0x39, 0x13, 0xdb, 0xe, 0x1a, 0xd9, 0xa9, 0x70, 0x7d, 0xdd, 0xaf, 0x64, 0x12, 0x27, 0xe5, 0x97, 0xa1, 0x34, 0xb8, 0x1a, 0x61, 0x48, 0x29, 0x61, 0x62, 0xe4, 0x40, 0xba, 0x5, 0x44, 0x24, 0x51, 0xc1, 0x9b, 0x8e, 0x62, 0xf2, 0x1c, 0x6f, 0xd6, 0x8, 0x3, 0xbe, 0x88, 0xf}

	require.Equal(t, expected, data)
	bn3 := deserializeBranchNode(data, nibbles{0x01, 0x02, 0x03})
	require.NoError(t, err)

	require.Equal(t, bn, bn3)
	bn.children[0] = makebacking(crypto.Digest{})
	require.NotEqual(t, bn, bn3)

	en := &extensionNode{}
	en.sharedKey = []byte("extensionkey")
	for i := range en.sharedKey {
		en.sharedKey[i] &= 0x0f
	}
	en.next = makebacking(crypto.Hash([]byte("extensionnext")))
	data, err = en.serialize()
	require.NoError(t, err)
	expected = []byte{0x2, 0xa7, 0xa7, 0xc, 0x66, 0xad, 0xa, 0xc3, 0xef, 0xd6, 0x24, 0x4b, 0x78, 0x46, 0xbb, 0x4, 0x39, 0x28, 0xb9, 0xe2, 0xcf, 0xe0, 0x3e, 0x35, 0xa3, 0x91, 0x8e,
		0x83, 0xad, 0x36, 0x8, 0xb7, 0x5b, 0x58, 0x45, 0xe3, 0x9f, 0xeb, 0x59}
	require.Equal(t, expected, data)
	en2 := deserializeExtensionNode(data, nibbles{0x01, 0x02, 0x03})
	require.NoError(t, err)
	require.Equal(t, en, en2)
	en.sharedKey = []byte("extensionke")
	for i := range en.sharedKey {
		en.sharedKey[i] &= 0x0f
	}
	data, err = en.serialize()
	require.NoError(t, err)
	expected = []byte{0x1, 0xa7, 0xa7, 0xc, 0x66, 0xad, 0xa, 0xc3, 0xef, 0xd6, 0x24, 0x4b, 0x78, 0x46, 0xbb, 0x4, 0x39, 0x28, 0xb9, 0xe2, 0xcf, 0xe0, 0x3e, 0x35, 0xa3, 0x91, 0x8e,
		0x83, 0xad, 0x36, 0x8, 0xb7, 0x5b, 0x58, 0x45, 0xe3, 0x9f, 0xeb, 0x50}
	require.Equal(t, expected, data)
	en3 := deserializeExtensionNode(data, nibbles{0x01, 0x02, 0x03})
	require.NoError(t, err)
	require.Equal(t, en, en3)

	broken := []byte{0x6, 0xa7, 0xa7, 0xc, 0x66, 0xad, 0xa, 0xc3, 0xef, 0xd6, 0x24, 0x4b, 0x78, 0x46, 0xbb, 0x4, 0x39, 0x28, 0xb9, 0xe2, 0xcf, 0xe0, 0x3e, 0x35, 0xa3, 0x91, 0x8e,
		0x83, 0xad, 0x36, 0x8, 0xb7, 0x5b, 0x58, 0x45, 0xe3, 0x9f, 0xeb, 0x50}
	deserializeExtensionNode(broken, nibbles{0x01, 0x02, 0x03})
	expected = []byte{0x1, 0xa7, 0xa7, 0xc, 0x66, 0xad, 0xa, 0xc3, 0xef, 0xd6, 0x24, 0x4b, 0x78, 0x46, 0xbb, 0x4, 0x39, 0x28, 0xb9, 0xe2, 0xcf, 0xe0, 0x3e, 0x35, 0xa3, 0x91, 0x8e,
		0x83, 0xad, 0x36, 0x8, 0xb7, 0x5b, 0x58, 0x45, 0xe3, 0x9f, 0xeb, 0x50}
	//deserializeLeafNode(expected, nibbles{0x01, 0x02, 0x03})
	//deserializeBranchNode(expected, nibbles{0x01, 0x02, 0x03})

}

func buildDotGraph(t *testing.T, mt *Trie, keys [][]byte, values [][]byte, fn string) {
	dot := dotGraph(mt, keys, values)
	file, err := os.Create(fn)
	require.NoError(t, err)
	defer file.Close()
	_, err = file.WriteString(dot)
}

func TestTrieAdd1kEveryTwoSeconds(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	for m := 0; m < 2; m++ {
		//        fmt.Println("Adding 250k random key/value accounts")
		fmt.Println(stats.String())

		var k []byte
		var v []byte
		for i := 0; i < 250000; i++ {
			k = make([]byte, 32)
			v = make([]byte, 32)
			rand.Read(k)
			rand.Read(v)
			for j := range k {
				k[j] = k[j] & 0x0f
			}
			mt.Add(k, v)
		}
		fmt.Println(time.Now().Unix())
		fmt.Println(stats.String())

	}
	fmt.Println("Done adding 1k random key/value accounts")
	buildDotGraph(t, mt, [][]byte{}, [][]byte{}, "/tmp/trie1k.dot")
	back.close()
}
func TestTrieAdd1kRandomKeyValues(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	fmt.Println("Adding 1k random key/value accounts")
	fmt.Println(stats.String())

	var k []byte
	var v []byte
	for i := 0; i < 1000; i++ {
		k = make([]byte, 32)
		v = make([]byte, 32)
		rand.Read(k)
		rand.Read(v)
		for j := range k {
			k[j] = k[j] & 0x0f
		}
		mt.Add(k, v)
	}
	fmt.Println("Done adding 1k random key/value accounts")
	fmt.Println("Committing 1k random key/value accounts")
	mt.Commit()
	fmt.Println("Done committing 1k random key/value accounts")
	fmt.Println(stats.String())
	//	buildDotGraph(t, mt, [][]byte{}, [][]byte{}, "/tmp/trie1k.dot")
	back.close()

}

func TestTrieStupidAddSimpleSequenceNoCache(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	var k []byte
	var v []byte
	var kk [][]byte
	var vv [][]byte
	k = []byte{0x01, 0x02, 0x03}
	v = []byte{0x04, 0x05, 0x06}
	kk = append(kk, k)
	vv = append(vv, v)

	fmt.Printf("1rootHash: %v\n", mt.root)
	mt.Add(k, v)
	fmt.Printf("2rootHash: %v\n", mt.root)
	mt.Commit()
	fmt.Printf("3rootHash: %v\n", mt.root)
	//	buildDotGraph(t, mt, kk, vv, "/tmp/cachetrie1.dot")
	v = []byte{0x04, 0x05, 0x07}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("4rootHash: %v\n", mt.root)
	mt.Commit()

	//	buildDotGraph(t, mt, kk, vv, "/tmp/cachetrie2.dot")
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("5rootHash: %v\n", mt.root)
	mt.Commit()
	//	buildDotGraph(t, mt, kk, vv, "/tmp/cachetrie3.dot")

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("6rootHash: %v\n", mt.root)
	mt.Commit()
	//	buildDotGraph(t, mt, kk, vv, "/tmp/cachetrie4.dot")

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x0a}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	mt.Commit()
	//	buildDotGraph(t, mt, kk, vv, "/tmp/cachetrie5.dot")

	k = []byte{0x01, 0x02, 0x03, 0x04}
	v = []byte{0x04, 0x05, 0x0b}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("7rootHash: %v\n", mt.root)
	mt.Commit()
	//	buildDotGraph(t, mt, kk, vv, "/tmp/cachetrie6.dot")

	k = []byte{0x01, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("8rootHash: %v\n", mt.root)
	mt.Commit()
	//	buildDotGraph(t, mt, kk, vv, "/tmp/cachetrie7.dot")

	k = []byte{0x01, 0x0d, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	fmt.Printf("9rootHash: %v\n", mt.root)
	mt.Add(k, v)
	fmt.Printf("arootHash: %v\n", mt.root)
	mt.Commit()
	fmt.Printf("5rootHash: %v\n", mt.root)
	buildDotGraph(t, mt, kk, vv, "/tmp/cachetrie8.dot")

	back.close()
}

func TestTrieAddSimpleSequence(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	var k []byte
	var v []byte
	var kk [][]byte
	var vv [][]byte
	k = []byte{0x01, 0x02, 0x03}
	v = []byte{0x03, 0x05, 0x06}
	kk = append(kk, k)
	vv = append(vv, v)

	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie1.dot")
	fmt.Printf("done with that")

	v = []byte{0x04, 0x05, 0x07}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)

	buildDotGraph(t, mt, kk, vv, "/tmp/trie2.dot")
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie3.dot")

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie4.dot")

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x0a}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie5.dot")

	k = []byte{0x01, 0x02, 0x03, 0x04}
	v = []byte{0x04, 0x05, 0x0b}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie6.dot")

	k = []byte{0x01, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie7.dot")

	k = []byte{0x01, 0x0d, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie8.dot")

	//duplicate key and value
	k = []byte{0x01, 0x0d, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie9.dot")

	err := mt.Commit()
	require.NoError(t, err)
}

func TestNibbleUtilities(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
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
		require.True(t, half == (len(n)%2 == 1))
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

	sampleSharedNibbles := [][]nibbles{
		{{0x0, 0x1, 0x2, 0x9, 0x2}, {0x0, 0x1, 0x2}},
		{{0x4, 0x1}, {0x4, 0x1}},
		{{0x9, 0x2, 0x3}, {}},
		{{0x0}, {0x0}},
		{{}, {}},
	}
	for i, n := range sampleSharedNibbles {
		shared := sharedNibbles(n[0], sampleNibbles[i])
		require.True(t, bytes.Equal(shared, n[1]))
		shared = sharedNibbles(sampleNibbles[i], n[0])
		require.True(t, bytes.Equal(shared, n[1]))
	}
	require.True(t, bytes.Equal(shiftNibbles(sampleNibbles[0], -2), sampleNibbles[0]))
	require.True(t, bytes.Equal(shiftNibbles(sampleNibbles[0], -1), sampleNibbles[0]))
	require.True(t, bytes.Equal(shiftNibbles(sampleNibbles[0], 0), sampleNibbles[0]))
}

// DotGraph returns a dot graph of the trie
func dotGraph(mt *Trie, keysAdded [][]byte, valuesAdded [][]byte) string {
	var keys string
	for i := 0; i < len(keysAdded); i++ {
		keys += fmt.Sprintf("%x = %x\\n", keysAdded[i], valuesAdded[i])
	}
	fmt.Printf("root: %v\n", mt.root)
	return fmt.Sprintf("digraph trie { key [shape=box, label=\"key/value inserted:\\n%s\"];\n %s }\n", keys, dotGraphHelper(mt, mt.root, nibbles{}))
}

// dot graph generation helper
func dotGraphHelper(mt *Trie, n node, path nibbles) string {

	switch tn := n.(type) {
	case *backingNode:
		return dotGraphHelper(mt, mt.store.get(path), path)
	case *parent:
		return dotGraphHelper(mt, tn.p, path)
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

//	returns a string with the number of nodes in the trie but
//
// does not follow parent nodes or load backing nodes
func countNodes(mt *Trie) string {
	if mt.root == nil {
		return "Empty trie"
	}
	var nc struct {
		branches int
		leaves   int
		exts     int
		parents  int
		backings int
		values   int
	}

	count := func() func(n node) {
		innerCount := func(n node) {
			switch n.(type) {
			case *branchNode:
				nc.branches++
				bn := n.(*branchNode)
				if bn.valueHash != (crypto.Digest{}) {
					nc.values++
				}
			case *leafNode:
				nc.values++
				nc.leaves++
			case *extensionNode:
				nc.exts++
			case *parent:
				nc.parents++
			case *backingNode:
				nc.backings++
			}
		}
		return innerCount
	}()
	mt.root.lambda(count)

	var nmem struct {
		branches int
		leaves   int
		exts     int
		parents  int
		backings int
	}

	mem := func() func(n node) {
		innerCount := func(n node) {
			switch v := n.(type) {
			//estimates
			case *branchNode:
				nmem.branches += 16*16 + 32 + 24 + len(v.key) + 8 + 32
			case *leafNode:
				nmem.leaves += 24 + len(v.key) + 24 + len(v.keyEnd) + 32 + 8 + 32
			case *extensionNode:
				nmem.exts += 24 + len(v.key) + 24 + len(v.sharedKey) + 8 + 32
			case *parent:
				nmem.parents += 8
			case *backingNode:
				nmem.backings += len(v.key) + 8 + 32
			}
		}
		return innerCount
	}()
	mt.root.lambda(mem)

	return fmt.Sprintf("[nodes: total %d / valued %d (branches: %d, leaves: %d, exts: %d, parents: %d, backings: %d), mem: total %d (branches: %d, leaves: %d, exts: %d, parents: %d, backings: %d), len(dels):%d]",
		nc.branches+nc.leaves+nc.exts+nc.parents, nc.values,
		nc.branches, nc.leaves, nc.exts, nc.parents, nc.backings,
		nmem.branches+nmem.leaves+nmem.exts+nmem.parents,
		nmem.branches, nmem.leaves, nmem.exts, nmem.parents, nmem.backings, len(mt.dels))

}
