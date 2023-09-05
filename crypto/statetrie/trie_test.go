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
	"encoding/gob"
	"fmt"
	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/crypto/statetrie/nibbles"
	"github.com/algorand/go-algorand/test/partitiontest"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
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

func TestTrieReadme(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())

	mt := MakeTrie(nil)
	key1 := nibbles.Nibbles{0x08, 0x0e, 0x02, 0x08}
	val1 := nibbles.Nibbles{0x03, 0x09, 0x0a, 0x0c}
	key2 := nibbles.Nibbles{0x08, 0x0d, 0x02, 0x08}
	val2 := nibbles.Nibbles{0x03, 0x09, 0x0a, 0x0c}

	debugTrie = true
	mt.Add(key1, val1)
	fmt.Println("K1:V1 Hash:", mt.Hash())

	mt.Add(key2, val2)
	fmt.Println("K1:V1,K2:V2 Hash:", mt.Hash())

	mt.Delete(key2)
	fmt.Println("K1:V1 Hash:", mt.Hash())
	debugTrie = false
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
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
	mt.Add(key3, key4)
	mt.Add(key4, key5)
	H1H3H4 := mt.Hash()
	fmt.Println("Hash:", mt.Hash(), " K1 K3 K4")
	reset(mt)
	mt.Add(key3, key4)
	mt.Add(key4, key5)
	H3H4 := mt.Hash()
	fmt.Println("Hash:", mt.Hash(), " K3 K4 ", countNodes(mt, false, false))

	reset(mt)
	mt.Add(key1, key2)
	mt.Add(key2, key3)
	mt.Add(key3, key4)
	mt.Add(key4, key5)
	mt.Delete(key2)
	fmt.Println("Hash:", mt.Hash(), " K1 K2 K3 K4 D2")
	H1H2H3H4D2 := mt.Hash()
	require.Equal(t, H1H2H3H4D2, H1H3H4)

	mt.Delete(key1)
	fmt.Println("Hash:", mt.Hash(), " K1 K2 K3 K4 D2 D1 ", countNodes(mt, false, false))
	H1H2H3H4D2D1 := mt.Hash()
	require.Equal(t, H1H2H3H4D2D1, H3H4)
	reset(mt)

	reset(mt)
	fmt.Println("mt", countNodes(mt, false, false))
	fmt.Println("Add key1")
	mt.Add(key1, key2)
	require.Equal(t, H1, mt.Hash())
	fmt.Println("Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt, false, false))
	fmt.Println("Making child.. child = mt.Child")
	ch := mt.Child()
	require.Equal(t, H1, mt.Hash())
	fmt.Println("mt", countNodes(mt, false, false))
	require.Equal(t, H1, ch.Hash())
	fmt.Println("ch", countNodes(ch, false, false))

	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch, false, false))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt, false, false))

	fmt.Println("Add key2 to Child")
	ch.Add(key2, key3)
	require.Equal(t, H1, mt.Hash())
	require.Equal(t, H1H2, ch.Hash())

	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch, false, false))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt, false, false))

	fmt.Println("Merge...")
	ch.Merge()
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2, ch.Hash())

	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch, false, false))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt, false, false))

	fmt.Println("Add key3 to child")
	ch.Add(key3, key4)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3, ch.Hash())

	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch, false, false))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt, false, false))

	fmt.Println("Add key4 to child")
	ch.Add(key4, key5)
	require.Equal(t, H1H2, mt.Hash())
	require.Equal(t, H1H2H3H4, ch.Hash())
	fmt.Println("Child Hash:", ch.Hash())
	fmt.Println("ch", countNodes(ch, false, false))
	fmt.Println("Parent Hash:", mt.Hash())
	fmt.Println("mt", countNodes(mt, false, false))
	fmt.Println("Del key2 from child")
	ch.Delete(key2)
	fmt.Println("ch", countNodes(ch, false, false))
	fmt.Println("mt", countNodes(mt, false, false))
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
	mt.Commit(nil)
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
	mt.Commit(nil)
}

// var prepTrie *Trie
var akbPrint bool = false

func addKeyBatches(b *testing.B, mt *Trie, accounts acctGetter, totalBatches int, keyLength int, prepopulateCount int, batchSize int, e Eviction) ([]int, []int) {
	fmt.Println("addKeyBatches: totalBatches", totalBatches, "keyLength", keyLength, "prepopulateCount", prepopulateCount, "batchSize", batchSize)
	// prepopulate the trie
	var times []int
	var pct []int
	//	if prepTrie == nil {
	if true {
		if akbPrint {
			fmt.Println("Prepopulating trie...")
		}
		epochStart := time.Now().Truncate(time.Millisecond)
		var statsStart = stats
		for m := 0; m < prepopulateCount; m++ {
			k := accounts.getAcct()
			mt.Add(k, k)
			if m%(prepopulateCount/10) == (prepopulateCount/10)-1 {
				if akbPrint {
					fmt.Printf("Prepopulated with %d accounts (%4.2f %%)\n", m, float64(m)/float64(prepopulateCount)*100)
				}
				mt.Commit(nil)
				epochEnd := time.Now().Truncate(time.Millisecond)
				var statsEnd = stats
				timeConsumed := epochEnd.Sub(epochStart)
				var statsDiff = statsEnd.diff(statsStart)
				fmt.Println("time", timeConsumed, "new hash:", mt.root.getHash(), statsDiff.String(), "len(mt.dels):", len(mt.dels))
				epochStart = time.Now().Truncate(time.Millisecond)
			}
		}
		//		prepTrie = &Trie{store: mt.store, root: mt.root}
		if akbPrint {
			fmt.Println("Finished prepopulating trie...")
		}
	}

	if b != nil {
		b.ResetTimer()
	}
	for m := 0; m < totalBatches; m++ {
		epochStart := time.Now().Truncate(time.Millisecond)
		var statsStart = stats
		for i := 0; i < batchSize; i++ {
			mt.Add(accounts.getAcct(), accounts.getAcct())
		}
		mt.Commit(e)
		epochEnd := time.Now().Truncate(time.Millisecond)
		var statsEnd = stats
		timeConsumed := epochEnd.Sub(epochStart)
		var statsDiff = statsEnd.diff(statsStart)
		if akbPrint {
			fmt.Println("time", timeConsumed, "new hash:", mt.root.getHash(), statsDiff.String(), "len(mt.dels):", len(mt.dels))
		}
		epochStart = time.Now().Truncate(time.Millisecond)
		if len(pct) == 0 || int(100.0*float64(m+1)/float64(totalBatches)) != pct[len(pct)-1] {
			times = append(times, int(timeConsumed.Milliseconds()))
			pct = append(pct, int(100.0*float64(m+1)/float64(totalBatches)))
			if akbPrint || true {
				fmt.Println("times:", times, "pct:", pct, "m:", m, "batchSize:", batchSize)
			}
		}
	}
	if b != nil {
		b.StopTimer()
	}
	return times, pct
}
func sum(data []int) (s int) {
	for _, v := range data {
		s += int(v)
	}
	return
}

func keysPerSecond(t *testing.T, mt *Trie, accts acctGetter, batchSize int, treeSize int, e Eviction) ([]int, []int) {
	totalBatches := treeSize / batchSize
	times, pct := addKeyBatches(nil, mt, accts, totalBatches, 64, 0, batchSize, e)
	kps := float64(batchSize) * float64(totalBatches) / float64(sum(times)) / float64(len(times))
	avg := float64(sum(times)) / float64(len(times))
	fmt.Println("treeSize:", treeSize, "batchSize:", batchSize, "totalBatches:", totalBatches, "kps:", kps, "avg round time:", avg, "ms")
	return times, pct
}

func arrayOfPerfs2(t *testing.T, ds *DataStore, label string) {
	//    back := makeNullBackstore()
	//    back := makeMemoryBackstore()
	//    back := makeFileBackstore()
	//    back := makePebbleBackstoreVFS()
	back := makePebbleBackstoreDisk("pebble.test", false)
	for i := 2; i < 50_000; i = i * 2 {
		mt := MakeTrie(back) /// yes make the trie from scratch each time
		times, pct := keysPerSecond(t, mt, acctsRand, i, 50*i, MakeEvictAll())
		ds.AddLine(pct, times, label+string("_")+strconv.Itoa(i))
	}
}

func arrayOfPerfs(t *testing.T, ds *DataStore, batchSize int, label string) {
	//    back := makeNullBackstore()
	//    back := makeMemoryBackstore()
	//    back := makeFileBackstore()
	//    back := makePebbleBackstoreVFS()
	back := makePebbleBackstoreDisk("/tmp/pebble/test/", true)
	mt := MakeTrie(back)
	times, pct := keysPerSecond(t, mt, acctsRand, batchSize, 64*1_048_576, MakeEvictAll())
	ds.AddLine(pct, times, label)
}

type LineData struct {
	x     []int
	y     []int
	label string
}
type DataStore struct {
	lines []LineData
}

func (ds *DataStore) AddLine(x []int, y []int, label string) {
	ds.lines = append(ds.lines, LineData{x: x, y: y, label: label})
}
func (ds *DataStore) DumpPythonScript(filename string) error {
	script := `import matplotlib.pyplot as plt
names = []
`

	for _, lineData := range ds.lines {
		xValues := make([]string, len(lineData.x))
		yValues := make([]string, len(lineData.y))
		for j, b := range lineData.x {
			xValues[j] = strconv.Itoa(int(b))
		}
		for j, b := range lineData.y {
			yValues[j] = strconv.Itoa(int(b))
		}
		script += fmt.Sprintf("d%s_x = [0.5 * a for a in [%s]]\n", lineData.label, strings.Join(xValues, ","))
		script += fmt.Sprintf("d%s_y = [%s]\n", lineData.label, strings.Join(yValues, ","))
		script += fmt.Sprintf("names.append([d%s_x, d%s_y, '%s'])\n", lineData.label, lineData.label, lineData.label)
	}

	script += `
plt.figure(figsize=(10,6))
for x, y, n in names:
    mp = len(x) // 2
    plt.annotate(n, (x[mp], y[mp]), textcoords="offset points", xytext=(0,10), ha='center')

`

	for _, lineData := range ds.lines {
		plotLine := fmt.Sprintf("plt.plot(d%s_x, d%s_y, '-o')\n", lineData.label, lineData.label)
		script += plotLine
	}

	script += `
plt.xlabel('Round')
plt.ylabel('Round time (ms)')
plt.savefig("image.jpg", dpi=300, bbox_inches='tight')
`

	return ioutil.WriteFile(filename, []byte(script), 0644)
}
func TestBackingTrieWriteKeys(t *testing.T) {
	fmt.Println(t.Name())
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	writeKeys(mt, "keys.bin", 64*1_048_576, 64*1_048_576)
}
func TestBackingTrieRootHash(t *testing.T) {
	fmt.Println(t.Name())
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	fmt.Println("root hash:", mt.Hash())
}
func TestVerifyBackingTrieCountNodes(t *testing.T) {
	fmt.Println(t.Name())
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(makeUntrustedBackstore(back))
	fmt.Println("count:", countNodes(mt, true, false))
}
func TestBackingTrieCountNodes(t *testing.T) {
	fmt.Println(t.Name())
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	fmt.Println("count:", countNodes(mt, true, false))
}
func TestSpewPebbleDisk(t *testing.T) {
	fmt.Println(t.Name())
	back := makePebbleBackstoreDisk("pebble.test", false)
	iter := back.db.NewIter(nil)
	for iter.SeekGE(nibbles.Nibbles{}); iter.Valid(); iter.Next() {
		k := iter.Key()
		fmt.Printf("len(k): %d k: %x\n", len(k), k)
	}
	back.close()

	//	iter := d.NewIter(readOptions)
	//	for iter.SeekGE(k); iter.Valid(); iter.Next() {
	//		fmt.Printf("key=%q value=%q\n", iter.Key(), iter.Value())
	//	}
	//	return iter.Close()
}
func TestRealtimeSpeedsWithP64SetPerfEvict(t *testing.T) { //nolint:paralleltest // Serial tests for trie for the momen t
	fmt.Println(t.Name())
	akbPrint = false
	resetPseudoRandomSeed()
	ds := &DataStore{}
	back := makePebbleBackstoreDisk("pebble.test", false)
	acctRatio := makeAcctGetterRandomFromPoolFileRatio(25, 50_000, "keys.bin")
	mt := MakeTrie(back) /// yes make the trie from scratch each time
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
	for i := 4; i <= 9; i++ {
		times, pct := keysPerSecond(t, mt, acctRatio, 16384, 15*16384, MakeEvictLevel(i))
		ds.AddLine(pct, times, string("P64_16384_evict_")+strconv.Itoa(i))

		runtime.ReadMemStats(&m)
		fmt.Printf("Levels deleted: %d\n", i)
		fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
		fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
		fmt.Printf("\tNumGC = %v\n", m.NumGC)
	}
	ds.DumpPythonScript("plot.py")
	back.close()
}
func TestRealtimeSpeedsWithP64SetPerf(t *testing.T) { //nolint:paralleltest // Serial tests for trie for the momen t
	fmt.Println(t.Name())
	akbPrint = false
	resetPseudoRandomSeed()
	ds := &DataStore{}
	back := makePebbleBackstoreDisk("pebble.test", false)
	cpuprof, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(cpuprof)
	acctRatio := makeAcctGetterRandomFromPoolFileRatio(25, 50_000, "keys.bin")
	mt := MakeTrie(back) /// yes make the trie from scratch each time
	keysPerSecond(t, mt, acctRatio, 32768, 15*32768, MakeEvictLevel(6))
	//        ds.AddLine(pct, times, string("P64_")+strconv.Itoa(25)+string("_")+strconv.Itoa(i))
	pprof.StopCPUProfile()
	cpuprof.Close()
	runtime.GC()
	memprof, _ := os.Create("mem.prof")
	pprof.WriteHeapProfile(memprof)
	memprof.Close()
	ds.DumpPythonScript("plot.py")
	back.close()
}

func TestFastFormatAdd64(t *testing.T) { //nolint:paralleltest // Serial tests for trie for the momen t
	fmt.Println(t.Name())
	akbPrint = false
	resetPseudoRandomSeed()
	//    evict := 6
	//	acctRatio := makeAcctGetterRandomFromPoolFileRatio(100, 50_000, "keys.bin")
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	preload := 5
	mt.Preload(preload)
	//    i := 65_536
	//    i := 65_536*2
	//    m := 0
	cpuprof, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(cpuprof)

	prepopulateCount := 1_048_576 * 16
	epochStart := time.Now().Truncate(time.Millisecond)
	var statsStart = stats
	for m := 0; m < prepopulateCount; m++ {
		k := acctsRand.getAcct()
		mt.Add(k, k)
		if m%(prepopulateCount/10) == (prepopulateCount/10)-1 {
			fmt.Printf("Prepopulated with %d accounts (%4.2f %%)\n", m, float64(m)/float64(prepopulateCount)*100)
			epochEnd := time.Now().Truncate(time.Millisecond)
			var statsEnd = stats
			timeConsumed := epochEnd.Sub(epochStart)
			var statsDiff = statsEnd.diff(statsStart)
			fmt.Println("time", timeConsumed, statsDiff.String(), "len(mt.dels):", len(mt.dels))
			epochStart = time.Now().Truncate(time.Millisecond)
			mt.Commit(nil)
		}
	}
	mt.Commit(nil)
	//		prepTrie = &Trie{store: mt.store, root: mt.root}
	fmt.Println("Finished prepopulating trie...")
	fmt.Println("hash", mt.Hash())
	pprof.StopCPUProfile()
	cpuprof.Close()
	back.close()
}

func TestRealtimeSpeedsWithP64SetKeepGoing(t *testing.T) { //nolint:paralleltest // Serial tests for trie for the momen t
	fmt.Println(t.Name())
	akbPrint = false
	resetPseudoRandomSeed()
	back := makePebbleBackstoreDisk("pebble.test", false)
	preload := 5
	//    evict := 6
	acctRatio := makeAcctGetterRandomFromPoolFileRatio(100, 50_000, "keys.bin")
	mt := MakeTrie(back)
	fmt.Println("Preloading 5")
	mt.Preload(preload)
	//    i := 65_536
	i := 65_536 * 2
	m := 0
	for {
		m++
		keysPerSecond(t, mt, acctRatio, i, 15*i, MakeEvictNone())
		fmt.Printf("i: %d m: %d = MiB: %4.1f\n", i, m, float64(i*m*15)/float64((1024*1024)))
	}
}

func TestRealtimeSpeedsWithP64Set5(t *testing.T) { //nolint:paralleltest // Serial tests for trie for the momen t
	fmt.Println(t.Name())
	akbPrint = false
	resetPseudoRandomSeed()
	back := makePebbleBackstoreDisk("pebble.test", false)
	//	back := makePebbleBackstoreVFS()
	//    var back backing
	preload := 5
	evict := 5
	//    ratio := 0
	fmt.Println("Reading accounts...")
	//	acctRatio := makeAcctGetterRandomFromPoolFileRatio(ratio, 50_000, "keys.bin")
	mt := MakeTrie(back)
	fmt.Println("Preloading", preload)
	mt.Preload(preload)
	ds := &DataStore{}
	//    var poolSizes = []int{250_000, 500_000, 1_000_000, 2_000_000, 4_000_000, 8_000_000, 16_000_000, 32_000_000, 64_000_000}
	//    var poolSizes = []int{25_000, 50_000, 75_000, 100_000, 200_000, 300_000}
	//	var poolSizes = []int{8_192, 16_384, 32_768}
	var poolSizes = []int{64_000_000}
	//	var poolSizes = []int{8_192}

	for _, ps := range poolSizes {
		runtime.GC()
		fmt.Println("loading pool")
		//		acctPool := makeAcctGetterRandomFromPoolFile(ps, "keys.bin")
		acctPool := makeAcctGetterRandomEachTime(64)
		cpuprof, _ := os.Create("cpu.prof")
		pprof.StartCPUProfile(cpuprof)
		debugTrie = false
		times, pct := keysPerSecond(t, mt, acctPool, 8192, 25*8192, MakeEvictLevel(evict))
		pprof.StopCPUProfile()
		cpuprof.Close()
		ds.AddLine(pct, times, string("P64_8192_")+strconv.Itoa(ps))
	}

	ds.DumpPythonScript("PS_8192_poolsize.py")

	//	acctSmallPool := makeAcctGetterRandomFromPoolFileRatio(ratio, 250_000, "keys.bin")
	//	acctBigPool := makeAcctGetterRandomFromPoolFileRatio(ratio, 250_000, "keys.bin")
	//	acctRando  := makeAcctGetterRandomEachTime(64)
	//    for bigOrRandRatio := 0; bigOrRandRatio <= 100; bigOrRandRatio += 25 {
	//        for fastAcctOrSlowRatio := 0; fastAcctOrSlowRatio <= 100; fastAcctOrSlowRatio += 25 {
	//            runtime.GC()
	//            acctRatio1 := makeAcctGetterRatio(bigOrRandRatio, acctBigPool, acctRando)
	//            acctRatio2 := makeAcctGetterRatio(fastAcctOrSlowRatio, acctSmallPool, acctRatio1)
	//            acctRatio := acctRatio2
	//    	for i := 8_192; i < 66_000; i = i * 2 {
	//            debugTrie = true
	//		times, pct := keysPerSecond(t, mt, acctRatio, i, 15*i, MakeEvictLevel(evict))
	//    		times, pct := keysPerSecond(t, mt, acctRatio, 8192, 15*8192, MakeEvictLevel(evict))
	//            times, pct := keysPerSecond(t, mt, acctRatio, 2, 3, MakeEvictLevel(5))
	//		//            debugTrie = false
	//    		ds.AddLine(pct, times, string("KPS_8192_15_0RandOrBig_") + strconv.Itoa(bigOrRandRatio) + "_100SmallOrOther" + strconv.Itoa(fastAcctOrSlowRatio))
	//        }
	//	}
	//back.close()
}
func TestRealtimeSpeeds(t *testing.T) { //nolint:paralleltest // Serial tests for trie for the momen t
	fmt.Println(t.Name())
	akbPrint = false
	resetPseudoRandomSeed()
	ds := &DataStore{}
	//	/ds.AddLine(ds, []byte{7, 14, 21}, "secondLine")
	//    arrayOfPerfs(t, ds,524288, string("KPB_524288_1"))
	//    arrayOfPerfs(t, ds,131072, string("KPB_131072_1"))
	//    arrayOfPerfs(t, ds,65536, string("KPB_65536_1"))
	//    arrayOfPerfs(t, ds, 262144, string("KPB_262144_1"))
	//    arrayOfPerfs(t, ds, 262144, string("KPB_262144_1"))
	arrayOfPerfs2(t, ds, string("Trie64MiB"))

	err := ds.DumpPythonScript("plot.py")
	if err != nil {
		fmt.Println("Error generating Python script:", err)
	}

	return
}

func TestSpeeds(t *testing.T) { //nolint:paralleltest // Serial tests for trie for the momen t
	// t.Parallel()

	fmt.Println(t.Name())
	akbPrint = false

	ds := &DataStore{}
	//	/ds.AddLine(ds, []byte{7, 14, 21}, "secondLine")
	//    arrayOfPerfs(t, ds,524288, string("KPB_524288_1"))
	//    arrayOfPerfs(t, ds,131072, string("KPB_131072_1"))
	//    arrayOfPerfs(t, ds,65536, string("KPB_65536_1"))
	//    arrayOfPerfs(t, ds, 262144, string("KPB_262144_1"))
	//    arrayOfPerfs(t, ds, 262144, string("KPB_262144_1"))
	arrayOfPerfs(t, ds, 262144, string("KPB_262144_1"))

	err := ds.DumpPythonScript("plot.py")
	if err != nil {
		fmt.Println("Error generating Python script:", err)
	}

	return
}

func BenchmarkTrieAddFromRandomNullStore250(b *testing.B) {
	back := makeNullBackstore()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, acctsRand, b.N, 63, 64*1_048_576, 250_000, MakeEvictNone())
	mt.store.close()
}
func BenchmarkTrieAddFrom64KiB32NullCommit25(b *testing.B) {
	back := makeNullBackstore()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, accts64KiB, b.N, 32, 1*65_536, 25_000, MakeEvictNone())
	mt.store.close()
}
func BenchmarkTrieAddFrom64KiB32Disk25(b *testing.B) {
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeyBatches(b, mt, accts64KiB, b.N, 32, 1*65_536, 25_000, MakeEvictNone())
	mt.store.close()
}
func BenchmarkTrieAddFrom64KiB64InMem25(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, accts64KiB, b.N, 64, 1*65_536, 25_000, MakeEvictNone())
	mt.store.close()
}
func BenchmarkTrieAddFrom64KiB32InMem25(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, accts64KiB, b.N, 32, 1*65_536, 25_000, MakeEvictNone())
	mt.store.close()
}
func BenchmarkTrieAddFrom16MiB32Disk250(b *testing.B) {
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	if accts16MiB == nil {
		accts16MiB = makeAcctGetterRandomFromPool(1_048_576*16, 64)
	}
	addKeyBatches(b, mt, accts16MiB, b.N, 32, 16*1_048_576, 250_000, MakeEvictNone())
	mt.store.close()
}
func BenchmarkTrieAddFrom8MiB32Disk250(b *testing.B) {
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeyBatches(b, mt, accts8MiB, b.N, 32, 8*1_048_576, 250_000, MakeEvictNone())
	mt.store.close()
}
func BenchmarkTrieAddFrom4MiB32Disk250(b *testing.B) {
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeyBatches(b, mt, accts4MiB, b.N, 32, 4*1_048_576, 250_000, MakeEvictNone())
	mt.store.close()
}
func BenchmarkTrieAddFrom2MiB32InMem250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, accts2MiB, b.N, 32, 2*1_048_576, 250_000, MakeEvictNone())
	mt.store.close()
}
func BenchmarkTrieAddFrom1MiB32InMem250(b *testing.B) {
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeyBatches(b, mt, accts1MiB, b.N, 32, 1*1_048_576, 250_000, MakeEvictNone())
	mt.store.close()
}

type acctGetter interface {
	getAcct() []byte
}

type acctGetterRandomEachTime struct {
	acctGetter
	buf       []byte
	keyLength int
}

func (acret *acctGetterRandomEachTime) getAcct() []byte {
	for i := 0; i < acret.keyLength; i++ {
		acret.buf[i] = byte(uint32(pseudoRand()) & 0x0f) // Nibbles only
	}

	return acret.buf
}
func makeAcctGetterRandomEachTime(keyLength int) *acctGetterRandomEachTime {
	acret := &acctGetterRandomEachTime{keyLength: keyLength}
	acret.buf = make([]byte, acret.keyLength)
	return acret
}

type acctGetterRatio struct {
	acctGetter
	acct1 acctGetter
	acct2 acctGetter
	ratio int
}

func (ac *acctGetterRatio) getAcct() []byte {
	if int(pseudoRand()%100) < ac.ratio {
		return ac.acct1.getAcct()
	}
	return ac.acct2.getAcct()
}

// ratio = 100 means all acct1
func makeAcctGetterRatio(ratio int, acct1 acctGetter, acct2 acctGetter) *acctGetterRatio {
	return &acctGetterRatio{acct1: acct1, acct2: acct2, ratio: ratio}
}

type acctGetterRandomFromPoolFileRatio struct {
	acctGetter
	acctPool *acctGetterRandomFromPoolFile
	acctr    *acctGetterRandomEachTime
	ratio    int
}

func (ac *acctGetterRandomFromPoolFileRatio) getAcct() []byte {
	if int(pseudoRand()%100) < ac.ratio {
		return ac.acctr.getAcct()
	}
	return ac.acctPool.getAcct()
}

// ratio = 1 means all new/random keys
func makeAcctGetterRandomFromPoolFileRatio(ratio int, acctCount int, filename string) *acctGetterRandomFromPoolFileRatio {
	agret := makeAcctGetterRandomEachTime(64)
	agrfprf := makeAcctGetterRandomFromPoolFile(acctCount, filename)

	return &acctGetterRandomFromPoolFileRatio{acctPool: agrfprf, acctr: agret, ratio: ratio}
}

type acctGetterRandomFromPoolFile struct {
	acctGetter
	acct [][]byte
}

func (ac *acctGetterRandomFromPoolFile) getAcct() []byte {
	randK := int(pseudoRand() % uint32(len(ac.acct)))
	return ac.acct[randK]
}
func makeAcctGetterRandomFromPoolFile(acctCount int, filename string) *acctGetterRandomFromPoolFile {
	f, _ := os.Open(filename)
	f.Seek(0, 0)
	dec := gob.NewDecoder(f)
	var out [][]byte
	for {
		var b []byte
		if err := dec.Decode(&b); err != nil {
			if err.Error() == "EOF" {
				break
			}
		}
		out = append(out, b)
	}

	f.Close()
	if len(out) < acctCount {
		panic("not enough accounts in the file")
	}
	var in [][]byte
	for len(in) < acctCount {
		acctGrabbed := int(pseudoRand() % uint32(len(out)))
		in = append(in, out[acctGrabbed])
		out[acctGrabbed] = out[len(out)-1]
		out = out[:len(out)-1]
	}
	fmt.Printf("Loaded %d accounts from %s\n", len(in), filename)
	return &acctGetterRandomFromPoolFile{acct: in}
}

type acctGetterRandomFromPool struct {
	acctGetter
	acct      []byte
	keyLength int
}

func (ac *acctGetterRandomFromPool) getAcct() []byte {
	randK := int(pseudoRand() % uint32(len(ac.acct)-ac.keyLength))
	return ac.acct[randK : randK+ac.keyLength]
}
func makeAcctGetterRandomFromPool(acctCount int, keyLength int) *acctGetterRandomFromPool {
	acct := make([]byte, acctCount+keyLength)
	rand.Read(acct)
	for j := range acct {
		acct[j] = acct[j] & 0x0f // Nibbles only
	}
	return &acctGetterRandomFromPool{acct: acct, keyLength: keyLength}
}

func addKeysNoopEvict(mt *Trie, accounts acctGetter, totalBatches int, keyLength int, prepopulateCount int, batchSize int, e Eviction) {
	fmt.Println("Prepopulating the trie with ", prepopulateCount, " accounts")
	fmt.Println("mt", countNodes(mt, false, false))
	mt.Hash()
	fmt.Printf("MakeTrie: %v\n", mt.root.getHash())

	for m := 0; m < prepopulateCount; m++ {
		k := accounts.getAcct()
		mt.Add(k, k) // just add the key as the value
		if m%(prepopulateCount/10) == (prepopulateCount/10)-1 {
			fmt.Printf("Prepopulated with %d accounts (%4.2f %%)\n", m, float64(m)/float64(prepopulateCount)*100)
			mt.Commit(nil) // don't evict
		}
	}
	mt.Commit(nil) // don't evict
	//	fmt.Println("mt", countNodes(mt))

	cpuprof, _ := os.Create("cpu.prof")
	pprof.StartCPUProfile(cpuprof)
	fmt.Println("Adding keys (batch size", batchSize, ", totalBatches", totalBatches, ", total keys:", totalBatches*batchSize, ")")
	for m := 0; m < totalBatches; m++ {

		fmt.Println("mt", countNodes(mt, false, false))
		epochStart := time.Now().Truncate(time.Millisecond)
		var statsStart = stats

		for i := 0; i < batchSize; i++ {
			mt.Add(accounts.getAcct(), accounts.getAcct())
		}
		fmt.Println("Committing", batchSize, "accounts")
		mt.Commit(e)

		epochEnd := time.Now().Truncate(time.Millisecond)
		var statsEnd = stats
		timeConsumed := epochEnd.Sub(epochStart)
		var statsDiff = statsEnd.diff(statsStart)

		fmt.Println("time", timeConsumed, "new hash:", mt.root.getHash(), statsDiff.String(), "len(mt.dels):", len(mt.dels))
	}
	fmt.Println("Done", batchSize, ", totalBatches", totalBatches, ", total keys:", totalBatches*batchSize, ")")
	pprof.StopCPUProfile()
	//	fmt.Println("mt", countNodes(mt))
	cpuprof.Close()
	runtime.GC()
	memprof, _ := os.Create("mem.prof")
	pprof.WriteHeapProfile(memprof)
	memprof.Close()

}

var accts1KiB acctGetter = makeAcctGetterRandomFromPool(1_024, 64)
var accts64KiB acctGetter = makeAcctGetterRandomFromPool(1_024*64, 64)
var accts1MiB acctGetter = makeAcctGetterRandomFromPool(1_048_576*1, 64)
var accts2MiB acctGetter = makeAcctGetterRandomFromPool(1_048_576*2, 64)
var accts4MiB acctGetter = makeAcctGetterRandomFromPool(1_048_576*4, 64)
var accts8MiB acctGetter = makeAcctGetterRandomFromPool(1_048_576*8, 64)
var accts16MiB acctGetter
var accts32MiB acctGetter
var accts64MiB acctGetter
var acctsRand acctGetter = makeAcctGetterRandomEachTime(64)

func resetPseudoRandomSeed() {
	x = x | uint32(time.Now().Unix())
}

func TestTrieAdd5KeysToPebbleDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())
	//	back := makePebbleBackstoreVFS()
	resetPseudoRandomSeed()
	back := makePebbleBackstoreDisk("pebble.test", false)
	//	back := makeNullBackstore()
	//	debugTrie = true
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, acctsRand, 3, 64, 0, 2, MakeEvictNone())
	mt.store.close()
}
func TestTrieAdd8MiBKeysToPebbleDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	resetPseudoRandomSeed()
	fmt.Println(t.Name())
	//	back := makePebbleBackstoreVFS()
	back := makePebbleBackstoreDisk("pebble.test", false)
	//	back := makeNullBackstore()
	mt := MakeTrie(back)
	//    debugTrie = true
	//	addKeysNoopEvict(mt, acctsRand, 6400, 64, 0, false, 1_048_576/24)
	addKeysNoopEvict(mt, acctsRand, 8*1_048_576/100, 64, 0, 100, MakeEvictNone())
	mt.store.close()
}

func TestTrieAddFromEmptyRandomNullStore250(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())
	//	back := makePebbleBackstoreVFS()
	back := makeNullBackstore()
	mt := MakeTrie(back)
	//    debugTrie = true
	addKeysNoopEvict(mt, acctsRand, 25, 64, 1_048_576*0, 1, MakeEvictNone())
	mt.store.close()
}

func TestTrieAddFromRandomNullStore250(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())
	//	back := makePebbleBackstoreVFS()
	back := makeNullBackstore()
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, acctsRand, 10, 64, 1_048_576*64, 250_000, MakeEvictNone())
	mt.store.close()
}

func TestTrieBobInMem(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, accts1KiB, 20, 64, 1_048_576, 250_000, MakeEvictNone())
	mt.store.close()
}
func TestTrieAdd10Batches250kIntoPreloadPebbleTest(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	fmt.Println("Preloading")
	addKeysNoopEvict(mt, acctsRand, 10, 64, 0, 250_000, MakeEvictNone())
	mt.store.close()
}
func TestTrieAdd10Batches250kIntoPebbleTest(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, acctsRand, 10, 64, 0, 250_000, MakeEvictNone())
	mt.store.close()
}
func TestTrieOriginalAddFrom2MiBInMem(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, accts2MiB, 5, 32, 1_048_576*2, 250_000, MakeEvictNone())
	mt.store.close()
}
func TestTrieProfileAdd200RoundsOfRandom250kNull(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())
	//	back := makePebbleBackstoreVFS()
	back := makeNullBackstore()
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, acctsRand, 200, 64, 1_048_576*0, 250_000, MakeEvictNone())
	mt.store.close()
}
func TestTrieProfileAdd20RoundsOf250kInMem(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())
	//	back := makePebbleBackstoreVFS()
	back := makeNullBackstore()
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, accts8MiB, 20, 64, 1_048_576*0, 250_000, MakeEvictNone())
	mt.store.close()
}
func TestTrieOriginalAddFrom2MiBDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	fmt.Println(t.Name())
	back := makePebbleBackstoreDisk("pebble.test", true)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, accts2MiB, 5, 32, 1_048_576*2, 250_000, MakeEvictNone())
	mt.store.close()
}
func TestTrieCreate16MiBDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	if accts16MiB == nil {
		accts16MiB = makeAcctGetterRandomFromPool(1_048_576*16, 64)
	}
	back := makePebbleBackstoreDisk("pebble.test", true)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, accts16MiB, 0, 32, 1_048_576*16, 250_000, MakeEvictNone())
	mt.store.close()
}

func TestTrieAddFrom1MiBDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, accts1MiB, 5, 32, 1_048_576*1, 104_857, MakeEvictNone())
	mt.store.close()
}
func TestTrieAddFrom4MiBDisk(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble2db", true)
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, accts4MiB, 5, 32, 1_048_576*4, 104_857, MakeEvictNone())
	mt.store.close()
}
func TestTrieAddFrom1MiB(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreVFS()
	mt := MakeTrie(back)
	addKeysNoopEvict(mt, accts1MiB, 5, 32, 1_048_576*1, 104_857, MakeEvictNone())
	mt.store.close()
}
func TestTriePreload(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreDisk("pebble.test", false)
	mt := MakeTrie(back)
	var m runtime.MemStats
	for i := 0; i <= 5; i++ {
		runtime.GC()
		runtime.ReadMemStats(&m)
		fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
		fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
		fmt.Printf("\tNumGC = %v\n", m.NumGC)
		fmt.Println("before preload", i, ", trie: ", countNodes(mt, false, true))
		mt.Preload(i)
		runtime.GC()
		runtime.ReadMemStats(&m)
		fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
		fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
		fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
		fmt.Printf("\tNumGC = %v\n", m.NumGC)
		fmt.Println("after preload", i, ", trie: ", countNodes(mt, false, true))
	}
}

func makebacking(cd crypto.Digest) node {
	return makeBackingNode(cd, []byte{0x01, 0x02, 0x03, 0x04})
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
	ln2 := deserializeLeafNode(data, nibbles.Nibbles{0x0, 0x1, 0x2, 0x3})
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
	ln3 := deserializeLeafNode(data, nibbles.Nibbles{0x0, 0x1, 0x2, 0x3})
	require.NoError(t, err)
	require.Equal(t, ln, ln3)

	bn := &branchNode{}
	bn.key = nibbles.Nibbles{0x01, 0x02, 0x03}
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
	bn2 := deserializeBranchNode(data, nibbles.Nibbles{0x01, 0x02, 0x03})
	bn2data, err := bn2.serialize()
	require.NoError(t, err)
	require.Equal(t, bn2data, data)

	bn.children[7] = nil
	data, err = bn.serialize()
	require.NoError(t, err)
	expected = []byte{0x5, 0xe8, 0x31, 0x2c, 0x27, 0xec, 0x3d, 0x32, 0x7, 0x48, 0xab, 0x13, 0xed, 0x2f, 0x67, 0x94, 0xb3, 0x34, 0x8f, 0x1e, 0x14, 0xe5, 0xac, 0x87, 0x6e, 0x7, 0x68, 0xd6, 0xf6, 0x92, 0x99, 0x4b, 0xc8, 0x2e, 0x93, 0xde, 0xf1, 0x72, 0xc8, 0x55, 0xbb, 0x7e, 0xd1, 0x1d, 0x38, 0x6, 0xd2, 0x97, 0xd7, 0x2, 0x2, 0x86, 0x93, 0x37, 0x57, 0xce, 0xa4, 0xc5, 0x7e, 0x4c, 0xd4, 0x50, 0x94, 0x2e, 0x75, 0xeb, 0xcd, 0x9b, 0x80, 0xa2, 0xf5, 0xf3, 0x15, 0x4a, 0xf2, 0x62, 0x6, 0x7d, 0x6d, 0xdd, 0xe9, 0x20, 0xe1, 0x1a, 0x95, 0x3b, 0x2b, 0xb9, 0xc1, 0xaf, 0x3e, 0xcb, 0x72, 0x1d, 0x3f, 0xad, 0xe9, 0xa6, 0x30, 0xc6, 0xc5, 0x65, 0xf, 0x86, 0xb2, 0x3a, 0x5b, 0x47, 0xcb, 0x29, 0x31, 0xf7, 0x8a, 0xdf, 0xe0, 0x41, 0x6b, 0x11, 0xc0, 0xd, 0xbc, 0x80, 0xa7, 0x48, 0x97, 0x21, 0xbd, 0xee, 0x6f, 0x36, 0xf4, 0x7b, 0x6d, 0x68, 0xa1, 0x43, 0x31, 0x90, 0xf8, 0x56, 0x69, 0x4c, 0xee, 0x88, 0x76, 0x9c, 0xd1, 0xde, 0xe4, 0xbd, 0x64, 0x7d, 0x18, 0xce, 0xd6, 0xdb, 0xf8, 0x85, 0x84, 0x88, 0x5d, 0x7e, 0xda, 0xe0, 0xf2, 0xa0, 0x6d, 0x24, 0x4f, 0xcf, 0xb, 0x8c, 0x34, 0x57, 0x2a, 0x13, 0x22, 0xd9, 0x8d, 0x79, 0x8, 0xa4, 0x22, 0x91, 0x45, 0x64, 0x7b, 0xf3, 0xad, 0xe8, 0x9b, 0x5f, 0x7c, 0x5c, 0xbd, 0x9, 0xd3, 0xc7, 0x3, 0xe2, 0xef, 0x6b, 0x8, 0x8, 0x98, 0x52, 0xb, 0xd1, 0x6a, 0x5a, 0x18, 0x89, 0x44, 0x4f, 0xf1, 0xb0, 0x37, 0xd9, 0x7f, 0x99, 0x3f, 0x6a, 0x84, 0x46, 0x83, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x84, 0x38, 0x6b, 0x6e, 0x7e, 0xf0, 0xef, 0xaa, 0x29, 0xa5, 0x13, 0x0, 0xef, 0xff, 0xdf, 0xb5, 0xd7, 0x4e, 0x41, 0x75, 0x4d, 0x2, 0x84, 0x20, 0xe2, 0x18, 0x50, 0x52, 0xae, 0xf4, 0xea, 0xeb, 0x84, 0xb3, 0x91, 0x85, 0xa8, 0xa, 0xba, 0xc9, 0x31, 0x9f, 0x5e, 0x3e, 0xf8, 0xb5, 0xf4, 0x4b, 0xf8, 0xf2, 0xf0, 0x76, 0xa1, 0x6d, 0xec, 0x57, 0x65, 0xbd, 0x2e, 0x78, 0xbe, 0xf4, 0x7c, 0xe4, 0xf2, 0x45, 0xc0, 0xaf, 0x94, 0xb, 0x45, 0x1b, 0xd3, 0xcf, 0x9f, 0x17, 0x7e, 0x1a, 0x52, 0x6d, 0x18, 0xe5, 0x1a, 0x7c, 0xd9, 0x9d, 0xef, 0x8a, 0xe3, 0xe9, 0xe6, 0xf6, 0x76, 0x5e, 0x12, 0xbf, 0xd2, 0xe8, 0xaa, 0x8, 0x88, 0x15, 0x81, 0x99, 0x4e, 0xa3, 0x12, 0x98, 0xc1, 0xb3, 0xde, 0x42, 0x53, 0x2, 0x29, 0x82, 0x87, 0xfe, 0x3d, 0x8, 0xe0, 0xc2, 0x3, 0x70, 0x56, 0xd, 0x9, 0xad, 0xe4, 0x1a, 0xa5, 0xf6, 0x4, 0xdb, 0x63, 0xd0, 0x49, 0x6b, 0x5b, 0xa2, 0x56, 0xb1, 0xd1, 0x4b, 0x56, 0xc3, 0x7e, 0x4b, 0xec, 0xb5, 0xdb, 0xd4, 0xd9, 0xe1, 0x20, 0x99, 0x80, 0x71, 0x9, 0x72, 0x3b, 0xc, 0x8b, 0x56, 0x4, 0x94, 0xe6, 0x4e, 0x35, 0xd, 0x3e, 0x7, 0x8b, 0x86, 0x73, 0x62, 0x5f, 0x61, 0x8d, 0x70, 0x68, 0x86, 0xe8, 0x65, 0xbe, 0x18, 0xa8, 0x4a, 0xac, 0x6d, 0x81, 0x15, 0xde, 0x1b, 0xe1, 0xb3, 0xe8, 0x6a, 0x46, 0xdf, 0xdc, 0xf1, 0x6, 0x3c, 0xa6, 0x1c, 0xc9, 0xcd, 0x12, 0x5e, 0x5f, 0x28, 0xd1, 0x71, 0x6e, 0x9f, 0xc7, 0xdc, 0x77, 0x98, 0x47, 0x7, 0x94, 0x38, 0x4, 0xc4, 0xc4, 0xfe, 0x17, 0x12, 0x1b, 0xcf, 0x96, 0xd8, 0xb1, 0xf2, 0x1e, 0x81, 0xab, 0x15, 0x86, 0x75, 0x5a, 0x39, 0x13, 0xdb, 0xe, 0x1a, 0xd9, 0xa9, 0x70, 0x7d, 0xdd, 0xaf, 0x64, 0x12, 0x27, 0xe5, 0x97, 0xa1, 0x34, 0xb8, 0x1a, 0x61, 0x48, 0x29, 0x61, 0x62, 0xe4, 0x40, 0xba, 0x5, 0x44, 0x24, 0x51, 0xc1, 0x9b, 0x8e, 0x62, 0xf2, 0x1c, 0x6f, 0xd6, 0x8, 0x3, 0xbe, 0x88, 0xf}

	require.Equal(t, expected, data)
	bn3 := deserializeBranchNode(data, nibbles.Nibbles{0x01, 0x02, 0x03})
	bn3data, err := bn3.serialize()
	require.NoError(t, err)

	require.Equal(t, bn3data, data)
	bn.children[0] = makebacking(crypto.Digest{})
	require.NotEqual(t, bn, bn3)

	en := &extensionNode{}
	en.key = nibbles.Nibbles{0x01, 0x02, 0x03}
	en.sharedKey = []byte("extensionkey")
	for i := range en.sharedKey {
		en.sharedKey[i] &= 0x0f
	}
	en.next = makebacking(crypto.Hash([]byte("extensionnext")))
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
	mt.store.close()
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
	mt.Commit(nil)
	fmt.Println("Done committing 1k random key/value accounts")
	fmt.Println(stats.String())
	mt.store.close()

}

func TestTriedAddSimpleSequenceNoCache(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
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
	mt.Commit(nil)
	fmt.Printf("3rootHash: %v\n", mt.root)
	v = []byte{0x04, 0x05, 0x07}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("4rootHash: %v\n", mt.root)
	mt.Commit(nil)

	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("5rootHash: %v\n", mt.root)
	mt.Commit(nil)

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("6rootHash: %v\n", mt.root)
	mt.Commit(nil)

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x0a}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	mt.Commit(nil)

	k = []byte{0x01, 0x02, 0x03, 0x04}
	v = []byte{0x04, 0x05, 0x0b}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("7rootHash: %v\n", mt.root)
	mt.Commit(nil)

	k = []byte{0x01, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	fmt.Printf("8rootHash: %v\n", mt.root)
	mt.Commit(nil)

	k = []byte{0x01, 0x0d, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	fmt.Printf("9rootHash: %v\n", mt.root)
	mt.Add(k, v)
	fmt.Printf("arootHash: %v\n", mt.root)
	mt.Commit(nil)
	fmt.Printf("5rootHash: %v\n", mt.root)
	buildDotGraph(t, mt, kk, vv, "/tmp/cachetrie8.dot", "Trie Simple Sequence")

	mt.store.close()
}

func TestTrieLeafAddTransitions(t *testing.T) { // nolint:paralleltest // Serial tests for trie for the moment
	partitiontest.PartitionTest(t)
	// t.Parallel()
	back := makePebbleBackstoreVFS()

	rootHashCP1s0, _ := crypto.DigestFromString(string("FFQBBL5UTISGD3D226DE3MBE57Z4F5J4KI5LWBZD35MHKOG2HWWQ"))
	rootHashCP1s2, _ := crypto.DigestFromString(string("3HXXEYIIWJHMQTGQF6SRREUM3BHYFC5DAIZ3YPRH73P2IAMV32LA"))
	rootHashCP2s0, _ := crypto.DigestFromString(string("FFQBBL5UTISGD3D226DE3MBE57Z4F5J4KI5LWBZD35MHKOG2HWWQ"))
	rootHashCP2s2, _ := crypto.DigestFromString(string("YEWXAECRC4UKLBV6TJJGQTE7Z5BMUWLO27CAIXV4QY5LYBFZCGGA"))
	rootHashCP3s0, _ := crypto.DigestFromString(string("EJKKWSBR6ND6FWXTHTKIANNHEAR5JZHNK4DCQIRAUN4K65F4P6KA"))
	rootHashCP3s2, _ := crypto.DigestFromString(string("VRSSXJ74NVQXRBQOIOIIQHRX5D6WEWBG3LKGKHEOYQPA7TEQ2U6A"))
	rootHashCP4s0, _ := crypto.DigestFromString(string("EJKKWSBR6ND6FWXTHTKIANNHEAR5JZHNK4DCQIRAUN4K65F4P6KA"))
	rootHashCP4s2, _ := crypto.DigestFromString(string("FS3AP7ELN3VXLPY7NZTU7DOX5YOV2E4IBK2PXQIU562EVFZAAP5Q"))
	rootHashCP5s0, _ := crypto.DigestFromString(string("FFQBBL5UTISGD3D226DE3MBE57Z4F5J4KI5LWBZD35MHKOG2HWWQ"))
	rootHashCP5s1, _ := crypto.DigestFromString(string("O5OVEUEVNBYQW4USTIBAQDU2JHNGUWGLNCVQXSS3NPUONVZ2LM6A"))
	rootHashCP5s2, _ := crypto.DigestFromString(string("DHL5EA3QIAQSNKHRUZBUBNYEY52HFHOOSX2CWDQDQCQORXJZRP2Q"))
	rootHashCP6s0, _ := crypto.DigestFromString(string("EJKKWSBR6ND6FWXTHTKIANNHEAR5JZHNK4DCQIRAUN4K65F4P6KA"))
	rootHashCP6s1, _ := crypto.DigestFromString(string("OHSKGDYIVZR34PLQ5YI7E7KYLLA5SFUDNN324YSHTPCLQB2WGXZA"))
	rootHashCP6s2, _ := crypto.DigestFromString(string("LLXHTSABWNRPDEV5Z7JB3OV4R576AZQ57UIODWRCOMN2I6ELMSTQ"))
	rootHashCP7s0, _ := crypto.DigestFromString(string("EJKKWSBR6ND6FWXTHTKIANNHEAR5JZHNK4DCQIRAUN4K65F4P6KA"))
	rootHashCP7s1, _ := crypto.DigestFromString(string("LTB7VIEISQQD5DN6WBKVD3ZB2BBXEFQVC7AVU7PXFFS6ZEU2ZQIQ"))

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

	debugTrie = true

	mt := MakeTrie(back)
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyA, valDEF)
	kk = append(kk, keyA)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath1.0.dot", "Leaf add: Codepath 1, before")
	require.Equal(t, rootHashCP1s0, mt.Hash())

	mt.Add(keyA, valGHI)
	kk = [][]byte{}
	vv = [][]byte{}
	kk = append(kk, keyA)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath1.2.dot", "Leaf add: Codepath 1, after")
	require.Equal(t, rootHashCP1s2, mt.Hash())

	mt = MakeTrie(back)
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyA, valDEF)
	kk = append(kk, keyA)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath2.0.dot", "Leaf add: Codepath 2, before")
	require.Equal(t, rootHashCP2s0, mt.Hash())
	mt.Add(keyAB, valGHI)
	kk = append(kk, keyAB)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath2.2.dot", "Leaf add: Codepath 2, after")
	require.Equal(t, rootHashCP2s2, mt.Hash())

	mt = MakeTrie(back)
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyAB, valDEF)
	kk = append(kk, keyAB)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath3.0.dot", "Leaf add: Codepath 3, before")
	require.Equal(t, rootHashCP3s0, mt.Hash())
	mt.Add(keyA, valGHI)
	kk = append(kk, keyA)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath3.2.dot", "Leaf add: Codepath 3, after")
	require.Equal(t, rootHashCP3s2, mt.Hash())

	mt = MakeTrie(back)
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyAB, valDEF)
	kk = append(kk, keyAB)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath4.0.dot", "Leaf add: Codepath 4, before")
	require.Equal(t, rootHashCP4s0, mt.Hash())
	mt.Add(keyAC, valGHI)
	kk = append(kk, keyAC)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath4.2.dot", "Leaf add: Codepath 4, after")
	require.Equal(t, rootHashCP4s2, mt.Hash())

	mt = MakeTrie(back)
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyA, valDEF)
	kk = append(kk, keyA)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath5.0.dot", "Leaf add: Codepath 5, setup")
	require.Equal(t, rootHashCP5s0, mt.Hash())
	mt.Add(keyB, valGHI)
	kk = append(kk, keyB)
	vv = append(vv, valGHI)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath5.1.dot", "Leaf add: Codepath 5, before")
	require.Equal(t, rootHashCP5s1, mt.Hash())
	mt.Add(keyAB, valJKL)
	kk = append(kk, keyAB)
	vv = append(vv, valJKL)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath5.2.dot", "Leaf add: Codepath 5, after")
	require.Equal(t, rootHashCP5s2, mt.Hash())

	mt = MakeTrie(back)
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyAB, valDEF)
	kk = append(kk, keyAB)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath6.0.dot", "Leaf add: Codepath 6, setup")
	require.Equal(t, rootHashCP6s0, mt.Hash())
	mt.Add(keyB, valGHI)
	kk = append(kk, keyB)
	vv = append(vv, valGHI)
	require.Equal(t, rootHashCP6s1, mt.Hash())
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath6.1.dot", "Leaf add: Codepath 6, before")
	mt.Add(keyA, valJKL)
	kk = append(kk, keyA)
	vv = append(vv, valJKL)
	require.Equal(t, rootHashCP6s2, mt.Hash())
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath6.2.dot", "Leaf add: Codepath 6, after")

	mt = MakeTrie(back)
	kk = [][]byte{}
	vv = [][]byte{}
	mt.Add(keyAB, valDEF)
	kk = append(kk, keyAB)
	vv = append(vv, valDEF)
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath7.0.dot", "Leaf add: Codepath 7, before")
	require.Equal(t, rootHashCP7s0, mt.Hash())
	mt.Add(keyCD, valGHI)
	kk = append(kk, keyCD)
	vv = append(vv, valGHI)
	require.Equal(t, rootHashCP7s1, mt.Hash())
	buildDotGraph(t, mt, kk, vv, "/tmp/codepath7.2.dot", "Leaf add: Codepath 7, after")

	err := mt.Commit(nil)
	require.NoError(t, err)
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
	buildDotGraph(t, mt, kk, vv, "/tmp/trie0.dot", "Trie Simple")
	fmt.Printf("done with that")

	v = []byte{0x04, 0x05, 0x07}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)

	buildDotGraph(t, mt, kk, vv, "/tmp/trie2.dot", "Trie Simple")
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie3.dot", "Trie Simple")

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x09}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie4.dot", "Trie Simple")

	k = []byte{0x01, 0x02}
	v = []byte{0x04, 0x05, 0x0a}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie5.dot", "Trie Simple")

	k = []byte{0x01, 0x02, 0x03, 0x04}
	v = []byte{0x04, 0x05, 0x0b}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie6.dot", "Trie Simple")

	k = []byte{0x01, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie7.dot", "Trie Simple")

	k = []byte{0x01, 0x0d, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie8.dot", "Trie Simple")

	//duplicate key and value
	k = []byte{0x01, 0x0d, 0x02, 0x03, 0x06, 0x06, 0x07, 0x06}
	v = []byte{0x04, 0x05, 0x0c}
	kk = append(kk, k)
	vv = append(vv, v)
	mt.Add(k, v)
	buildDotGraph(t, mt, kk, vv, "/tmp/trie9.dot", "Trie Simple")

	err := mt.Commit(nil)
	require.NoError(t, err)
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

func writeKeys(mt *Trie, filename string, keysToWrite int, trieSize int) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	f, _ := os.Create(filename)
	defer f.Close()

	var wrote int
	write := func() func(n node) {
		innerCount := func(n node) {
			switch n.(type) {
			case *branchNode:
				bn := n.(*branchNode)
				if !bn.valueHash.IsZero() {
					rand := int(uint32(pseudoRand()) % uint32(trieSize))
					if rand < keysToWrite {
						if wrote%(keysToWrite/10) == 0 {
							fmt.Printf("wrote %d keys\n", wrote)
						}
						wrote++
						if wrote < keysToWrite {
							enc.Encode(bn.key)
							f.Write(buf.Bytes())
							buf.Reset()
						}
					}
				}
			case *leafNode:
				ln := n.(*leafNode)
				rand := int(uint32(pseudoRand()) % uint32(trieSize))
				if rand < keysToWrite {
					if wrote%(keysToWrite/10) == 0 {
						fmt.Printf("wrote %d keys\n", wrote)
					}
					wrote++
					if wrote < keysToWrite {
						enc.Encode(append(ln.key[:], ln.keyEnd...))
						f.Write(buf.Bytes())
						buf.Reset()
					}
				}
			}
		}
		return innerCount
	}()
	mt.root.lambda(write, mt.store)
}

// returns a string with the number of nodes in the trie
func countNodes(mt *Trie, follow bool, mem bool) string {
	if mt.root == nil {
		return "Empty trie"
	}
	var nc struct {
		total    int
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
			nc.total++
			if nc.total%1_000_000 == 0 {
				fmt.Printf("counting nodes: %d\n", nc.total)
			}
		}
		return innerCount
	}()
	if follow {
		mt.root.lambda(count, mt.store)
	} else {
		mt.root.lambda(count, nil)
	}

	countString := fmt.Sprintf("[nodes: total %d / valued %d (branches: %d, leaves: %d, exts: %d, parents: %d, backings: %d), len(dels):%d]",
		nc.branches+nc.leaves+nc.exts+nc.parents, nc.values,
		nc.branches, nc.leaves, nc.exts, nc.parents, nc.backings, len(mt.dels))

	if mem {
		var nmem struct {
			branches int
			leaves   int
			exts     int
			parents  int
			backings int
		}

		memfn := func() func(n node) {
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

		if follow {
			mt.root.lambda(memfn, mt.store)
		} else {
			mt.root.lambda(memfn, nil)
		}
		memString := fmt.Sprintf("mem: total %d (branches: %d, leaves: %d, exts: %d, parents: %d, backings: %d)",
			nmem.branches+nmem.leaves+nmem.exts+nmem.parents,
			nmem.branches, nmem.leaves, nmem.exts, nmem.parents, nmem.backings)
		return countString + " " + memString
	}
	return countString
}
