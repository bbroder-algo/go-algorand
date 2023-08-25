
****State trie****

The state trie, commonly known as a prefix tree, is a tree-like data structure
used for storing an associative array where the keys are sequences of 4-bit
bytes and the values are SHA-512/256 hashes of the key values.  A proof can be
provided to the user to show membership of a key value by showing its hash
provides the necessary missing value to hash to the known root hash.

The trie operates on 'nibbles', which are sequences of 4 bits. This allows for
a more compact representation compared to standard binary tries and smaller
proofs.

This trie has built-in support for backing stores, which are essential for
persistent data storage. It is designed to work seamlessly with both in-memory
and disk-based storage solutions.

***Key Features:***

Hashing: The trie provides a SHA-512/256 checksum at its root, ensuring data
integrity.

Adding and removing key/value pairs: Through specific operations, users can
efficiently add new key-value pairs to the trie or remove existing ones. The
trie ensures consistent state transitions and optimal space usage during these
operations.

Child and Merge Operations: The trie supports operations to manage child tries,
enabling the creation, discard, and merge actions for subtries.

Backstore commit: The trie supports committing changes to the trie to a backing
store that fuctions like a batched kv interface.

Preloading: Though the trie is designed to keep only parts of it in memory for
efficiency, it offers a preloading feature to sweep all nodes above a provided
level out of the backstore and into memory if required.

***Trie operation and usage:***

Tries are initialized against a backing store (an empty memory one will be
constructed if not provided by the user) where the full trie ultimately resides
on Commit.

```
mt := MakeTrie(nil)
key1 := nibbles{0x08, 0x0e, 0x02, 0x08}
val1 := nibbles{0x03, 0x09, 0x0a, 0x0c}
key2 := nibbles{0x08, 0x0d, 0x02, 0x08}
val2 := nibbles{0x03, 0x09, 0x0a, 0x0c}

mt.Add(key1, val1)
fmt.Println("K1:V1 Hash:", mt.Hash())

mt.Add(key2, val2)
fmt.Println("K1:V1,K2:V2 Hash:", mt.Hash())

mt.Delete(key2)
fmt.Println("K1:V1 Hash:", mt.Hash())

mt.Commit()
mt.Evict(func(node) bool { 
  return true
})
```

The trie maintains an interface reference to the root of the trie, which is one
of five possible trie node types described below.  Trie operations Add and
Delete descend the trie from this node, loading in nodes from the backstore (as
necessary), creating new nodes (if the key added is unique or a key is found
for deletion), and keeping track of nodes that can be deleted from the
backstore on the next Commit.

Think of a hypothetical "trie" living on a massive backing store.  New
statetrie objects that operate on this trie are created by `MakeTrie (store)`
and are initialized by loading and deserializing the root node from the store.
All references pointing down from this node are represented by backing node
objects.  When Add or Delete operations want to descend through one of these
backing nodes, the bytes are obtained from the backing store and deserialized
into one of the three main trie node types (branch, extension, or leaf).

In this way, trie operations 'unroll' paths from the trie store into working
memory as necessary to complete the operation.  

```
Trie on backing store:
                _____
               | BR1 |
           ____|_____|____
          /              \
         /                \
      __O__              __O__
     /     \            /     \
    O       O          O       O
   / \     / \        / \     / \  
  O   O   O   O      O   O   O   O
 / \ / \ / \ / \    / \ / \ / \ / \
O   O   O   O   O  O   O   O   O   O


statetrie Trie of that backing store trie, immediately after MakeTrie, with the
two child nodes held as backing nodes (labeled 'b') with known hashes (the root
hash of the backing store can be calculated immediately without descending):

          ___
         |BR1|
         /   \
        /     \
       b       b  

statetrie Trie of that backing store trie after a few update Add operations,
with more paths unrolled from the backing store:

          ___
         | R1|
         /   \
        /     \
      __O__   __O__
     /     \       \
    O       O       O
   /       / \     / \
  O       O   b   b   O
 /       /           / \
O       O           O   O
```

Nodes that can be reached from the statetrie root node represent:

1. uncommitted new intermediary or leaf nodes created in support of the Add or Delete and are not yet hashed

2. altered nodes created from prior operations that were never evicted (replaced with backing nodes), with their hash now zeroed

3. unaltered nodes created from prior operations in the past that were never evicted (replaced with backing nodes), have a known hash

4. references to nodes on the backing store, whose hash is known

5. references to nodes in the parent trie, which act as lazy copies of the parent nodes and disappear on merge

On Commit, the first two node categories reachable from the root node
(following parent links) are hashed and committed to the backstore, and any
keys marked for deletion are removed from the store.

Unmodified unrolls or committed nodes can either stay in memory or face
eviction from their parent node by an eviction function in a call to Evict.
Eviction of branching and extension nodes replaces their lower subtrie with a
backing node. The eviction function in the sample above is called after Commit,
guaranteeing all nodes are either unmodified or committed, and evicts all of
them, thus reducing the representation of the backing store trie in the
statetrie object to a single in-memory node.

***Trie node types:***

All trie nodes hold a key representing the nibble position of the node in the
trie, and a hash of the node itself.  

The node key is the key used with the backing store to insert, alter or delete
the serialized node. The key is limited to MaxKeyLength (65,536) nibbles in
size, and cannot be empty (the root node is the empty nibble).  

The node hash is set to the zero value if it is not yet known or if the
contents of the node were altered in a trie operation.  The hash is calculated
by either of the trie methods `Hash()` or `Commit()`, the later which hashes
and commits the node changes with node method `hashingCommit(store)`. In these
operations, the node hash is set to the SHA-256 hash of the serialization of
the node.  The hashing scheme requires the lower levels of the trie to be
hashed before the higher levels.

There are five possible trie nodes. Any of the node types can be the root of a
trie.  Only the first three are committed to the backing store.

## Trie Node Types

| Node Type      | Description                                                                                           | Value Holding | Stored in Backstore |
|----------------|-------------------------------------------------------------------------------------------------------|---------------|---------------------|
| Leaf Nodes     | Contains the remainder of the search key (`keyEnd`) and the hash of the value.                         | Yes           | Yes                 |
| Branch Nodes   | Holds references to 16 children nodes and an optional "value slot" for keys that terminate at the node. | Optional      | Yes                 |
| Extension Nodes| Contains a run of commonly shared key nibbles that lead to the next node. No value is held.            | No            | Yes                 |
| Parent Nodes   | Soft-links back to a node in a parent trie. They expand into copies if edited.                         | Varies        | No                  |
| Backing Nodes  | Soft links back to a node in the backing store. They are expanded into one of the main nodes if read.  | Varies        | No                  |


**Leaf nodes**

This value-holding nodes contain the remainder of the search key (the `keyEnd`)
and the hash of the value.

**Branch nodes**

Branch nodes hold references to 16 children nodes indexed by the next nibble of
the search key, plut a "value slot" to hold values for keys th at terminate at
the branch node. 

**Extension nodes**

Extension nodes contain an addition run of commonly shared key nibbles that
send you along to the next node.  No value is held at an extension node. There
are no extension nodes with no next node.

**Parent nodes**

These nodes are soft-links back to a node in a parent trie from a child trie.
They are expanded into copies of their nodes they link to if the node is edited
or replaced in an Add or Delete operation.  

**Backing nodes**

These nodes are soft links back to a node in the backing store, containing the
key and the hash of the node.  They are expanded into one of the three main
nodes if the node is read.

When the trie is hashed, these nodes contain their own hash and thus do not
require the hash algorithm to descend that subtree from the backing store any
further.  In this way the hashing function continues to function without
loading the entire trie structure into memory.

When operated on, backing nodes deserialize themselves from the backing store
by calling `get`, which calls a node deserialization method to determine the
node type (from a prefix), and then the specific node type handles the rest of
the deserialization into a node object. This deserialization provides a hash
value to the new node object, as this value is recorded from the
deserialization of its parent node in the trie when the backing node was
constructed.  

If the deserialized branch or extension node points at another node, that
"pointed-at" node reference is stored as another backing node with its key set
to the location in the trie and with its hash set to the SHA-256 hash of the
node taken from the store bytes. If later trie operations need to descend
through these nodes, they are in turn deseralized as described.

***Nibbles:***

4-bit keys are maintained as `nibbles` slices with utility `pack` and `unpack` 
methods to move them into and out of 8-byte data slices, with a half/full
last-byte bit.

***Trie child and merge operations:***

Child tries are represented as tries with unexplored node references ("parent
nodes") that point back to unmodified node objects of the parent trie. 

Obtaining a child trie from a trie allows the user to easily dispose of stacks
of changes to a child trie at an arbitrary time while retaining the parent.

Parent tries must remain read-only until after the child is disregared or until
after it is merged back into the parent.

When a child trie is initialized, it is anchored to the parent by initializing
its root node to a parent node object that points back to the parent trie root
node object. Accessing this parent node to service an Add or Delete operation
converts the parent node into a copy of the original parent node (with the
`child` node method), and from there the operations continue with the copy
holding any alterations.

When merging child tries back into their parents, The in-memory node objects in
a child trie undergoes a traversal when merging back into the parent. This
search aims to identify parent nodes, which are then replaced by their original
references, effectively stitching the child trie's modifications into the
parent trie. 

Node deletion lists are propagated into the parent in a merge to be handled by
a future parent backstore commit.

***Eviction:***

Nodes can be evicted from memory after Commit and all their subtree replaced by
a single backing node according to the binary output of a user-defined function
which operates on each node.  The nodes would have to be read back in from the
backing store to resume operations on them.  Eviction of a node only affects
branch and extension nodes, who replace their children with backing nodes. 


***Raising:***

Some delete operations require a trie transformation that relocates a node
"earlier" in the trie. These relocations shorten the key from the original key.
Relocating a leaf node merely reassigns the key value and adjusts the ending
key value in the node to compensate. But raising a branch node creates a new
extension node and places it just above the branch node. Raising an extension
node extends its shared key and relocates its key.  Raising a backing node gets
the node from the store and then immediately raises it.  Similarly, raising a
parent node copies the parent node by evoking `child` on it and immediately
raises it.  After a raising operation, there is guaranteed to be a node at the
new location in the trie.


***Backing stores:***

In large backing store tries, only a fraction of the trie nodes are represented
by in-memory trie node objects.  The rest of the nodes live in the backing
store.

Backing stores are kv stores which maintain all the mapping between committed
trie keys and node serialization data (which includes the hash of the key
value).

Backing stores must "set" byte data containing serialized nodes, and "get"
nodes back from the store by deserializing them into trie nodes that (may)
contain deferred references to further backing store nodes.  A simple backing
store is a golang map from strings to nodes which uses the provided node
serialization / deserialization utilites.  This is implemented as
`memoryBackstore`.

`BatchStart()` methods on backing stores called before any store set operations
are begun, and `BatchEnd()` is called after there are no more, to allow for
preparations around batch commits. 

Committing the trie to the backing store will trigger hashing of the trie, as
committing requires node serialization and node serialization requires the hash
of subtree elements in branch and extension nodes.

***Preloading:***

Normally only part of the trie is kept in memory.  However, the trie can sweep
nodes out of the backstore and into memory by calling Preload.  

Preload loads into trie memory all backing nodes reachable from the root that
keys with length less than or equal to the one provided by obtaining them from
the store.

In a full (and therefore balanced) trie, preloading lengths has the effect of 
loading the top levels of the trie.  This could accelerate future trie operations.
