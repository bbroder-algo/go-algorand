
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
efficiency, it offers a preloading feature to sweep all nodes out of the
backstore and into memory if required.

***Trie operation and usage:***

Tries are initialized against a backing store (a memory one will be constructed
if not provided by the user).

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
```

***Trie nodes:***

There are six possible trie nodes.  Any of the node types can be the root of a
trie.  Only the first three are committed to the backing store. 

**Leaf nodes**

This value-holding nodes contain the remainder of the search key (the `keyEnd`)
and the hash of the value.

**Branch nodes**

Branch nodes hold references to 16 children nodes indexed by the next nibble 
of the search key, plut a "value slot" to hold values for keys th
at terminate at the branch node. 

**Extension nodes**

Extension nodes contain an addition run of commonly shared key nibbles that
send you along to the next node.  No value is held at an extension node.

**Parent nodes**

These nodes are soft-links back to a node in a parent trie from a child trie.
They are expanded into copies of their links if the node is edited or replaced
in an add or delete operation.

**Backing nodes**

These nodes are soft links back to a node in the backing store.  They are
expanded into one of the three main nodes if the node is read.

***Trie lazy expansion from initialization:***

When constructing the trie, the backing store is queried using an empty nibble
key. The response from the backing store is then deserialized and positioned at
the trie's root. If the root node contains references to child nodes (whether
as children in branches or subsequent nodes in an extension), these are
constructed as backing store nodes within the trie. In the absence of a root
node, the root node is nil.

When adding a new entry, the trie undergoes a traversal, reaching the final
leaf or branch node where the value hash is to be contained. Throughout this
traversal, any encountered backing store nodes or parent nodes are promoted.
This means they're transformed into one of the trie's primary node types. This
promotion ensures that the trie maintains its structural and operational
integrity as new entries are integrated. 

When a child trie is initialized, it's anchored to its parent by setting its
root node as a parent node that points back to the parent trie's root.
Accessing this child trie for add or delete operations involves converting
these parent nodes into full-fledged trie nodes. This conversion happens by
copying the original parent node and then modifying or replacing it as required
by the specific trie operation in question. 

***Nibbles:***

4-bit keys are maintained as `nibbles` slices with utility `pack` and `unpack` 
methods to move them into and out of 8-byte data slices, with a half/full
last-byte bit.

***Trie child and merge operations:***

Child tries are represented as tries with unexplored node references ("parent
nodes") that point back to unmodified parts of the parent trie. 

Obtaining a child trie from a trie allows the user to easily dispose of stacks
of changes to a parent trie at an arbitrary time.

Parent tries must remain read-only until after the child is disregared or until
after it is merged back into the parent.

When merging child tries back into their parents, the trie undergoes a
traversal. This search aims to identify parent nodes, which are then replaced
with their original references, effectively stitching the child trie's
modifications into the parent trie. 

Node deletion list are propagated into the parent to be handled by a future
parent backstore commit.

This mechanism ensures that the trie remains dynamic and efficient, capable of
expanding from the backstore when necessary while also maintaining the ability
to handle modifications through child tries.

***Backing stores:***

Backing stores are kv stores which maintain the mapping between trie keys and
node serialization data.  

Backing stores must "set" byte data containing serialized nodes, and "get"
nodes back from the store by deserializing them into trie nodes that (may)
contain deferred references to further backing store nodes.  The simplest
backing store is a golang map from byte slices to nodes, and uses the provided
node serialization / deserialization utilites.  

`BatchStart()` is called before any store operations are begun, and
`BatchEnd()` is called after there are no more, to allow for batch commits. 

Committing the trie to the backing store will trigger hashing of the trie, if
it is modified since the last hashing operation.

***Preloading:***

Normally only part of the trie is kept in memory.  However, the trie can sweep
all nodes out of the backstore and into memory by calling `preload`. 

***Trie transitions during Add operation:***

An add results in a group of one or more trie transitions from a group of 25.

**Leaf nodes**

- LN.ADD.1: Store the new value in the existing leaf node, overwriting it.

- LN.ADD.2: Store the existing leaf value in a new branch node value space.

- LN.ADD.3: Store the existing leaf value in a new leaf node attached to a new branch node.

- LN.ADD.4: Store the new value in the new branch node value space.

- LN.ADD.5: Store the new value in a new leaf node attached to the new branch node.

- LN.ADD.6: Replace the leaf node with a new extention node in front of the new branch node.

- LN.ADD.7: Replace the leaf node with a second new branch node in front of the new branch node.

- LN.ADD.8: Replace the leaf node with the branch node created earlier.

Operation sets (1 + 2x2 + 2x2x2 = 13 sets):

  * LN.1

  This updates the existing node with a new value, deleting the old value.
    
  * LN.2|LN.3 then LN.4|LN.5 

  This accomodates both the old and new values remaining in the trie,
  adding either 2 or 3 nodes (1 branch and 1 or 2 leaves)

  * LN.2|LN.3 then LN.4|LN.5 then LN.6|LN.7

  This accomodates both the old and new values remaining in the trie,
  and a shared extension, adding either 3 or 4 nodes (1 branch and 1
  or 2 leaves, plus either a extension or branch node)

**Extension nodes**

- EN.ADD.1: Point the existing extension node at a (possibly new or existing) node resulting
          from performing the add operation on the child node.

- EN.ADD.2: Create an extension node for the current child and store it in a new branch node child slot.

- EN.ADD.3: Store the existing extension node child in a new branch node child slot.

- EN.ADD.4: Store the new value in a new leaf node stored in an available child slot of the new branch node.

- EN.ADD.5: Store the new value in the value slot of the new branch node.

- EN.ADD.6: Modify the existing extension node shared key and point the child at the new branch node.

- EN.ADD.7: Replace the extension node with the branch node created earlier.

Operation sets (1 + 2x2 + 2x2 = 9 sets) :

  * EN1

  This redirects the extension node to a new/existing node resulting from
  performing the add operation on the extension child.

  * EN2|EN3 then EN4|EN5 then EN6

  This stores the current extension node child in either a new branch node
  child slot or by creating a new extension node at a new key pointing at the
  child, and attaching that to a new branch node.  Either way, the new branch
  node also receives a new leaf node with the new value or has its value slot
  assigned, and another extension node is created to replace it pointed at the
  branch node as its target.

  * EN2|EN3 then EN4|EN5 then EN7

  Same as above, only the new branch node replaceds the existing extension node
  outright, without the additional extension node.

**Branch nodes:**

Three operational transitions:

  * BN.ADD.1

  Store the new value in the branch node value slot. This overwrites the branch
  node slot value.

  * BN.ADD.2

  Make a new leaf node with the new value, and point an available branch child
  slot at it. This stores a new leaf node in a child slot.

  * BN.ADD.3

  This repoints the child node to a new/existing node resulting from performing
  the add operation on the child node.

***Trie transitions during Delete operation:***

A delete results in a group of one or more trie transitions from a group of 6.

**Leaf nodes**

  * LN.DEL.1

  Delete this leaf node that matches the delete key.  Pointers to this node
  from a branch or extension node are replaced with nil.  The node is added to
  the trie's list of deleted keys for later backstore commit.

**Extension nodes**

  * EN.DEL.1

  The extension node can be deleted because the child was deleted after finding
  the key in the lower subtrie.

  * EN.DEL.2

  Raise up the results of the successful deletion operation on the extension
  node child to replace the existing node (possibly with another extension
  nodes, as branches are raised up the trie by placing extension nodes in front
  of them)

**Branch nodes**

  One of the following three operations:

  * BN.DEL.1

  Copy the empty hash into the value slot, mark the node for rehashing.
   
  * BN.DEL.2

  Raise up the only child left to replace the branch node.

  * BN.DEL.3

  Delete the childless and valueless branch node.  Add it to the trie's list of
  deleted keys for later backstore commit.

  * BN.DEL.4

  Replace the child slot with a new node created by deleting the node lower in the trie 
  Mark for rehashing.

  * BN.DEL.5 

  Replace the branch node with a leaf node valued by the branch node value slot.

  * BN.DEL.6

  Replace the branch node with the only child left raised up a nibble.

  * BN.DEL.7

  Delete the childless and valueless branch node.  Add it to the trie's list of
  deleted keys for later backstore commit.
