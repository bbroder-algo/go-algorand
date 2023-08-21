
***Trie operation and usage:***

Tries are initialized against a backing store (a memory one will be constructed if not 
provided by the user).

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
fmt.Println("K1:V1,K2:V2,D2 Hash:", mt.Hash())
```

The trie provides a SHA-512/256 checksum at the root.  The trie is a 16(nibble)-ary 
trie.  Keys are maintained as `nibbles` slices with pack and unpack methods that
compress them into 8-byte data slices with a half/full ending bit.

***Trie transitions during Add operation:***

An add results in a group of one or more trie transitions from a group of 25.

**Leaf nodes**

LN.1: Store the new value in the existing leaf node, overwriting it.
LN.2: Store the existing leaf value in a new branch node value space.
LN.3: Store the existing leaf value in a new leaf node attached to a new branch node.
LN.4: Store the new value in the new branch node value space.
LN.5: Store the new value in a new leaf node attached to the new branch node.
LN.6: Replace the leaf node with a new extention node in front of the new branch node.
LN.7: Replace the leaf node with a second new branch node in front of the new branch node.
LN.8: Replace the leaf node with the branch node created earlier.

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

EN.1: Point the existing extension node at a (possibly new or existing) node resulting
      from performing the add operation on the child node.
EN.2: Create an extension node for the current child and store it in a new branch node child slot.
EN.3: Store the existing extension node child in a new branch node child slot.
EN.4: Store the new value in a new leaf node stored in an available child slot of the new branch node.
EN.5: Store the new value in the value slot of the new branch node.
EN.6: Modify the existing extension node shared key and point the child at the new branch node.
EN.7: Replace the extension node with the branch node created earlier.

Operation sets (1 + 2x2 + 2x2 = 9 sets) :

  * EN1

  This redirects the extension node to a new/existing node resulting from performing the 
  add operation on the extension child.

  * EN2|EN3 then EN4|EN5 then EN6

  This stores the current extension node child in either a new branch node child
  slot or by creating a new extension node at a new key pointing at the child, and
  attaching that to a new branch node.  Either way, the new branch node also receives a new
  leaf node with the new value or has its value slot assigned, and another extension 
  node is created to replace it pointed at the branch node as its target.

  * EN2|EN3 then EN4|EN5 then EN7

  Same as above, only the new branch node replaceds the existing extension node  
  outright, without the additional extension node.

**Branch nodes:**

BN.1: Store the new value in the branch node value slot.
BN.2: Make a new leaf node with the new value, and point an available branch child slot at it.
BN.3: Repoint a child slot at a (possibly new or existing) node resulting from performing
      the add operation on the child.

Operation sets (1 + 1 + 1 = 3 sets) :

  * BN.1

  This overwrites the branch node slot value.

  * BN.2

  This stores a new leaf node in a child slot.

  * BN.3

  This repoints the child node to a new/existing node resulting from performing
  the add operation on the child node.

***Trie transitions during Delete operation:***

A delete results in a group of one or more trie transitions from a group of 25.

**Leaf nodes**

  * LN.DEL.1

  Delete this leaf node that matches the delete key.  Pointers to this node from
  a branch or extension node are replaced with nil.  The node is added to the trie's
  list of deleted keys for later backstore commit.

**Branch nodes**

BN.DEL.1: Empty the value in the branch node value space.
BN.DEL.2: Delete the branch node.
BN.DEL.3: Repoint a child slot at a (possibly new or existing) node 
          resulting from performing the delete operation on the child.

Operation sets (1 + 1 + 1 = 3 sets):

  * BN.DEL.1

  Copy the empty hash into the value slot and mark the node for rehashing.

   
  * BN.DEL.2

  Delete the branch node, as the delete removed the value slot and there are no 
  children.  Add it to the list of the trie's deleted nodes for backstore commit.

  * BN.DEL.3

  Replace the child slot with a new node, as the delete key was found in the child
  subtrie.  Mark the node for rehashing.

**Extension nodes**

EN.DEL.1: Delete this node.
EN.DEL.2: Repoint a child slot at a (possibly new or existing) node 
          resulting from performing the delete operation on the child.

Operation sets (1 + 1 = 2 sets)

  * EN.DEL.1

  The extension node can be deleted because the child was deleted after finding
  the key in the lower subtrie.

  * EN.DEL.2

  The extension node is now pointing at a new child as a result of finding the
  key in the lower subtrie.  Mark the node for rehashing.


***Trie child and merge operations:***

Child tries are represented as tries with unexplored node references ("parent nodes") back 
to unmodified parts of the parent trie. 

Obtaining a child trie from a trie allows the user to easily dispose of stacks of changes 
to a parent trie at an arbitrary time.

Parent tries must be read-only until after the child is disregared or after it is merged back
into the parent.

The merge operation stitches the references into the parent trie and sets the parent root
to the child root.  Node deletion list are propagated into the parent to be handled by a 
future parent backstore commit.

***Backing stores:***

Backing stores are kv stores which maintain the mapping between trie keys and node
serialization data.  

Backing stores must "set" byte data containing serialized nodes, and "get" nodes back
from the store by deserializing them into trie nodes that (may) contain deferred 
references to further backing store nodes.  The simplest backing store is a golang 
map from byte slices to nodes, and uses the provided node serialization / deserialization 
utilites.  

`BatchStart()` is called before any store operations are begun, and `BatchEnd()` is 
called after there are no more, to allow for batch commits. 

Committing the trie to the backing store will trigger hashing of the trie, if it is
modified since the last hashing operation.

***Preloading:***

Normally only part of the trie is kept in memory.  However, the trie can sweep all nodes 
out of the backstore and into memory by calling `preload`. 

