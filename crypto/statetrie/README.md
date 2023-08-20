
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

Atomic operations sets (1 + 2x2 + 2x2x2 = 13 operation sets):
  (1) LN.1

  This updates the existing node with a new value, deleting the old value.
  
    
  (2) LN.2|LN.3 then LN.4|LN.5 

  This accomodates both the old and new values remaining in the trie,
  adding either 2 or 3 nodes (1 branch and 1 or 2 leaves)

  (3) LN.2|LN.3 then LN.4|LN.5 then LN.6|LN.7

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

Atomic operation sets (1 + 2x2 + 2x2 = 9 operation sets) :

  EN1

  This redirects the extension node to a new/existing node resulting from performing the 
  add operation on the extension child.

  EN2|EN3 then EN4|EN5 then EN6

  This stores the current extension node child in either a new branch node child
  slot or by creating a new extension node at a new key pointing at the child, and
  attaching that to a new branch node.  Either way, the new branch node also receives a new
  leaf node with the new value or has its value slot assigned, and another extension 
  node is created to replace it pointed at the branch node as its target.

  EN2|EN3 then EN4|EN5 then EN7

  Same as above, only the new branch node replaceds the existing extension node  
  outright, without the additional extension node.

**Branch nodes:**

BN.1: Store the new value in the branch node value slot.
BN.2: Make a new leaf node with the new value, and point an available branch child slot at it.
BN.3: Repoint a child slot at a (possibly new or existing) node resulting from performing
      the add operation on the child.

Atomic operation sets (1 + 1 + 1 = 3 operation sets) :

  BN.1

  This overwrites the branch node slot value.

  BN.2

  This stores a new leaf node in a child slot.

  BN.3

  This repoints the child node to a new/existing node resulting from performing
  the add operation on the child node.
