using System;

namespace BPlusTree
{
    public partial class BTreeBase<TKey>
    {
        /// <summary>
        /// Insert an entry into a node, reporting the entry to add
        /// to its parent if it has to split.
        /// </summary>
        /// <typeparam name="TValue">The type of values stored in the node,
        /// which is the user-defined type of value, or <see cref="NodeLink"/>
        /// if the node is internal.  The logic to split an internal node
        /// slightly differs from that for leaf nodes.
        /// </typeparam>
        /// <param name="key">The key to insert into the node, which must
        /// respect the existing ordering of keys. </param>
        /// <param name="value">The value to insert, associated to the key.
        /// </param>
        /// <param name="node">The node to insert the new entry into. </param>
        /// <param name="numEntries">Reference to the variable that
        /// counts the number of active entries in the node.
        /// </param>
        /// <param name="index">
        /// The index to insert the new entry at.  Entries occurring at
        /// and after this index are shifted forwards in the node.
        /// For internal nodes, this index may not be zero.
        /// </param>
        /// <param name="addToParent">
        /// The entry that must be inserted into the parent node
        /// for the node that was split off from <paramref name="node"/>.
        /// </param>
        /// <returns>
        /// Whether the node to insert into has to be split into two
        /// because it has no more slots for entries.
        /// </returns>
        internal bool InsertWithinNode<TValue>(TKey key,
                                               TValue value,
                                               Entry<TKey, TValue>[] node,
                                               ref int numEntries,
                                               int index,
                                               out Entry<TKey, NodeLink> addToParent)
        {
            var entries = node.AsSpan();
            var newEntry = new Entry<TKey, TValue>(key, value);

            // Assumed to be even and equal to Order for leaf nodes,
            // odd and equal to Order + 1 for internal nodes
            int length = node.Length;

            // Node is not yet full.
            if (numEntries < length)
            {
                // Insert new entry at index,
                // shifting existing elements at [index, halfLength) to the right.
                entries[index..numEntries].CopyTo(entries[(index + 1)..]);
                entries[index] = newEntry;

                ++numEntries;
                addToParent = default;
                return false;
            }

            // Node is full and needs to be split.
            else
            {
                // Rounds down for internal nodes
                int halfLength = length >> 1;

                // Prepare new node to split off entries to
                var splitNode = new Entry<TKey, TValue>[length];
                var newEntries = splitNode.AsSpan();
                int numSplitEntries;

                // Add to left-hand node after splitting.
                if (index <= halfLength)
                {
                    // Move entries in [halfLength, length) from the left node to the right node.
                    entries[halfLength..length].CopyTo(newEntries);

                    // Insert new entry at index in the left node,
                    // shifting existing elements at [index, halfLength) to the right.
                    entries[index..halfLength].CopyTo(entries[(index + 1)..(halfLength + 1)]);
                    entries[index] = newEntry;
                }

                // Add to right-hand node after splitting.
                else // index >= halfLength + 1
                {
                    // Move entries in [halfLength+1, index) from the left node to the right node. 
                    entries[(halfLength + 1)..index].CopyTo(newEntries);

                    // Insert new entry in the right node,
                    // then move the rest of the elements from the left node,
                    // originally at [index, length), over to the right node. 
                    newEntries[index - (halfLength + 1)] = newEntry;
                    entries[index..length].CopyTo(newEntries[(index - halfLength)..]);
                }

                // Update counts of entries.  Note that the right node has a count of halfLength
                // if it is a leaf node, but halfLength + 1 if it is an internal node.
                // Recall that an internal node's count is biased by one because its slot 0
                // is always used to hold the link to the left-most child.  So both the left
                // and right nodes after splitting an internal node hold halfLength keys.
                numEntries = halfLength + 1;
                numSplitEntries = length - halfLength;

                // Clear out entries in the left node whose data have been moved over
                entries[(halfLength + 1)..length].Clear();

                TKey pivotKey;
                if (typeof(TValue) == typeof(NodeLink))
                {
                    // The key present in slot 0 of the right internal node should
                    // be moved "up" to the parent as the pivot key.  Slot 0 should
                    // not have any key but is only used to hold NodeLink.
                    ref var slot0Key = ref splitNode[0].Key;
                    pivotKey = slot0Key;
                    slot0Key = default!;
                }
                else
                {
                    // For leaf nodes, the pivot key should be copied from the
                    // left node's last entry.
                    pivotKey = entries[halfLength].Key;
                }

                addToParent = new Entry<TKey, NodeLink>(pivotKey, new NodeLink(splitNode, numSplitEntries));
                return true;
            }
        }
    }

    public partial class BTree<TKey, TValue> : BTreeBase<TKey>
    {
        /// <summary>
        /// Insert an entry into the B+Tree by following a previously discovered path,
        /// splitting nodes as necessary.
        /// </summary>
        /// <param name="key">The key to insert at the end of the path, which
        /// must respect the existing ordering of keys in the B+Tree.
        /// </param>
        /// <param name="value">The value associated to the key to be inserted.
        /// </param>
        /// <param name="path">Path from the root to the location in the B+Tree 
        /// where the new entry is to be inserted.
        /// </param>
        private void Insert(TKey key, TValue value, ref BTreePath path)
        {
            int level = path.Depth;

            // Insert into leaf first
            ref var leafEntriesCount = ref GetNodeEntriesCount(ref path, level);
            ref var leafStep = ref path.Steps[level];
            var leafNode = AsLeafNode(leafStep.Node!);
            if (!InsertWithinNode(key, value,
                                  leafNode, ref leafEntriesCount, leafStep.Index,
                                  out var addToParent))
                return;

            // Loop and insert into successive parents if nodes need to split
            while (level > 0)
            {
                --level;

                ref var internalEntriesCount = ref GetNodeEntriesCount(ref path, level);
                ref var internalStep = ref path.Steps[level];
                var internalNode = AsInternalNode(internalStep.Node!);

                if (!InsertWithinNode(addToParent.Key, addToParent.Value,
                                      internalNode, ref internalEntriesCount, internalStep.Index + 1,
                                      out addToParent))
                    return;
            };

            // Root node needs to split
            var newRootNode = new Entry<TKey, NodeLink>[Order + 1];
            newRootNode[0].Value = _root;
            newRootNode[1] = addToParent;
            _root = new NodeLink(newRootNode, 2);
        }

    }
}

