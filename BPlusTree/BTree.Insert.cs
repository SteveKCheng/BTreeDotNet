using System;
using System.Runtime.CompilerServices;

namespace BPlusTree
{
    internal static partial class BTreeCore
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
        /// The index to insert the new entry at, to a maximum of 
        /// <paramref name="numEntries"/>.  Entries occurring at
        /// and after this index are shifted forwards in the node.
        /// For internal nodes, this index may not be zero.
        /// This index may be equal to the length of <paramref name="node"/>,
        /// which immediately implies it needs to be split.
        /// </param>
        /// <param name="addToParent">
        /// The entry that must be inserted into the parent node
        /// for the node that was split off from <paramref name="node"/>.
        /// </param>
        /// <returns>
        /// Whether the node to insert into has to be split into two
        /// because it has no more slots for entries.
        /// </returns>
        public static bool InsertWithinNode<TKey, TValue>(TKey key,
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
                // Rounds down for interior nodes
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

    public partial class BTree<TKey, TValue>
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
        private void BasicInsert(TKey key, TValue value, ref BTreePath path)
        {
            int level = path.Depth;

            // Insert into leaf first
            ref var leafEntriesCount = ref GetNodeEntriesCount(ref path, level);
            ref var leafStep = ref path[level];
            var leafNode = AsLeafNode(leafStep.Node!);
            int index = leafStep.Index;
            if (!BTreeCore.InsertWithinNode(key, value,
                                            leafNode, ref leafEntriesCount,
                                            index,
                                            out var addToParent))
                return;

            // Update leaf step in the path to point to the new entry
            int halfLength = leafNode.Length >> 1;
            bool isLeft = (index <= halfLength);
            leafStep = new BTreeStep(isLeft ? leafNode : addToParent.Value.Child!,
                                     isLeft ? index : index - (halfLength - 1));

            // Loop and insert into successive parents if nodes need to split
            while (level > 0)
            {
                --level;

                ref var interiorEntriesCount = ref GetNodeEntriesCount(ref path, level);
                ref var interiorStep = ref path[level];
                var interiorNode = BTreeCore.AsInteriorNode<TKey>(interiorStep.Node!);

                index = interiorStep.Index + (isLeft ? 0 : 1);

                // Add the right node after splitting from the level once below
                if (!BTreeCore.InsertWithinNode(addToParent.Key, addToParent.Value,
                                                interiorNode, ref interiorEntriesCount,
                                                interiorStep.Index + 1,
                                                out addToParent))
                {
                    interiorStep.Index = index;
                    return;
                }

                // Update interior step in the path to select the correct node one level below
                isLeft = (index <= halfLength);
                interiorStep = new BTreeStep(isLeft ? interiorNode : addToParent.Value.Child!,
                                             isLeft ? index : index - (halfLength - 1));
            };

            // Root node needs to split
            var newRootNode = new Entry<TKey, NodeLink>[Order + 1];
            newRootNode[0].Value = _root;
            newRootNode[1] = addToParent;
            _root = new NodeLink(newRootNode, 2);
            ++Depth;

            // FIXME need to add a new step to path
        }

        private void Insert(TKey key, TValue value, ref BTreePath path)
        {
            BasicInsert(key, value, ref path);
            ++Count;
        }

        /// <summary>
        /// Add an item to the B+Tree if another item of the same key is not
        /// already present.
        /// </summary>
        /// <param name="key">The key of the item to add. </param>
        /// <param name="value">The associated value of the item to add. </param>
        /// <returns>
        /// True if the key does not already exist in the B+Tree, and the
        /// specified item has just been inserted.  False if another item
        /// in the B+Tree already has the specified key.
        /// </returns>
        public bool TryAdd(TKey key, TValue value)
        {
            var path = NewPath();
            try
            {
                if (!Unsafe.IsNullRef(ref FindEntry(ref path, key)))
                    return false;

                ++_version;
                Insert(key, value, ref path);
                return true;
            }
            finally
            {
                path.Dispose();
            }
        }

        /// <summary>
        /// Add an item to the B+Tree whose key is not already present.
        /// </summary>
        /// <param name="key">The key of the item to add. </param>
        /// <param name="value">The associated value of the item to add. </param>
        public void Add(TKey key, TValue value)
        {
            if (!TryAdd(key, value))
                throw new InvalidOperationException("There is already an entry with the same key as the entry to add in the B+Tree. ");
        }
    }
}

