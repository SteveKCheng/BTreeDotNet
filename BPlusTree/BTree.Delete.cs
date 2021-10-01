using System;
using System.Runtime.CompilerServices;

namespace BPlusTree
{
    public partial class BTreeBase<TKey>
    {
        /// <summary>
        /// Delete an entry at the given index in a node without regard to the number
        /// of entries remaining.
        /// </summary>
        /// <param name="node">The node to delete the entry from. </param>
        /// <param name="index">The index of the entry to delete. </param>
        /// <param name="numEntries">The variable holding the number of active entries
        /// in the node; it will be decremented by one. </param>
        internal void RemoveEntryWithinNode<TValue>(Entry<TKey, TValue>[] node, int index, ref int numEntries)
        {
            var entries = node.AsSpan();
            entries[(index + 1)..numEntries].CopyTo(entries[index..]);
            entries[--numEntries] = default;
        }

        internal void DeleteEntryAndShiftFromLeft<TValue>(Entry<TKey, TValue>[] leftNode, 
                                                          Entry<TKey, TValue>[] rightNode,
                                                          ref int leftEntriesCount,
                                                          ref int rightEntriesCount,
                                                          int leftIndex,
                                                          int rightIndex,
                                                          ref TKey pivotKey)
        {
            var leftEntries = leftNode.AsSpan();
            var rightEntries = rightNode.AsSpan();

            int movedCount = leftEntriesCount - leftIndex;

            // Make room in the right node for the entries to be shifted from
            // the left, and at the same time delete the entry at rightIndex.
            rightEntries[(rightIndex + 1)..].CopyTo(rightEntries[(movedCount + rightIndex)..]);
            rightEntries[0..rightIndex].CopyTo(rightEntries[movedCount..]);

            // Move the entries from the left node into the left-most slots of the right node.
            var movedEntries = leftEntries[leftIndex..leftEntriesCount];
            movedEntries.CopyTo(rightEntries);
            movedEntries.Clear();

            // Update counts
            leftEntriesCount -= movedCount;
            rightEntriesCount += movedCount - 1;

            // After shifting entries for interior nodes, rotate the pivot key
            // present in the parent node.  
            if (typeof(TValue) == typeof(NodeLink))
            {
               // The old pivot is demoted to the slot in the right node just
               // after the entries that were moved from the left node.
                rightEntries[movedCount].Key = pivotKey;

                // The new pivot is the left-most key whose associated link
                // now appears as the left-most child of the right node.
                ref var slot0Key = ref rightEntries[0].Key;
                pivotKey = slot0Key;
                slot0Key = default!;
            }

            // After shifting entries for leaf nodes, if the left node is
            // not to be deleted afterwards, update the pivot key between
            // the left and right nodes to be the last key in the left node.
            //
            // If the left node is to be deleted (leftEntriesCount == 0), the
            // pivot key should be updated to the same as the pivot key between
            // the left node and its preceding node (which must exist according
            // to the overall algorithm for deletion).  But this function does
            // not have access to that key, so we skip handling that case here.
            else if (leftEntriesCount > 0)
            {
                pivotKey = leftEntries[leftEntriesCount - 1].Key;
            }
        }

        internal void DeleteEntryAndShiftFromRight<TValue>(Entry<TKey, TValue>[] leftNode, 
                                                           Entry<TKey, TValue>[] rightNode,
                                                           ref int leftEntriesCount,
                                                           ref int rightEntriesCount,
                                                           int leftIndex,
                                                           int rightIndex,
                                                           ref TKey pivotKey)
        {
            var leftEntries = leftNode.AsSpan();
            var rightEntries = rightNode.AsSpan();

            leftEntries[(leftIndex + 1)..leftEntriesCount].CopyTo(leftEntries[leftIndex..]);
            rightEntries[0..rightIndex].CopyTo(leftEntries[(leftEntriesCount - 1)..]);

            rightEntries[rightIndex..rightEntriesCount].CopyTo(rightEntries[0..]);
            rightEntries[(rightEntriesCount - rightIndex)..rightEntriesCount].Clear();

            int movedCount = rightIndex;
            leftEntriesCount += movedCount - 1;
            rightEntriesCount -= movedCount;

            // After shifting entries for interior nodes, rotate the pivot key
            // present in the parent node.  
            if (typeof(TValue) == typeof(NodeLink))
            {
                // The old pivot key is demoted to the slot in the left node
                // that comes from the left-most slot in the right node.
                leftEntries[leftEntriesCount - movedCount].Key = pivotKey;

                // The new pivot is the left-most key whose associated link
                // now appears as the left-most child of the right node.
                ref var slot0Key = ref rightEntries[0].Key;
                pivotKey = slot0Key;
                slot0Key = default!;
            }

            // After shifting entries for leaf nodes, if the left node is
            // not to be deleted afterwards, update the pivot key between
            // the left and right nodes to be the last key in the left node.
            //
            // If the left node is to be deleted (leftEntriesCount == 0), the
            // pivot key should be updated to the same as the pivot key between
            // the left node and its preceding node (which must exist according
            // to the overall algorithm for deletion).  But this function does
            // not have access to that key, so we skip handling that case here.
            else if (leftEntriesCount > 0)
            {
                pivotKey = leftEntries[leftEntriesCount - 1].Key;
            }
        }

        /// <summary>
        /// Get the the slot for the last key in the B+Tree that is compared
        /// to select the specified node versus its left neighbor.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The left neighbor of a node is defined as the node at the same
        /// level in the B+Tree that holds the immediately preceding keys.
        /// It must exist unless the specified node contains the very first
        /// key, for the given level of the B+Tree. 
        /// </para>
        /// <para>
        /// The ancestor node that contains the pivot key is sometimes called
        /// the (left) anchor node in the literature.  The left anchor node is located
        /// on the specified node's path, at the point farthest from the root
        /// where the path may diverge to the left.  
        /// </para>
        /// </remarks>
        /// <param name="path">Path from the root to reach the specified node.
        /// </param>
        /// <param name="targetLevel">
        /// The level of the specified node, which may be less than the depth
        /// of the B+Tree but not zero.
        /// </param>
        /// <param name="pivotLevel">
        /// On return, the level of the B+Tree where the pivot key lives is set here,
        /// or -1 if the left neighbor does not exist.
        /// It will be necessarily less than <paramref name="targetLevel"/>. 
        /// </param>
        /// <returns>
        /// Reference to the key held in an ancestor node of the specified node
        /// in the B+Tree, if the left neighbor exists.  If not, the null reference
        /// is returned.
        /// </returns>
        internal ref TKey GetLeftPivotKey(ref BTreePath path, 
                                          int targetLevel, 
                                          out int pivotLevel)
        {
            // Move up the B+Tree one level at a time from the target level 
            int level = targetLevel;
            while (level-- >= 0)
            {
                ref var step = ref path.Steps[level];

                // Stop iteration as soon as there is a node to the left at this level
                int index = step.Index;
                if (index > 0)
                {
                    pivotLevel = level;

                    // Pivot key is at the same index as the link recorded in the path
                    return ref AsInternalNode(step.Node!)[index].Key;
                }
            }

            pivotLevel = level;
            return ref Unsafe.NullRef<TKey>();
        }

        /// <summary>
        /// Get the the slot for the last key in the B+Tree that is compared
        /// to select the specified node versus its right neighbor.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The right neighbor of a node is defined as the node at the same
        /// level in the B+Tree that holds the immediately following keys.
        /// It must exist unless the specified node contains the very last
        /// key, for the given level of the B+Tree. 
        /// </para>
        /// <para>
        /// The ancestor node that contains the pivot key is sometimes called
        /// the (right) anchor node in the literature.  The right anchor node is located
        /// on the specified node's path, at the point farthest from the root
        /// where the path may diverge to the right.  
        /// </para>
        /// </remarks>
        /// <param name="path">Path from the root that touches the specified node.
        /// </param>
        /// <param name="targetLevel">
        /// The level of the target node in the B+Tree, which may be less than 
        /// the depth of the B+Tree but not zero.  The path is cut off at this level
        /// to specify the desired node.
        /// </param>
        /// <param name="pivotLevel">
        /// On return, the level of the B+Tree where the pivot key lives is set here,
        /// or -1 if the right neighbor does not exist.  This number is called
        /// the pivot level for short.
        /// It will be necessarily less than <paramref name="targetLevel"/>. 
        /// </param>
        /// <returns>
        /// Reference to the key held in an ancestor node of the specified node
        /// in the B+Tree, if the right neighbor exists.  If not, the null reference
        /// is returned.
        /// </returns>
        internal ref TKey GetRightPivotKey(ref BTreePath path, 
                                           int targetLevel, 
                                           out int pivotLevel)
        {
            // Move up the B+Tree one level at a time from the target level 
            int level = targetLevel;
            while (level-- >= 0)
            {
                ref var step = ref path.Steps[level];
                var node = AsInternalNode(step.Node!);

                // Stop iteration as soon as there is a node to the right at this level
                if (step.Index + 1 < node.Length)
                {
                    // Pivot key is at one plus the index of the link recorded in the path
                    var index = step.Index + 1;
                    ref var entry = ref node[index];

                    if (entry.Value.Child != null)
                    {
                        pivotLevel = level;
                        return ref entry.Key;
                    }
                }
            }

            pivotLevel = level;
            return ref Unsafe.NullRef<TKey>();
        }

        /// <summary>
        /// Get the left neighbor for a node in the B+Tree when its pivot level is known.
        /// </summary>
        /// <remarks>
        /// See <see cref="GetLeftNeighborPivotKey"/> for the definition of the left neighbor
        /// and the left pivot level for a node.
        /// </remarks>
        /// <param name="path">Path from the root that touches the specified node.
        /// </param>
        /// <param name="targetLevel">
        /// The level of the target node in the B+Tree, which may be less than 
        /// the depth of the B+Tree but not zero.  The path is cut off at this level
        /// to specify the desired node.
        /// </param>
        /// <param name="pivotLevel">Pivot level output from <see cref="GetLeftNeighborPivotKey" />
        /// for the same <paramref name="path"/> and <paramref name="targetLevel" />.
        /// </param>
        /// <returns>
        /// Reference to the entry in the neighbor's parent node where 
        /// the neighbor node is linked from.  This node, by definition, is at the
        /// same level of the B+Tree as the specified node.
        /// If there is no left neighbor, i.e. when <paramref name="pivotLevel"/> is -1,
        /// then the returned reference is null.
        /// </returns>
        internal ref NodeLink GetLeftNeighbor(ref BTreePath path, int targetLevel, int pivotLevel)
        {
            if (pivotLevel < 0)
                return ref Unsafe.NullRef<NodeLink>();

            // Move left once in the path at the pivot level
            ref var pivotStep = ref path.Steps[pivotLevel];
            var node = AsInternalNode(pivotStep.Node!);
            int index = pivotStep.Index - 1;
            
            // Go down to the target level keeping to the right-most node at each level
            while (++pivotLevel < targetLevel)
            {
                ref var nodeLink = ref node[index].Value;
                node = AsInternalNode(nodeLink.Child!);
                index = nodeLink.EntriesCount - 1;
            }

            return ref node[index].Value;
        }

        /// <summary>
        /// Get the right neighbor for a node in the B+Tree when its pivot level is known.
        /// </summary>
        /// <remarks>
        /// See <see cref="GetRightNeighborPivotKey"/> for the definition of the right neighbor
        /// and the right pivot level for a node.
        /// </remarks>
        /// <param name="path">Path from the root that touches the specified node.
        /// </param>
        /// <param name="targetLevel">
        /// The level of the target node in the B+Tree, which may be less than 
        /// the depth of the B+Tree but not zero.  The path is cut off at this level
        /// to specify the desired node.
        /// </param>
        /// <param name="pivotLevel">Pivot level output from <see cref="GetRightNeighborPivotKey" />
        /// for the same <paramref name="path"/> and <paramref name="targetLevel" />.
        /// </param>
        /// <returns>
        /// Reference to the entry in the neighbor's parent node where 
        /// the neighbor node is linked from.  This node, by definition, is at the
        /// same level of the B+Tree as the specified node.
        /// If there is no right neighbor, i.e. when <paramref name="pivotLevel"/> is -1,
        /// then the returned reference is null.
        /// </returns>
        internal ref NodeLink GetRightNeighbor(ref BTreePath path, int targetLevel, int pivotLevel)
        {
            if (pivotLevel < 0)
                return ref Unsafe.NullRef<NodeLink>();

            // Move right once in the path at the pivot level
            ref var pivotStep = ref path.Steps[pivotLevel];
            var node = AsInternalNode(pivotStep.Node!);
            int index = pivotStep.Index + 1;

            // Go down to the target level keeping to the left-most node at each level
            while (++pivotLevel < targetLevel)
            {
                node = AsInternalNode(node[index].Value.Child!);
                index = 0;
            }

            return ref node[index].Value;
        }

        internal int RemoveEntryAndRebalanceNode<TValue>(ref BTreePath path, int level, int index)
        {
            ref var parentStep = ref path.Steps[level - 1];
            var parentNode = AsInternalNode(parentStep.Node!);
            ref var nodeLink = ref parentNode[parentStep.Index].Value;
            var currentNode = (Entry<TKey, TValue>[])nodeLink.Child!;
            ref var numEntries = ref nodeLink.EntriesCount;

            var halfLength = (currentNode.Length + 1) >> 1;

            // If there are enough entries remaining in the node, it does not need
            // to be re-balanced after deleting the entry.
            if (numEntries > halfLength)
            {
                RemoveEntryWithinNode(currentNode, index, ref numEntries);
                return -1;
            }

            // Gather left and right neighbor nodes for re-balancing or merging
            ref var leftPivotKey = ref GetLeftPivotKey(ref path, level, out int leftPivotLevel);
            ref var rightPivotKey = ref GetRightPivotKey(ref path, level, out int rightPivotLevel);
            ref var leftNeighbor = ref GetLeftNeighbor(ref path, level, leftPivotLevel);
            ref var rightNeighbor = ref GetRightNeighbor(ref path, level, rightPivotLevel);

            static bool CanDonateEntries(ref NodeLink neighbor, int minEntries)
            {
                return !Unsafe.IsNullRef(ref neighbor) && neighbor.EntriesCount > minEntries;
            }

            // Check the left and right neighbors if they have surplus entries.
            // If so we can re-balance entries from them, and the deletion
            // process can terminate.
            if (CanDonateEntries(ref leftNeighbor, halfLength))
            {
                DeleteEntryAndShiftFromLeft((Entry<TKey, TValue>[])leftNeighbor.Child!,
                                            currentNode,
                                            ref leftNeighbor.EntriesCount,
                                            ref numEntries,
                                            leftNeighbor.EntriesCount - halfLength,
                                            index,
                                            ref leftPivotKey);
                return -1;
            }
            else if (CanDonateEntries(ref rightNeighbor, halfLength))
            {
                // Delete the selected entry in the current node and
                // take entries from the right neighbor.
                DeleteEntryAndShiftFromRight(currentNode,
                                             (Entry<TKey, TValue>[])rightNeighbor.Child!,
                                             ref numEntries,
                                             ref rightNeighbor.EntriesCount,
                                             index,
                                             rightNeighbor.EntriesCount - halfLength,
                                             ref rightPivotKey);
                return -1;
            }

            // At this point, all neighbors have too few nodes.  Pick the
            // neighbor that has the same parent as the current node, which
            // must exist.  Then, prepare to delete the neighbor in the next
            // iteration of the loop.
            if (leftPivotLevel == level - 1)
            {
                // Delete the desired entry and then merge in all entries
                // from the left neighbor.
                DeleteEntryAndShiftFromLeft((Entry<TKey, TValue>[])leftNeighbor.Child!,
                                            currentNode,
                                            ref leftNeighbor.EntriesCount,
                                            ref numEntries,
                                            leftNeighbor.EntriesCount,
                                            index,
                                            ref leftPivotKey);

                return parentStep.Index - 1;
            }
            else
            {
                // Delete the desired entry and then merge in all entries
                // from the left neighbor.
                DeleteEntryAndShiftFromRight(currentNode,
                                             (Entry<TKey, TValue>[])rightNeighbor.Child!,
                                             ref numEntries,
                                             ref rightNeighbor.EntriesCount,
                                             index,
                                             rightNeighbor.EntriesCount,
                                             ref rightPivotKey);

                return parentStep.Index + 1;
            }
        }
    }

    public partial class BTree<TKey, TValue>
    {
        private void Remove(ref BTreePath path)
        {
            int depth = path.Depth;
            int index = path.Steps[depth].Index;

            // Root node is a leaf node.
            if (depth == 0)
            {
                RemoveEntryWithinNode(AsLeafNode(_root.Child!), index, ref _root.EntriesCount);
                return;
            }

            // Remove the entry from the non-root node.
            // Do nothing else if the node still has at least the minimum number of entries.
            index = RemoveEntryAndRebalanceNode<TValue>(ref path, depth, index);
            if (index < 0)
                return;
            
            for (int level = depth - 1; level > 0; --level)
            {
                index = RemoveEntryAndRebalanceNode<NodeLink>(ref path, level, index);
                if (index < 0)
                    return;
            }

            var rootNode = AsInternalNode(_root.Child!);
            RemoveEntryWithinNode(rootNode, index, ref _root.EntriesCount);

            // Collapse a (non-leaf) root node when it has only one child left.
            if (_root.EntriesCount == 1)
                _root = rootNode[0].Value;
        }

    }
}
