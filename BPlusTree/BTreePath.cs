using System;
using System.Buffers;

namespace BPlusTree
{
    /// <summary>
    /// Records the path from the root node to the leaf node
    /// inside <see cref="BTree{TKey, TValue}" />.
    /// </summary>
    /// <remarks>
    /// This structure is used as an "iterator" to the B+Tree.
    /// </remarks>
    internal struct BTreePath : IDisposable
    {
        /// <summary>
        /// Each step along the path.
        /// </summary>
        /// <remarks>
        /// Step 0 selects an entry in the root node,
        /// Step 1 selects an entry in the B+Tree of level 1,
        /// and so forth, until step N (N = <see cref="Depth" />) 
        /// selects an entry in the leaf node.
        /// </remarks>
        internal BTreeStep[] Steps { get; }

        /// <summary>
        /// The depth of the B+Tree.
        /// </summary>
        /// <remarks>
        /// The depth, as a number, is the number of layers
        /// in the B+Tree before the layer of leaf nodes.
        /// 0 means there is only the root node, or the B+Tree 
        /// is completely empty.  
        /// </remarks>
        internal int Depth { get; }

        /// <summary>
        /// Version number to try to detect
        /// when this path has been invalidated by a change
        /// to the B+Tree.
        /// </summary>
        internal int Version { get; set; }

        internal BTreePath(BTreeStep[] steps, int depth, int version)
        {
            Steps = steps;
            Depth = depth;
            Version = version;
        }

        public void Dispose()
        {
            var steps = Steps;
            this = default;

            if (steps != null)
                ArrayPool<BTreeStep>.Shared.Return(steps);
        }
    }

    /// <summary>
    /// Describes one selection step when moving down the B+Tree in <see cref="BTreePath" />.
    /// </summary>
    internal struct BTreeStep
    {
        /// <summary>
        /// The B+Tree node present at the level of the B+Tree given by the 
        /// index of this instance within <see cref="BTreePath.Steps" />.
        /// </summary>
        public object? Node;

        /// <summary>
        /// The index of the entry selected within <see cref="Node" />.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This index ranges from 0, to <see cref="NodeLink.EntriesCount"/> minus one,
        /// which is just the order of the B+Tree.
        /// </para>
        /// <para>
        /// For interior nodes, this index thus covers all the possible links
        /// to children and no more.  For leaf nodes, the maximum index value
        /// does not point to any valid entry, but is an intermediate state
        /// signaling that the end of the leaf node's entries have been reached,
        /// when iterating through the B+Tree's data entries forward.
        /// </para>
        /// </remarks>
        public int Index;

        public BTreeStep(object node, int index)
        {
            Node = node;
            Index = index;
        }
    }
}
