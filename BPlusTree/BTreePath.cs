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
        /// The B+Tree that this path applies to.
        /// </summary>
        private object? _owner;

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

        internal BTreePath(object? owner, BTreeStep[] steps, int depth)
        {
            _owner = owner;
            Steps = steps;
            Depth = depth;
        }

        public void Dispose()
        {
            ArrayPool<BTreeStep>.Shared.Return(Steps);
            this = default;
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
        /// This index ranges from 0 to the number of active entries in the
        /// node, as recorded in <see cref="NodeLink.EntriesCount"/>, minus one.
        /// So for leaf nodes, the maximum index possible is the order of the
        /// B+Tree minus one, while for interior nodes the maximum index 
        /// is the order of the B+Tree.
        /// </remarks>
        public int Index;

        public BTreeStep(object node, int index)
        {
            Node = node;
            Index = index;
        }
    }
}
