// © 2021 Steve Cheng.
// See LICENSE.txt for copyright licensing of this work.

using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace GoldSaucer.BTree
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
        /// Array, which may be over-allocated, to store information
        /// on each step along the path.
        /// </summary>
        /// <remarks>
        /// The steps are stored "backwards", e.g. the step
        /// at an index is for the B+Tree level being
        /// Depth minus index.  B+Trees always grow or shrink
        /// from the root node so storing the steps backwards
        /// allows the path to grow or shrink without having
        /// to copy existing elements.
        /// </remarks>
        private BTreeStep[] _steps;

        /// <summary>
        /// Select one step along the path.
        /// </summary>
        /// <remarks>
        /// Step 0 selects an entry in the root node,
        /// Step 1 selects an entry in the B+Tree of level 1,
        /// and so forth, until step N (N = <see cref="Depth" />) 
        /// selects an entry in the leaf node.
        /// </remarks>
        internal ref BTreeStep this[int level]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return ref _steps[Depth - level];
            }
        }

        /// <summary>
        /// Select the step for the leaf node in the path.
        /// </summary>
        internal ref BTreeStep Leaf
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return ref _steps[0];
            }
        }

        /// <summary>
        /// Shrink this path because the B+Tree it refers to has collapsed
        /// a root node and decreased its depth.
        /// </summary>
        internal void DecreaseDepth()
        {
            // Delete the step for the old root node, in the path
            _steps[Depth--] = default;
        }

        /// <summary>
        /// Extend this path because the B+Tree it refers to has spawned
        /// a new root node and increased its depth.
        /// </summary>
        /// <param name="rootStep">Information on the step for the new root node.
        /// </param>
        internal void IncreaseDepth(BTreeStep rootStep)
        {
            int depth = Depth;
            var steps = _steps;
            if (steps.Length < depth + 2)
            {
                var newSteps = ArrayPool<BTreeStep>.Shared.Rent(depth + 4);
                var oldSteps = steps.AsSpan()[0..(depth + 1)];
                oldSteps.CopyTo(newSteps);
                oldSteps.Clear();
                _steps = newSteps;
                ArrayPool<BTreeStep>.Shared.Return(steps);
                steps = newSteps;
            }

            steps[++depth] = rootStep;
            Depth = depth;
        }

        /// <summary>
        /// The depth of the B+Tree.
        /// </summary>
        /// <remarks>
        /// The depth, as a number, is the number of layers
        /// in the B+Tree before the layer of leaf nodes.
        /// 0 means there is only the root node, or the B+Tree 
        /// is completely empty.  
        /// </remarks>
        internal int Depth { get; private set; }

        /// <summary>
        /// Version number to try to detect
        /// when this path has been invalidated by a change
        /// to the B+Tree.
        /// </summary>
        internal int Version { get; set; }

        /// <summary>
        /// Prepare to record a path through the B+Tree.
        /// </summary>
        /// <remarks>
        /// The array used to record the steps of the path is pooled.
        /// It is initially over-allocated so that, for a small number of
        /// insertions, updating the path does not require allocating
        /// a new array.  
        /// </remarks>
        internal BTreePath(int depth, int version)
        {
            Depth = depth;
            Version = version;
            _steps = ArrayPool<BTreeStep>.Shared.Rent(depth + 3);
        }

        /// <summary>
        /// Dispose of this structure, returning the array used to 
        /// record the path to the shared pool.
        /// </summary>
        public void Dispose()
        {
            var steps = _steps;
            this = default;

            if (steps != null)
            {
                steps.AsSpan()[0..(Depth + 1)].Clear();
                ArrayPool<BTreeStep>.Shared.Return(steps);
            }
        }
    }

    /// <summary>
    /// Describes one selection step when moving down the B+Tree in <see cref="BTreePath" />.
    /// </summary>
    [DebuggerDisplay("Index: {Index}")]
    internal struct BTreeStep
    {
        /// <summary>
        /// The B+Tree node present at the level of the B+Tree given by the 
        /// index of this instance within <see cref="BTreePath._steps" />.
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
