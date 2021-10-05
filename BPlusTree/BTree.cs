using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace BPlusTree
{
    /// <summary>
    /// Base class that implements a B+Tree held entirely in managed memory.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The B+Tree is a well-known generalization of binary search trees
    /// with a branching factor that may be greater than 2. 
    /// On modern computer architectures where sequentially memory accesses are faster
    /// than random access, B+Trees work better than binary search trees.  B+Tree also
    /// incur less overhead from inter-node links.
    /// </para>
    /// <para>
    /// This implementation tries hard to minimize object allocations, even at the
    /// expense of internal complexity.  In particular, nodes are simple arrays
    /// of key-value pairs.
    /// </para>
    /// <para>
    /// Addition and removal of entries take O(Depth) running time,
    /// where Depth is the depth of the B+Tree, which is approximately log_B(N) for N
    /// being the number of entries in the B+Tree and B being the branching factor (order).
    /// </para>
    /// </remarks>
    /// <typeparam name="TKey">The type of the look-up key. </typeparam>
    /// <typeparam name="TValue">The type of the data value associated to each 
    /// key. </typeparam>
    public partial class BTree<TKey, TValue>
    {
        /// <summary>
        /// The maximum branching factor (or "order") supported by
        /// this implementation.
        /// </summary>
        public static int MaxOrder => 1024;

        /// <summary>
        /// The branching factor of the B+Tree, or its "order".
        /// </summary>
        /// <remarks>
        /// This is the number of keys held in each node in the B+Tree.
        /// This implementation requires it to be even, and not exceed
        /// <see cref="MaxOrder" />.
        /// </remarks>
        public int Order { get; }

        /// <summary>
        /// A total ordering of keys which this B+Tree will follow.
        /// </summary>
        internal readonly IComparer<TKey> _keyComparer;

        /// <summary>
        /// The depth of the B+Tree.
        /// </summary>
        /// <remarks>
        /// The depth, as a number, is the number of layers
        /// in the B+Tree before the layer of leaf nodes.
        /// 0 means there is only the root node, or the B+Tree 
        /// is completely empty.  
        /// </remarks>
        public int Depth { get; private set; }

        /// <summary>
        /// The number of data items inside the B+Tree.
        /// </summary>
        public int Count { get; private set; }

        /// <summary>
        /// Points to the root node of the B+Tree.
        /// </summary>
        internal NodeLink _root;

        /// <summary>
        /// A counter incremented by one for every change to the B+Tree
        /// to try to detect iterator invalidation.
        /// </summary>
        internal int _version;

        /// <summary>
        /// Construct an empty B+Tree.
        /// </summary>
        /// <param name="order">The desired order of the B+Tree. 
        /// This must be a positive even number not greater than <see cref="MaxOrder" />.
        /// </param>
        /// <param name="keyComparer">
        /// An ordering used to arrange the look-up keys in the B+Tree.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="order"/> is invalid. </exception>
        /// <exception cref="ArgumentNullException"><paramref name="keyComparer"/> is null. </exception>
        internal BTree(int order, IComparer<TKey> keyComparer)
        {
            if (order < 0 || (order & 1) != 0)
                throw new ArgumentOutOfRangeException(nameof(order), "The order of the B+Tree must be a positive even number. ");
            if (order > MaxOrder)
                throw new ArgumentOutOfRangeException(nameof(order), $"The order of the B+Tree may not exceed {MaxOrder}. ");

            _keyComparer = keyComparer ?? throw new ArgumentNullException(nameof(keyComparer));
            Order = order;

            // Always create an empty root node so we do not have to
            // check for the root node being null everywhere.
            _root = new NodeLink(new Entry<TKey, TValue>[order], 0);
        }

        /// <summary>
        /// Search for where a key could be found or inserted in the B+Tree,
        /// and record the path to get there.
        /// </summary>
        /// <param name="key">The key to look for. </param>
        /// <param name="forUpperBound">Whether to return the "lower bound"
        /// or "upper bound" index.  See <see cref="BTreeBase{TKey}.BTreeCore.SearchKeyWithinNode{TValue}" />.
        /// </param>
        /// <param name="path">
        /// On successful return, this method records the path to follow here.
        /// </param>
        /// <returns>
        /// Whether the exact key has been found in the B+Tree.
        /// </returns>
        internal bool FindKey(TKey key, bool forUpperBound, ref BTreePath path)
        {
            var currentLink = _root;

            int depth = Depth;
            int index;

            for (int level = 0; level < depth; ++level)
            {
                var internalNode = BTreeCore.AsInteriorNode<TKey>(currentLink.Child!);
                index = BTreeCore.SearchKeyWithinNode(_keyComparer, key, forUpperBound, 
                                                      internalNode, currentLink.EntriesCount,
                                                      out _);

                path[level] = new BTreeStep(internalNode, index);
                currentLink = internalNode[index].Value;
            }

            var leafNode = AsLeafNode(currentLink.Child!);
            index = BTreeCore.SearchKeyWithinNode(_keyComparer, key, forUpperBound, 
                                                  leafNode, currentLink.EntriesCount,
                                                  out bool found);

            path.Leaf = new BTreeStep(leafNode, index);
            return found;
        }
        
        /// <summary>
        /// Cast an object reference as a leaf node.
        /// </summary>
        internal static Entry<TKey, TValue>[] AsLeafNode(object node)
            => (Entry<TKey, TValue>[])node;

        /// <summary>
        /// Get a reference to the variable that stores the number of non-empty
        /// entries in a node.
        /// </summary>
        /// <remarks>
        /// Because nodes are represented as .NET arrays, to avoid an extra
        /// allocation of a class, the count of the number of entries cannot
        /// be stored with the node but as part of the <see cref="NodeLink"/>
        /// from the parent node.  This function retrieves the reference
        /// to that count.
        /// </remarks>
        /// <param name="path">Path to the desired node from the root. </param>
        /// <param name="level">The level of the desired node in the path. </param>
        private ref int GetNodeEntriesCount(ref BTreePath path, int level)
        {
            if (level > 0)
            {
                ref var parentStep = ref path[level -1];
                var parentNode = BTreeCore.AsInteriorNode<TKey>(parentStep.Node!);
                return ref parentNode[parentStep.Index].Value.EntriesCount;
            }
            else
            {
                return ref _root.EntriesCount;
            }
        }

        /// <summary>
        /// Create a new instance of the structure used to record a path
        /// through the B+Tree.
        /// </summary>
        /// <remarks>
        /// The array used to record the path is pooled.
        /// </remarks>
        internal BTreePath NewPath()
        {
            var steps = ArrayPool<BTreeStep>.Shared.Rent(Depth + 1);
            return new BTreePath(steps, Depth, _version);
        }

        /// <summary>
        /// Find the first entry with the given key.
        /// </summary>
        /// <param name="key">The desired key. </param>
        /// <returns>
        /// Reference to the entry in a leaf node that has the desired key,
        /// or null if it does not exist.
        /// </returns>
        internal ref Entry<TKey, TValue> FindEntry(ref BTreePath path, TKey key)
        {
            if (FindKey(key, false, ref path))
            {
                ref var step = ref path.Leaf;
                return ref AsLeafNode(step.Node!)[step.Index];
            }

            return ref Unsafe.NullRef<Entry<TKey, TValue>>();
        }

        /// <summary>
        /// Returns whether an entry with the given key exists in this B+Tree.
        /// </summary>
        /// <param name="key">The key to look for. </param>
        /// <returns>
        /// Whether the entry keyed with <paramref name="key"/> exists in this B+Tree.
        /// </returns>
        internal bool ContainsKey(TKey key)
        {
            var path = NewPath();
            try
            {
                return FindKey(key, false, ref path);
            }
            finally
            {
                path.Dispose();
            }
        }

        /// <summary>
        /// Remove all entries from this B+Tree.
        /// </summary>
        public void Clear()
        {
            ++_version;
            Count = 0;
            _root.EntriesCount = 0;

            if (Depth == 0)
            {
                AsLeafNode(_root.Child!).AsSpan().Clear();
            }
            else
            {
                Depth = 0;
                _root.Child = new Entry<TKey, TValue>[Order];
            }
        }
    }
}
