using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

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

    internal struct BTreeStep
    {
        public object? Node;
        public int Index;

        public BTreeStep(object node, int index)
        {
            Node = node;
            Index = index;
        }
    }

    /// <summary>
    /// The data associated with a key in an internal node
    /// in <see cref="BTree{TKey, TValue}"/>. 
    /// </summary>
    internal struct NodeLink
    {
        /// <summary>
        /// Link to a child node of the internal node that contains this value.
        /// </summary>
        /// <remarks>
        /// This node is to be followed when the key being sought for compares
        /// greater (and not equal) to the key associated to this value.
        /// </remarks>
        public object? Child;

        public int EntriesCount;

        public NodeLink(object child, int count)
        {
            Child = child;
            EntriesCount = count;
        }
    }

    /// <summary>
    /// An entry within a node in <see cref="BTree{TKey, TValue}"/>.
    /// </summary>
    /// <remarks>
    /// Semantically this structure is no different than
    /// <see cref="KeyValuePair{TKey, TValue}" />, but the key
    /// and value are defined as public fields rather than properties,
    /// so the implementation of <see cref="BTree{TKey, TValue}" />
    /// can take references to them.  For internal nodes,
    /// <typeparamref name="TValue" /> is <see cref="NodeLink" />.
    /// </remarks>
    internal struct Entry<TKey, TValue>
    {
        /// <summary>
        /// The key to this entry in the node of the B+Tree. 
        /// </summary>
        /// <remarks>
        /// For internal nodes, the node being linked to from
        /// <see cref="Value" /> has keys which compare greater
        /// than (and not equal to) this key.
        /// </remarks>
        public TKey Key;

        /// <summary>
        /// The value or data item associated with the key.
        /// </summary>
        public TValue Value;

        public Entry(TKey key, TValue value)
        {
            Key = key;
            Value = value;
        }
    }

    /// <summary>
    /// Implementation logic for <see cref="BTree{TKey, TValue}"/>
    /// that is independent of the type of value.
    /// </summary>
    /// <typeparam name="TKey">The type of look-up key in the B+Tree. </typeparam>
    public partial class BTreeBase<TKey>
    {
        /// <summary>
        /// A partial ordering of keys that can be evaluated for any two keys.
        /// </summary>
        public IComparer<TKey> KeyComparer { get; }

        internal BTreeBase(int order,
                           IComparer<TKey> keyComparer)
        {
            if (order < 0 || (order & 1) != 0)
                throw new ArgumentOutOfRangeException(nameof(order), "The order of the B+Tree must be a positive even number. ");
            if (order > 32768)
                throw new ArgumentOutOfRangeException(nameof(order), "The order of the B+Tree may not exceed 32768. ");

            KeyComparer = keyComparer ?? throw new ArgumentNullException(nameof(keyComparer));
        }

        /// <summary>
        /// The branching factor of the B+Tree, or its "order".
        /// </summary>
        /// <remarks>
        /// This is the number of keys held in each node.  
        /// This implementation requires it to be even.
        /// </remarks>
        public int Order { get; }

        /// <summary>
        /// Points to the root node of the B+Tree.
        /// </summary>
        internal NodeLink _root;

        /// <summary>
        /// Find the index within a node where an entry can be inserted for the given key.
        /// </summary>
        /// <typeparam name="TValue">The type of data value to insert along with the key. 
        /// Must be <see cref="NodeLink"/> for internal nodes.
        /// </typeparam>
        /// <param name="key">The key to search for. </param>
        /// <param name="forUpperBound">If false, this method returns the "lower bound" index.
        /// If true, this method returns the "upper bound" index.
        /// </param>
        /// <param name="entries">The entries in the node. </param>
        /// <param name="numEntries">The number of active entries in the node. </param>
        /// <returns>
        /// For "lower bound": the first index where an entry with the given key, 
        /// or a preceding key, can be inserted without violating ordering.  For 
        /// "upper bound": the first index where an entry with a following (greater) key 
        /// can be inserted without violating ordering. For internal nodes, the index is 
        /// shifted down by one, so it starts from 0 while the keys are stored starting 
        /// at index 1: thus a "lower bound" search on internal nodes yields the index to 
        /// follow down the B+Tree.
        /// </returns>
        internal int SearchKeyWithinNode<TValue>(TKey key, 
                                                 bool forUpperBound, 
                                                 Entry<TKey, TValue>[] entries, 
                                                 int numEntries)
        {
            // Keys in internal nodes are stored starting from index 1,
            // but we still return 0-based index
            int shift = (typeof(TValue) == typeof(NodeLink)) ? 1 : 0;

            // The closed interval [left,right] brackets the returned index
            int left = shift;
            int right = numEntries;

            // Bisect until the interval brackets only one choice of index
            while (left != right)
            {
                // B+Tree order is capped so this index calculation cannot overflow
                int mid = (left + right) >> 1;

                var comparison = KeyComparer.Compare(entries[mid].Key, key);
                if (comparison < 0 || (forUpperBound && (comparison == 0)))
                    left = mid + 1;
                else
                    right = mid;
            }

            return left - shift;
        }

        /// <summary>
        /// Cast an object reference as an interior (non-leaf) node of the B+Tree.
        /// </summary>
        internal static Entry<TKey, NodeLink>[] AsInteriorNode(object node)
            => (Entry<TKey, NodeLink>[])node;
    }

    /// <summary>
    /// A B+Tree held entirely in managed memory.
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
    /// Items with duplicate keys may be placed in the B+Tree.
    /// </para>
    /// </remarks>
    /// <typeparam name="TKey">The type of the look-up key. </typeparam>
    /// <typeparam name="TValue">The type of the data value associated to each 
    /// key. </typeparam>
    public partial class BTree<TKey, TValue> : BTreeBase<TKey>
    {
        /// <summary>
        /// Construct an empty B+Tree.
        /// </summary>
        /// <param name="order">The desired order of the B+Tree. 
        /// </param>
        /// <param name="keyComparer">
        /// An ordering used to arrange the look-up keys in the B+Tree.
        /// </param>
        public BTree(int order, IComparer<TKey> keyComparer)
            : base(order, keyComparer)
        {
            // Always create an empty root node so we do not have to
            // check for the root node being null everywhere.
            _root = new NodeLink(new Entry<TKey, TValue>[order], 0);
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
        public int Depth { get; private set; }

        /// <summary>
        /// The number of data items inside the B+Tree.
        /// </summary>
        public int Count { get; private set; }

        /// <summary>
        /// Search for where a key could be found or inserted in the B+Tree,
        /// and record the path to get there.
        /// </summary>
        /// <param name="key">The key to look for. </param>
        /// <param name="forUpperBound">Whether to return the "lower bound"
        /// or "upper bound" index.  See <see cref="BTreeBase{TKey}.SearchKeyWithinNode{TValue}" />.
        /// </param>
        /// <param name="path">
        /// On successful return, this method records the path to follow here.
        /// </param>
        private void FindKey(TKey key, bool forUpperBound, ref BTreePath path)
        {
            var currentLink = _root;

            int depth = Depth;
            int index;

            for (int level = 0; level < depth; ++level)
            {
                var internalNode = AsInteriorNode(currentLink.Child!);
                index = SearchKeyWithinNode(key, forUpperBound, internalNode, currentLink.EntriesCount);

                path.Steps[level] = new BTreeStep(internalNode, index);
                currentLink = internalNode[index].Value;
            }

            var leafNode = AsLeafNode(currentLink.Child!);
            index = SearchKeyWithinNode(key, forUpperBound, leafNode, currentLink.EntriesCount);

            path.Steps[depth] = new BTreeStep(leafNode, index);
        }
        
        /// <summary>
        /// Cast an object reference as a leaf node.
        /// </summary>
        private static Entry<TKey, TValue>[] AsLeafNode(object node)
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
                ref var parentStep = ref path.Steps[level - 1];
                var parentNode = AsInteriorNode(parentStep.Node!);
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
        private BTreePath NewPath()
        {
            var steps = ArrayPool<BTreeStep>.Shared.Rent(Depth + 1);
            return new BTreePath(this, steps, Depth);
        }

        /// <summary>
        /// Find the first entry with the given key.
        /// </summary>
        /// <param name="key">The desired key. </param>
        /// <returns>
        /// Reference to the entry in a leaf node that has the desired key,
        /// or null if it does not exist.
        /// </returns>
        private ref Entry<TKey, TValue> FindEntry(ref BTreePath path, TKey key)
        {
            FindKey(key, false, ref path);
            int numEntries = GetNodeEntriesCount(ref path, path.Depth);
            ref var step = ref path.Steps[path.Depth];
            if (step.Index < numEntries)
            {
                var leafNode = AsLeafNode(step.Node!);
                ref var entry = ref leafNode[step.Index];
                if (KeyComparer.Compare(entry.Key, key) == 0)
                    return ref entry;
            }

            return ref Unsafe.NullRef<Entry<TKey, TValue>>();
        }

        /// <summary>
        /// Get or set a data item associated with a key.
        /// </summary>
        /// <param name="key">The look-up key. </param>
        /// <returns>The data value associated with the look-up key. </returns>
        public TValue this[TKey key]
        {
            get
            {
                var path = NewPath();
                try
                {
                    ref var entry = ref FindEntry(ref path, key);
                    if (Unsafe.IsNullRef(ref entry))
                        throw new KeyNotFoundException($"The key {key} is not found in the B+Tree. ");

                    return entry.Value;
                }
                finally
                {
                    path.Dispose();
                }
            }

            set
            {
                var path = NewPath();
                try
                {
                    ref var entry = ref FindEntry(ref path, key);
                    if (Unsafe.IsNullRef(ref entry))
                    {
                        Insert(key, value, ref path);
                        return;
                    }

                    entry.Value = value;
                }
                finally
                {
                    path.Dispose();
                }
            }
        }
    }
}
