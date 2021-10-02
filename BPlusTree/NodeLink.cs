using System;
using System.Collections.Generic;

namespace BPlusTree
{
    /// <summary>
    /// The data associated with a key in an interior node
    /// in <see cref="BTree{TKey, TValue}"/>. 
    /// </summary>
    /// <remarks>
    /// This structure should always be passed by reference to allow
    /// in-place updating of <see cref="EntriesCount" />.
    /// </remarks>
    internal struct NodeLink
    {
        /// <summary>
        /// Link to a child node, of the interior node that contains this instance.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This node is to be followed when the key being sought for compares
        /// greater (and not equal) to the key associated to this value.
        /// </para>
        /// <para>
        /// The child node may be an interior node or leaf node.  The reader
        /// of this structure has to decide based on the current level of the
        /// B+Tree being processed, and cast appropriately.
        /// </para>
        /// </remarks>
        public object? Child;

        /// <summary>
        /// The number of active (non-blank) entries in the child node.
        /// </summary>
        /// <remarks>
        /// For interior nodes, this count is biased by one, because the
        /// first entry in an interior node records the left-most child
        /// and always has a blank key.  In other words this count refers
        /// to the number of entries, not the number of keys which is always
        /// one less.
        /// </remarks>
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
}
