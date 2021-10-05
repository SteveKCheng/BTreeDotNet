using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace BPlusTree
{
    /// <summary>
    /// A dictionary with ordered keys implemented by a B+Tree in managed memory. 
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
    /// Keys are ordered according to <see cref="KeyComparer" />.  There may not be 
    /// duplicate keys.
    /// </para>
    /// </remarks>
    /// <typeparam name="TKey">The type of the look-up key. </typeparam>
    /// <typeparam name="TValue">The type of the data value associated to each 
    /// key. </typeparam>
    public class BTreeMap<TKey, TValue> : BTree<TKey, TValue>
                                        , IDictionary<TKey, TValue>
                                        , IReadOnlyDictionary<TKey, TValue>
    {
        /// <summary>
        /// A total ordering of keys which this B+Tree will follow.
        /// </summary>
        /// <remarks>
        /// The results from <see cref="IComparer{T}.Compare" /> 
        /// must be consistent with a total ordering; if not, this B+Tree will
        /// not work correctly.  Keys are considered equal when 
        /// <see cref="IComparer{T}.Compare" /> returns zero, so if there
        /// are equivalence classes of keys, at most one member from each equivalence
        /// class can be stored as a key in the B+Tree.
        /// </remarks>
        public new IComparer<TKey> KeyComparer => base.KeyComparer;

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
        public BTreeMap(int order, IComparer<TKey> keyComparer)
            : base(order, keyComparer)
        {
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
                if (TryGetValue(key, out var value))
                    return value;

                throw new KeyNotFoundException($"The key {key} is not found in this B+Tree. ");
            }

            set
            {
                var path = NewPath();
                try
                {
                    ref var entry = ref FindEntry(ref path, key);
                    if (Unsafe.IsNullRef(ref entry))
                        Insert(key, value, ref path);
                    else
                        entry.Value = value;
                }
                finally
                {
                    path.Dispose();
                }
            }
        }

        /// <summary>
        /// Retrieve the value of the entry with the given key, if it exists.
        /// </summary>
        /// <param name="key">The key of the desired entry. </param>
        /// <param name="value">On return, this variable is 
        /// set to the value associated to the key
        /// on return if the entry exists, otherwise the default value.
        /// </param>
        /// <returns>
        /// Whether the entry keyed with <paramref name="key"/> exists in this B+Tree.
        /// </returns>
        public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value)
        {
            var path = NewPath();
            try
            {
                ref var entry = ref FindEntry(ref path, key);
                if (Unsafe.IsNullRef(ref entry))
                {
                    value = default;
                    return false;
                }

                value = entry.Value;
                return true;
            }
            finally
            {
                path.Dispose();
            }
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
                if (FindKey(key, false, ref path))
                    return false;

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

        /// <summary>
        /// Add an item to the B+Tree whose key is not already present.
        /// </summary>
        /// <param name="item">The key and value of the item to add. </param>
        public void Add(KeyValuePair<TKey, TValue> item) => Add(item.Key, item.Value);

        /// <summary>
        /// Returns whether an entry with the given key exists in this B+Tree.
        /// </summary>
        /// <param name="key">The key to look for. </param>
        /// <returns>
        /// Whether the entry keyed with <paramref name="key"/> exists in this B+Tree.
        /// </returns>
        public new bool ContainsKey(TKey key) => base.ContainsKey(key);

        /// <summary>
        /// Returns whether an entry with the given key exists
        /// and has the specified value.
        /// </summary>
        /// <param name="item">The key and value to look for.  The key
        /// is compared with <see cref="KeyComparer" /> while the value
        /// is compared with <see cref="EqualityComparer{TValue}" />.
        /// </param>
        /// <returns>
        /// Whether the entry with the specified key and value exists.
        /// </returns>
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            if (TryGetValue(item.Key, out var value))
                return EqualityComparer<TValue>.Default.Equals(item.Value, value);

            return false;
        }

        /// <summary>
        /// Remove the entry with the given key, if it exists.
        /// </summary>
        /// <param name="key">The key of the entry to remove. </param>
        /// <returns>Whether the entry with the key existed (and has been removed). </returns>
        public bool Remove(TKey key)
        {
            var path = NewPath();
            try
            {
                if (FindKey(key, false, ref path))
                {
                    DeleteAtPath(ref path);
                    return true;
                }

                return false;
            }
            finally
            {
                path.Dispose();
            }
        }

        /// <summary>
        /// Delete an entry that matches the given key and value.
        /// </summary>
        /// <param name="item">The key and value to look for.  The key
        /// is compared with <see cref="KeyComparer" /> while the value
        /// is compared with <see cref="EqualityComparer{TValue}" />.
        /// </param>
        /// <returns>
        /// Whether the entry with the specified key and value exists
        /// (and has been removed).
        /// </returns>
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            var path = NewPath();
            try
            {
                ref var entry = ref FindEntry(ref path, item.Key);
                if (!Unsafe.IsNullRef(ref entry) && 
                    EqualityComparer<TValue>.Default.Equals(item.Value, entry.Value))
                {
                    DeleteAtPath(ref path);
                    return true;
                }

                return false;
            }
            finally
            {
                path.Dispose();
            }
        }

        /// <summary>
        /// Prepare to enumerate the entries in this B+Tree, in
        /// forwards or backwards order according to the ordering
        /// by <see cref="KeyComparer" />.
        /// </summary>
        /// <param name="toBeginning">
        /// If true, the enumerator is positioned at the start of all entries.
        /// If false, the enumerator is positioned at the end of all entries.
        /// </param>
        public new Enumerator GetEnumerator(bool toBeginning) => base.GetEnumerator(toBeginning);

        /// <summary>
        /// Get the sequence of entries in this B+Tree, 
        /// presented sorted by key according to <see cref="KeyComparer" />.
        /// </summary>
        public Enumerator GetEnumerator() => GetEnumerator(toBeginning: true);

        /// <inheritdoc cref="IEnumerable{T}.GetEnumerator" />
        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
            => base.GetEnumerator(toBeginning: true);

        /// <inheritdoc cref="IEnumerable.GetEnumerator" />
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Get the read-only sequence of all keys currently in this B+Tree, 
        /// presented in the order specified by <see cref="KeyComparer" />.
        /// </summary>
        public KeysCollection Keys => new KeysCollection(this);

        /// <summary>
        /// Get all values in this B+Tree, presented in the same sequence
        /// as their corresponding keys from <see cref="Keys" />.
        public ValuesCollection Values => new ValuesCollection(this);

        /// <inheritdoc cref="IDictionary{TKey, TValue}.Keys" />
        ICollection<TKey> IDictionary<TKey, TValue>.Keys => Keys;

        /// <inheritdoc cref="IDictionary{TKey, TValue}.Values" />
        ICollection<TValue> IDictionary<TKey, TValue>.Values => Values;

        /// <inheritdoc cref="IReadOnlyDictionary{TKey, TValue}.Keys" />
        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => Keys;

        /// <inheritdoc cref="IReadOnlyDictionary{TKey, TValue}.Values" />
        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => Values;

        /// <summary>
        /// Always false: signals this container can be modified.
        /// </summary>
        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;

        /// <inheritdoc cref="ICollection{T}.CopyTo(T[], int)" />
        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
            => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);
    }
}
