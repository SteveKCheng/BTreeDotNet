using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace BPlusTree
{
    public class BTreeMap<TKey, TValue> : BTree<TKey, TValue>
                                        , IDictionary<TKey, TValue>
                                        , IReadOnlyDictionary<TKey, TValue>
    {
        /// <inheritdoc cref="IDictionary{TKey, TValue}.Keys" />
        ICollection<TKey> IDictionary<TKey, TValue>.Keys => Keys;

        /// <inheritdoc cref="IDictionary{TKey, TValue}.Values" />
        ICollection<TValue> IDictionary<TKey, TValue>.Values => Values;

        /// <inheritdoc cref="IReadOnlyDictionary{TKey, TValue}.Keys" />
        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => Keys;

        /// <inheritdoc cref="IReadOnlyDictionary{TKey, TValue}.Values" />
        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => Values;

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

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
            => Add(item.Key, item.Value);

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            if (TryGetValue(item.Key, out var value))
                return EqualityComparer<TValue>.Default.Equals(item.Value, value);

            return false;
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            throw new NotImplementedException();
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

        public new Enumerator GetEnumerator(bool toBeginning) => base.GetEnumerator(toBeginning);

        public Enumerator GetEnumerator() => GetEnumerator(toBeginning: true);

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
            => base.GetEnumerator(toBeginning: true);

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Always false: signals this container can be modified.
        /// </summary>
        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;

        public KeysCollection Keys => new KeysCollection(this);

        public ValuesCollection Values => new ValuesCollection(this);

        /// <inheritdoc cref="ICollection{T}.CopyTo(T[], int)"/>
        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
            => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);

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
    }
}
