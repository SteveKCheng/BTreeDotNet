// © 2001 Steve Cheng.
// See LICENSE.txt for copyright licensing of this work.

using System;
using System.Collections;
using System.Collections.Generic;

namespace GoldSaucer.BTree
{
    /// <summary>
    /// Read-only sequence of the values in a B+Tree, presented in the
    /// same order as their corresponding keys.
    /// </summary>
    /// <typeparam name="TKey">The type of key in the B+Tree. </typeparam>
    /// <typeparam name="TValue">The type of value associated to a key in the B+Tree. </typeparam>
    public struct BTreeValuesCollection<TKey, TValue> : ICollection<TValue>, IReadOnlyCollection<TValue>
    {
        internal BTree<TKey, TValue> Owner { get; }

        internal BTreeValuesCollection(BTree<TKey, TValue> owner) => Owner = owner;

        /// <inheritdoc cref="ICollection{T}.Count" />
        public int Count => Owner.Count;

        /// <inheritdoc cref="ICollection{T}.Contains" />
        public bool Contains(TKey item) => Owner.ContainsKey(item);

        /// <inheritdoc cref="IEnumerable{T}.GetEnumerator" />
        public IEnumerator<TValue> GetEnumerator() => new Enumerator(Owner);

        /// <inheritdoc cref="IEnumerable.GetEnumerator" />
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <inheritdoc cref="ICollection{T}.IsReadOnly" />
        bool ICollection<TValue>.IsReadOnly => true;

        /// <inheritdoc cref="ICollection{T}.Add" />
        void ICollection<TValue>.Add(TValue item)
            => throw new NotSupportedException("Values cannot be added to the B+Tree through this interface. ");

        /// <inheritdoc cref="ICollection{T}.Remove" />
        bool ICollection<TValue>.Remove(TValue item)
            => throw new NotSupportedException("Values cannot be removed from the B+Tree through this interface. ");

        /// <inheritdoc cref="ICollection{T}.Clear" />
        void ICollection<TValue>.Clear()
            => throw new NotSupportedException("Values cannot be cleared from the B+Tree through this interface. ");

        /// <inheritdoc cref="ICollection{T}.CopyTo" />
        void ICollection<TValue>.CopyTo(TValue[] array, int arrayIndex)
            => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);

        /// <inheritdoc cref="ICollection{T}.Contains" />
        bool ICollection<TValue>.Contains(TValue item)
        {
            var comparer = EqualityComparer<TValue>.Default;
            var enumerator = GetEnumerator();
            try
            {
                while (enumerator.MoveNext())
                {
                    if (comparer.Equals(enumerator.Current, item))
                        return true;
                }

                return false;
            }
            finally
            {
                enumerator.Dispose();
            }
        }

        /// <summary>
        /// Enumerates the values from <see cref="BTreeValuesCollection{TKey, TValue}" />.
        /// </summary>
        public struct Enumerator : IEnumerator<TValue>
        {
            /// <summary>
            /// Enumerates the items from the B+Tree which get projected to their values.
            /// </summary>
            private BTreeEnumerator<TKey, TValue> _itemsEnumerator;

            internal Enumerator(BTree<TKey, TValue> owner)
            {
                _itemsEnumerator = new BTreeEnumerator<TKey, TValue>(owner, toBeginning: true);
            }

            /// <inheritdoc cref="IEnumerator{T}.Current" />
            public TValue Current => _itemsEnumerator.Current.Value;

            /// <inheritdoc cref="IEnumerator.Current" />
            object IEnumerator.Current => Current!;

            /// <inheritdoc cref="IDisposable.Dispose" />
            public void Dispose() => _itemsEnumerator.Dispose();

            /// <inheritdoc cref="IEnumerator.MoveNext" />
            public bool MoveNext() => _itemsEnumerator.MoveNext();

            /// <inheritdoc cref="IEnumerator.Reset" />
            public void Reset() => _itemsEnumerator.Reset();
        }
    }
}
