// © 2021 Steve Cheng.
// See LICENSE.txt for copyright licensing of this work.

using System;
using System.Collections;
using System.Collections.Generic;

namespace GoldSaucer.BTree
{
    /// <summary>
    /// Read-only sequence of the keys in a B+Tree, presented in their
    /// natural order.
    /// </summary>
    /// <typeparam name="TKey">The type of key in the B+Tree. </typeparam>
    /// <typeparam name="TValue">The type of value associated to a key in the B+Tree. </typeparam>
    public struct BTreeKeysCollection<TKey, TValue> : ICollection<TKey>, IReadOnlyCollection<TKey>
    {
        internal BTree<TKey, TValue> Owner { get; }

        internal BTreeKeysCollection(BTree<TKey, TValue> owner) => Owner = owner;

        /// <inheritdoc cref="ICollection{T}.Count" />
        public int Count => Owner.Count;

        /// <inheritdoc cref="ICollection{T}.Contains" />
        public bool Contains(TKey item) => Owner.ContainsKey(item);

        /// <inheritdoc cref="IEnumerable{T}.GetEnumerator" />
        public IEnumerator<TKey> GetEnumerator() => new Enumerator(Owner);

        /// <inheritdoc cref="IEnumerable.GetEnumerator" />
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <inheritdoc cref="ICollection{T}.IsReadOnly" />
        bool ICollection<TKey>.IsReadOnly => true;

        /// <inheritdoc cref="ICollection{T}.Add" />
        void ICollection<TKey>.Add(TKey item)
            => throw new NotSupportedException("Keys cannot be added to the B+Tree through this interface. ");

        /// <inheritdoc cref="ICollection{T}.Remove" />
        bool ICollection<TKey>.Remove(TKey item)
            => throw new NotSupportedException("Keys cannot be removed from the B+Tree through this interface. ");

        /// <inheritdoc cref="ICollection{T}.Clear" />
        void ICollection<TKey>.Clear()
            => throw new NotSupportedException("Keys cannot be cleared from the B+Tree through this interface. ");

        /// <inheritdoc cref="ICollection{T}.CopyTo" />
        void ICollection<TKey>.CopyTo(TKey[] array, int arrayIndex)
            => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);

        /// <summary>
        /// Enumerates the keys from <see cref="BTreeKeysCollection{TKey, TValue}" />.
        /// </summary>
        public struct Enumerator : IEnumerator<TKey>
        {
            /// <summary>
            /// Enumerates the items from the B+Tree which get projected to their keys.
            /// </summary>
            private BTreeEnumerator<TKey, TValue> _itemsEnumerator;

            internal Enumerator(BTree<TKey, TValue> owner)
            {
                _itemsEnumerator = new BTreeEnumerator<TKey, TValue>(owner, toBeginning: true);
            }

            /// <inheritdoc cref="IEnumerator{T}.Current" />
            public TKey Current => _itemsEnumerator.Current.Key;

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
