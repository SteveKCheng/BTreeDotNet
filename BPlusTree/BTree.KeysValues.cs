﻿using System;
using System.Collections;
using System.Collections.Generic;

namespace BPlusTree
{
    public partial class BTree<TKey, TValue>
    {
        public struct KeysCollection : ICollection<TKey>
        {
            public BTree<TKey, TValue> Owner { get; }

            public KeysCollection(BTree<TKey, TValue> owner) => Owner = owner;

            public int Count => Owner.Count;

            public bool Contains(TKey item) => Owner.ContainsKey(item);

            public IEnumerator<TKey> GetEnumerator() => new KeysEnumerator(Owner);

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            bool ICollection<TKey>.IsReadOnly => true;

            void ICollection<TKey>.Add(TKey item)
                => throw new NotSupportedException("Keys cannot be added to the B+Tree through this interface. ");

            bool ICollection<TKey>.Remove(TKey item)
                => throw new NotSupportedException("Keys cannot be removed from the B+Tree through this interface. ");

            void ICollection<TKey>.Clear()
                => throw new NotSupportedException("Keys cannot be cleared from the B+Tree through this interface. ");

            void ICollection<TKey>.CopyTo(TKey[] array, int arrayIndex)
                => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);
        }

        public struct KeysEnumerator : IEnumerator<TKey>
        {
            private BTreeEnumerator<TKey, TValue> _itemsEnumerator;

            public KeysEnumerator(BTree<TKey, TValue> owner)
            {
                _itemsEnumerator = new BTreeEnumerator<TKey, TValue>(owner, toBeginning: true);
            }

            public TKey Current => _itemsEnumerator.Current.Key;

            object IEnumerator.Current => Current!;

            public void Dispose() => _itemsEnumerator.Dispose();

            public bool MoveNext() => _itemsEnumerator.MoveNext();

            public void Reset() => _itemsEnumerator.Reset();
        }

        public struct ValuesCollection : ICollection<TValue>
        {
            public BTree<TKey, TValue> Owner { get; }

            public ValuesCollection(BTree<TKey, TValue> owner) => Owner = owner;

            public int Count => Owner.Count;

            public IEnumerator<TValue> GetEnumerator() => new ValuesEnumerator(Owner);

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

            bool ICollection<TValue>.IsReadOnly => true;

            void ICollection<TValue>.Add(TValue item)
                => throw new NotSupportedException("Values cannot be added to the B+Tree through this interface. ");

            bool ICollection<TValue>.Remove(TValue item)
                => throw new NotSupportedException("Values cannot be removed from the B+Tree through this interface. ");

            void ICollection<TValue>.Clear()
                => throw new NotSupportedException("Values cannot be cleared from the B+Tree through this interface. ");

            void ICollection<TValue>.CopyTo(TValue[] array, int arrayIndex)
                => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);

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
        }

        public struct ValuesEnumerator : IEnumerator<TValue>
        {
            private BTreeEnumerator<TKey, TValue> _itemsEnumerator;

            public ValuesEnumerator(BTree<TKey, TValue> owner)
            {
                _itemsEnumerator = new BTreeEnumerator<TKey, TValue>(owner, toBeginning: true);
            }

            public TValue Current => _itemsEnumerator.Current.Value;

            object IEnumerator.Current => Current!;

            public void Dispose() => _itemsEnumerator.Dispose();

            public bool MoveNext() => _itemsEnumerator.MoveNext();

            public void Reset() => _itemsEnumerator.Reset();
        }
    }
}
