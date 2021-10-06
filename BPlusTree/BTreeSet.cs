// © 2001 Steve Cheng.
// See LICENSE.txt for copyright licensing of this work.

using System;
using System.Collections;
using System.Collections.Generic;

namespace BPlusTree
{
    public struct VoidValue { }

    public class BTreeSet<T> : BTree<T, VoidValue>
                             , ISet<T>
                             , IReadOnlySet<T>
    {
        public BTreeSet(int order, IComparer<T> comparer)
            : base(order, comparer)
        {
        }

        public IComparer<T> Comparer => base._keyComparer;

        bool ICollection<T>.IsReadOnly => false;

        public bool Add(T item)
        {
            var path = NewPath();
            try
            {
                if (FindKey(item, false, ref path))
                    return false;

                InsertAtPath(item, default, ref path);
                return true;
            }
            finally
            {
                path.Dispose();
            }
        }
        void ICollection<T>.Add(T item) => Add(item);

        public bool Remove(T item) => DeleteByKey(item);

        public bool Contains(T item)
        {
            var path = NewPath();
            try
            {
                return FindKey(item, false, ref path);
            }
            finally
            {
                path.Dispose();
            }
        }

        public void CopyTo(T[] array, int arrayIndex)
            => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);

        public struct Enumerator : IEnumerator<T>
        {
            private BTreeEnumerator<T, VoidValue> _itemsEnumerator;

            internal Enumerator(BTreeSet<T> owner) 
            {
                _itemsEnumerator = new BTreeEnumerator<T, VoidValue>(owner, true);
            }

            /// <inheritdoc cref="IEnumerator{T}.Current" />
            public T Current => _itemsEnumerator.Current.Key;

            /// <inheritdoc cref="IEnumerator.Current" />
            object IEnumerator.Current => Current!;

            /// <inheritdoc cref="IDisposable.Dispose" />
            public void Dispose() => _itemsEnumerator.Dispose();

            /// <inheritdoc cref="IEnumerator.MoveNext" />
            public bool MoveNext() => _itemsEnumerator.MoveNext();

            /// <inheritdoc cref="IEnumerator.Reset" />
            public void Reset() => _itemsEnumerator.Reset();

        }

        public Enumerator GetEnumerator() => new Enumerator(this);

        IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void ExceptWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public void IntersectWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public void UnionWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool IsSupersetOf(IEnumerable<T> other)
        {
            foreach (var item in other)
            {
                if (!Contains(item))
                    return false;
            }

            return true;
        }

        public bool Overlaps(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        public bool SetEquals(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }
    }
}
