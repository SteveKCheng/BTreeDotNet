// © 2001 Steve Cheng.
// See LICENSE.txt for copyright licensing of this work.

using System;
using System.Collections;
using System.Collections.Generic;

namespace GoldSaucer.BTree
{
    /// <summary>
    /// Dummy type used as the type of value to adapt associative containers 
    /// into implementations of sets.
    /// </summary>
    public struct VoidValue { }

    /// <summary>
    /// An ordered set of objects, implemented by a B+Tree in managed memory. 
    /// </summary>
    /// <typeparam name="T">The type of data item to store in the set. </typeparam>
    public class BTreeSet<T> : BTree<T, VoidValue>
                             , ISet<T>
                             , IReadOnlySet<T>
    {
        /// <summary>
        /// Prepare an initially empty set.
        /// </summary>
        /// <param name="order">The desired order of the B+Tree. 
        /// This must be a positive even number not greater than <see cref="MaxOrder" />.
        /// </param>
        /// <param name="comparer">
        /// An ordering to arrange the members of the ordered set.
        /// </param>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="order"/> is invalid. </exception>
        /// <exception cref="ArgumentNullException"><paramref name="comparer"/> is null. </exception>
        public BTreeSet(int order, IComparer<T> comparer)
            : base(order, comparer)
        {
        }

        /// <summary>
        /// A total ordering of all items that can be inserted into this container.
        /// </summary>
        /// <remarks>
        /// The results from <see cref="IComparer{T}.Compare" /> 
        /// must be consistent with a total ordering; if not, the B+Tree implementation
        /// will not work correctly.  Members of the set are considered equal when 
        /// <see cref="IComparer{T}.Compare" /> returns zero, so if there
        /// are equivalence classes of keys, at most one member from each equivalence
        /// class can be stored in this set.
        /// </remarks>
        public IComparer<T> Comparer => base._keyComparer;

        bool ICollection<T>.IsReadOnly => false;

        /// <summary>
        /// Add a member to this set unless it is already in.
        /// </summary>
        /// <param name="item">The member to add to the set. </param>
        /// <returns>True if the member has been freshly added; false if it already existed.
        /// </returns>
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

        /// <summary>
        /// Remove an item from this set if it is a member.
        /// </summary>
        /// <param name="item">The item to remove from the set. </param>
        /// <returns>True if the item existed as a member and has been removed; false otherwise.
        /// </returns>
        public bool Remove(T item) => DeleteByKey(item);

        /// <summary>
        /// Returns whether an item exists in this set.
        /// </summary>
        /// <param name="item">The item to look for. </param>
        /// <returns>True if the item is in this set; false otherwise. </returns>
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

        /// <summary>
        /// Copy the members of this set into an array, in order. 
        /// </summary>
        /// <param name="array">The array to copy the data items into. </param>
        /// <param name="arrayIndex">The index in the array to start storing the items at. </param>
        public void CopyTo(T[] array, int arrayIndex)
            => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);

        /// <summary>
        /// Enumerates items from <see cref="BTreeSet{T}" />, in their natural order.
        /// </summary>
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

        /// <summary>
        /// Prepare to list out the items of this set, in forwards order
        /// starting from the first. 
        /// </summary>
        public Enumerator GetEnumerator() => new Enumerator(this);

        IEnumerator<T> IEnumerable<T>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <inheritdoc cref="ISet{T}.ExceptWith" />
        public void ExceptWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="ISet{T}.IntersectWith" />
        public void IntersectWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="ISet{T}.SymmetricWith" />
        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="ISet{T}.UnionWith" />
        public void UnionWith(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="IReadOnlySet{T}.IsProperSupersetOf" />
        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="IReadOnlySet{T}.IsProperSupersetOf" />
        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="IReadOnlySet{T}.IsSubsetOf" />
        public bool IsSubsetOf(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="IReadOnlySet{T}.IsSupersetOf" />
        public bool IsSupersetOf(IEnumerable<T> other)
        {
            foreach (var item in other)
            {
                if (!Contains(item))
                    return false;
            }

            return true;
        }

        /// <inheritdoc cref="IReadOnlySet{T}.Overlaps" />
        public bool Overlaps(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc cref="IReadOnlySet{T}.SetEquals" />
        public bool SetEquals(IEnumerable<T> other)
        {
            throw new NotImplementedException();
        }
    }
}
