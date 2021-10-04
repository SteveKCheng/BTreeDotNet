using System;
using System.Collections;
using System.Collections.Generic;

namespace BPlusTree
{
    internal static partial class BTreeCore
    {
        public static void CopyFromEnumeratorToArray<TItem, TEnumerator>
            (TEnumerator enumerator, int count, TItem[] array, int arrayIndex)
            where TEnumerator : IEnumerator<TItem>
        {
            try
            {
                if (array.Length - arrayIndex < count)
                    throw new ArgumentOutOfRangeException(nameof(array), "Array is not large enough to hold all the items from this B+Tree. ");

                while (enumerator.MoveNext())
                    array[arrayIndex++] = enumerator.Current;
            }
            finally
            {
                enumerator.Dispose();
            }
        }
    }

    public partial class BTree<TKey, TValue>
    {
        /// <summary>
        /// Iterates through the key/value pairs inside <see cref="BTree{TKey, TValue}"/>,
        /// in the order defined by the key comparer.
        /// </summary>
        public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            /// <inheritdoc cref="IEnumerator{T}.Current" />
            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    if (!_valid)
                        throw new InvalidOperationException("Cannot retrieve the value from this enumerator because it is not pointing to a valid entry in the B+Tree. ");
                    return _current;
                }
            }

            /// <inheritdoc cref="IEnumerator.Current" />
            object IEnumerator.Current => Current;

            /// <summary>
            /// Frees up temporary scratchpad memory used for iterating through
            /// the B+Tree.
            /// </summary>
            public void Dispose()
            {
                _valid = false;
                _ended = false;
                _path.Dispose();
            }

            /// <summary>
            /// Move <see cref="_path" /> to be positioned at the first entry
            /// of the next leaf node (right neighbor), if it exists.
            /// </summary>
            /// <returns>
            /// True if moving to the next leaf node is successful.
            /// False if there is no next leaf node; in this case 
            /// <see cref="_path"/> remains unchanged.
            /// </returns>
            private bool MoveToNextLeafNode()
            {
                for (int level = _path.Depth; level > 0; --level)
                {
                    ref var parentStep = ref _path.Steps[level - 1];
                    var parentNode = BTreeCore.AsInteriorNode<TKey>(parentStep.Node!);
                    int parentIndex = parentStep.Index;
                    if (parentIndex + 1 < parentNode.Length)
                    {
                        ref var currentLink = ref parentNode[parentIndex + 1].Value;

                        // Found the pivot for the right neighbor to the current leaf node
                        if (currentLink.Child != null)
                        {
                            ++parentStep.Index;
                            ResetPathPartially(node: currentLink, level: level, left: true);
                            return true;
                        }
                    }
                }

                // The current leaf node is the last one and has no right neighbor.
                return false;
            }

            /// <summary>
            /// Move <see cref="_path" /> to be positioned at the last entry
            /// of the previous leaf node (left neighbor), if it exists.
            /// </summary>
            /// <returns>
            /// True if moving to the previous leaf node is successful.
            /// False if there is no previous leaf node; in this case 
            /// <see cref="_path"/> remains unchanged.
            /// </returns>
            private bool MoveToPreviousLeafNode()
            {
                for (int level = _path.Depth; level > 0; --level)
                {
                    ref var parentStep = ref _path.Steps[level - 1];
                    var parentNode = BTreeCore.AsInteriorNode<TKey>(parentStep.Node!);
                    int parentIndex = parentStep.Index;
                    if (parentIndex > 0)
                    {
                        ref var currentLink = ref parentNode[parentIndex - 1].Value;

                        --parentStep.Index;
                        ResetPathPartially(node: currentLink, level: level, left: false);
                        return true;
                    }
                }

                return false;
            }

            /// <summary>
            /// Move forwards to the following entry in the B+Tree.
            /// </summary>
            /// <remarks>
            /// <para>
            /// The first call to this method,
            /// after this enumerator has been initialized to be
            /// "at the beginning", via <see cref="Reset"/> or the 
            /// <see cref="Enumerator"/> constructor, will cause
            /// <see cref="Current" /> to output the first entry of the B+Tree.
            /// </para>
            /// <para>
            /// Calls to this method may be mixed with <see cref="MovePrevious" />.
            /// </para>
            /// </remarks>
            /// <returns>
            /// True if this enumerator now points to the following entry;
            /// false if it has reached the end.  
            /// </returns>
            public bool MoveNext()
            {
                ref var step = ref _path.Steps[_path.Depth];

                // Do not increment step.Index on the very first call (after Reset)
                if (_valid)
                    ++step.Index;
                else if (_ended)
                    return false;

                // If the index went past all the active slots in the leaf node, 
                // then we need to trace the path back up the B+Tree to find
                // find the next neighboring leaf node.
                if (step.Index >= _entriesCount && !MoveToNextLeafNode())
                {
                    _valid = false;
                    _ended = true;
                    return false;
                }

                var leafNode = AsLeafNode(step.Node!);
                ref var entry = ref leafNode[step.Index];
                _current = new KeyValuePair<TKey, TValue>(entry.Key, entry.Value);
                _valid = true;
                return true;
            }

            /// <summary>
            /// Move backwards to preceding entry in the B+Tree.
            /// </summary>
            /// <remarks>
            /// <para>
            /// The first call to this method,
            /// after this enumerator has been initialized to be
            /// "at the end", via <see cref="Reset"/> or the 
            /// <see cref="Enumerator"/> constructor, will cause
            /// <see cref="Current" /> to output the last entry of the B+Tree.
            /// </para>
            /// <para>
            /// Calls to this method may be mixed with <see cref="MoveNext" />.
            /// </para>
            /// </remarks>
            /// <returns>
            /// True if this enumerator now points to the preceding entry;
            /// false if there is none.  
            /// </returns>
            public bool MovePrevious()
            {
                if (!_valid && !_ended)
                    return false;

                ref var step = ref _path.Steps[_path.Depth];
                if (step.Index == 0 && !MoveToPreviousLeafNode())
                {
                    _valid = false;
                    _ended = false;
                    return false;
                }

                --step.Index;
                var leafNode = AsLeafNode(step.Node!);
                ref var entry = ref leafNode[step.Index];
                _current = new KeyValuePair<TKey, TValue>(entry.Key, entry.Value);
                _valid = true;
                _ended = false;
                return true;
            }

            /// <summary>
            /// Modifies <see cref="_path"/>
            /// to be the left-most path
            /// or right-most path starting from a given of the B+Tree.
            /// </summary>
            /// <param name="node">Points to the starting node existing 
            /// at the level of the B+tree given by <paramref name="level" />.
            /// </param>
            /// <param name="level">The level of the B+Tree to start moving downwards from.
            /// </param>
            /// <param name="left">True to take left-most path; false to take the right-most path.
            /// </param>
            private void ResetPathPartially(NodeLink node, int level, bool left)
            {
                int depth = _path.Depth;
                int index;
                while (level < depth)
                {
                    index = left ? 0 : node.EntriesCount;
                    _path.Steps[level] = new BTreeStep(node.Child!, index);
                    node = BTreeCore.AsInteriorNode<TKey>(node.Child!)[index].Value;
                    ++level;
                }

                index = left ? 0 : node.EntriesCount;
                _path.Steps[depth] = new BTreeStep(node.Child!, index);
                _entriesCount = node.EntriesCount;
            }

            /// <inheritdoc cref="IEnumerator.Reset" />.
            public void Reset() => Reset(true);

            /// <summary>
            /// Reset this enumerator to either the beginning or end
            /// of the B+Tree.
            /// </summary>
            /// <param name="toBeginning">
            /// If true, resets to the beginning of the B+Tree, so that the next call
            /// to <see cref="MoveNext" /> retrieves the first entry
            /// of the B+Tree.  If false, resets to the end, so that the
            /// next call to <see cref="MovePrevious" /> retrieves
            /// the last entry of the B+Tree.
            /// </param>
            public void Reset(bool toBeginning)
            {
                var owner = Owner;
                int depth = _path.Depth;

                if (depth != owner.Depth)
                {
                    _path.Dispose();
                    _path = owner.NewPath();
                }

                ResetPathPartially(node: owner._root, level: 0, left: toBeginning);
                _valid = false;
                _ended = !toBeginning;
            }

            /// <summary>
            /// True if this enumerator is pointing to a valid entry,
            /// so that the <see cref="Current" /> property can be queried.
            /// </summary>
            /// <remarks>
            /// This flag is the same as what has been returned in the last
            /// call to <see cref="MovePrevious"/> or <see cref="MoveNext"/>.
            /// If there has been no call to those methods after resetting
            /// or initializing this enumerator, this flag is false.
            /// </remarks>
            public bool IsValid => _valid;

            /// <summary>
            /// Backing field for <see cref="IsValid" />.
            /// </summary>
            /// <remarks>
            /// If this member is false while <see cref="_ended"/> is false, that means
            /// this instance has just been reset to the beginning of the B+Tree,
            /// and the first entry will be reported from the next call to
            /// <see cref="MoveNext" />.  If <see cref="_ended" /> is true
            /// then this member is necessarily false.
            /// </remarks>
            private bool _valid;

            /// <summary>
            /// Set to ended when <see cref="MoveNext" /> moves past the last leaf node.
            /// </summary>
            private bool _ended;

            /// <summary>
            /// Remembers the path from the root of the B+Tree down to the leaf node
            /// so that the enumerator can move to the neighboring leaf node.
            /// </summary>
            private BTreePath _path;

            /// <summary>
            /// Copy of the item in the B+Tree updated by <see cref="MoveNext" />.
            /// </summary>
            private KeyValuePair<TKey, TValue> _current;

            /// <summary>
            /// Cached count of the entries in the current leaf node.
            /// </summary>
            private int _entriesCount;

            /// <summary>
            /// The B+Tree that this enumerator comes from.
            /// </summary>
            public BTree<TKey, TValue> Owner { get; }

            /// <summary>
            /// Prepare to enumerate items in the B+Tree ordered by item key.
            /// </summary>
            /// <param name="owner">The B+Tree to enumerate items from. </param>
            /// <param name="toBeginning">
            /// If true, the enumerator is positioned to the beginning of the B+Tree, 
            /// so that the following call to <see cref="MoveNext" /> retrieves the first entry
            /// of the B+Tree.  If false, the enumerator is positioned to the end, so 
            /// that the next call to <see cref="MovePrevious" /> retrieves
            /// the last entry of the B+Tree.
            /// </param>
            public Enumerator(BTree<TKey, TValue> owner, bool toBeginning)
            {
                Owner = owner;
                _path = default;
                _entriesCount = 0;
                _valid = false;
                _ended = false;
                _current = default;

                Reset(toBeginning);
            }
        }

        /// <summary>
        /// Prepare to enumerate entries in the B+Tree from the first onward.
        /// </summary>
        public Enumerator GetEnumerator() => GetEnumerator(toBeginning: true);

        /// <summary>
        /// Prepare to enumerate entries in the B+Tree.
        /// </summary>
        /// <param name="toBeginning">
        /// If true, entries will be enumerated in forward order, from the first
        /// entry onwards.  If false, entries can be enumerated backwards, from
        /// the last entry, by calling <see cref="Enumerator.MovePrevious" />
        /// on the returned object.
        /// </param>
        public Enumerator GetEnumerator(bool toBeginning) => new Enumerator(this, toBeginning);

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
            => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;

        /// <inheritdoc cref="ICollection{T}.CopyTo(T[], int)"/>
        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
            => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);
    }
}
