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

            /// <inheritdoc cref="IEnumerator.MoveNext" />
            public bool MoveNext()
            {
                if (!_valid)
                {
                    if (_ended)
                        return false;
                }

                ref var step = ref _path.Steps[_path.Depth];

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
                ++step.Index;
                return true;
            }

            /// <summary>
            /// Move backwards to preceding entry in the B+Tree.
            /// </summary>
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

            /// <inheritdoc cref="IEnumerator.Reset" />
            public void Reset()
            {
                var owner = Owner;
                int depth = _path.Depth;

                if (depth != owner.Depth)
                {
                    _path.Dispose();
                    _path = owner.NewPath();
                }

                ResetPathPartially(node: owner._root, level: 0, left: true);
                _valid = false;
                _ended = false;
            }

            /// <summary>
            /// True if this enumerator is pointing to a valid entry.
            /// </summary>
            public bool IsValid => _valid;

            /// <summary>
            /// Backing field for <see cref="IsValid" />.
            /// </summary>
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
            /// Prepare to enumerate items in the B+Tree starting from the first,
            /// when ordered by item key.
            /// </summary>
            /// <param name="owner">The B+Tree to enumerate items from. </param>
            public Enumerator(BTree<TKey, TValue> owner)
            {
                Owner = owner;
                _path = default;
                _entriesCount = 0;
                _valid = false;
                _ended = false;
                _current = default;

                Reset();
            }
        }

        public Enumerator GetEnumerator() => new Enumerator(this);

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
            => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;

        /// <inheritdoc cref="ICollection{T}.CopyTo(T[], int)"/>
        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
            => BTreeCore.CopyFromEnumeratorToArray(GetEnumerator(), Count, array, arrayIndex);
    }
}
