B+Trees for .NET
================

This library implements ordered associative containers and sets with B+Trees.
It is written entirely in C#, with care to minimize the number of allocations.

The algorithms used are essentially the standard ones that can be found
in textbooks and academic papers.  There are no special considerations
for concurrency, e.g. using copy-on-write.  The data structures are not 
thread-safe.

Addition, removal, and moving forward or backwards in the B+Tree are
operations that take O(log N) time where N is the number of members
in the container.

This library does not attempt to implement advanced features like
persistence.  It is just a replacement for the standard classes
``Dictionary<TKey, TValue>`` and ``HashSet<T>`` when you need 
the members to be ordered.  It assumes you can store your data 
in managed (GC) memory.  Your data can be any normal .NET object
or structure.

