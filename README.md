# FolderStorage

Trivial abstraction around POSIX folders.  This allows us to switch between different
types of storage (e.g. POSIX vs. Cloud storage) in packages that use the abstract
`Container` type found in https://github.com/ChevronETC/AbstractStorage.jl.  In
other words FolderStorage can be convenient for package development but is not intended
for direct use.
