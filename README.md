# FolderStorage

[![](https://img.shields.io/badge/docs-stable-blue.svg)](https://ChevronETC.github.io/FolderStorage.jl/stable)
[![](https://img.shields.io/badge/docs-dev-blue.svg)](https://ChevronETC.github.io/FolderStorage.jl/dev)
https://github.com/ChevronETC/FolderStorage.jl/workflows/Run%20tests/badge.svg


Trivial abstraction around POSIX folders.  This allows us to switch between different
types of storage (e.g. POSIX vs. Cloud storage) in packages that use the abstract
`Container` type found in https://github.com/ChevronETC/AbstractStorage.jl.  In
other words FolderStorage can be convenient for package development but is not intended
for direct use.
