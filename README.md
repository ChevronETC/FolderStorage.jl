# FolderStorage

Abstraction around reading/writing arrays to/from files.  We provide the
`Folder<:Container` type, and use dispatch to provide methods for
`mkpath,rm,copy,isfile,read` etc.  This allows us to switch between different
types of storage (e.g. file-system storage vs. cloud storage) in packages that
use the abstract `Container` type.

The main consumer of this package is `FileBlockArrays`.

see also:
http://chevron.visualstudio.com/ETC-ESD-GCPStorage.jl
http://chevron.visualstudio.com/ETC-ESD-AbstractStorage.jl

## Example
```julia
using FolderStorage

f = Folder("foo")
mkpath(f)
write(f, "o", rand(10))
x = read!(f, "o", Vector{Float64}(undef, 10))
rm(f)
```
