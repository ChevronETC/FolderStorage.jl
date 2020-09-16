var documenterSearchIndex = {"docs":
[{"location":"reference/#Reference","page":"Reference","title":"Reference","text":"","category":"section"},{"location":"reference/","page":"Reference","title":"Reference","text":"Modules = [FolderStorage]\nOrder   = [:function, :type]","category":"page"},{"location":"reference/#Base.Filesystem.mkpath-Tuple{Folder}","page":"Reference","title":"Base.Filesystem.mkpath","text":"mkpath(c::Folder)\n\nequivalent to mkpath(c.foldername).\n\n\n\n\n\n","category":"method"},{"location":"reference/#Base.Filesystem.rm-Tuple{Folder,Any}","page":"Reference","title":"Base.Filesystem.rm","text":"rm(c::Folder, filename)\n\nEquivalent to rm(joinpath(c.foldername, filename)).\n\n\n\n\n\n","category":"method"},{"location":"reference/#Base.Filesystem.rm-Tuple{Folder}","page":"Reference","title":"Base.Filesystem.rm","text":"rm(c::Folder)\n\nEquivalent to rm(c.foldername).\n\n\n\n\n\n","category":"method"},{"location":"reference/#Base.read!-Union{Tuple{T}, Tuple{Folder,AbstractString,DenseArray{T,N} where N}} where T<:Number","page":"Reference","title":"Base.read!","text":"Base.read!(c::Folder, filename, String, data)\n\nEquivalent to read!(joinpath(c.foldername, filename), String, data).\n\n\n\n\n\n","category":"method"},{"location":"reference/#Base.write-Tuple{Folder,AbstractString,AbstractString}","page":"Reference","title":"Base.write","text":"write(c::Folder, filename, data)\n\nEquivalent to write(joinpath(c.foldername, filename), data)\n\n\n\n\n\n","category":"method"},{"location":"reference/#AbstractStorage.Container","page":"Reference","title":"AbstractStorage.Container","text":"Container(Folder, d::Dict)\n\nReturn a representation of a POSIX folder where, for example, d=Dict(\"foldername\"=>\"name\", \"nretry\"=>10).\n\n\n\n\n\n","category":"type"},{"location":"reference/#FolderStorage.Folder-Tuple{Any}","page":"Reference","title":"FolderStorage.Folder","text":"Folder(name[; nretry=10])\n\nReturn a representation of a POSIX folder.\n\n\n\n\n\n","category":"method"},{"location":"#FolderStorage","page":"FolderStorage","title":"FolderStorage","text":"","category":"section"},{"location":"","page":"FolderStorage","title":"FolderStorage","text":"Abstraction around POSIX folders.  This allows us to switch between different types of storage (e.g. POSIX vs. Cloud storage) in packages that use the abstract Container type found in https://github.com/ChevronETC/AbstractStorage.jl. In addition, FolderStorage threads read operations using  PARTR.","category":"page"},{"location":"","page":"FolderStorage","title":"FolderStorage","text":"FolderStorage can be convenient for package development but is not intended for direct use.","category":"page"}]
}
