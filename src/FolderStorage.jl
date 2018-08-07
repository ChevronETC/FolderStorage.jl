__precompile__(true)

module FolderStorage

const _libFolderStorage = normpath(joinpath(Base.source_path(),"../../deps/usr/lib/libFolderStorage"))
const _haslibFolderStorage = isfile(_libFolderStorage*".so")

using AbstractStorage, Lumberjack

struct Folder <: Container
    foldername::String
    nretry::Int
end
Folder(foldername; nretry=10) = Folder(foldername, nretry)

Base.mkpath(c::Folder) = mkpath(c.foldername)

function writebytes(c::Folder, o::AbstractString, data::AbstractArray{UInt8})
    filename = joinpath(c.foldername, o)
    for i = 1:c.nretry
        write(filename, data) == sizeof(data) && return nothing
        Lumberjack.warn("problem writing to $c/$o, attempt $i.")
        sleep(0.1*2^(i-1))
    end
    Lumberjack.error("problem writing to $c/$o in 10 attempts.")
end

function Base.write(c::Folder, o::AbstractString, data::AbstractArray{T}) where {T}
    if T <: Number
        writebytes(c, o, reinterpret(UInt8, vec(data)))
        return nothing
    end
    io = IOBuffer()
    serialize(io, data)
    databytes = take!(io)
    writebytes(c, o, databytes)
    nothing
end

function readbytes!(c::Folder, o::String, data::Vector{UInt8}, nthreads)
    nthreads = clamp(nthreads, 1, length(data))
    filename = joinpath(c.foldername, o)
    if _haslibFolderStorage
        r = ccall((:readbytes_threaded_single_file, _libFolderStorage), Int,
            (Cstring,  Ptr{UInt8}, Csize_t,      Cint,     Cint),
             filename, data,       length(data), nthreads, c.nretry)
        r == 0 || Lumberjack.error("problem reading from $c/$o.")
        return nothing
    end

    for i = 1:c.nretry
        try
            read!(joinpath(c.foldername,o), data)
            return nothing
        catch
            Lumberjack.warn("problem reading from $c/$o, attempt $i.")
            sleep(0.1*2^(i-1))
        end
    end
    Lumberjack.error("problem reading from $c/$o in 10 attempts.")
    nothing
end

function Base.read!(c::Folder, o::String, data::Array{T}, nthreads=Sys.CPU_CORES) where {T}
    if T <: Number
        databytes = reinterpret(UInt8, vec(data))
    else
        databytes = Vector{UInt8}(filesize(c, o))
    end

    readbytes!(c, o, databytes, nthreads)

    if T <: Number
        return nothing
    end

    io = IOBuffer(databytes)
    data .= deserialize(io)
    nothing
end

function Base.read(c::Folder, o::String, _T::Type{T}, n::NTuple{N}, nthreads=Sys.CPU_CORES)  where {T,N}
    data = Array{T,N}(n)
    read!(c, o, data, nthreads)
    return data
end

function writebytes_pieces(c::Folder, o::String, data::AbstractArray{UInt8}, nthreads)
    nthreads = clamp(nthreads, 1, length(data))
    filename = joinpath(c.foldername, o)
    res = ccall((:writebytes_threaded, _libFolderStorage), Int,
        (Cstring,  Ptr{UInt8}, Csize_t,      Cint,     Cint),
         filename, data,       length(data), nthreads, c.nretry)
    res == 0 || Lumberjack.error("response code is $res")
    nothing
end

function AbstractStorage.writepieces(c::Folder, o::String, data::AbstractArray{T}, nthreads::Int=Sys.CPU_CORES) where {T}
    if T <: Number
        writebytes_pieces(c, o, reinterpret(UInt8,vec(data)), nthreads)
        return nothing
    end
    io = IOBuffer()
    serialize(io, data)
    databytes = take!(io)
    writebytes_pieces(c, o, databytes, nthreads)
    nothing
end

function readbytes_pieces!(c::Folder, o::String, data::AbstractArray{UInt8}, nthreads::Int)
    filename = joinpath(c.foldername, o)
    nthreads = clamp(nthreads, 1, length(data))
    res = ccall((:readbytes_threaded_many_files, _libFolderStorage), Int,
        (Cstring,  Ptr{UInt8}, Csize_t,      Cint,     Cint),
         filename, data,       length(data), nthreads, c.nretry)
    res == 0 || Lumberjack.error("response code is $res")
    nothing
end

function AbstractStorage.readpieces!(c::Folder, o::String, data::AbstractArray{T}, nthreads::Int=Sys.CPU_CORES) where {T}
    if T <: Number
        readbytes_pieces!(c, o, reinterpret(UInt8,vec(data)), nthreads)
        return nothing
    end
    n = 0
    filename = joinpath(c.foldername, o)
    for threadid = 1:nthreads
        n += filesize(string(filename,"-",threadid))
    end
    databytes = Vector{UInt8}(n)
    readbytes_pieces!(c, o, databytes, nthreads)
    io = IOBuffer(databytes)
    data .= deserialize(io)
    nothing
end

function AbstractStorage.readpieces(c::Folder, o::String, _T::Type{T}, n::NTuple{N}, nthreads::Int=Sys.CPU_CORES) where {T,N}
    data = Array{T,N}(n)
    readpieces!(c, o, data, nthreads)
    data
end

function Base.deepcopy(src::Folder)
    dst = Folder(src.foldername*"-copy-"*randstring(4))
    isdir(src.foldername) && cp(src, dst)
    dst
end

Base.filesize(c::Folder, o::AbstractString) = filesize(joinpath(c.foldername,o))
Base.cp(src::Folder, dst::Folder) = cp(src.foldername, dst.foldername, remove_destination=true)
Base.copy(src::Folder) = Folder(src.foldername*"-copy-"*randstring(4))
Base.readdir(src::Folder) = readdir(src.foldername)
Base.isdir(src::Folder) = isdir(src.foldername)

function Base.isfile(c::Folder, object::String)
    # TODO: this feels like a kludge.  the trouble is that the file can
    # be written to jointpath(c,object) or a set of files, one for each
    # thread.
    isfile(joinpath(c.foldername, object)) || isfile(string(joinpath(c.foldername, object), "-1"))
end

function Base.rm(c::Folder)
    for itry = 1:c.nretry
        try
            rm(c.foldername, recursive=true, force=true)
            break
        catch
            Lumberjack.warn("unable to remove $(c.foldername), trial $itry")
            itry == c.nretry && rethrow()
        end
        sleep(0.1*2^(itry-1))
    end
    nothing
end

export Folder

end
