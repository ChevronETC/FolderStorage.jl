module FolderStorage

using Serialization

const _libFolderStorage = normpath(joinpath(Base.source_path(),"../../deps/usr/lib/libFolderStorage"))
const _haslibFolderStorage = isfile(_libFolderStorage*".so")

function __init__()
    if !_haslibFolderStorage
        @warn "FolderStorage is not built, we will not use openmp code paths."
    end
end

using AbstractStorage, Random

struct Folder <: Container
    foldername::String
    nretry::Int
end
Folder(foldername; nretry=10) = Folder(foldername, nretry)

Base.mkpath(c::Folder) = mkpath(c.foldername)

function writebytes(c::Folder, o::AbstractString, data::AbstractArray{UInt8})
    filename = joinpath(c.foldername, o)
    for i = 1:c.nretry
        write(filename, data) == length(data) && return nothing
        @warn "problem writing to $c/$o, attempt $i."
        sleep(0.1*2^(i-1))
    end
    error("problem writing to $c/$o in 10 attempts.")
end

function Base.write(c::Folder, o::AbstractString, data::AbstractArray{T}) where {T}
    if T <: Number
        databytes = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), sizeof(data))
        writebytes(c, o, databytes)
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
        function _readbytes!(c, o, data, nthreads)
            ccall((:readbytes_threaded_single_file, _libFolderStorage), Int,
                (Cstring,  Ptr{UInt8}, Csize_t,      Cint,     Cint),
                 filename, data,       length(data), nthreads, c.nretry)
        end
        r = _readbytes!(c, o, data, nthreads)
        r == 0 || error("problem reading from $c/$o.")
        return data
    end

    for i = 1:c.nretry
        try
            read!(joinpath(c.foldername,o), data)
            return data
        catch
            @warn "problem reading from $c/$o, attempt $i."
            sleep(0.1*2^(i-1))
        end
    end
    error("problem reading from $c/$o in 10 attempts.")
    data
end

function Base.read!(c::Folder, o::String, data::Array{T}, nthreads=Sys.CPU_THREADS) where {T}
    if T <: Number
        databytes = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), (sizeof(data),))
    else
        databytes = Vector{UInt8}(undef, filesize(c, o))
    end

    readbytes!(c, o, databytes, nthreads)

    if T <: Number
        return data
    end

    io = IOBuffer(databytes)
    data .= deserialize(io)
    data
end

function writebytes_pieces(c::Folder, o::String, data::AbstractArray{UInt8}, nthreads)
    nthreads = clamp(nthreads, 1, length(data))

    if _haslibFolderStorage
        filename = joinpath(c.foldername, o)
        function _writebytes_pieces(c, o, data, nthreads)
            ccall((:writebytes_threaded, _libFolderStorage), Int,
                (Cstring,  Ptr{UInt8}, Csize_t,      Cint,     Cint),
                 filename, data,       length(data), nthreads, c.nretry)
        end
        r = _writebytes_pieces(c, o, data, nthreads)
        r == 0 || error("response code is $r")
        return nothing
    end

    thread_datasize, thread_dataremainder = divrem(length(data), nthreads)
    for threadid = 1:nthreads
        thread_firstbyte = (threadid - 1)*thread_datasize + 1
        _thread_datasize = thread_datasize
        if threadid <= thread_dataremainder
            thread_firstbyte += threadid - 1
            _thread_datasize += 1
        else
            thread_firstbyte += thread_dataremainder
        end
        thread_lastbyte = thread_firstbyte + _thread_datasize - 1

        thread_objectname = string(o, "-", threadid)
        writebytes(c, thread_objectname, data[thread_firstbyte:thread_lastbyte])
    end
    nothing
end

function AbstractStorage.writepieces(c::Folder, o::String, data::AbstractArray{T}, nthreads::Int=Sys.CPU_THREADS) where {T}
    if T <: Number
        databytes = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), (sizeof(data),))
        writebytes_pieces(c, o, databytes, nthreads)
        return nothing
    end
    io = IOBuffer()
    serialize(io, data)
    databytes = take!(io)
    writebytes_pieces(c, o, databytes, nthreads)
    nothing
end

function readbytes_pieces!(c::Folder, o::String, data::AbstractArray{UInt8}, nthreads::Int)
    nthreads = clamp(nthreads, 1, length(data))

    if _haslibFolderStorage
        filename = joinpath(c.foldername, o)
        function _readbytes_pieces!(c, o, data, nthreads)
            ccall((:readbytes_threaded_many_files, _libFolderStorage), Int,
                (Cstring,  Ptr{UInt8}, Csize_t,      Cint,     Cint),
                 filename, data,       length(data), nthreads, c.nretry)
        end
        r = _readbytes_pieces!(c, o, data, nthreads)
        r == 0 || error("response code is $r")
        return data
    end

    thread_datasize, thread_dataremainder = divrem(length(data), nthreads)
    for threadid = 1:nthreads
        thread_firstbyte = (threadid - 1)*thread_datasize + 1
        _thread_datasize = thread_datasize
        if threadid <= thread_dataremainder
            thread_firstbyte += threadid - 1
            _thread_datasize += 1
        else
            thread_firstbyte += thread_dataremainder
        end
        thread_lastbyte = thread_firstbyte + _thread_datasize - 1

        thread_objectname = string(o, "-", threadid)
        data[thread_firstbyte:thread_lastbyte] .= readbytes!(c, thread_objectname, Vector{UInt8}(undef, _thread_datasize), nthreads)
    end
    return data
end

function AbstractStorage.readpieces!(c::Folder, o::String, data::AbstractArray{T}, nthreads::Int=Sys.CPU_THREADS) where {T}
    if T <: Number
        databytes = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), (sizeof(data),))
        readbytes_pieces!(c, o, databytes, nthreads)
        return data
    end
    n = 0
    filename = joinpath(c.foldername, o)
    for threadid = 1:nthreads
        n += filesize(string(filename,"-",threadid))
    end
    databytes = readbytes_pieces!(c, o, Vector{UInt8}(undef, n), nthreads)
    io = IOBuffer(databytes)
    data .= deserialize(io)
    data
end

function Base.deepcopy(src::Folder)
    dst = Folder(src.foldername*"-copy-"*randstring(4))
    isdir(src.foldername) && cp(src, dst)
    dst
end

Base.filesize(c::Folder, o::AbstractString) = filesize(joinpath(c.foldername,o))
Base.cp(src::Folder, dst::Folder) = cp(src.foldername, dst.foldername, force=true)
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
            @warn "unable to remove $(c.foldername), trial $itry"
            itry == c.nretry && rethrow()
        end
        sleep(0.1*2^(itry-1))
    end
    nothing
end

export Folder

end
