module FolderStorage

using AbstractStorage, Random, Serialization

struct Folder <: Container
    foldername::String
    nretry::Int
end

"""
    Folder(name[; nretry=10])

Return a representation of a POSIX folder.
"""
Folder(foldername; nretry=10) = Folder(isabspath(foldername) ? foldername : normpath(joinpath(pwd(), foldername)), nretry)

"""
    Container(Folder, d::Dict)

Return a representation of a POSIX folder where, for example, `d=Dict("foldername"=>"name", "nretry"=>10)`.
"""
AbstractStorage.Container(::Type{Folder}, d::Dict, session=nothing) = Folder(d["foldername"]; nretry = d["nretry"])

"""
    mkpath(c::Folder)

equivalent to mkpath(c.foldername).
"""
Base.mkpath(c::Folder) = mkpath(c.foldername)
Base.mkpath(c::Folder, o::AbstractString) = mkpath(joinpath(c.foldername, splitpath(o)[1:end-1]...))

function writebytes(c::Folder, o::AbstractString, data::AbstractArray{UInt8})
    mkpath(c, o)
    filename = joinpath(c.foldername, o)
    for i = 1:c.nretry
        write(filename, data) == length(data) && return nothing
        @warn "problem writing to $c/$o, attempt $i."
        sleep(0.1*2^(i-1))
    end
    error("problem writing to $c/$o in 10 attempts.")
end

"""
    write(c::Folder, filename, data)

Equivalent to write(joinpath(c.foldername, filename), data)
"""
function Base.write(c::Folder, o::AbstractString, data::AbstractString)
    mkpath(c, o)
    write(joinpath(c.foldername, o), data)
end

function Base.write(c::Folder, o::AbstractString, data::DenseArray{T}) where {T<:Number}
    databytes = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), sizeof(data), own=false)
    writebytes(c, o, databytes)
end

function Base.write(c::Folder, o::AbstractString, data::DenseArray)
    io = IOBuffer()
    serialize(io, data)
    databytes = take!(io)
    writebytes(c, o, databytes)
end

function byterange(iblock, block_size, block_remainder)
    isremainder = iblock <= block_remainder
    firstbyte = (iblock - 1)*block_size + (isremainder ? iblock : block_remainder + 1)
    lastbyte = firstbyte + (isremainder ? block_size : block_size - 1)
    firstbyte,lastbyte
end

function readbytes_thread(c, o, data, threadid, thread_size, thread_remainder)
    firstbyte,lastbyte = byterange(threadid, thread_size, thread_remainder)
    io = open(joinpath(c.foldername, o))
    seek(io, firstbyte-1)
    read!(io, view(data, firstbyte:lastbyte))
    close(io)
    nothing
end

function readbytes!(c::Folder, o::String, data::Vector{UInt8})
    _nthreads = clamp(Threads.nthreads(), 1, length(data))
    thread_size, thread_remainder = divrem(length(data), _nthreads)
    @sync for threadid = 1:_nthreads
        @async Threads.@spawn readbytes_thread(c, o, data, threadid, thread_size, thread_remainder)
    end
    data
end

Base.read(c::Folder, o::AbstractString, ::Type{String}) = read(joinpath(c.foldername, o), String)

function Base.read(c::Folder, o::AbstractString)
    databytes = readbytes!(c, o, Vector{UInt8}(undef, filesize(c, o)))
    io = IOBuffer(databytes)
    deserialize(io)
end

"""
    Base.read!(c::Folder, filename, String, data)

Equivalent to `read!(joinpath(c.foldername, filename), String, data).`
"""
function Base.read!(c::Folder, o::AbstractString, data::DenseArray{T}) where {T<:Number}
    databytes = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), (sizeof(data),))
    readbytes!(c, o, databytes)
    data
end

function Base.read!(c::Folder, o::AbstractString, data::DenseArray{T}) where {T}
    databytes = Vector{UInt8}(undef, filesize(c, o))
    readbytes!(c, o, databytes)
    io = IOBuffer(databytes)
    data .= deserialize(io)
    data
end

function Base.deepcopy(src::Folder)
    dst = Folder(src.foldername*"-copy-"*randstring(4))
    isdir(src.foldername) && cp(src, dst)
    dst
end

Base.filesize(c::Folder, o::AbstractString) = filesize(joinpath(c.foldername, o))
Base.cp(src::Folder, dst::Folder) = cp(src.foldername, dst.foldername, force=true)
Base.copy(src::Folder) = Folder(src.foldername*"-copy-"*randstring(4))
Base.readdir(src::Folder) = readdir(src.foldername)
Base.isdir(src::Folder) = isdir(src.foldername)

function Base.isfile(c::Folder, object::AbstractString)
    # TODO: this feels like a kludge.  the trouble is that the file can
    # be written to jointpath(c,object) or a set of files, one for each
    # thread.
    isfile(joinpath(c.foldername, object)) || isfile(string(joinpath(c.foldername, object), "-1"))
end

"""
    rm(c::Folder)

Equivalent to `rm(c.foldername)`.
"""
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

"""
    rm(c::Folder, filename)

Equivalent to `rm(joinpath(c.foldername, filename))`.
"""
function Base.rm(c::Folder, filename)
    for itry = 1:c.nretry
        try
            rm(joinpath(c.foldername, filename), recursive=true, force=true)
            break
        catch
            @warn "unable to remove $(joinpath(c.foldername, filename)), trial $itry"
            itry == c.nretry && rethrow()
        end
        sleep(0.1*2^(itry-1))
    end
    nothing
end

export Folder

end
