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
AbstractStorage.Container(::Type{Folder}, d::Dict, session=nothing; nretry=10) =
    Folder(d["foldername"]; nretry = get(d, "nretry", nretry))

Base.:(==)(x::Folder, y::Folder) = x.foldername == y.foldername

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

_iscontiguous(data::DenseArray) = isbitstype(eltype(data))
_iscontiguous(data::SubArray) = isbitstype(eltype(data)) && Base.iscontiguous(data)
_iscontiguous(data::AbstractArray) = false

function Base.write(c::Folder, o::AbstractString, data::AbstractArray{T}) where {T<:Number}
    if _iscontiguous(data)
        databytes = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), sizeof(data), own=false)
        writebytes(c, o, databytes)
    else
        error("FolderStorage: `write` is not supported for non-contiguous arrays")
    end
end

function Base.write(c::Folder, o::AbstractString, data::AbstractArray)
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

function readbytes_thread(c, o, data, threadid, thread_size, thread_remainder; offset=0)
    firstbyte,lastbyte = byterange(threadid, thread_size, thread_remainder)
    local io
    try
        io = open(joinpath(c.foldername, o))
    catch e
        if !isfile(joinpath(c.foldername, o))
            throw(FileDoesNotExistError())
        end
        throw(e)
    end
    seek(io, offset+firstbyte-1)
    read!(io, view(data, firstbyte:lastbyte))
    close(io)
    nothing
end

function readbytes_threaded!(c::Folder, o::String, data::Vector{UInt8}; offset=0)
    _nthreads = clamp(Threads.nthreads(), 1, length(data))
    thread_size, thread_remainder = divrem(length(data), _nthreads)
    try
        @sync for threadid = 1:_nthreads
            Threads.@spawn readbytes_thread(c, o, data, threadid, thread_size, thread_remainder; offset)
        end
    catch e
        if isa(e, CompositeException)
            for ex in e
                if isa(ex, TaskFailedException)
                    stk = current_exceptions(ex.task)
                    for stkex in stk
                        if isa(stkex.exception, FileDoesNotExistError)
                            throw(FileDoesNotExistError())
                        end
                    end
                end
            end
        end
        throw(e)
    end
    data
end

function readbytes_serial!(c::Folder, o::String, data::Vector{UInt8}; offset=0)
    local io
    try
        io = open(joinpath(c.foldername, o))
    catch e
        @info "one"
        if !isfile(joinpath(c.foldername, o))
            @info "two"
            showerror(stderr, e)
            @info "three"
            throw(FileDoesNotExistError())
        end
        throw(e)
    end
    seek(io, offset)
    read!(io, data)
    close(io)
    nothing
end

function readbytes!(c::Folder, o::String, data::Vector{UInt8}; offset=0, serial=false)
    if serial
        readbytes_serial!(c, o, data; offset)
    else
        readbytes_threaded!(c, o, data; offset)
    end
    _nthreads = clamp(Threads.nthreads(), 1, length(data))
    thread_size, thread_remainder = divrem(length(data), _nthreads)
    @sync for threadid = 1:_nthreads
        @async Threads.@spawn readbytes_thread(c, o, data, threadid, thread_size, thread_remainder; offset)
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
    Base.read!(c::Folder, filename::String, data; offset=0, serial=false)

Equivalent to `read!(joinpath(c.foldername, filename), String, data).`
"""
function Base.read!(c::Folder, o::AbstractString, data::AbstractArray{T}; offset=0, serial=false) where {T<:Number}
    if _iscontiguous(data)
        databytes = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(data)), (sizeof(data),))
        readbytes!(c, o, databytes; offset=offset*sizeof(T), serial)
    else
        error("FolderStorage: `read` is not supported for non-contiguous arrays.")
    end
    data
end

function Base.read!(c::Folder, o::AbstractString, data::AbstractArray{T}; offset=0) where {T}
    databytes = Vector{UInt8}(undef, filesize(c, o))
    readbytes!(c, o, databytes)
    io = IOBuffer(databytes)
    _data = deserialize(io)
    data .= @view _data[1+offset:end]
    data
end

function Base.deepcopy(src::Folder)
    dst = Folder(src.foldername*"-copy-"*randstring(4))
    isdir(src.foldername) && cp(src, dst)
    dst
end

function Base.cp(src::Folder, src_object::AbstractString, dst::Folder, dst_object::AbstractString)
    pth = joinpath(dst.foldername, split(dst_object, '/')[1:end-1]...)
    isdir(pth) || mkpath(pth)
    cp(joinpath(src.foldername, src_object), joinpath(dst.foldername, dst_object), force=true)
end

Base.filesize(c::Folder, o::AbstractString) = filesize(joinpath(c.foldername, o))
Base.cp(src::Folder, dst::Folder) = cp(src.foldername, dst.foldername, force=true)
Base.joinpath(src::Folder, object::AbstractString) = joinpath(src.foldername, object)
Base.copy(src::Folder) = Folder(src.foldername*"-copy-"*randstring(4))
Base.readdir(src::Folder) = readdir(src.foldername)
Base.isdir(src::Folder) = isdir(src.foldername)

function Base.touch(c::Folder, o::AbstractString)
    file = joinpath(c.foldername, o)
    _folder = splitpath(file)[1:end-1]
    if length(_folder) > 0
        folder = VERSION >= v"1.7" ? joinpath(_folder) : joinpath(_folder...)
        isdir(folder) || mkpath(folder)
    end
    touch(file)
end

AbstractStorage.scrubsession(c::Folder) = c

AbstractStorage.minimaldict(c::Folder) = Dict("foldername"=>c.foldername)

AbstractStorage.backend(c::Folder) = "posix"

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
