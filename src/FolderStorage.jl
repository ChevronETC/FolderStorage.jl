module FolderStorage

using AbstractStorage#=, FolderStorage_jll=#, Random, Serialization

const libFolderStorage = normpath(joinpath(Base.source_path(),"..","..","deps","usr","lib","libFolderStorage"))

struct Folder <: Container
    foldername::String
    nthreads::Int
    nretry::Int
end

"""
    Folder(name[; nthreads=Sys.CPU_THREADS, nretry=10])

Return a representation of a POSIX folder.
"""
Folder(foldername; nthreads=Sys.CPU_THREADS, nretry=10) = Folder(isabspath(foldername) ? foldername : normpath(joinpath(pwd(), foldername)), nthreads, nretry)

"""
    Container(Folder, d::Dict)

Return a representation of a POSIX folder where, for example, `d=Dict("foldername"=>"name", "nthreads"=>8, "nretry"=>10)`.
"""
AbstractStorage.Container(::Type{Folder}, d::Dict, session=nothing) = Folder(d["foldername"]; nthreads = d["nthreads"], nretry = d["nretry"])

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

function readbytes!(c::Folder, o::String, data::Vector{UInt8})
    nthreads = clamp(c.nthreads, 1, length(data))
    filename = joinpath(c.foldername, o)
    function _readbytes!(c, o, data, nthreads)
        ccall((:readbytes_threaded, libFolderStorage), Int,
            (Cstring,  Ptr{UInt8}, Csize_t,      Cint,     Cint),
                filename, data,       length(data), nthreads, c.nretry)
    end
    r = _readbytes!(c, o, data, nthreads)
    r == 0 || error("problem reading from $c/$o.")
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
