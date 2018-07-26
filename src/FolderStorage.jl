__precompile__(true)

module FolderStorage

using AbstractStorage

struct Folder <: Container
    foldername::String
    nretry::Int
end
Folder(foldername; nretry=10) = Folder(foldername, nretry)

Base.mkpath(c::Folder) = mkpath(c.foldername)

function Base.write(c::Folder, o::AbstractString, data::AbstractArray)
    filename = joinpath(c.foldername, o)
    for i = 1:c.nretry
        try
            if eltype(data) <: Number
                write(filename, data)
                if filesize(filename) != sizeof(data)
                    throw(ErrorException())
                end
                return nothing
            else # then there is no cononical binary representation of `data`
                io = IOBuffer()
                serialize(io, data)
                x = take!(io)
                write(filename, x)
                if filesize(filename) != sizeof(x)
                    throw(ErrorException())
                end
                return nothing
            end
        catch
            Lumberjack.warn("problem writing to $c/$o, attempt $i.")
            sleep(0.1*2^(i-1))
        end
    end
    Lumberjack.error("problem writing to $c/$o in 10 attempts")
end

function Base.read(c::Folder, o::String, T, n)
    filename = joinpath(c.foldername, o)
    for i = 1:c.nretry
        try
            if T <: Number
                return read(filename, T, n)
            end
            io = open(filename)
            x = deserialize(io)
            close(io)
            return x
        catch
            Lumberjack.warn("problem reading from $c/$o, attempt $i.")
            sleep(0.1*2^(i-1))
        end
    end
    Lumberjack.error("problem reading from $c/$o in 10 attempts")
end

function Base.read!(c::Folder, o::String, A::Array)
    filename = joinpath(c.foldername, o)
    for i = 1:c.nretry
        try
            if eltype(A) <: Number
                read!(filename, A)
                return nothing
            else
                io = open(filename)
                A .= deserialize(io)
                close(io)
                return nothing
            end
        catch
            Lumberjack.warn("problem reading from $c/$o, attempt $i.")
            sleep(0.1*2^(i-1))
        end
    end
    Lumberjack.error("problem reading from $c/$o in 10 attempts.")
end

function Base.deepcopy(src::Folder)
    dst = Folder(src.foldername*"-copy-"*randstring(4))
    isdir(src.foldername) && cp(src, dst)
    dst
end

Base.rm(c::Folder) = rm(c.foldername, recursive=true, force=true)
Base.cp(src::Folder, dst::Folder) = cp(src.foldername, dst.foldername, remove_destination=true)
Base.isfile(container::Folder, object::String) = isfile(joinpath(container.foldername,object))
Base.copy(src::Folder) = Folder(src.foldername*"-copy-"*randstring(4))
Base.readdir(src::Folder) = readdir(src.foldername)
Base.isdir(src::Folder) = isdir(src.foldername)

export Folder

end
