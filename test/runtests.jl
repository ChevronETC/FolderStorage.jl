using AbstractStorage, FolderStorage, Serialization, Test

@testset "mkpath" begin
    c = Folder("foo")
    mkpath(c)
    @test isdir("foo")
    rm(c)
end

@testset "rm" begin
    c = Folder("foo")
    mkpath(c)
    rm(c)
    @test !isdir("foo")
end

@testset "copy" begin
    c = Folder("foo")
    mkpath(c)
    write(c, "o", rand(10))
    d = copy(c)
    @test !isdir(d)
    mkpath(d)
    @test isempty(readdir(d))
    rm(c)
    rm(d)
end

@testset "deepcopy" begin
    c = Folder("foo")
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    d = deepcopy(c)
    @test isdir(d)
    @test read!(d, "o", Vector{Float64}(undef, 10)) == x
    rm(c)
end

@testset "isfile" begin
    c = Folder("foo")
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    @test isfile(c, "o")
    @test !isfile(c, "j")
    rm(c)

    c = Folder("foo", )
    mkpath(c)
    nthreads = 2
    writepieces(c, "o", x, nthreads)
    @test isfile(c, "o")
    rm(c)
end

@testset "filesize" begin
    c = Folder("foo")
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    @test filesize(c, "o") == 80
    rm(c)
end

@testset "write, canonical" begin
    c = Folder("foo")
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    @test read!(c.foldername*"/o", Vector{Float64}(undef, 10)) ≈ x
    rm(c)
end

@testset "read!, canonical, nthreads=$nthreads" for nthreads in (1, 4)
    c = Folder("foo")
    mkpath(c)
    x = rand(10)
    write(c.foldername*"/o", x)
    @test read!(c, "o", Vector{Float64}(undef, 10), nthreads) == x
    rm(c)
end

struct Foo
    x::Int
    y::Float64
end
@testset "write, serialized" begin
    c = Folder("foo")
    mkpath(c)
    x = [Foo(1,2.0)]
    write(c, "o", x)
    io = open(c.foldername*"/o")
    _x = deserialize(io)
    close(io)
    @test _x[1].x == x[1].x
    @test _x[1].y ≈ x[1].y
    rm(c)
end

@testset "read!, serialized, nthreads=$nthreads" for nthreads in (1,4)
    c = Folder("foo")
    mkpath(c)
    io = open(c.foldername*"/o", "w")
    x = [Foo(1,2.0)]
    serialize(io, x)
    close(io)
    _x = read!(c, "o", Vector{Foo}(undef, 1), nthreads)
    @test _x[1].x == x[1].x
    @test _x[1].y ≈ x[1].y
end

@testset "read!/writepieces, canonical, nthreads=$nthreads" for nthreads in (1,4)
    c = Folder("foo")
    mkpath(c)
    x = rand(10)
    writepieces(c, "o", x, nthreads)
    _x = readpieces!(c, "o", Vector{Float64}(undef, 10), nthreads)
    @test x ≈ _x
    rm(c)
end

@testset "read!/writepieces, serialized, nthreads=$nthreads" for nthreads in (1,4)
    c = Folder("foo")
    mkpath(c)
    x = [Foo(1,2.0), Foo(2,3.0), Foo(3,4.0), Foo(4,5.0)]
    writepieces(c, "o", x, nthreads)
    _x = readpieces!(c, "o", Vector{Foo}(undef, 4), nthreads)
    for i = 1:4
        @test x[i].x == _x[i].x
        @test x[i].y ≈ _x[i].y
    end
    rm(c)
end
