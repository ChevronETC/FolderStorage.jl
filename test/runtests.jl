using FolderStorage, Base.Test

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
    @test read(d, "o", Float64, 10) == x
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
end

@testset "write, canonical" begin
    c = Folder("foo")
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    @test read(c.foldername*"/o", Float64, 10) ≈ x
    rm(c)
end

@testset "read, canonical" begin
    c = Folder("foo")
    mkpath(c)
    x = rand(10)
    write(c.foldername*"/o", x)
    @test read(c, "o", Float64, 10) ≈ x
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

@testset "read, serialized" begin
    c = Folder("foo")
    mkpath(c)
    io = open(c.foldername*"/o", "w")
    x = [Foo(1,2.0)]
    serialize(io, x)
    close(io)
    _x = read(c, "o", Foo, 1)
    @test _x[1].x == x[1].x
    @test _x[1].y ≈ x[1].y
end
