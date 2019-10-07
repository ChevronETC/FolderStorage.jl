using AbstractStorage, FolderStorage, JSON, Serialization, Test

base = get(ENV, "FOLDERSTORAGE_TESTDIR", ".")
@info "running tests in $base"

@testset "mkpath" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    @test isdir("foo")
    rm(c)
end

@testset "rm" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    rm(c)
    @test !isdir(joinpath(base,"foo"))
end

@testset "copy" begin
    c = Folder(joinpath(base,"foo"))
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
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    d = deepcopy(c)
    @test isdir(d)
    @test read!(d, "o", Vector{Float64}(undef, 10)) == x
    rm(c)
end

@testset "isfile" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    @test isfile(c, "o")
    @test !isfile(c, "j")
    rm(c)
end

@testset "filesize" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    @test filesize(c, "o") == 80
    rm(c)
end

@testset "write, canonical" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    @test read!(c.foldername*"/o", Vector{Float64}(undef, 10)) ≈ x
    rm(c)
end

@testset "read!, canonical, nthreads=$nthreads" for nthreads in (1, 4)
    c = Folder(joinpath(base,"foo"), nthreads=nthreads)
    mkpath(c)
    x = rand(10)
    write(c.foldername*"/o", x)
    @test read!(c, "o", Vector{Float64}(undef, 10)) == x
    rm(c)
end

struct Foo
    x::Int
    y::Float64
end
@testset "write, serialized" begin
    c = Folder(joinpath(base,"foo"))
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
    c = Folder(joinpath(base,"foo"), nthreads=nthreads)
    mkpath(c)
    io = open(c.foldername*"/o", "w")
    x = [Foo(1,2.0)]
    serialize(io, x)
    close(io)
    _x = read!(c, "o", Vector{Foo}(undef, 1))
    @test _x[1].x == x[1].x
    @test _x[1].y ≈ x[1].y
end

@testset "read/write string" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    write(c, "bar", "hello")
    @test read(c, "bar", String) == "hello"
    rm(c)
end

@testset "implicit pathing" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    write(c, "bar/baz", "hello")
    @test read(joinpath(c.foldername,"bar/baz"), String) == "hello"
    rm(c)
end

@testset "json" begin
    c = Container(Folder, JSON.parse(json(Folder(joinpath(base,"foo"), nthreads=2, nretry=5))))
    if isabspath(normpath(joinpath(base,"foo")))
        @test c.foldername == normpath(joinpath(base,"foo"))
    else
        @test c.foldername == normpath(joinpath(pwd(),base,"foo"))
    end
    @test c.nthreads == 2
    @test c.nretry == 5
    rm(c)
end
