using AbstractStorage, FolderStorage, JSON, Serialization, Test

base = get(ENV, "FOLDERSTORAGE_TESTDIR", ".")
@info "running tests in $base"

@testset "mkpath" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    @test isdir("foo")
    rm(c)
end

@testset "rm folder" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    rm(c)
    @test !isdir(joinpath(base,"foo"))
end

@testset "rm file" begin
    c = Folder(joinpath(base,"foo"))
    write(c, "bar", rand(2))
    @test isfile(joinpath(base,"foo","bar"))
    rm(c, "bar")
    @test !isfile(joinpath(base,"foo","bar"))
    @test isdir(joinpath(base,"foo"))
    rm(c)
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
    rm(d)
end

@testset "touch" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    touch(c, "bar")
    @test isfile(c, "bar")
    @test filesize(c, "bar") == 0
    rm(c)
end

@testset "touch with sub-folder" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    touch(c, "bar/baz")
    @test isfile(c, "bar/baz")
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

@testset "read!, canonical" begin
    c = Folder(joinpath(base,"foo"))
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

@testset "read!, serialized" begin
    c = Folder(joinpath(base,"foo"))
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
    c = Container(Folder, JSON.parse(json(Folder(joinpath(base,"foo"), nretry=5))))
    if isabspath(normpath(joinpath(base,"foo")))
        @test c.foldername == normpath(joinpath(base,"foo"))
    else
        @test c.foldername == normpath(joinpath(pwd(),base,"foo"))
    end
    @test c.nretry == 5
    rm(c)
end

@testset "minimal dictionary" begin
    c = Container(Folder, JSON.parse(json(Folder(joinpath(base,"foo"), nretry=5))))
    _c = minimaldict(c)
    @test _c["foldername"] == normpath(joinpath(pwd(),base,"foo"))
    @test length(_c) == 1
end

@testset "cp file" begin
    c = Folder(joinpath(base,"foo"))
    mkpath(c)
    x = rand(10)
    write(c, "o", x)
    d = Folder(joinpath(base,"bar"))
    mkpath(d)
    cp(c, "o", d, "p")
    @test read!(d, "p", Vector{Float64}(undef,10)) ≈ x
    rm(c)
    rm(d)
end

@testset "joinpath" begin
    c = Folder(joinpath(base, "foo"))
    mkpath(c)
    o = joinpath(c, "o")
    write(o, "hello")
    @test read(o, String) == "hello"
    rm(c)
end