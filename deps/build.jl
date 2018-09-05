if !Sys.iswindows()
    try
        rm("usr/lib",recursive=true,force=true)
    catch
        warn("problem removing $pwd/usr/lib")
    end
    mkpath("usr/lib")
    cd("../src")
    run(`make`)
    run(`make clean`)
end
