using AbstractStorage, Documenter, FolderStorage

makedocs(sitename="FolderStorage", modules=[FolderStorage])

deploydocs(
    repo = "github.com/ChevronETC/FolderStorage.jl.git",
)