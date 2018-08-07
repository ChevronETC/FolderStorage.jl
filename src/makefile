all:
	gcc -O3 -fopenmp -fPIC -c FolderStorage.c
	gcc -shared -fopenmp -o libFolderStorage.so FolderStorage.o
	cp libFolderStorage.so ../deps/usr/lib/

clean:
	rm -rf *.so *.o
