#include <omp.h>
#include <stdio.h>

#define BUFFER_SIZE 1024

#define MAX(x, y) (((x) > (y)) ? (x) : (y))

int
writebytes(
        char   *filename,
        char   *data,
        size_t  datasize,
        int     nretry)
{
    int res = 1;
    int iretry;
    for (iretry = 0; iretry < nretry; iretry++) {
        FILE *fp = fopen(filename, "wb");
        if (fp == NULL) {
            continue;
        }
        size_t nbytes = fwrite(data, 1, datasize, fp);
        fclose(fp);
        if (nbytes == datasize) {
            res = 0;
            break;
        }
    }
    return res;
}

int
writebytes_threaded(
        char   *filename,
        char   *data,
        size_t  datasize,
        int     nthreads,
        int     nretry)
{
    size_t thread_datasize = datasize/nthreads;
    size_t thread_dataremainder = datasize%nthreads;

    int thread_res[nthreads];

#pragma omp parallel num_threads(nthreads)
{
    int threadid = omp_get_thread_num();
    size_t thread_firstbyte = threadid*thread_datasize;
    size_t _thread_datasize = thread_datasize;
    if (threadid < thread_dataremainder) {
        thread_firstbyte += threadid;
        _thread_datasize += 1;
    } else {
        thread_firstbyte += thread_dataremainder;
    }

    char thread_filename[BUFFER_SIZE];
    snprintf(thread_filename, BUFFER_SIZE, "%s-%d", filename, threadid+1);
    thread_res[threadid] = writebytes(thread_filename, data+thread_firstbyte, _thread_datasize, nretry);
} /* end of #pragma omp */
    int res = 0;
    int threadid;
    for (threadid = 0; threadid < nthreads; threadid++) {
        res = MAX(res, thread_res[threadid]);
    }
    return res;
}

int readbytes(
        char   *filename,
        char   *data,
        size_t  datasize,
        size_t  fileoffset,
        int     nretry)
{
    int res = 1;
    int iretry;
    for (iretry = 0; iretry < nretry; iretry++) {
        FILE *fp = fopen(filename, "rb");
        if (fp == NULL) {
            continue;
        }
        res = fseek(fp, fileoffset, SEEK_SET);
        if (res != 0) {
            continue;
        }
        size_t nbytes = fread(data, datasize, 1, fp);
        fclose(fp);
        if (nbytes == datasize) {
            res = 0;
            break;
        }
    }
    return res;
}

int
readbytes_threaded_single_file(
        char   *filename,
        char   *data,
        size_t  datasize,
        int     nthreads,
        int     nretry)
{
    size_t thread_datasize = datasize/nthreads;
    size_t thread_dataremainder = datasize%nthreads;

    int thread_res[nthreads];

#pragma omp parallel num_threads(nthreads)
{
    int threadid = omp_get_thread_num();
    size_t thread_firstbyte = threadid*thread_datasize;
    size_t _thread_datasize = thread_datasize;
    if (threadid < thread_dataremainder) {
        thread_firstbyte += threadid;
        _thread_datasize += 1;
    } else {
        thread_firstbyte += thread_dataremainder;
    }

    thread_res[threadid] = readbytes(filename, data+thread_firstbyte, _thread_datasize, thread_firstbyte, nretry);
} /* end of #pragma omp */
    int res = 0;
    int threadid;
    for (threadid = 0; threadid < nthreads; threadid++) {
        res = MAX(res, thread_res[threadid]);
    }
    return res;
}

int
readbytes_threaded_many_files(
        char   *filename,
        char   *data,
        size_t  datasize,
        int     nthreads,
        int     nretry)
{
    size_t thread_datasize = datasize/nthreads;
    size_t thread_dataremainder = datasize%nthreads;

    int thread_res[nthreads];

#pragma omp parallel num_threads(nthreads)
{
    int threadid = omp_get_thread_num();
    size_t thread_firstbyte = threadid*thread_datasize;
    size_t _thread_datasize = thread_datasize;
    if (threadid < thread_dataremainder) {
        thread_firstbyte += threadid;
        _thread_datasize += 1;
    } else {
        thread_firstbyte += thread_dataremainder;
    }

    char thread_filename[BUFFER_SIZE];
    snprintf(thread_filename, BUFFER_SIZE, "%s-%d", filename, threadid+1);
    thread_res[threadid] = readbytes(thread_filename, data+thread_firstbyte, _thread_datasize, 0, nretry);
} /* end of #pragma omp */
    int res = 0;
    int threadid;
    for (threadid = 0; threadid < nthreads; threadid++) {
        res = MAX(res, thread_res[threadid]);
    }
    return res;
}
