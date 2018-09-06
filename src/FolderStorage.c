#include <omp.h>
#include <math.h>
#include <stdio.h>
#include <time.h>

#define BUFFER_SIZE 1024
#define EXPONENTIAL_BACKOFF_FAIL 99

#define MAX(x, y) (((x) > (y)) ? (x) : (y))

int
exponential_backoff(int i)
{
    double sleeptime = 0.1*pow(2.0, (double)i);
    double sleeptime_seconds = floor(sleeptime);
    double sleeptime_nanoseconds = (long)((sleeptime - sleeptime_seconds) * 1000000000.0);

    struct timespec ts_sleeptime, ts_remainingtime;

    ts_sleeptime.tv_sec = (long)sleeptime_seconds;
    ts_sleeptime.tv_nsec = (long)sleeptime_nanoseconds;

    return nanosleep(&ts_sleeptime, &ts_remainingtime);
}

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
            printf("Warning, unable to open %s for writing (attempt %d), line %d in %s.\n", filename, iretry, __LINE__, __FILE__);
        } else {
            size_t nbytes = fwrite(data, 1, datasize, fp);
            fclose(fp);
            if (nbytes == datasize) {
                res = 0;
                break;
            }
            printf("Warning, bad write %zu/%zu bytes written, retrying, %d/%d, line %d in %s.\n", nbytes, datasize, iretry, nretry, __LINE__, __FILE__);
        }
        if (exponential_backoff(iretry) != 0) {
            res = EXPONENTIAL_BACKOFF_FAIL;
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
            printf("Warning, unable to open %s for reading (attempt %d), line %d in %s.\n", filename, iretry, __LINE__, __FILE__);
        } else {
            size_t nbytes = 0L;
            if (fseek(fp, fileoffset, SEEK_SET) == 0) {
                nbytes = fread(data, 1, datasize, fp);
                fclose(fp);
                if (nbytes == datasize) {
                    res = 0;
                    break;
                }
            }
            printf("Warning, bad read, %zu/%zu bytes read, retrying, %d/%d.\n", nbytes, datasize, iretry, nretry);
        }
        if (exponential_backoff(iretry) != 0) {
            res = EXPONENTIAL_BACKOFF_FAIL;
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
