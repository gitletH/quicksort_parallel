# What is it

This is a simple parallel quicksort prototype that is capable to sort a large csv file that does not fit into the memory.

# How to use it

```
git clone --recursive https://github.com/gitletH/quicksort_parallel.git
cd quicksort_parallel
make
./parallel_quicksort <input csv> <output csv> <columns to be sorted> <number of threads>
```

For example

```
./parallel_quicksort sample.csv sample_out.csv "1,2" 3
```

# Basic ideas
* Get a threadpool to do the jobs. For simplicity, I am just gonna to find a threadpool library from Github.
* Each job read a csv file, partition the file into two, add a new job for each of the partition. After the two child jobs are finished, merge their results.