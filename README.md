# What is it
This is a simple parallel quicksort prototype that is capable to sort a large csv file that does not fit into the memory.

# How to use it
```
git clone --recursive https://github.com/gitletH/quicksort_parallel.git
cd quicksort_parallel
make
./parallel_quicksort <input_csv> <output_csv> <columns_to_be_sorted> <number_of_threads> [<max_row>]
```
For example
```
./parallel_quicksort sample.csv sample_out.csv "1,2" 3
```

Where max_row is the number of rows can be fitted into the memory per thread. max_row is default to 2 if not specified. Please make it large when sorting large files otherwise the performance will suffer.
For example
```
./parallel_quicksort rankings.csv rankings_out.csv "1,2" 3 10000
```

# Dependencies
It relies on libuuid on generating random filenames. To install libuuid, do `sudo apt install uuid-dev`

# Constrains
* There's no more than one copy of this program running at a time
* There's no NULL, two consecutive commas, or whitespaces in the csv

# Supported OS
* Tested on Ubuntu 18.04

# Basic ideas
* Get a threadpool to do the jobs. For simplicity, I am just gonna to find a threadpool library from Github.
* Each job read a csv file, partition the file into two, add a new job for each of the partition. After the two child jobs are finished, merge their results.
