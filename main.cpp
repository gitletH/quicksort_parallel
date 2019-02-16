#include <algorithm>
#include <cassert>
#include <string>
#include <vector>

// #include "CTPL/cptl.h"
#include "quicksort.hpp"

using std::string;
using std::vector;

int main (int argc, char* argv[]) {
    // Parse input arguments
    assert(argc == 5);
    const string in_filename(argv[1]);
    const string out_filename(argv[2]);
    const vector<string> temp = split_string_by_comma(argv[3]);
    vector<int> columns_to_sort;
    std::transform(temp.begin(), temp.end(), std::back_inserter(columns_to_sort), [](const string& str) -> int { return std::stoi(str); });
    const int num_threads = std::stoi(argv[4]);

    // Allocate threadpool and do work
    ctpl::thread_pool threadpool(num_threads);
    threadpool.push(quicksort_parallel, &threadpool, columns_to_sort, 1, in_filename, out_filename);

    return 0;
}
