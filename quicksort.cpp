#include "quicksort.hpp"

#include <string>
#include <vector>
#include <fstream>
#include "CTPL/ctpl_stl.h"

using std::vector;
using std::string;

void quicksort_parallel(int id, ctpl::thread_pool* threadpool, const std::string in_filename, const std::string out_filename) {
    // If the file is empty, no more sorting is needed to be done
    // This base case is very naive. But optimizting it is not in the scope of this project
    std::ifstream ifile(in_filename);
    const bool empty_input = ifile.peek() == std::ifstream::traits_type::eof();
    if (empty_input) return;

    // Choose a pivot
    // Again, this is really naive, but whatever
    string pivot;
    std::getline(ifile, pivot);

    // Partition the data
    // x goes into large_partition_file if x >= pivot.
    // Otherwise, x goes into small_partition_file.

    string line;
    while(std::getline(ifile, line)) {
        
    }

    // Push jobs into queue

    // Merge results
}



std::vector<std::string> split_string_by_comma(const string& str) {
    vector<string> rtn;
    string temp;
    for (const char& c : str) {
        if (c != ',' || c != ' ') {
            temp += c;
        } else {
            if (!temp.empty()) {
                rtn.push_back(temp);
            }
        }
    }
    return rtn;
}