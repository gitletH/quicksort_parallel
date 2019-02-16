#ifndef QUICKSORT_HPP
#define QUICKSORT_HPP

#include <string>
#include <vector>
#include "CTPL/ctpl_stl.h"

enum DataType {
    NUMBER,
    STRING
};

// Do quick sort on `in_filename` then write output to `out_filename`
void quicksort_parallel(int id, ctpl::thread_pool* threadpool, const std::string in_filename, const std::string out_filename = "");

// Helpers
std::vector<std::string> split_string_by_comma(const std::string& str);
// Get data type of a row
std::vector<DataType> get_datatype(const std::vector<std::string>& row);


#endif // QUICKSORT_HPP