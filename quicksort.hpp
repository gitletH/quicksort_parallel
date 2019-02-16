#ifndef QUICKSORT_HPP
#define QUICKSORT_HPP

#include "CTPL/ctpl_stl.h"
#include <string>
#include <vector>

enum DataType { NUMBER, STRING };

// Do quick sort on `in_filename` then write output to `out_filename`
// The `node_index` indicate which node is it in the recursion tree which help
// debugging
void quicksort_parallel(int id, ctpl::thread_pool *threadpool,
                        const std::vector<int> &columns_to_sort, int node_index,
                        const std::string in_filename,
                        const std::string out_filename);

// Helpers
std::vector<std::string> split_string_by_comma(const std::string &str);
// Get data type of a row
std::vector<DataType> get_datatypes(const std::string &row);
// Return true if `row_a` is smaller than `row_b`
bool isRowSmaller(const std::string &row_a, const std::string &row_b,
                  const std::vector<DataType> datatypes,
                  const std::vector<int> &columns_to_sort);

#endif // QUICKSORT_HPP