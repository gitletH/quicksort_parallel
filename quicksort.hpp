#ifndef QUICKSORT_HPP
#define QUICKSORT_HPP

#include <string>
#include <vector>
#include "CTPL/ctpl_stl.h"

// Helpers
std::vector<std::string> split_string_by_comma(const std::string& str);

void quicksort_parallel(int id, ctpl::thread_pool* threadpool, const std::string in_filename, const std::string out_filename = "");

#endif // QUICKSORT_HPP