#include "quicksort.hpp"

#include "CTPL/ctpl_stl.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

using std::string;
using std::vector;

// This function does three scans to the input file.
// It is very naive, but whatever
void quicksort_parallel(int id, ctpl::thread_pool *threadpool,
                        const std::vector<int> &columns_to_sort, int node_index,
                        const std::string in_filename,
                        const std::string out_filename) {
  // Do one scan to find the size of the file
  // If the file has only zero or one row, no more sorting is needed to be done
  // This base case is very naive. But optimizting it is not in the scope of
  // this project
  int size = 0;
  {
    std::ifstream ifile(in_filename);
    size = std::count(std::istreambuf_iterator<char>(ifile),
                      std::istreambuf_iterator<char>(), '\n');
  }
  if (size <= 1) {
    if (in_filename != out_filename) {
      std::ofstream(out_filename) << std::ifstream(in_filename).rdbuf();
    }
    return;
  }

  // Do another scan to choose the midpoint of the file as the pivot
  // Again, this is really naive, but whatever
  string pivot;
  {
    std::ifstream ifile(in_filename);
    for (int i = 0; i < size / 2; ++i) {
      std::getline(ifile, pivot);
    }
    assert(!pivot.empty());
  }

  // Get datatype(schema) of this file
  // There's overhead here, but we don't care
  const std::vector<DataType> datatypes = get_datatypes(pivot);

  // Prepare partition files
  // small is the left branch and large is the right branch
  const int left_node_index = 2 * node_index;
  const string small_part_file_name =
      std::to_string(left_node_index) + in_filename;
  std::ofstream small_partition(small_part_file_name);
  const int right_node_index = 2 * node_index + 1;
  const string large_part_file_name =
      std::to_string(right_node_index) + in_filename;
  std::ofstream large_partition(large_part_file_name);

  // Do the third scan to partition the data
  // x goes into large_partition if x >= pivot.
  // Otherwise, x goes into small_partition.
  std::ifstream ifile(in_filename);
  string line;
  while (std::getline(ifile, line)) {
    if (isRowSmaller(line, pivot, datatypes, columns_to_sort)) {
      small_partition << line << std::endl;
    } else {
      large_partition << line << std::endl;
    }
  }
  ifile.close();
  small_partition.close();
  large_partition.close();

  // Push jobs into queue
  threadpool->push(quicksort_parallel, threadpool, columns_to_sort,
                   left_node_index, small_part_file_name, small_part_file_name);
  threadpool->push(quicksort_parallel, threadpool, columns_to_sort,
                   right_node_index, large_part_file_name,
                   large_part_file_name);

  // Merge results
  std::ofstream ofile(out_filename);
  ofile << std::ifstream(large_part_file_name).rdbuf();
  ofile << std::ifstream(small_part_file_name).rdbuf();
  std::remove(large_part_file_name.c_str());
  std::remove(small_part_file_name.c_str());
}

// Implementations for helpers
std::vector<std::string> split_string_by_comma(const string &str) {
  vector<string> rtn;
  string temp;
  for (const char &c : str) {
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

std::vector<DataType> get_datatypes(const std::string &row) { return {}; }

bool isRowSmaller(const std::string &row_a, const std::string &row_b,
                  const std::vector<DataType> datatypes,
                  const std::vector<int> &columns_to_sort) {
  return row_a < row_b;
}