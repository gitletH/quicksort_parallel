#include "quicksort.hpp"

#include "CTPL/ctpl_stl.h"
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <string>
#include <uuid/uuid.h>
#include <vector>

using std::string;
using std::vector;

#define DEBUG 0

// This function does THREE passes to scan the input file.
// It is very slow and naive, but whatever
void quicksort_parallel(int id, ctpl::thread_pool *threadpool,
                        std::shared_ptr<MergeMetaData> merge_meta,
                        const std::vector<int> &columns_to_sort,
                        long long node_index, const std::string in_filename,
                        const std::string out_filename, int max_row) {
#if DEBUG
  assert(node_index >= 0);
#endif

  // Do one scan to find the size of the file
  // Base case: size <= max_row
  // Sort file in-memory using std::sort for simplicity
  int size = 0;
  {
    std::ifstream ifile(in_filename);
    string line;
    while (std::getline(ifile, line)) {
      size++;
    }
  }

  if (size <= max_row) {
    if (size <= 1) {
      if (in_filename != out_filename) {
        std::ofstream(out_filename) << std::ifstream(in_filename).rdbuf();
      }
    } else {
      sort_in_memory(in_filename, out_filename, columns_to_sort);
    }
    mergeResullt(merge_meta);
    return;
  }

  // Do another scan to choose the midpoint of the file as the pivot
  // Again, this is really naive, but whatever
  string pivot;
  {
    int pivot_index = rand() % size + 1;
    std::ifstream ifile(in_filename);
    for (int i = 0; i < pivot_index; ++i) {
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
  const int right_node_index = 2 * node_index + 1;
#if DEBUG
  const string small_part_file_name = std::to_string(left_node_index) + ".tmp";
  const string large_part_file_name = std::to_string(right_node_index) + ".tmp";
#else
  const string small_part_file_name = generateUUID() + ".tmp";
  const string large_part_file_name = generateUUID() + ".tmp";
#endif
  std::ofstream small_partition(small_part_file_name);
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
  std::shared_ptr<MergeMetaData> new_merge_meta =
      std::make_shared<MergeMetaData>();
  new_merge_meta->out_filename = out_filename;
  new_merge_meta->small_filename = small_part_file_name;
  new_merge_meta->large_filename = large_part_file_name;
  new_merge_meta->parent = merge_meta;
  threadpool->push(quicksort_parallel, threadpool, new_merge_meta,
                   columns_to_sort, left_node_index, small_part_file_name,
                   small_part_file_name, max_row);
  threadpool->push(quicksort_parallel, threadpool, new_merge_meta,
                   columns_to_sort, right_node_index, large_part_file_name,
                   large_part_file_name, max_row);
  return;
}

// Implementations for helpers
void sort_in_memory(const std::string &in_filename,
                    const std::string &out_filename,
                    const std::vector<int> &columns_to_sort) {
  std::vector<string> rows;
  std::ifstream ifile(in_filename);
  for (string row; std::getline(ifile, row);) {
    rows.push_back(row);
  }
  assert(!rows.empty());
  const std::vector<DataType> datatypes = get_datatypes(rows[0]);

  std::sort(rows.begin(), rows.end(),
            [&columns_to_sort, &datatypes](const string &a, const string &b) {
              return isRowSmaller(a, b, datatypes, columns_to_sort);
            });

  std::ofstream ofile(out_filename);
  for (const string &row : rows) {
    ofile << row << std::endl;
  }
}

std::vector<std::string> split_string_by_comma(const string &str) {
  vector<string> rtn;
  string temp;
  for (const char &c : str) {
    if (c != ',' && c != ' ') {
      temp += c;
    } else {
      rtn.push_back(temp);
      temp = "";
    }
  }
  if (!temp.empty()) {
    rtn.push_back(temp);
  }
  return rtn;
}

std::vector<DataType> get_datatypes(const std::string &row) {
  std::vector<DataType> rtn;
  for (const string &str : split_string_by_comma(row)) {
    assert(!str.empty());
    char c = str[0];
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
      rtn.push_back(DataType::STRING);
    } else {
      rtn.push_back(DataType::NUMBER);
    }
  }
  return rtn;
}

bool isRowSmaller(const std::string &row_a, const std::string &row_b,
                  const std::vector<DataType> &datatypes,
                  const std::vector<int> &columns_to_sort) {
  return isRowSmaller(split_string_by_comma(row_a),
                      split_string_by_comma(row_b), datatypes, columns_to_sort);
}

bool isRowSmaller(const std::vector<std::string> &row_a,
                  const std::vector<std::string> &row_b,
                  const std::vector<DataType> &datatypes,
                  const std::vector<int> &columns_to_sort, int index) {
  if (index == columns_to_sort.size()) {
    return false;
  }
  assert(row_a.size() == row_b.size());
  assert(row_a.size() == datatypes.size());
  assert(index >= 0 && index < columns_to_sort.size());

  const int column = columns_to_sort[index];
  string a = row_a.at(column);
  string b = row_b.at(column);

  switch (datatypes[column]) {
  case DataType::STRING:
    if (a != b) {
      return a < b;
    }
    break;
  case DataType::NUMBER:
    if (std::stod(a) != std::stod(b)) {
      return std::stod(a) < std::stod(b);
    }
    break;

  default:
    assert(false);
    break;
  }
  return isRowSmaller(row_a, row_b, datatypes, columns_to_sort, index + 1);
}

void mergeResullt(std::shared_ptr<MergeMetaData> meta) {
  if (meta == nullptr) {
    return;
  }
  std::lock_guard<std::mutex>(meta->mtx);
  meta->cnt++;
  if (meta->cnt < 2) {
    return;
  }

#if DEBUG
  std::cout << "merging " << meta->small_filename << " and "
            << meta->large_filename << " to " << meta->out_filename
            << std::endl;
#endif
  {
    std::ofstream(meta->out_filename)
        << std::ifstream(meta->small_filename).rdbuf() << std::flush;
  }
  {
    std::ofstream(meta->out_filename, std::ios_base::app)
        << std::ifstream(meta->large_filename).rdbuf() << std::flush;
  }

#if !DEBUG
  std::remove(meta->small_filename.c_str());
  std::remove(meta->large_filename.c_str());
#endif
  mergeResullt(meta->parent);
  return;
}

std::string generateUUID() {
  uuid_t uuid;
  char str[36];
  uuid_generate(uuid);
  uuid_unparse(uuid, str);
  return string(str);
}