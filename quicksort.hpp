#ifndef QUICKSORT_HPP
#define QUICKSORT_HPP

#include "CTPL/ctpl_stl.h"
#include <memory>
#include <mutex>
#include <string>
#include <vector>

enum DataType
{
  NUMBER,
  STRING
};

// Contains the metadata for merging
// Each is a node in the merging tree
struct MergeMetaData
{
  std::mutex mtx;
  int cnt = 0;
  std::string small_filename;
  std::string large_filename;
  std::string out_filename;
  std::shared_ptr<MergeMetaData> parent;
};

// Do quick sort on `in_filename` then write output to `out_filename`
// The `node_index` indicate which node is it in the recursion tree which help
// debugging
void quicksort_parallel(int id, ctpl::thread_pool *threadpool,
                        std::shared_ptr<MergeMetaData> merge_meta,
                        const std::vector<int> &columns_to_sort,
                        long long node_index, const std::string in_filename,
                        const std::string out_filename);

// Helpers
std::vector<std::string> split_string_by_comma(const std::string &str);
// Get data type of a row
std::vector<DataType> get_datatypes(const std::string &row);
// Return true if `row_a` is smaller than `row_b`
bool isRowSmaller(const std::vector<std::string> &row_a,
                  const std::vector<std::string> &row_b,
                  const std::vector<DataType> datatypes,
                  const std::vector<int> &columns_to_sort, int index = 0);
bool isRowSmaller(const std::string &row_a, const std::string &row_b,
                  const std::vector<DataType> datatypes,
                  const std::vector<int> &columns_to_sort);
// Merge result when ready
// It is being called in the child branch, and each call increment meta->cnt
// Merge when cnt == 2
void mergeResullt(std::shared_ptr<MergeMetaData> meta);

#endif // QUICKSORT_HPP