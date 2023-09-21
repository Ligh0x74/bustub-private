//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetSize()) {
    return INVALID_PAGE_ID;
  }
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Search(const KeyType &key, const KeyComparator &comparator) const -> int {
  auto cmp = [&](const KeyType &v, const MappingType &p) { return comparator(v, p.first) < 0; };
  return std::upper_bound(array_ + 1, array_ + GetSize(), key, cmp) - array_ - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(BPlusTreeInternalPage &new_page) -> KeyType {
  auto new_array = new_page.array_;
  for (int i = GetMinSize(); i <= GetSize(); i++) {
    new_array[i - GetMinSize()] = array_[i];
  }
  new_page.SetSize(GetSize() - GetMinSize());
  SetSize(GetMinSize());
  return new_array[0].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const std::optional<KeyType> &opt, const ValueType &value,
                                            const KeyComparator &comparator) {
  if (opt == std::nullopt) {
    array_[0].second = value;
    IncreaseSize(1);
    return;
  }
  auto key = opt.value();
  int index = Search(key, comparator) + 1;
  for (int i = GetSize(); i >= index; i--) {
    array_[i + 1] = array_[i];
  }
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, const KeyComparator &comparator) {
  int index = Search(key, comparator);
  for (int i = index + 1; i < GetSize(); i++) {
    array_[i - 1] = array_[i];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Redistribute(BPlusTreeInternalPage &from, bool is_right) -> KeyType {
  if (is_right) {
    array_[GetSize()] = from.array_[0];
    IncreaseSize(1);
    for (int i = 0; i < from.GetSize() - 1; i++) {
      from.array_[i] = from.array_[i + 1];
    }
    from.IncreaseSize(-1);
    return from.array_[0].first;
  }
  for (int i = GetSize() - 1; i >= 0; i--) {
    array_[i + 1] = array_[i];
  }
  array_[0] = from.array_[GetSize() - 1];
  IncreaseSize(1);
  from.IncreaseSize(-1);
  return array_[0].first;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(BPlusTreeInternalPage &from) -> KeyType {
  for (int i = 0; i < from.GetSize(); i++) {
    array_[GetSize() + i] = from.array_[i];
  }
  IncreaseSize(from.GetSize());
  return from.array_[0].first;
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
