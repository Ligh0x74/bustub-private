/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, ReadPageGuard read_guard,
                                  const LeafPage *leaf_page, int index)
    : bpm_(buffer_pool_manager), read_guard_(std::move(read_guard)), leaf_page_(leaf_page), index_(index) {
  page_id_ = read_guard_.PageId();
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return leaf_page_ == nullptr || (leaf_page_->GetNextPageId() == INVALID_PAGE_ID && leaf_page_->GetSize() == index_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_->KeyValueAt(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (++index_ == leaf_page_->GetSize() && leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
    read_guard_ = bpm_->FetchPageRead(leaf_page_->GetNextPageId());
    leaf_page_ = static_cast<const LeafPage *>(read_guard_.As<BPlusTreePage>());
    index_ = 0;
    page_id_ = read_guard_.PageId();
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
  return page_id_ == itr.page_id_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
  return page_id_ != itr.page_id_ || index_ != itr.index_;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
