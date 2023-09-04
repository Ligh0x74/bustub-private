//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  std::cout << pool_size << "\n";
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::FetchFrame(frame_id_t *frame_id, page_id_t page_id) -> bool {
  Page *page;
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[*frame_id];
  } else {
    if (!replacer_->Evict(frame_id)) {
      return false;
    }
    page = &pages_[*frame_id];
    page_table_.erase(page->page_id_);
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    page->is_dirty_ = false;
    page->ResetMemory();
  }
  if (page_id == INVALID_PAGE_ID) {
    page->page_id_ = AllocatePage();
  } else {
    page->page_id_ = page_id;
    disk_manager_->ReadPage(page->page_id_, page->data_);
  }
  page->pin_count_ = 1;
  page_table_.emplace(page->page_id_, *frame_id);
  replacer_->RecordAccess(*frame_id);
  replacer_->SetEvictable(*frame_id, false);
  return true;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> latch(latch_);
  frame_id_t frame_id;
  if (!FetchFrame(&frame_id, INVALID_PAGE_ID)) {
    return nullptr;
  }
  *page_id = pages_[frame_id].page_id_;
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> latch(latch_);
  frame_id_t frame_id;
  if (page_table_.count(page_id) == 0) {
    if (!FetchFrame(&frame_id, page_id)) {
      return nullptr;
    }
  } else {
    frame_id = page_table_.at(page_id);
    if (pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    ++pages_[frame_id].pin_count_;
  }
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  auto frame_id = page_table_.at(page_id);
  auto &page = pages_[frame_id];
  if (page.pin_count_ == 0) {
    return false;
  }
  if (--page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  page.is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }
  auto &page = pages_[page_table_.at(page_id)];
  if (page.is_dirty_) {
    disk_manager_->WritePage(page_id, page.data_);
    page.is_dirty_ = false;
  }
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> latch(latch_);
  for (auto &[page_id, frame_id] : page_table_) {
    if (page_table_.count(page_id) == 0) {
      continue;
    }
    auto &page = pages_[frame_id];
    if (page.is_dirty_) {
      disk_manager_->WritePage(page_id, page.data_);
      page.is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> latch(latch_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }
  auto frame_id = page_table_.at(page_id);
  auto &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }
  replacer_->Remove(frame_id);
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  page.page_id_ = INVALID_PAGE_ID;
  page.is_dirty_ = false;
  page.ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
