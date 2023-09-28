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

  buffer_header_lock_ = new std::mutex[pool_size_];
  bm_io_in_progress_ = new int[pool_size_];
  bm_io_cond_ = new std::condition_variable[pool_size_];

  //  latch_.lock();
  //  std::cout << "BufferPoolManager:" << " pool_size_ " << pool_size_ << std::endl;
  //  latch_.unlock();
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete[] buffer_header_lock_;
  delete[] bm_io_in_progress_;
  delete[] bm_io_cond_;
}

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id = -1;

  std::unique_lock<std::mutex> buffer_strategy_lock(buffer_strategy_lock_);
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  buffer_strategy_lock.unlock();

  std::unique_lock<std::shared_mutex> buf_mapping_lock(buf_mapping_lock_);
  if (frame_id == -1) {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
  }

  *page_id = AllocatePage();
  page_table_.erase(pages_[frame_id].page_id_);
  page_table_.emplace(*page_id, frame_id);
  std::unique_lock<std::mutex> buffer_header_lock(buffer_header_lock_[frame_id]);
  buf_mapping_lock.unlock();
  if (pages_[frame_id].is_dirty_) {
    pages_[frame_id].is_dirty_ = false;
    bm_io_in_progress_[frame_id] = 1;
    buffer_header_lock.unlock();
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    buffer_header_lock.lock();
  }

  bm_io_in_progress_[frame_id] = 0;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_++;
  buffer_header_lock.unlock();
  bm_io_cond_[frame_id].notify_all();

  replacer_->RecordAccess(frame_id);
  // replacer_->SetEvictable(frame_id, false);

  //  latch_.lock();
  //  std::cout << "NewPage:" << " page_id " << *page_id << " frame_id " << frame_id << std::endl;
  //  latch_.unlock();

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t frame_id = -1;

  std::unique_lock<std::shared_mutex> buf_mapping_lock(buf_mapping_lock_);
  if (page_table_.count(page_id) == 1) {
    frame_id = page_table_.at(page_id);
    std::unique_lock<std::mutex> buffer_header_lock(buffer_header_lock_[frame_id]);
    if (pages_[frame_id].pin_count_++ == 0) {
      replacer_->SetEvictable(frame_id, false);
    }
    buf_mapping_lock.unlock();
    bm_io_cond_[frame_id].wait(buffer_header_lock, [&] {
      if (access_type == AccessType::Get) {
        return bm_io_in_progress_[frame_id] != 1;
      }
      return bm_io_in_progress_[frame_id] == 0;
    });
    buffer_header_lock.unlock();
  } else {
    std::unique_lock<std::mutex> buffer_strategy_lock(buffer_strategy_lock_);
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    }
    buffer_strategy_lock.unlock();

    if (frame_id == -1) {
      if (!replacer_->Evict(&frame_id)) {
        return nullptr;
      }
    }

    page_table_.erase(pages_[frame_id].page_id_);
    page_table_.emplace(page_id, frame_id);
    std::unique_lock<std::mutex> buffer_header_lock(buffer_header_lock_[frame_id]);
    buf_mapping_lock.unlock();

    if (pages_[frame_id].is_dirty_) {
      pages_[frame_id].is_dirty_ = false;
      bm_io_in_progress_[frame_id] = 1;
      buffer_header_lock.unlock();
      disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
      buffer_header_lock.lock();
    }

    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_++;
    bm_io_in_progress_[frame_id] = 1;
    buffer_header_lock.unlock();

    disk_manager_->ReadPage(pages_[frame_id].page_id_, pages_[frame_id].data_);
    buffer_header_lock.lock();
    bm_io_in_progress_[frame_id] = 0;
    buffer_header_lock.unlock();
    bm_io_cond_[frame_id].notify_all();

    // replacer_->SetEvictable(frame_id, false);
  }

  replacer_->RecordAccess(frame_id, access_type);

  //  latch_.lock();
  //  std::cout << "FetchPage:" << " page_id " << page_id << " frame_id " << frame_id << std::endl;
  //  latch_.unlock();

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  //  latch_.lock();
  //  std::cout << "UnpinPage:" << " page_id " << page_id << std::endl;
  //  latch_.unlock();

  std::shared_lock<std::shared_mutex> buf_mapping_lock(buf_mapping_lock_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_.at(page_id);
  std::unique_lock<std::mutex> buffer_header_lock(buffer_header_lock_[frame_id]);
  buf_mapping_lock.unlock();

  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  //  latch_.lock();
  //  std::cout << "FlushPage:" << " page_id " << page_id << std::endl;
  //  latch_.unlock();

  std::shared_lock<std::shared_mutex> buf_mapping_lock(buf_mapping_lock_);
  if (page_table_.count(page_id) == 0) {
    return false;
  }

  frame_id_t frame_id = page_table_.at(page_id);
  std::unique_lock<std::mutex> buffer_header_lock(buffer_header_lock_[frame_id]);
  buf_mapping_lock.unlock();

  bool is_dirty = pages_[frame_id].is_dirty_;
  pages_[frame_id].is_dirty_ = false;
  if (pages_[frame_id].pin_count_++ == 0) {
    replacer_->SetEvictable(frame_id, false);  // prevent evict
  }
  bm_io_in_progress_[frame_id] = 2;  // prevent scan
  buffer_header_lock.unlock();
  if (is_dirty) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].data_);
  }

  buffer_header_lock.lock();
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  bm_io_in_progress_[frame_id] = 0;
  buffer_header_lock.unlock();
  bm_io_cond_[frame_id].notify_all();

  return true;
}

void BufferPoolManager::FlushAllPages() {
  //  latch_.lock();
  //  std::cout << "FlushAllPages" << std::endl;
  //  latch_.unlock();

  std::shared_lock<std::shared_mutex> buf_mapping_lock(buf_mapping_lock_);
  std::unordered_map<frame_id_t, std::thread> threads;
  for (auto &[page_id, frame_id] : page_table_) {
    std::unique_lock<std::mutex> buffer_header_lock(buffer_header_lock_[frame_id]);
    if (pages_[frame_id].is_dirty_) {
      pages_[frame_id].is_dirty_ = false;
      if (pages_[frame_id].pin_count_++ == 0) {
        replacer_->SetEvictable(frame_id, false);  // prevent evict
      }
      bm_io_in_progress_[frame_id] = 2;  // prevent scan
      std::thread t(&DiskManager::WritePage, disk_manager_, pages_[frame_id].page_id_, pages_[frame_id].data_);
      threads.emplace(frame_id, std::move(t));
    }
  }
  buf_mapping_lock.unlock();

  for (auto &[frame_id, t] : threads) {
    t.join();
    std::unique_lock<std::mutex> buffer_header_lock(buffer_header_lock_[frame_id]);
    if (--pages_[frame_id].pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    bm_io_in_progress_[frame_id] = 0;
    buffer_header_lock.unlock();
    bm_io_cond_[frame_id].notify_all();
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  //  latch_.lock();
  //  std::cout << "DeletePage:" << " page_id " << page_id << std::endl;
  //  latch_.unlock();

  std::unique_lock<std::shared_mutex> buf_mapping_lock(buf_mapping_lock_);
  if (page_table_.count(page_id) == 0) {
    return true;
  }

  frame_id_t frame_id = page_table_.at(page_id);
  std::unique_lock<std::mutex> buffer_header_lock(buffer_header_lock_[frame_id]);

  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  buf_mapping_lock.unlock();

  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  buffer_header_lock.unlock();

  std::unique_lock<std::mutex> buffer_strategy_lock(buffer_strategy_lock_);
  free_list_.push_back(frame_id);
  buffer_strategy_lock.unlock();

  DeallocatePage(page_id);  // need lock or not
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = FetchPage(page_id, AccessType::Get);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = FetchPage(page_id, AccessType::Scan);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
