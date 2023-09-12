//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> latch(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  size_t max_k_distance = 0;
  size_t earliest_timestamp = SIZE_MAX;
  for (const auto &[id, node] : node_store_) {
    if (!node.IsEvictable()) {
      continue;
    }
    if (node.IsWrite()) {
      *frame_id = id;
      break;
    }
    if (node.Size() < k_) {
      max_k_distance = SIZE_MAX;
      if (earliest_timestamp > node.Front()) {
        earliest_timestamp = node.Front();
        *frame_id = id;
      }
    } else {
      auto k_distance = current_timestamp_ - node.Front();
      if (k_distance > max_k_distance) {
        max_k_distance = k_distance;
        *frame_id = id;
      }
    }
  }
  node_store_.erase(*frame_id);
  curr_size_--;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("LRUKReplacer::Frame id is invalid.");
  }
  std::lock_guard<std::mutex> latch(latch_);
  bool is_write = access_type == AccessType::Scan;
  if (node_store_.count(frame_id) == 1) {
    node_store_.at(frame_id).Add(current_timestamp_, is_write);
  } else {
    node_store_.emplace(frame_id, LRUKNode(k_, current_timestamp_, is_write));
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    throw Exception("LRUKReplacer::Frame id is invalid.");
  }
  std::lock_guard<std::mutex> latch(latch_);
  if (node_store_.count(frame_id) == 1) {
    auto &node = node_store_.at(frame_id);
    if (node.IsEvictable() != set_evictable) {
      curr_size_ += set_evictable ? 1 : -1;
      node.Set(set_evictable);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> latch(latch_);
  if (node_store_.count(frame_id) == 1) {
    auto &node = node_store_.at(frame_id);
    if (!node.IsEvictable()) {
      throw Exception("LRUKReplacer::Frame id is non-evictable.");
    }
    node_store_.erase(frame_id);
    curr_size_--;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> latch(latch_);
  return curr_size_;
}

}  // namespace bustub
