#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.

  if (root_ == nullptr) {
    return nullptr;
  }
  auto cur = root_;
  for (char c : key) {
    if (cur->children_.count(c) == 0) {
      return nullptr;
    }
    cur = cur->children_.at(c);
  }
  auto node = dynamic_cast<const TrieNodeWithValue<T> *>(cur.get());
  return node == nullptr ? nullptr : node->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  std::shared_ptr<TrieNode> root;
  if (key.empty()) {
    if (root_ == nullptr) {
      root = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    } else {
      root = std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move(value)));
    }
    return Trie(root);
  }
  if (root_ == nullptr) {
    root = std::make_shared<TrieNode>();
  } else {
    root = std::shared_ptr<TrieNode>(root_->Clone());
  }
  auto it = key.cbegin();
  auto new_cur = root;
  if (root_ != nullptr) {
    auto old_cur = root_;
    for (; it < key.cend() - 1 && old_cur->children_.count(*it) != 0; ++it) {
      old_cur = old_cur->children_.at(*it);
      auto new_nxt = std::shared_ptr<TrieNode>(old_cur->Clone());
      new_cur->children_[*it] = new_nxt;
      new_cur = new_nxt;
    }
    if (it == key.cend() - 1 && old_cur->children_.count(*it) != 0) {
      old_cur = old_cur->children_.at(*it);
      new_cur->children_[*it] =
          std::make_shared<TrieNodeWithValue<T>>(old_cur->children_, std::make_shared<T>(std::move(value)));
      return Trie(root);
    }
  }
  for (; it < key.cend() - 1; ++it) {
    auto new_nxt = std::make_shared<TrieNode>();
    new_cur->children_[*it] = new_nxt;
    new_cur = new_nxt;
  }
  new_cur->children_[*it] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
  return Trie(root);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (root_ == nullptr) {
    return *this;
  }
  if (key.empty()) {
    return Trie(std::shared_ptr<TrieNode>(root_->TrieNode::Clone()));
  }
  std::stack<std::shared_ptr<const TrieNode>> stk;
  stk.push(root_);
  for (char c : key) {
    if (stk.top()->children_.count(c) == 0) {
      return *this;
    }
    stk.push(stk.top()->children_.at(c));
  }
  std::shared_ptr<TrieNode> root;
  auto it = key.cend();
  if (stk.top()->children_.empty()) {
    for (stk.pop(); --it > key.cbegin(); stk.pop()) {
      if (stk.top()->is_value_node_ || stk.top()->children_.size() != 1) {
        break;
      }
    }
    root = std::shared_ptr<TrieNode>(stk.top()->Clone());
    root->children_.erase(*it);
  } else {
    root = std::shared_ptr<TrieNode>(stk.top()->TrieNode::Clone());
  }
  for (stk.pop(); --it >= key.cbegin(); stk.pop()) {
    auto temp = std::shared_ptr<TrieNode>(stk.top()->Clone());
    temp->children_[*it] = root;
    root = temp;
  }
  return Trie(root);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
