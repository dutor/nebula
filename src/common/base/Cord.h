/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_CORD_H_
#define COMMON_BASE_CORD_H_

#include "base/Base.h"
#include <folly/io/IOBuf.h>

namespace nebula {

class Cord {
public:
    Cord() = default;

    explicit Cord(size_t reserve) {
        makeRoomForWrite(reserve);
    }

    ~Cord() = default;

    size_t size() const noexcept {
        DCHECK(head_ || size_ == 0);
        DCHECK(!head_ || size_ == head_->computeChainDataLength());
        return size_;
    }

    bool empty() const noexcept {
        return size() == 0UL;
    }

    void clear() {
        head_.reset();
        size_ = 0;
    }

    // Convert the cord content to a new string
    std::string str() const {
        std::string buf;
        appendTo(buf);
        return buf;
    }

    std::unique_ptr<folly::IOBuf> clone() const {
        DCHECK(head_ != nullptr);
        return head_->clone();
    }

    std::unique_ptr<folly::IOBuf> cloneAsOne() const {
        DCHECK(head_ != nullptr);
        return head_->cloneCoalesced();
    }

    std::unique_ptr<folly::IOBuf> move() noexcept {
        return std::move(head_);
    }

    // The caller should guarantee that `header' must not be being shared,
    // if there are subsequent writes to this Cord.
    void prependHeader(std::unique_ptr<folly::IOBuf> header) {
        auto size = header->computeChainDataLength();
        if (head_ == nullptr) {
            head_ = std::move(header);
            size_ = size;
            return;
        }
        header->prependChain(std::move(head_));
        head_ = std::move(header);
        size_ += size;
    }

    // Apply each block to the visitor until the end or the visitor
    // returns false
    using Visitor = std::function<bool(const char*, size_t)>;
    bool apply(Visitor visitor) const;

    // Append the cord content to the given string
    size_t appendTo(std::string& str) const;

    void makeRoomForWrite(size_t size) {
        if (head_ == nullptr) {
            head_ = folly::IOBuf::create(alignedSize(size));
            return;
        }

        auto room = last()->tailroom();
        if (size <= room) {
            return;
        }
        makeRoomForWriteSlow(size);
    }

    char* tail() noexcept {
        return reinterpret_cast<char*>(last()->writableTail());
    }

    void advance(size_t amount) noexcept {
        last()->append(amount);
        size_ += amount;
    }

    template <typename T>
    static constexpr bool is_char_v = std::is_same<T, char>::value ||
                                      std::is_same<T, signed char>::value ||
                                      std::is_same<T, unsigned char>::value;
    template <typename T,
              typename = std::enable_if_t<std::is_integral<T>::value ||
                                          std::is_floating_point<T>::value>>
    Cord& operator<<(T value) {
        makeRoomForWrite(sizeof(value));
        *reinterpret_cast<T*>(tail()) = value;
        advance(sizeof(value));
        return *this;
    }

    template <typename T,
              typename = std::enable_if_t<std::is_same<T, std::string>::value ||
                                          std::is_same<T, folly::StringPiece>::value ||
                                          std::is_same<T, folly::ByteRange>::value>>
    Cord& operator<<(const T &value) {
        makeRoomForWrite(value.size());
        ::memcpy(tail(), value.data(), value.size());
        advance(value.size());
        return *this;
    }

    template <typename T, typename = std::enable_if_t<is_char_v<T>>>
    Cord& operator<<(const T *value) {
        auto size = ::strlen(value);
        makeRoomForWrite(size);
        ::memcpy(tail(), value, size);
        advance(size);
        return *this;
    }

    Cord& write(const char *value, size_t len) {
        return operator<<(folly::StringPiece(value, len));
    }

    Cord& operator<<(const Cord& rhs);

private:
    static constexpr auto kBufferAlignment = 256UL;
    static_assert((kBufferAlignment & (kBufferAlignment - 1)) == 0,
                  "Alignment must be power of 2");
    static constexpr auto kMaxGrowthSize = 256UL << 10;
    size_t alignedSize(size_t size) {
        return (size + kBufferAlignment - 1) & ~(kBufferAlignment - 1);
    }

    folly::IOBuf* last() noexcept {
        return head_->prev();
    }

    void makeRoomForWriteSlow(size_t size);

private:
    using IOBufPtr = std::unique_ptr<folly::IOBuf>;
    // Since the IOBuf might be chained, and size() might be
    // accessed frequently, so we record the data size by ourselves.
    size_t          size_{0};
    IOBufPtr        head_;
};

}  // namespace nebula
#endif  // COMMON_BASE_CORD_H_

