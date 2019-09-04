/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "base/Cord.h"

namespace nebula {

bool Cord::apply(Visitor visitor) const {
    if (this->empty()) {
        return true;
    }
    auto iter = head_->begin();
    auto end = head_->end();
    while (iter != end) {
        auto *buf = reinterpret_cast<const char*>(iter->data());
        auto size = iter->size();
        if (!visitor(buf, size)) {
            return false;
        }
        ++iter;
    }
    return true;
}


size_t Cord::appendTo(std::string& str) const {
    if (empty()) {
        return 0;
    }

    auto size = this->size();
    str.reserve(str.size() + size);

    this->apply([&str] (auto *buf, auto len) {
        str.append(buf, len);
        return true;
    });

    return size;
}


Cord& Cord::operator<<(const Cord& rhs) {
    auto rhsSize = rhs.size();
    if (rhsSize == 0UL) {
        return *this;
    }

    auto newTail = folly::IOBuf::create(rhsSize);
    rhs.apply([dest = newTail.get()] (auto *buf, auto len) {
        ::memcpy(dest->writableTail(), buf, len);
        dest->append(len);
        return true;
    });
    head_->prependChain(std::move(newTail));
    size_ += rhsSize;

    return *this;
}


void Cord::makeRoomForWriteSlow(size_t size) {
    auto room = last()->tailroom();
    auto length = last()->length();
    auto capacity = last()->capacity();
    if (length <= capacity / 2) {
        auto need = size - room;
        last()->reserve(0, alignedSize(capacity + need));
        return;
    }

    auto newCapacity = last()->capacity() * 2;
    newCapacity = newCapacity > kMaxGrowthSize ? kMaxGrowthSize : newCapacity;
    newCapacity = newCapacity > size ? newCapacity : size;
    auto newTail = folly::IOBuf::create(newCapacity);
    head_->prependChain(std::move(newTail));
}

}  // namespace nebula
