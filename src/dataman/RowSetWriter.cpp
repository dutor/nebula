/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "dataman/RowSetWriter.h"

namespace nebula {

using nebula::meta::SchemaProviderIf;

RowSetWriter::RowSetWriter(std::shared_ptr<const SchemaProviderIf> schema,
                           int64_t reservedSize)
        : schema_(std::move(schema)) {
    data_.reserve(reservedSize);
}


void RowSetWriter::writeRowLength(int64_t len) {
    VLOG(3) << "Write row length " << len;
    uint8_t buf[10];
    size_t lenBytes = folly::encodeVarint(len, buf);
    DCHECK_GT(lenBytes, 0UL);
    data_.append(reinterpret_cast<char*>(buf), lenBytes);
}


void RowSetWriter::writeRowLength(folly::IOBuf *buf) {
    auto len = buf->computeChainDataLength();
    uint8_t bytes[10];
    len = folly::encodeVarint(len, bytes);
    DCHECK_GE(buf->headroom(), len);
    buf->prepend(len);
    auto *beg = buf->writableData();
    ::memcpy(beg, bytes, len);
}


void RowSetWriter::addRow(RowWriter& writer) {
    writeRowLength(writer.size());
    writer.encodeTo(data_);
}


void RowSetWriter::addRow(RowWriter &&writer) {
    auto row = writer.move();
    writeRowLength(row.get());
    if (!head_) {
        head_ = std::move(row);
        return;
    }
    head_->prependChain(std::move(row));
}


void RowSetWriter::addRow(std::unique_ptr<folly::IOBuf> row) {
    DCHECK(!row->isShared());
    writeRowLength(row.get());
    if (!head_) {
        head_ = std::move(row);
        return;
    }
    head_->prependChain(std::move(row));
}


void RowSetWriter::addRow(const std::string& data) {
    writeRowLength(data.size());
    data_.append(data);
}

void RowSetWriter::addAll(const std::string& data) {
    data_.append(data);
}


void RowSetWriter::addAll(std::unique_ptr<folly::IOBuf> rows) {
    if (!head_) {
        head_ = std::move(rows);
        return;
    }
    head_->prependChain(std::move(rows));
}

}  // namespace nebula

