/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "dataman/RowWriter.h"

namespace nebula {

using cpp2::Schema;
using cpp2::SupportedType;
using meta::SchemaProviderIf;

RowWriter::RowWriter(std::shared_ptr<const SchemaProviderIf> schema)
        : schema_(std::move(schema)) {
    if (!schema_) {
        // Need to create a new schema
        schemaWriter_.reset(new SchemaWriter());
        schema_ = schemaWriter_;
    }
}


int64_t RowWriter::size() const noexcept {
    auto offsetBytes = calcOccupiedBytes(cord_.size());
    auto verBytes = 0L;
    if (schema_->getVersion() > 0) {
        verBytes = calcOccupiedBytes(schema_->getVersion());
    }
    return cord_.size()  // data length
           + offsetBytes * blockOffsets_.size()  // block offsets length
           + verBytes  // version number length
           + 1;  // Header
}


std::string RowWriter::encode() noexcept {
    std::string encoded;
    // Reserve enough space so resize will not happen
    encoded.reserve(sizeof(int64_t) * blockOffsets_.size() + cord_.size() + 11);
    encodeTo(encoded);

    return encoded;
}


void RowWriter::encodeTo(std::string& encoded) noexcept {
    if (!encoded_) {
        encodeToIOBuf();
    }
    cord_.appendTo(encoded);
}


std::unique_ptr<folly::IOBuf> RowWriter::encodeToIOBuf() {
    if (encoded_) {
        return cord_.clone();
    }

    if (!schemaWriter_) {
        operator<<(Skip(schema_->getNumFields() - colNum_));
    }

    constexpr auto maxRowLenBytes = 10UL;
    auto version = schema_->getVersion();
    auto offsetBytes = calcOccupiedBytes(cord_.size());
    auto verBytes = calcOccupiedBytes(version);
    auto headerBytes = offsetBytes * blockOffsets_.size() + verBytes + 1;
    char header = offsetBytes - 1;

    auto hbuffer = folly::IOBuf::create(headerBytes + maxRowLenBytes);
    hbuffer->advance(maxRowLenBytes);
    auto *tail = hbuffer->writableTail();

    if (version > 0) {
        header |= verBytes << 5;
        ::memcpy(tail, &header, 1);
        tail += 1;
        ::memcpy(tail, &version, verBytes);
        tail += verBytes;
    } else {
        ::memcpy(tail, &header, 1);
        tail += 1;
        headerBytes -= verBytes;
    }

    for (auto offset : blockOffsets_) {
        ::memcpy(tail, &offset, offsetBytes);
        tail += offsetBytes;
    }

    hbuffer->append(headerBytes);
    cord_.prependHeader(std::move(hbuffer));

    encoded_ = true;

    return cord_.clone();
}


Schema RowWriter::moveSchema() {
    if (schemaWriter_) {
        return schemaWriter_->moveSchema();
    } else {
        return Schema();
    }
}


int64_t RowWriter::calcOccupiedBytes(uint64_t v) const noexcept {
    if (v == 0) return 1;
    return 8 - __builtin_clzl(v) / 8;
}


/****************************
 *
 * Data Stream
 *
 ***************************/
RowWriter& RowWriter::operator<<(bool v) noexcept {
    RW_GET_COLUMN_TYPE(BOOL)

    switch (type->get_type()) {
        case SupportedType::BOOL:
            cord_ << v;
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"bool\"";
            // Output a default value
            cord_ << false;
            break;
    }

    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(float v) noexcept {
    RW_GET_COLUMN_TYPE(FLOAT)

    switch (type->get_type()) {
        case SupportedType::FLOAT:
            cord_ << v;
            break;
        case SupportedType::DOUBLE:
            cord_ << static_cast<double>(v);
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"float\"";
            cord_ << static_cast<float>(0.0);
            break;
    }

    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(double v) noexcept {
    RW_GET_COLUMN_TYPE(DOUBLE)

    switch (type->get_type()) {
        case SupportedType::FLOAT:
            cord_ << static_cast<float>(v);
            break;
        case SupportedType::DOUBLE:
            cord_ << v;
            break;
        default:
            LOG(ERROR) << "Incompatible value type \"double\"";
            cord_ << static_cast<double>(0.0);
            break;
    }

    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(const std::string& v) noexcept {
    return operator<<(folly::StringPiece(v));
}

RowWriter& RowWriter::operator<<(folly::StringPiece v) noexcept {
    RW_GET_COLUMN_TYPE(STRING)

    switch (type->get_type()) {
        case SupportedType::STRING: {
            writeInt(v.size());
            cord_ << v;
            break;
        }
        default: {
            LOG(ERROR) << "Incompatible value type \"string\"";
            writeInt(0);
            break;
        }
    }

    RW_CLEAN_UP_WRITE()
    return *this;
}


RowWriter& RowWriter::operator<<(const char* v) noexcept {
    return operator<<(folly::StringPiece(v));
}


/****************************
 *
 * Control Stream
 *
 ***************************/
RowWriter& RowWriter::operator<<(ColName&& colName) noexcept {
    DCHECK(!!schemaWriter_) << "ColName can only be used when a schema is missing";
    colName_.reset(new ColName(std::move(colName)));
    return *this;
}


RowWriter& RowWriter::operator<<(ColType&& colType) noexcept {
    DCHECK(!!schemaWriter_) << "ColType can only be used when a schema is missing";
    colType_.reset(new ColType(std::move(colType)));
    return *this;
}


RowWriter& RowWriter::operator<<(Skip&& skip) noexcept {
    DCHECK(!schemaWriter_) << "Skip can only be used when a schema is provided";
    if (skip.toSkip_ <= 0) {
        VLOG(2) << "Nothing to skip";
        return *this;
    }

    int32_t skipTo = std::min(colNum_ + skip.toSkip_,
                              static_cast<int64_t>(schema_->getNumFields()));
    for (int i = colNum_; i < skipTo; i++) {
        switch (schema_->getFieldType(i).get_type()) {
            case SupportedType::BOOL: {
                cord_ << false;
                break;
            }
            case SupportedType::INT:
            case SupportedType::TIMESTAMP: {
                writeInt(0);
                break;
            }
            case SupportedType::FLOAT: {
                cord_ << static_cast<float>(0.0);
                break;
            }
            case SupportedType::DOUBLE: {
                cord_ << static_cast<double>(0.0);
                break;
            }
            case SupportedType::STRING: {
                writeInt(0);
                break;
            }
            case SupportedType::VID: {
                cord_ << static_cast<uint64_t>(0);
                break;
            }
            default: {
                LOG(FATAL) << "Support for this value type has not been implemented";
            }
        }

        // Update block offsets
        if (i != 0 && (i >> 4 << 4) == i) {
            // We need to record block offset for every 16 fields
            blockOffsets_.emplace_back(cord_.size());
        }
    }
    colNum_ = skipTo;

    return *this;
}

}  // namespace nebula
