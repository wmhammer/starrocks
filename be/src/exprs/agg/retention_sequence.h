// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstring>
#include <limits>
#include <type_traits>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/datum.h"
#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gen_cpp/Data_types.h"
#include "glog/logging.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "thrift/protocol/TJSONProtocol.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks::vectorized {

using ByteBufferPtr = std::shared_ptr<std::vector<char>>;

struct RetentionSequenceState {
    ByteBufferPtr context;

    void allocate(int capacity) {
        context = std::make_shared<std::vector<char>>(capacity);
    }

    uint8 serializeLength() {
        if (context == nullptr) {
            return 0;
        }

        return context->capacity();
    }

    size_t capacity() {
        if (context == nullptr) {
            return 0;
        }
        return this->context->capacity();
    }

    ByteBufferPtr getContext() {
        return context;
    }

    void setContext(ByteBufferPtr buffer) {
        this->context = std::move(buffer);
    }

    void reset() {
        if (this->serializeLength() > 0) {
            std::fill(context->begin(), context->end(), (char)0);
        }
    }
};

class RetentionSequenceAggregateFunction final
        : public AggregateFunctionBatchHelper<RetentionSequenceState, RetentionSequenceAggregateFunction> {
public:
    // 事件是首访事件时的标识
    const static constexpr int INDEX_BEGIN = 1;
    // 事件是回访事件时的标识
    const static constexpr int INDEX_END = 2;

    // 在上下文中先存储时间窗口(int) 和转化周期(int), Integer.BYTES * 2
    const static constexpr int FLAG_LENGTH = sizeof(int) * 2;

private:
    void putInt(ByteBufferPtr ptr, int index, int value) const {
        *((uint32_t *)&(*ptr)[index]) = htonl(value);
    }
    int getInt(ByteBufferPtr ptr, int index) const {
        return htonl(*((uint32_t *)&(*ptr)[index]));
    }
public:
    void update(FunctionContext* ctx, const Column** columns,
                AggDataPtr __restrict state, size_t row_num) const override {
        int event_arr = down_cast<const Int32Column *>(columns[0])->get_data()[row_num];
        int time_diff = down_cast<const Int32Column *>(columns[1])->get_data()[row_num];
        int time_window = down_cast<const Int32Column *>(columns[2])->get_data()[row_num];
        int conversion_window = down_cast<const Int32Column *>(columns[3])->get_data()[row_num];

        DCHECK_GT(time_window, 0);
        DCHECK_GT(conversion_window, 0);

        ByteBufferPtr context = this->data(state).context;
        if (context == nullptr) {
            int capacity = FLAG_LENGTH
                           + time_window * sizeof(int)
                           + (time_window + conversion_window) * sizeof(int);
            context = std::make_shared<std::vector<char>>(capacity);
            // 数据中保存 时间窗口的长度 和 转化周期的长度
            putInt(context, 0, time_window);
            putInt(context, 4, conversion_window);
        }

        // 小于等于0既不是首访事件 也不是 回访事件
        // 等于1时 只是首访事件
        // 等于2时 只是回访事件
        // 等于3时 既是首访事件 也是 回访事件，例如 买了又买
        if (event_arr <= 3 && event_arr > 0) {
            // businessTime < 0  事件时间 在1971-01-01 00::00::00之前  不做处理
            // timeDiff < 0 事件时间必生在 时间窗口左边界之前
            if (time_diff >= 0 && time_diff < time_window + conversion_window) {
                // 开始计算
                int index;
                int isRepeat = 0;

                // 是首访事件
                index = event_arr & INDEX_BEGIN;
                if (index == INDEX_BEGIN && time_diff < time_window) {
                    // 未判断时间 在输入数据时做判定  case when 或where 中添加时间的判断
                    isRepeat = 1;
                    putInt(context, FLAG_LENGTH + time_diff * sizeof(int), 1);
                }

                // 是回访事件
                index = event_arr & INDEX_END;
                if (index == INDEX_END) { // 未判断时间 在输入数据时做判定  case when 或where 中添加时间的判断
                    // 同一事件即是首访事件 也是回访事件时 将其值标识为2 和 只是回访事件 进行区分
                    int pos = FLAG_LENGTH + (time_window + time_diff) * sizeof(int);
                    if (getInt(context, pos) == 0) {
                        putInt(context, pos, 1 + isRepeat);
                    } else {
                        putInt(context, pos, 1);
                    }
                }
            }
        }

        this->data(state).setContext(context);
    }

    void merge(FunctionContext* ctx, const Column* column1, AggDataPtr __restrict state1, size_t row_num) const override {
        if (column1 == nullptr || column1->capacity() == 0) {
            return ;
        }
        auto &state = data(state1);
        const BinaryColumn *column = down_cast<const BinaryColumn *>(column1);
        auto buffer = column->get_bytes();
        int capacity = buffer.size();
        if (state.capacity() == 0) {
            ByteBufferPtr value = std::make_shared<std::vector<char>>(capacity);
            for (int i = 0; i < capacity; ++i) {
                (*value)[i] = buffer[i];

            }
            state.setContext(value);
            return;
        }
        ByteBufferPtr context = state.getContext();
        for (int index = FLAG_LENGTH; index < capacity; index += sizeof(int)) {
            int value = getInt(context, index);
            // 当前context中没有收到回访事件 则以其它context为准
            if (value == 0) {
                putInt(context, index, htonl(*((uint32_t *)&(buffer)[index])));
            }
                // 当前context中 回访事件 和 首访事件一样 则只要其它context有回访事件 即标识为有回访事件
            else if (value == 2 && htonl(*((uint32_t *)&(buffer)[index])) > 0) {
                putInt(context, index, 1);
            }
            //  context.getInt(index) 为第三种可能值1时 回访事件已存在 不需要更新
        }
        state.setContext(context);
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
       if (state != nullptr) {
           ByteBufferPtr context = this->data(state).context;
           if (context != nullptr) {
               auto *column = down_cast<BinaryColumn *>(to);
               column->append(Slice(&(*context)[0], context->capacity()));
           }
       }
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        const BinaryColumn* col_max = nullptr;
        if (src[1]->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(src[1].get());
            col_max = down_cast<const BinaryColumn*>(nullable_column->data_column().get());
        } else {
            col_max = down_cast<const BinaryColumn*>(src[1].get());
        }

        BinaryColumn* result = nullptr;
        if ((*dst)->is_nullable()) {
            auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
            result = down_cast<BinaryColumn*>(dst_nullable_column->data_column().get());

            if (src[1]->is_nullable())
                dst_nullable_column->null_column_data() =
                        down_cast<const NullableColumn*>(src[1].get())->immutable_null_column_data();
            else
                dst_nullable_column->null_column_data().resize(chunk_size, 0);

        } else {
            result = down_cast<BinaryColumn*>((*dst).get());
        }

        Bytes& bytes = result->get_bytes();
        result->get_offset().resize(chunk_size + 1);

        size_t old_size = bytes.size();
        for (size_t i = 0; i < chunk_size; ++i) {
            if (src[1]->is_null(i)) {
                auto* dst_nullable_column = down_cast<NullableColumn*>((*dst).get());
                dst_nullable_column->set_has_null(true);
                result->get_offset()[i + 1] = old_size;
            } else {
                Slice value = col_max->get(i).get_slice();
                size_t max_size = value.size;
                size_t serde_size = src[0]->serialize_size(i);
                size_t new_size = old_size + 2 * sizeof(size_t) + max_size + serde_size;
                bytes.resize(new_size);
                unsigned char* c = bytes.data() + old_size;
                memcpy(c, &max_size, sizeof(size_t));
                c += sizeof(size_t);
                memcpy(c, value.data, max_size);
                c += max_size;
                memcpy(c, &serde_size, sizeof(size_t));
                c += sizeof(size_t);
                src[0]->serialize(i, c);
                result->get_offset()[i + 1] = new_size;
                old_size = new_size;
            }
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state1, Column* to) const override {
        if (state1 == nullptr) {
            return;
        }
        ByteBufferPtr context = data(state1).context;
        if (context->capacity() > 0) {

            int timeWindow = getInt(context, 0);
            int conversionWindow = getInt(context, 4);
            int valueLength = timeWindow + (timeWindow + conversionWindow);

            std::vector<int> result(valueLength);
            for (int i = 0; i < valueLength; i++) {
                result[i] = getInt(context,(i + 2) * sizeof(int));
            }

            //  针对没有首访行为的情况返回null
            int flag = 0;
            for (int i = 0; i < timeWindow; i++) {
                flag += result[i];
            }

            if (flag == 0) {
                to->append_default();
            } else {
                if (result.size() > 0) {
                    std::string str;
                    str += std::to_string(result[0]);
                    for (int i = 1; i < result.size(); ++i) {
                        str.push_back(',');
                        str += std::to_string(result[i]);
                    }
                    auto *column = down_cast<BinaryColumn *>(to);
                    column->append(Slice(str.c_str(), str.size()));
                }
            }
        }
    }


    std::string get_name() const override { return "retention_sequence"; }
};

} // namespace starrocks::vectorized
