// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "column/binary_column.h"
#include "column/object_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "gutil/casts.h"
#include "types/hll.h"

namespace starrocks {

class AggregateUniqState {
    int64_t count = 0;

    public:
    AggregateUniqState() = default;

    AggregateUniqState(const Slice& src) {
        deserialize(src);
    }

    void merge(const AggregateUniqState& other) {
        count += other.count;
    }

    void update(uint64_t value) {
        count += value;
    }

    size_t serialize(uint8_t* dst) const {
        *reinterpret_cast<int64_t*>(dst) = count;
        return sizeof(int64_t);
    }

    bool deserialize(const Slice& slize) {
        count = *reinterpret_cast<const int64_t*>(slize.data);
        return true;
    }

    size_t serialized_size() const {
        return sizeof(int64_t);
    }

    int64_t get_value() const {
        return count;
    }

    void clear() {
        count = 0;
    }
};

/**
 * RETURN_TYPE: TYPE_BIGINT
 * ARGS_TYPE: ALL TYPE
 * SERIALIZED_TYPE: TYPE_VARCHAR
 */
template <LogicalType LT, typename T = RunTimeCppType<LT>>
class UniqAggregateFunction final
        : public AggregateFunctionBatchHelper<AggregateUniqState, UniqAggregateFunction<LT, T>> {
public:
    using ColumnType = RunTimeColumnType<LT>;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        this->data(state).clear();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        const auto& column = down_cast<const Int64Column&>(*columns[0]);
        uint64_t value = column.get_data()[row_num];
        this->data(state).update(value);
    }

    // AggStateTableKind agg_state_table_kind(bool is_append_only) const override { return AggStateTableKind::RESULT; }

    // void retract(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
    //              size_t row_num) const override {
    //     const auto& column = down_cast<const Int64Column&>(*columns[0]);
    //     this->data(state).count -= column.get_data()[row_num];
    // }

    // void update_batch_single_state(FunctionContext* ctx, size_t chunk_size, const Column** columns,
    //                                AggDataPtr __restrict state) const override {
    //     const auto* column = down_cast<const Int64Column*>(columns[0]);
    //     const auto* data = column->get_data().data();
    //     for (size_t i = 0; i < chunk_size; ++i) {
    //         this->data(state).count += data[i];
    //     }
    // }

    void update_batch_single_state_with_frame(FunctionContext* ctx, AggDataPtr __restrict state, const Column** columns,
                                              int64_t peer_group_start, int64_t peer_group_end, int64_t frame_start,
                                              int64_t frame_end) const override {
        const auto* column = down_cast<const Int64Column*>(columns[0]);

        const auto& v = column->get_data();
        for (size_t i = frame_start; i < frame_end; ++i) {
            this->data(state).update(v[i]);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());

        const auto* state_column = down_cast<const BinaryColumn*>(column);
        AggregateUniqState uniq_state(state_column->get(row_num).get_slice());
        this->data(state).merge(uniq_state);
    }

    void get_values(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* dst, size_t start,
                    size_t end) const override {
        DCHECK_GT(end, start);
        auto* column = down_cast<Int64Column*>(dst);
        int64_t result = this->data(state).get_value();

        for (size_t i = start; i < end; ++i) {
            column->get_data()[i] = result;
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        size_t size = this->data(state).serialized_size();
        uint8_t result[size];

        size = this->data(state).serialize(result);
        column->append(Slice(result, size));
    }

    // void batch_serialize(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
    //                      size_t state_offset, Column* to) const override {
    //     auto* column = down_cast<Int64Column*>(to);
    //     Buffer<int64_t>& result_data = column->get_data();
    //     for (size_t i = 0; i < chunk_size; i++) {
    //         result_data.emplace_back(this->data(agg_states[i] + state_offset).count);
    //     }
    // }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_numeric());

        auto* column = down_cast<Int64Column*>(to);
        column->append(this->data(state).get_value());
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     ColumnPtr* dst) const override {
        auto* column = down_cast<Int64Column*>((*dst).get());
        column->get_data().assign(chunk_size, 1);
    }

    // void batch_finalize_with_selection(FunctionContext* ctx, size_t chunk_size, const Buffer<AggDataPtr>& agg_states,
    //                                    size_t state_offset, Column* to,
    //                                    const std::vector<uint8_t>& selection) const override {
    //     DCHECK(to->is_numeric());
    //     int64_t values[chunk_size];
    //     size_t selected_length = 0;
    //     for (size_t i = 0; i < chunk_size; i++) {
    //         values[selected_length] = this->data(agg_states[i] + state_offset).count;
    //         selected_length += !selection[i];
    //     }
    //     if (selected_length) {
    //         CHECK(to->append_numbers(values, selected_length * sizeof(int64_t)));
    //     }
    // }

    std::string get_name() const override {
        return "uniq";
    }
};

} // namespace starrocks
