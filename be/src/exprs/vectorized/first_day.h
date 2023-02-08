// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "exprs/vectorized/function_helper.h"
#include "exprs/vectorized/string_functions.h"
#include "time.h"
#include "time_functions.h"
#include "runtime/time_types.h"
#include "types/date_value.h"
#include "runtime/datetime_value.h"
#include "types/timestamp_value.h"

using namespace std;

namespace starrocks::vectorized {
    class FirstDayFunction {
    public:
        /**
         * @param columns: [BinaryColumn, ...]
         * @return IntColumn
         */
        DEFINE_VECTORIZED_FN(first_day);
    };

    inline std::string call_internal(string date, uint8_t date_field, uint8_t first_day_of_week) {
        if (date_field == 0) {
            return date;
        }

        DateTimeValue current_date;
        current_date.from_date_str(date.c_str(), date.size());

        DateTimeValue result_date = current_date;
        switch (date_field) {
            case 2: {
                if (first_day_of_week < 1 || first_day_of_week > 7) {
                    throw "一周的第一天的取值区间是[1,7]";
                }
                uint8_t day_of_week = current_date.weekday() + 1;
                if (first_day_of_week == day_of_week) {
                    return date;
                }

                TimeInterval interval;
                if (first_day_of_week < day_of_week) {
                    interval.day = first_day_of_week - day_of_week;
                }
                else {
                    interval.day = first_day_of_week - day_of_week - 7;
                }

                result_date.date_add_interval(interval, starrocks::TimeUnit::DAY);
                break;
            }
            case 3: {
                TimeInterval interval;
                interval.day = current_date.day() - 1;
                result_date.date_add_interval(interval, starrocks::TimeUnit::DAY);
                break;
            }
            default:
                break;
        }

        char buf[11];
        result_date.to_string(buf);
        return buf;
    }

    inline ColumnPtr FirstDayFunction::first_day(FunctionContext* context,
                                                 const starrocks::vectorized::Columns& columns) {
        ColumnViewer<TYPE_VARCHAR> viewer(columns[0]);

        uint8_t date_field = *columns[1]->raw_data();
        uint8_t first_day_of_week = *columns[2]->raw_data();

        size_t size = columns[0]->size();
        ColumnBuilder<TYPE_VARCHAR> builder(size);

        for (int row = 0; row < size; ++row) {
            string date = viewer.value(row).to_string();
            string result = call_internal(date, date_field, first_day_of_week);

            builder.append(result);
        }

        return builder.build(ColumnHelper::is_all_const(columns));
    }
} // namespace starrocks