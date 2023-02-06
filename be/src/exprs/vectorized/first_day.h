// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "exprs/vectorized/function_helper.h"
#include "exprs/vectorized/string_functions.h"
#include "time.h"

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

    inline time_t stringToDatetime(string str)
    {
        if (str.length() == 10) {
            str.append(" 00:00:00");
        }

        char *cha = (char*)str.data();             // 将string转换成char*。
        tm tm_;                                    // 定义tm结构体。
        int year, month, day, hour, minute, second;// 定义时间的各个int临时变量。
        sscanf(cha, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second);// 将string存储的日期时间，转换为int临时变量。
        tm_.tm_year = year - 1900;                 // 年，由于tm结构体存储的是从1900年开始的时间，所以tm_year为int临时变量减去1900。
        tm_.tm_mon = month - 1;                    // 月，由于tm结构体的月份存储范围为0-11，所以tm_mon为int临时变量减去1。
        tm_.tm_mday = day;                         // 日。
        tm_.tm_hour = hour;                        // 时。
        tm_.tm_min = minute;                       // 分。
        tm_.tm_sec = second;                       // 秒。
        tm_.tm_isdst = 0;                          // 非夏令时。
        time_t t_ = mktime(&tm_);               // 将tm结构体转换成time_t格式。
        return t_;                                 // 返回值。
    }

    inline std::string call_internal(string date, int date_field, int first_day_of_week) {
        if (date_field == 0) {
            return date;
        }

        time_t curr_date =  stringToDatetime(date);
        tm* curr_date_tm = localtime(&curr_date);

        time_t result_date;
        switch (date_field) {
            case 1:
                result_date = curr_date;
                break;
            case 2: {
                if (first_day_of_week < 1 || first_day_of_week > 7) {
                    throw "一周的第一天的取值区间是[1,7]";
                }
                int day_of_week = curr_date_tm ->tm_wday;
                if (first_day_of_week == day_of_week) {
                    return date;
                }

                if (first_day_of_week < day_of_week) {
                    curr_date_tm->tm_mday += first_day_of_week - day_of_week ;
                }
                else {
                    curr_date_tm->tm_mday += first_day_of_week - day_of_week -7;
                }

                result_date = mktime(curr_date_tm);
                break;
            }
            case 3:
                curr_date_tm->tm_mday = 1;
                result_date = mktime(curr_date_tm);
                break;
            default:
                result_date = curr_date;
                break;
        }

        char buf[11];
        strftime(buf, sizeof(buf), "%Y-%m-%d", localtime(&result_date));

        return buf;
    }

    inline ColumnPtr FirstDayFunction::first_day(FunctionContext* context,
                                                 const starrocks::vectorized::Columns& columns) {
        ColumnViewer<TYPE_VARCHAR> viewer(columns[0]);

        RunTimeCppType<TYPE_INT> date_field_column = ColumnHelper::get_const_value<TYPE_INT>(columns[1]);
        RunTimeCppType<TYPE_INT> first_day_week_column = ColumnHelper::get_const_value<TYPE_INT>(columns[2]);
        int date_field = abs(date_field_column);
        int first_day_of_week = abs(first_day_week_column);

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