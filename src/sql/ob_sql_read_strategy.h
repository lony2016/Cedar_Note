/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_read_strategy.h
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#ifndef _OB_SQL_READ_STRATEGY_H
#define _OB_SQL_READ_STRATEGY_H 1

#include "ob_sql_expression.h"
#include "common/ob_schema.h"
#include "common/ob_obj_cast.h"
#include "common/ob_range2.h"

namespace oceanbase
{
  namespace sql
  {
    /*
     * 通过sql语句的where条件判断使用get还是使用scan
     *
     * exmaple: select * from t where pk1 = 0 and pk2 = 1;
     * exmaple: select * from t where pk1, pk2 in ((0,1))
     *
     */
    class ObSqlReadStrategy//slwang note:Scan:对单表的查询方法，如果该表的filter里面中没有全主键，则方法为scan
                           //             Get:对单表的查询方法，如果该表的filter中确定了全主键，则方法为get
    {
      public:
        ObSqlReadStrategy();
        virtual ~ObSqlReadStrategy();

        void reset();
        //add wanglei [semi join] 20170417:b
        void reset_for_semi_join();
        void remove_last_inexpr();
        void remove_last_expr();
        //add wanglei [semi join] 20170417:e
        //add xsl [semi join]
        //ObSEArray::iterator<ObSqlExpression> get_simple_cond_filter_list_begin();
        //ObSEArray::iterator<ObSqlExpression> get_simple_cond_filter_list_end();
        //add e
        inline void set_rowkey_info(const common::ObRowkeyInfo &rowkey_info)
        {
          rowkey_info_ = &rowkey_info;
        }

        int add_filter(const ObSqlExpression &expr);
        int get_read_method(ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &objs_allocator, int32_t &read_method);
        void destroy();

        int find_rowkeys_from_equal_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &objs_allocator);
        int find_rowkeys_from_in_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, common::PageArena<ObObj,common::ModulePageAllocator> &objs_allocator);
        int find_scan_range(ObNewRange &range, bool &found, bool single_row_only);
        //add wanglei [semi join in expr] 20161130:b
        typedef struct Rowkey_Objs
        {
            common::ObObj row_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];

        }RowKey_Objs,*pRowKey_Objs;
        struct Comparer ;
        int sort_mutiple_range() ;
        int print_ranges() const;
        int malloc_rowkey_objs_space( int64_t num);
        int find_scan_range(bool &found, bool single_row_only);
        bool has_next_range();
        int get_next_scan_range(ObNewRange &range,bool &has_next);
        int find_closed_column_range_simple_con(bool real_val, const ObRowkeyColumn *column,int64_t column_idx, int64_t idx_of_ranges, int64_t cond_op, ObObj cond_val, bool &found_start, bool &found_end, bool single_row_only);
        int find_closed_column_range_simple_btw(bool real_val, const ObRowkeyColumn *column, int64_t column_idx, int64_t idx_of_ranges, /*int64_t cond_op, ObObj cond_val,*/ ObObj cond_start, ObObj cond_end,  bool &found_start, bool &found_end, bool single_row_only);
        int resolve_close_column_range_in(bool real_val, const ObRowkeyColumn *column, ObObj cond_val,int64_t column_idx, int64_t idx_of_ranges, bool &found_start, bool &found_end, bool single_row_only);
        int find_closed_column_range_ex(bool real_val, int64_t idx, uint64_t column_id, bool &found_start, bool &found_end, bool single_row_only/*,bool ignore_in_expr*/);
        int compare_range(const sql::ObSqlReadStrategy::RowKey_Objs objs1,const sql::ObSqlReadStrategy::RowKey_Objs objs2) const;
        int eraseDuplicate(pRowKey_Objs tmp_start_rowkeys, pRowKey_Objs tmp_end_rowkeys, bool forward, int64_t position) ;
        int release_rowkey_objs();
        //add wanglei [semi join in expr] 20161130:e
        int assign(const ObSqlReadStrategy *other, ObPhyOperator *owner_op = NULL);
        int64_t to_string(char* buf, const int64_t buf_len) const;
      public:
        static const int32_t USE_METHOD_UNKNOWN = 0;
        static const int32_t USE_SCAN = 1;
        static const int32_t USE_GET = 2;

      private:
        static const int64_t COMMON_FILTER_NUM = 8;
        ObSEArray<ObSqlExpression, COMMON_FILTER_NUM> simple_in_filter_list_;
        ObSEArray<ObSqlExpression, COMMON_FILTER_NUM> simple_cond_filter_list_;
        const common::ObRowkeyInfo *rowkey_info_;
        common::ObObj start_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        common::ObObj end_key_objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        char* start_key_mem_hold_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        char* end_key_mem_hold_[OB_MAX_ROWKEY_COLUMN_NUMBER];
        //add wanglei [semi join in expr] 20161130:b
        int64_t range_count_ ;
        int64_t range_count_cons_ ;
        int64_t idx_key_  ;
        common::ObObj * mutiple_start_key_objs_;
        common::ObObj *mutiple_end_key_objs_ ;
        char**mutiple_start_key_mem_hold_ ;
        char**mutiple_end_key_mem_hold_ ;
        int64_t in_sub_query_idx_;
        //add wanglei [semi join in expr] 20161130:e
      private:
        int find_single_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found);
        int find_closed_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found_start, bool &found_end, bool single_row_only);
    };
  }
}

#endif /* _OB_SQL_READ_STRATEGY_H */
