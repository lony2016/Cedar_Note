/**
 * Copyright (C) 2013-2016 ECNU_DaSE.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_sql_expression.h
 * @brief sql expression class
 *
 * modified by longfei??
 * 1.add function: set_table_id()
 * modified by Qiushi FAN: add some functions to create a new expression
 * modified by maoxiaoxiao: add interface: get_decoded_expression_v3()
 * modified by zhutao:add a is_var_expr function
 *
 * @version CEDAR 0.2 
 * @author longfei <longfei@stu.ecnu.edu.cn>
 * @author Qiushi FAN <qsfan@ecnu.cn>
 * @author maoxiaoxiao <51151500034@ecnu.edu.cn>
 * @date 2016_07_27
 * @author zhutao <zhutao@stu.ecnu.edu.cn>
 *
 * @date 2016_07_30
 */

/** * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_expression.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_SQL_EXPRESSION_H
#define _OB_SQL_EXPRESSION_H 1
#include "common/ob_object.h"
#include "ob_postfix_expression.h"
#include "common/ob_row.h"
#include "common/dlist.h"
class ObAggregateFunctionTest;

namespace oceanbase
{
  namespace sql
  {

    class ObSqlExpression: public common::DLink
    {
      public:
        typedef ObSEArray<ObObj, 64> ExprArray;//add wanglei [semi join] 20170417
        ObSqlExpression();
        virtual ~ObSqlExpression();

        ObSqlExpression(const ObSqlExpression &other);
        ObSqlExpression& operator=(const ObSqlExpression &other);

        void set_int_div_as_double(bool did);

        void set_tid_cid(const uint64_t tid, const uint64_t cid);
        const uint64_t get_column_id() const;
        const uint64_t get_table_id() const;

        /**
         * @brief set_table_id
         * @param tid
         */
        void set_table_id(uint64_t tid); // add longfei [secondary index select] 20151102 e

        void set_aggr_func(ObItemType aggr_fun, bool is_distinct);
        int get_aggr_column(ObItemType &aggr_fun, bool &is_distinct) const;
        /**
         * 设置表达??
         * @param expr [in] 表达式，表达方式与实现相关，目前定义为后缀表达??
         *
         * @return error code
         */
        int add_expr_obj(const ObObj &obj);
        int add_expr_item(const ExprItem &item);
        int add_expr_item_end();
        void reset();

        /**
         * 获取解码后的表达??
         */
        inline const ObPostfixExpression &get_decoded_expression() const;
        //add wenghaixing for fix insert bug decimal key 2014/10/11
        inline  ObPostfixExpression &get_decoded_expression_v2() ;
        //add e
        /*add maoxx [bloomfilter_join] 20160722*/
        /**
         * @brief get_decoded_expression_v3
         * @return &post_expr_
         */
        inline ObPostfixExpression *get_decoded_expression_v3();
        /*add e*/
        inline bool is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const;
        /**
         * 根据表达式语义对row的值进行计??
         *
         * @param row [in] 输入??
         * @param result [out] 计算结果
         *
         * @return error code
         */
        //mod weixing [implementation of sub_query]20160116
        //int calc(const common::ObRow &row, const common::ObObj *&result);
        int calc(const common::ObRow &row, const common::ObObj *&result, hash::ObHashMap<common::ObRowkey, common::ObRowkey, common::hash::NoPthreadDefendMode>* hash_map = NULL, bool second_check = false);
        //mod e
        /// 打印表达??
        int64_t to_string(char* buf, const int64_t buf_len) const;

        // check expression type
        inline int is_const_expr(bool &is_const_type) const;
        //add fanqiushi [semi_join] [0.1] 20150910:b
        /**
        * @brief create a new expression to filter the data of right table.
        * @param an array of distinct values of left table,tid and cid of join column.
        * @param void.
        * @return void.
        */
        inline void set_post_expr(common::ObArray<common::ObObj> *tmp_set,uint64_t tid,uint64_t cid);
        //add:e
        /**
         * @brief is_var_expr
         * judge whether is variable expression
         * @param is_var_type returned flag
         * @param var_name variable name
         * @return error code
         */
        inline int is_var_expr(bool &is_var_type, ObObj &var_name) const; //add by zt 20160617

        inline int is_column_index_expr(bool &is_idx_type) const;
        inline int is_simple_condition(bool &is_simple_cond_type) const;
        inline int get_column_index_expr(uint64_t &tid, uint64_t &cid, bool &is_idx_type) const;
        inline int merge_expr(const ObSqlExpression &expr1, const ObSqlExpression &expr2, const ExprItem &op);
        inline bool is_simple_condition(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &const_val, ObPostfixExpression::ObPostExprNodeType *val_type = NULL) const;
        inline bool is_simple_between(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &cond_start, ObObj &cond_end) const;
        inline bool is_simple_in_expr(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
            common::PageArena<ObObj,common::ModulePageAllocator> &allocator) const;
        inline bool is_aggr_func() const;
        inline bool is_empty() const;
        inline void set_owner_op(ObPhyOperator *owner_op);
        inline ObPhyOperator* get_owner_op();
        //add wanglei [semi join in expr ] 20170417:b
        inline bool is_in_expr_with_ex_rowkey(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
                    common::PageArena<ObObj,common::ModulePageAllocator> &allocator) const;
        //add wanglei [semi join in expr ] 20170417:e
        //add wanglei [semi join] 20170417:b
        inline ExprArray &get_expr_array();
        inline bool is_have_main_cid(uint64_t mian_column_id);
        inline int find_cid(uint64_t &column_id);
        inline bool is_all_expr_cid_in_indextable(uint64_t index_tid,const ObSchemaManagerV2 *sm_v2);
        inline bool is_this_expr_can_use_index(uint64_t &index_tid,uint64_t main_tid,const ObSchemaManagerV2 *sm_v2);
        inline int get_all_cloumn(ObArray<uint64_t> &column_index);
        int change_tid(uint64_t &array_index);
        int get_cid(uint64_t &cid);
        int copy(ObSqlExpression* res);
        //add wanglei [semi join] 20170417:e
        NEED_SERIALIZE_AND_DESERIALIZE;

        //add weixing [implementation of sub_query]20160106
        void set_has_bloomfilter();
        int get_bloom_filter(ObBloomFilterV1 *& bloom_filter) {bloom_filter = &bloom_filter_; return OB_SUCCESS;}
        //add e
        //add dhc [second index fix/query_optimizer] 20160425:b
        inline bool is_expr_has_more_than_two_columns();
        //add dhc [second index fix/query_optimizer] 20160425:e
        //add tianz [SubQuery_for_Instmt] [JHOBv0.1]20140404:b

        //add dhc tianz [SubQuery_for_Instmt/query_optimizer] [JHOBv0.1]20140404:b
        /*Exp:count on sub_query num in current expression*/
        void add_sub_query_num(){sub_query_num_++;};
        /*Exp:get sub_query num in current expression
        * @return sub_query_num
        */
        int64_t get_sub_query_num(){return sub_query_num_;};

      public:
        static ObSqlExpression* alloc();
        static void free(ObSqlExpression* ptr);
      private:
        friend class ::ObAggregateFunctionTest;
        // data members
        ObPostfixExpression post_expr_;
        uint64_t column_id_;
        uint64_t table_id_;
        bool is_aggr_func_;
        bool is_distinct_;
        ObItemType aggr_func_;
        int64_t sub_query_num_ ;						//count current expression's sub_query num

        //add weixing [implement of sub_query]20160106
        common::ObBloomFilterV1 bloom_filter_;
        bool use_bloom_filter_;
        //add e

      private:
        // method
        int serialize_basic_param(char* buf, const int64_t buf_len, int64_t& pos) const;
        int deserialize_basic_param(const char* buf, const int64_t data_len, int64_t& pos);
        int64_t get_basic_param_serialize_size(void) const;
    };
    typedef common::ObArray<ObSqlExpression, ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > ObExpressionArray;
    class ObSqlExpressionUtil
    {
      public:
        static int make_column_expr(const uint64_t tid, const uint64_t cid, ObSqlExpression &expr);
      private:
        DISALLOW_COPY_AND_ASSIGN(ObSqlExpressionUtil);
        ObSqlExpressionUtil();
        ~ObSqlExpressionUtil();
    };

    inline void ObSqlExpression::reset(void)
    {
      DLink::reset();
      post_expr_.reset();
      column_id_ = OB_INVALID_ID;
      table_id_ = OB_INVALID_ID;
      is_aggr_func_ = is_distinct_ = false;
      //add weixing [implementation of sub_query]20160106
      if(use_bloom_filter_)
      {
        bloom_filter_.destroy();
        use_bloom_filter_ = false;
      }
      //add e
      sub_query_num_ = 0;
    }

    inline void ObSqlExpression::set_int_div_as_double(bool did)
    {
      post_expr_.set_int_div_as_double(did);
    }

    inline void ObSqlExpression::set_tid_cid(const uint64_t tid, const uint64_t cid)
    {
      table_id_ = tid;
      column_id_ = cid;
    }

    inline const uint64_t ObSqlExpression::get_column_id() const
    {
      return column_id_;
    }

    inline const uint64_t ObSqlExpression::get_table_id() const
    {
      return table_id_;
    }

    // add longfei [secondary index select] 20151102 :b
    inline void ObSqlExpression::set_table_id(uint64_t tid)
    {
            table_id_ = tid;
    }
    // add e

    inline int ObSqlExpression::get_aggr_column(ObItemType &aggr_fun, bool &is_distinct) const
    {
      int ret = OB_SUCCESS;
      if (!is_aggr_func_)
      {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "this expression is not an aggr function");
      }
      else
      {
        aggr_fun = aggr_func_;
        is_distinct = is_distinct_;
      }
      return ret;
    }

    inline void ObSqlExpression::set_aggr_func(ObItemType aggr_func, bool is_distinct)
    {
      OB_ASSERT(aggr_func >= T_FUN_MAX && aggr_func <= T_FUN_AVG);
      is_aggr_func_ = true;
      aggr_func_ = aggr_func;
      is_distinct_ = is_distinct;
    }

    inline const ObPostfixExpression &ObSqlExpression::get_decoded_expression() const
    {
      return post_expr_;
    }

    inline ObPostfixExpression &ObSqlExpression::get_decoded_expression_v2()
    {
      return post_expr_;
    }

    /*add maoxx [bloomfilter_join] 20160722*/
    inline ObPostfixExpression *ObSqlExpression::get_decoded_expression_v3()
    {
      return &post_expr_;
    }
    /*add e*/

    inline bool ObSqlExpression::is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const
    {
      return post_expr_.is_equijoin_cond(c1, c2);
    }
    inline int ObSqlExpression::is_const_expr(bool &is_const_type) const
    {
      return post_expr_.is_const_expr(is_const_type);
    }

    //add fanqiushi [semi_join] [0.1] 20150910:b
    inline void ObSqlExpression::set_post_expr(common::ObArray<common::ObObj> *tmp_set,uint64_t tid,uint64_t cid)
    {
      post_expr_.set_for_semi_join(tmp_set,tid,cid);
    }

    //add:e

    //add by zt 20160117:b
    inline int ObSqlExpression::is_var_expr(bool &is_var_type, ObObj &var_name) const
    {
      return post_expr_.is_var_expr(is_var_type, var_name);
    }
    //add by zt 20160117:e

    inline int ObSqlExpression::get_column_index_expr(uint64_t &tid, uint64_t &cid, bool &is_idx_type) const
    {
      return post_expr_.get_column_index_expr(tid, cid, is_idx_type);
    }
    inline int ObSqlExpression::is_column_index_expr(bool &is_idx_type) const
    {
      return post_expr_.is_column_index_expr(is_idx_type);
    }
    inline bool ObSqlExpression::is_simple_condition(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &const_val, ObPostfixExpression::ObPostExprNodeType *val_type) const
    {
      return post_expr_.is_simple_condition(real_val, column_id, cond_op, const_val, val_type);
    }
    inline bool ObSqlExpression::is_simple_between(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &cond_start, ObObj &cond_end) const
    {
      return post_expr_.is_simple_between(real_val, column_id, cond_op, cond_start, cond_end);
    }
    inline bool ObSqlExpression::is_simple_in_expr(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
        common::PageArena<ObObj,common::ModulePageAllocator>  &allocator) const
    {
      return post_expr_.is_simple_in_expr(real_val, info, rowkey_array, allocator);
    }
    inline bool ObSqlExpression::is_aggr_func() const
    {
      return is_aggr_func_;
    }
    inline bool ObSqlExpression::is_empty() const
    {
      return post_expr_.is_empty();
    }
    inline void ObSqlExpression::set_owner_op(ObPhyOperator *owner_op)
    {
      post_expr_.set_owner_op(owner_op);
    }
    inline ObPhyOperator* ObSqlExpression::get_owner_op()
    {
      return post_expr_.get_owner_op();
    }
    inline int ObSqlExpression::merge_expr(const ObSqlExpression &expr1, const ObSqlExpression &expr2, const ExprItem &op)
    {
      reset();
      return post_expr_.merge_expr(expr1.post_expr_, expr2.post_expr_, op);
    }
    //add wanglei [semi join in expr] 20161130:b
    inline bool ObSqlExpression::is_in_expr_with_ex_rowkey(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
        common::PageArena<ObObj,common::ModulePageAllocator>  &allocator) const
    {
      return post_expr_.is_in_expr_with_ex_rowkey(real_val, info, rowkey_array, allocator);
    }
    //add wanglei [semi join in expr] 20161130:e
    //add wanglei [semi join] 20170417:b
    inline ObSqlExpression::ExprArray &ObSqlExpression::get_expr_array()//add wanglei [semi join] 20161130
    {
      return post_expr_.get_expr_array();
    }
    inline bool ObSqlExpression::is_have_main_cid(uint64_t mian_column_id)
    {
        return post_expr_.is_have_main_cid(mian_column_id);
    }
    inline int ObSqlExpression::find_cid(uint64_t &column_id)
    {
        return post_expr_.find_cid(column_id);
    }
    inline bool ObSqlExpression::is_all_expr_cid_in_indextable(uint64_t index_tid,const ObSchemaManagerV2 *sm_v2)
    {
        return post_expr_.is_all_expr_cid_in_indextable(index_tid,sm_v2);
    }
    inline int ObSqlExpression::get_all_cloumn(ObArray<uint64_t> &column_index)
    {
        return post_expr_.get_all_cloumn(column_index);
    }

    inline bool ObSqlExpression::is_this_expr_can_use_index(uint64_t &index_tid,uint64_t main_tid,const ObSchemaManagerV2 *sm_v2)
    {
        return post_expr_.is_this_expr_can_use_index(index_tid,main_tid,sm_v2);
    }
    //add wanglei [semi join] 20170417:e

    //add dhc [second index fix/query_optimizer] 20160425:b
    inline bool ObSqlExpression::is_expr_has_more_than_two_columns()
    {
        return post_expr_.is_expr_has_more_than_two_columns();
    }
    //add dhc [second index fix/query_optimizer]  20160425:e

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_SQL_EXPRESSION_H */
