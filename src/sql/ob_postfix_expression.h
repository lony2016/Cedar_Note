<<<<<<< HEAD
﻿/**
 * Copyright (C) 2013-2016 DaSE .
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_postfix_expression.h
 * @brief postfix expression class definition
 *
 * modified by longfei��add interface: get_expr()
 * modified by Qiushi FAN: add some functions to craete a new expression
 * modified by maoxiaoxiao: add interface: get_expr_v2()
 *
 * @version CEDAR 0.2
 * @author longfei <longfei@stu.ecnu.edu.cn>
 * @author Qiushi FAN <qsfan@ecnu.cn>
 * @date 2016_07_28
 * @author maoxiaoxiao <51151500034@ecnu.edu.cn>
 * @date 2016_07_27
 */

=======
>>>>>>> refs/remotes/origin/master
/*
 * (C) 2007-2011 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * ob_postfix_expression.h is for what ...
 *
 * Version: $id: ob_postfix_expression.h, v 0.1 7/29/2011 14:39 xiaochu Exp $
 *
 * Authors:
 *   xiaochu <xiaochu.yh@taobao.com>
<<<<<<< HEAD
 *     - ��׺���ʽ��ֵ�������ڸ����е���Ҫ֧�ָ�����ֵ�ĳ���
 *
 */
=======
 *     - 后缀表达式求值，可用于复合列等需要支持复杂求值的场合
 *
 */



>>>>>>> refs/remotes/origin/master
#ifndef OCEANBASE_SQL_OB_POSTFIX_EXPRESSION_H_
#define OCEANBASE_SQL_OB_POSTFIX_EXPRESSION_H_
#include "ob_item_type.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_string_search.h"
#include "common/ob_array.h"
#include "common/ob_object.h"
#include "common/ob_result.h"
#include "common/ob_row.h"
#include "common/ob_expr_obj.h"
#include "common/ob_se_array.h"
#include "ob_phy_operator.h"
<<<<<<< HEAD

//add weixing [implementation of sub_query]20160111
#include "common/bloom_filter.h"
//add e

=======
>>>>>>> refs/remotes/origin/master
using namespace oceanbase::common;

namespace oceanbase
{
  namespace common
  {
    class ObRowkeyInfo;
  };
  namespace sql
  {
    struct ExprItem
    {
      struct SqlCellInfo{
        uint64_t tid;
        uint64_t cid;
      };

      ObItemType  type_;
      common::ObObjType data_type_;
      /* for:
               * 1. INTNUM
               * 2. BOOL
               * 3. DATE_VALUE
               * 4. query reference
               * 5. column reference
               * 6. expression reference
               * 7. num of operands
               */
      union{
        bool      bool_;
        int64_t   datetime_;
        int64_t   int_;
        float float_;
        double    double_;
        struct SqlCellInfo cell_;  // table_id, column_id
      }value_;
      // due to compile restriction, cant put string_ into union.
      // reason: ObString default constructor has parameters
<<<<<<< HEAD
      ObDecimal dec;   //add xsl ECNU_DECIMAL 2016_12
      uint32_t len;        //add xsl
=======
>>>>>>> refs/remotes/origin/master
      ObString  string_;        // const varchar obj or system function name
      public:
        int assign(const common::ObObj &obj);
    };

    enum ObSqlSysFunc
    {
      SYS_FUNC_LENGTH = 0,
      SYS_FUNC_SUBSTR,
      SYS_FUNC_CAST,
      SYS_FUNC_CUR_USER,
      SYS_FUNC_TRIM,
      SYS_FUNC_LOWER,
      SYS_FUNC_UPPER,
      SYS_FUNC_COALESCE,
      SYS_FUNC_HEX,
      SYS_FUNC_UNHEX,
      SYS_FUNC_IP_TO_INT,
      SYS_FUNC_INT_TO_IP,
      SYS_FUNC_GREATEST,
      SYS_FUNC_LEAST,
      /* SYS_FUNC_NUM is always in the tail */
      SYS_FUNC_NUM
    };

    enum ObSqlParamNumFlag
    {
      TWO_OR_THREE = -3,
      OCCUR_AS_PAIR = -2,
      MORE_THAN_ZERO = -1,
<<<<<<< HEAD
      //add fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
      /*
        * for cast????
      */
      TWO_OR_FOUR=-4,
      //add:e
=======
>>>>>>> refs/remotes/origin/master
    };

    struct ObInRowOperator
    {
      ObArray<ObExprObj> vec_;
      ObInRowOperator()
      {
      }

      int push_row(const ObExprObj *stack, int &top, const int count)
      {
        int i = 0;
        int ret = OB_SUCCESS;
        if (top < count)
        {
          TBSYS_LOG(WARN, "not enough row elements in stack. top=%d, count=%d", top, count);
          ret = OB_INVALID_ARGUMENT;
        }
        else
        {
          for (i = 0; i < count; i++)
          {
            if (OB_SUCCESS != (ret = vec_.push_back(stack[--top])))
            {
              TBSYS_LOG(WARN, "fail to push element to IN vector. ret=%d", ret);
            }
          }
        }
        return ret;
      }

      int get_result(ObExprObj *stack, int &top, ObExprObj &result)
      {
        int ret = OB_SUCCESS;
        int64_t left_start_idx = 0;
        int64_t right_start_idx = 0;
        int64_t right_elem_count = 0;
        int64_t width = 0;
        int64_t dim = 0;
        int64_t vec_top = 0;
        int64_t i = 0;
        ObExprObj cmp;
        ObExprObj width_obj;

        OB_ASSERT(NULL != stack);
        OB_ASSERT(top >= 2);

        if (OB_SUCCESS != (ret = stack[--top].get_int(right_elem_count)))
        {
          TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", top, ret);
        }
        else if (OB_SUCCESS != (ret = stack[--top].get_int(dim)))
        {
          TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", top, ret);
        }
        else
        {
          vec_top = vec_.count();
          switch (dim)
          {
            case 1:
              right_start_idx = vec_top - right_elem_count;
              left_start_idx  = right_start_idx - 1; // only 1 element
              if (OB_SUCCESS != (ret = check_is_in_row(result, left_start_idx, right_start_idx, right_elem_count, 1)))
              {
                TBSYS_LOG(WARN, "fail to check element in row. left_start_idx=%ld, right_start_idx=%ld, elem_count=%ld, width=%ld",
                    left_start_idx, right_start_idx, right_elem_count, 1L);
              }
              break;
            case 2:
              if (OB_SUCCESS != (ret = vec_.at(vec_top - 1, width_obj)))
              {
                TBSYS_LOG(WARN, "fail to get width_obj from array. vec_top=%ld, ret=%d", vec_top, ret);
              }
              else if (OB_SUCCESS != (ret = width_obj.get_int(width)))
              {
                TBSYS_LOG(WARN, "fail to get_int from stack. top=%d, ret=%d", top, ret);
              }
              else
              {
                right_start_idx = vec_top - right_elem_count - right_elem_count * width;
                left_start_idx  = right_start_idx - 1 - width;
                if (OB_SUCCESS != (ret = check_is_in_row(result, left_start_idx, right_start_idx, right_elem_count, width)))
                {
                  TBSYS_LOG(WARN, "fail to check element in row. left_start_idx=%ld, right_start_idx=%ld, elem_count=%ld, width=%ld",
                      left_start_idx, right_start_idx, right_elem_count, width);
                }
              }
              break;
            default:
              TBSYS_LOG(WARN, "invalid dim. dim=%ld", dim);
              ret = OB_ERR_UNEXPECTED;
              break;
          }
          if (OB_SUCCESS == ret)
          {
            for (i = left_start_idx; i < vec_top; i++)
            {
              vec_.pop_back();
            }
          }
        }
        return ret;
      }

      // consider cases: 10 in (null, 10) = true;
      // 10 in (null, 20) = null;
      // null in (10, 20) = null
      int check_is_in_row(ObExprObj &result, const int64_t left_start_idx,
          const int64_t right_start_idx, const int64_t right_elem_count, const int64_t width)
      {
        int64_t right_idx = 0;
        int64_t i = 0;
        int ret = OB_SUCCESS;
        ObExprObj left, right, cmp;

        bool is_in = false;
        bool has_null = false;
        for (right_idx = right_start_idx;
            OB_SUCCESS == ret && right_idx < right_start_idx + right_elem_count * width;
            right_idx += width)
        {
          is_in = true;
          for (i = 0; OB_SUCCESS == ret && i < width; i++)
          {
            if (OB_SUCCESS == (ret = vec_.at(left_start_idx + i,left)) && OB_SUCCESS == (ret= vec_.at(right_idx + i, right)))
            {
              left.eq(right, cmp);
              if (cmp.is_true())
              {
                // go forward, try to match next in current row
              }
              else // is_false or is_null
              {
                if (left.is_null() && right.is_null())
                {
                  // skip. null in null = true (special case)
                }
                else
                {
                  is_in = false;
                  if (cmp.is_null())
                  {
                    has_null = true;
                  }
                  break;
                }
              }
            }
            else
            {
              TBSYS_LOG(WARN, "fail to get element from array. ret=%d. vec.count=%ld, width[%ld], i[%ld], left_idx[%ld], right_idx[%ld]" ,
                  ret, vec_.count(), width, i, left_start_idx + i, right_idx + i);
            }
          }
          if (true == is_in)
          {
            break; // no need to search more elements
          }
        }
        if (is_in)
        {
          result.set_bool(true);
        }
        else if (has_null)
        {
          result.set_null();
        }
        else
        {
          result.set_bool(false);
        }
        return ret;
      }
    };

    struct ObPostExprExtraParams
    {
      bool did_int_div_as_double_;
      int32_t operand_count_;
      ObInRowOperator in_row_operator_;
      char varchar_buf_[OB_MAX_VARCHAR_LENGTH];
      ObStringBuf *str_buf_;
      ObPostExprExtraParams()
        :did_int_div_as_double_(false), operand_count_(0), in_row_operator_(), str_buf_(NULL)
      {
      }
    };
    typedef int(*op_call_func_t)(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);

    struct ObPostfixExpressionCalcStack
    {
      // should be larger than MAX_SQL_EXPRESSION_SYMBOL_COUNT(=256)
      static const int64_t STACK_SIZE = 512;
      ObExprObj stack_[STACK_SIZE];
    };

    class ObPostfixExpression
    {
      public:
        enum ObPostExprNodeType {
          BEGIN_TYPE = 0,
          COLUMN_IDX,
          CONST_OBJ,
          PARAM_IDX,
          SYSTEM_VAR,
          TEMP_VAR,
<<<<<<< HEAD
          ARRAY_VAR,   //add zt 20151126
=======
>>>>>>> refs/remotes/origin/master
          OP,
          CUR_TIME_OP,
          UPS_TIME_OP,
          END, /* postfix expression terminator */
<<<<<<< HEAD
          END_TYPE,
          QUERY_ID //add weixing [implementation of sub_query]20160116
=======
          END_TYPE
>>>>>>> refs/remotes/origin/master
        };
      public:
        ObPostfixExpression();
        ~ObPostfixExpression();
        ObPostfixExpression& operator=(const ObPostfixExpression &other);
        void set_int_div_as_double(bool did);

        // add expression object into array directly,
        // user assure objects of postfix expr sequence.
        int add_expr_obj(const common::ObObj &obj);
        int add_expr_item(const ExprItem &item);
        int add_expr_item_end();
<<<<<<< HEAD
        //add fanqiushi [semi_join] [0.1] 20150910:b
        /**
        * @brief create a new expression for right table scan.
        * @param an array of distinct values of left table,tid and cid of join column.
        * @param void.
        * @return Error code.
        */
        int set_for_semi_join(common::ObArray<common::ObObj> *tmp_set,uint64_t tid,uint64_t cid);
        //add:e
        void reset(void);

        /* ��row�е�ֵ���뵽expr������ */
        //mod weixing [impplementation of sub_query]20160116
        //int calc(const common::ObRow &row, const ObObj *&result);
        int calc(const common::ObRow &row, const ObObj *&result, hash::ObHashMap<common::ObRowkey, common::ObRowkey, common::hash::NoPthreadDefendMode>* hash_map, bool second_check);
        //mod e

        /*
         * �жϱ��ʽ���ͣ��Ƿ���const, column_index, etc
         * ������ʽ����Ϊcolumn_index,�򷵻�index??
         */
        int is_const_expr(bool &is_type) const;
        int is_column_index_expr(bool &is_type) const;
        int is_var_expr(bool &is_type, ObObj &var_name) const;  //add by zt 20160617
=======
        void reset(void);

        /* 将row中的值代入到expr计算结果 */
        int calc(const common::ObRow &row, const ObObj *&result);

        /*
         * 判断表达式类型：是否是const, column_index, etc
         * 如果表达式类型为column_index,则返回index值
         */
        int is_const_expr(bool &is_type) const;
        int is_column_index_expr(bool &is_type) const;
>>>>>>> refs/remotes/origin/master
        int get_column_index_expr(uint64_t &tid, uint64_t &cid, bool &is_type) const;
        int merge_expr(const ObPostfixExpression &expr1, const ObPostfixExpression &expr2, const ExprItem &op);
        bool is_empty() const;
        bool is_equijoin_cond(ExprItem::SqlCellInfo &c1, ExprItem::SqlCellInfo &c2) const;
        // NB: Ugly interface, wish it will not exist in futher. In fact, this interfaces should not appears in post-expression
        // Since it so ugly, we do not change is_simple_between() and is_simple_in_expr() because their out values are not used so far.
        // val_type: 0 - const, 1 - system variable,
        bool is_simple_condition(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &const_val, ObPostExprNodeType *val_type = NULL) const;
        bool is_simple_between(bool real_val, uint64_t &column_id, int64_t &cond_op, ObObj &cond_start, ObObj &cond_end) const;
        bool is_simple_in_expr(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
            common::PageArena<ObObj,common::ModulePageAllocator> &allocator) const;
        inline void set_owner_op(ObPhyOperator *owner_op);
        inline ObPhyOperator* get_owner_op();
        static const char *get_sys_func_name(enum ObSqlSysFunc func_id);
        static int get_sys_func_param_num(const common::ObString& name, int32_t& param_num);
        // print the postfix expression
        int64_t to_string(char* buf, const int64_t buf_len) const;

<<<<<<< HEAD
        //add weixing [implementation of sub_query]20160111
        void set_has_bloomfilter(ObBloomFilterV1 *bloom_filter) {bloom_filter_ = bloom_filter;}
        //add e
        //add wanglei [semi join in expr] 20170417:b
        bool is_in_expr_with_ex_rowkey(bool real_val, const ObRowkeyInfo &info, ObIArray<ObRowkey> &rowkey_array,
                  common::PageArena<ObObj,common::ModulePageAllocator> &rowkey_objs_allocator) const;
        //add wanglei [semi join in expr] 20170417:e
        //add wanglei [semi join secondary index] 20170417:b
        bool is_have_main_cid(uint64_t main_column_id);
        int find_cid(uint64_t &cid);
        bool is_all_expr_cid_in_indextable(uint64_t index_tid,const ObSchemaManagerV2 *sm_v2);
        bool is_this_expr_can_use_index(uint64_t &index_tid,uint64_t main_tid,const ObSchemaManagerV2 *sm_v2);
        int get_all_cloumn(ObArray<uint64_t> &column_index);
        int change_tid(uint64_t& array_index);
        int get_cid(uint64_t& cid);
        //add wanglei [semi join secondary index] 20170417:e
      public:
        // add longfei [secondary index select]
        /**
         * @brief get_type_num: get different ObPostExprNodeType's params number
         * @param idx
         * @param type
         * @return error code
         */
        int64_t get_type_num(int64_t idx,int64_t type) const;
        /**
         * @brief get_expr_by_index
         * @param index
         * @return expr_
         */
        ObObj& get_expr_by_index(int64_t index);
        // add e

        NEED_SERIALIZE_AND_DESERIALIZE;

        //add zt 20151109 :b
        int serialize_variables(char *buf, const int64_t buf_len, int64_t &pos, int64_t type, const ObObj &expr_node) const;
//        int deserialize_variables(int64_t type, const ObObj &expr_node);
        //add zt 20151109 :e
		
		bool is_expr_has_more_than_two_columns();	//add dhc [query_optimizer] 20170727
=======
        NEED_SERIALIZE_AND_DESERIALIZE;
>>>>>>> refs/remotes/origin/master
      private:
        class ExprUtil
        {
          public:
          static inline bool is_column_idx(const ObObj &obj);
          static inline bool is_const_obj(const ObObj &obj);
          static inline bool is_op(const ObObj &obj);
          static inline bool is_end(const ObObj &obj);

          static inline bool is_value(const ObObj &obj, int64_t value);
          static inline bool is_op_of_type(const ObObj &obj, ObItemType type);
        };
      private:
        ObPostfixExpression(const ObPostfixExpression &other);
        static inline int nop_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int reserved_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        /* compare function list:
         * >   gt_func
         * >=  ge_func
         * <=  le_func
         * <   lt_func
         * ==  eq_func
         * !=  neq_func
         */
        static inline int do_gt_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int gt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_ge_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int ge_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_lt_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);

        static inline int do_le_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int le_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int lt_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_eq_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int eq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_neq_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int neq_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_is_not_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int is_not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int do_is_func(const ObExprObj &obj1, const ObExprObj &obj2, ObExprObj &result);
        static inline int is_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int add_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sub_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int mul_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int div_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int mod_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int and_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int or_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int minus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int plus_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int in_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_in_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_like_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int not_btw_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_length(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_substr(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_cast(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_current_timestamp(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_cur_user(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_trim(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_lower(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_upper(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_coalesce(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_greatest(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_least(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int concat_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int left_param_end_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int row_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int arg_case_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int case_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_hex(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_unhex(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_ip_to_int(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
        static inline int sys_func_int_to_ip(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params);
<<<<<<< HEAD

        //add weixing [implementation of sub_query]20160116
        static inline int in_sub_query_func(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params, ObBloomFilterV1* bloom_filter, hash::ObHashMap<common::ObRowkey, common::ObRowkey, common::hash::NoPthreadDefendMode>* hash_map, int &sub_query_idx, bool second_check);
        static int get_result(ObExprObj *stack_i, int &idx_i, ObExprObj &result, const ObPostExprExtraParams &params, common::ObBloomFilterV1* bloom_filter, hash::ObHashMap<common::ObRowkey, common::ObRowkey, common::hash::NoPthreadDefendMode>* hash_map, int &sub_query_idx, bool second_check);
        //add e

        // ���������������ʽ�Ƿ��ʾconst����column index
        int check_expr_type(const int64_t type_val, bool &is_type, const int64_t stack_len) const;
        int get_sys_func(const common::ObString &sys_func, ObSqlSysFunc &func_type) const;
        int get_var_obj(ObPostExprNodeType type, const ObObj& expr_node, const ObObj*& val) const;
        int get_array_var(const ObObj& expr_node, int64_t idx_type, const ObObj &idx_val, const ObObj*& val) const; //add zt 20151126
=======
        // 辅助函数，检查表达式是否表示const或者column index
        int check_expr_type(const int64_t type_val, bool &is_type, const int64_t stack_len) const;
        int get_sys_func(const common::ObString &sys_func, ObSqlSysFunc &func_type) const;
        int get_var_obj(ObPostExprNodeType type, const ObObj& expr_node, const ObObj*& val) const;
>>>>>>> refs/remotes/origin/master
      private:
        static const int64_t DEF_STRING_BUF_SIZE = 64 * 1024L;
        static const int64_t BASIC_SYMBOL_COUNT = 64;
        static op_call_func_t call_func[T_MAX_OP - T_MIN_OP - 1];
        static op_call_func_t SYS_FUNCS_TAB[SYS_FUNC_NUM];
        static const char* const SYS_FUNCS_NAME[SYS_FUNC_NUM];
        static int32_t SYS_FUNCS_ARGS_NUM[SYS_FUNC_NUM];
      public:
        typedef ObSEArray<ObObj, BASIC_SYMBOL_COUNT> ExprArray;
<<<<<<< HEAD
        //add  fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
         ObObj& get_expr() ;
         int fix_varchar_and_decimal(uint32_t p,uint32_t s);
        //add e
      public:
        // add longfei [secondary index select] 20151031 :b
        // to my opinion, sometimes we need to know what the actually expr_ is, so i add this interface
        // and i used this in the series of secondary index service functions
        /**
         * @brief get_expr
         * @return expr_
         */
        const ExprArray& get_expr() const
        {
          return expr_;
        }
        // add e
        /*add maoxx [bloomfilter_join] 20160722*/
        /**
         * @brief get_expr_v2
         * @return &expr_
         */
        ExprArray* get_expr_v2()
        {
          return &expr_;
        }
        /*add e*/
        //add wanglei [semi join] 20170417:b
        inline  ExprArray & get_expr_array()
        {
          return expr_;
        }
        //add wanglei [semi join] 20170417:e
=======
>>>>>>> refs/remotes/origin/master
      private:
        ExprArray expr_;
        ObExprObj *stack_;
        bool did_int_div_as_double_;
        ObObj result_;
        ObStringBuf str_buf_;
        ObPhyOperator *owner_op_;
        ObStringBuf calc_buf_;
<<<<<<< HEAD
        //add weixing [implementation of sub_query]20160111
        common::ObBloomFilterV1 *bloom_filter_;
        //add e
    }; // class ObPostfixExpression

=======
    }; // class ObPostfixExpression
>>>>>>> refs/remotes/origin/master
    inline void ObPostfixExpression::set_int_div_as_double(bool did)
    {
      did_int_div_as_double_ = did;
    }
    inline bool ObPostfixExpression::is_empty() const
    {
      int64_t type = 0;
      return 0 == expr_.count()
        || (1 == expr_.count()
            && common::OB_SUCCESS == expr_[0].get_int(type)
            && END == type);
    }
    inline void ObPostfixExpression::reset(void)
    {
      //str_buf_.reset();
      str_buf_.clear();
      expr_.clear();
      owner_op_ = NULL;
      calc_buf_.clear();
    }
    inline void ObPostfixExpression::set_owner_op(ObPhyOperator *owner_op)
    {
      owner_op_ = owner_op;
    }
    inline ObPhyOperator* ObPostfixExpression::get_owner_op()
    {
      return owner_op_;
    }
  } // namespace commom
}// namespace oceanbae

#endif
