<<<<<<< HEAD

/**
* Copyright (C) 2013-2016 ECNU_DaSE.
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* @file ob_select_stmt.h
* @brief the select statement class definition
*
* modified by zhutao:add a data member
*
* @version __DaSE_VERSION
* @author zhutao <zhutao@stu.ecnu.edu.cn>
* @author wangdonghui <zjnuwangdonghui@163.com>
* @date 2016_07_29
*/

=======
>>>>>>> refs/remotes/origin/master
#ifndef OCEANBASE_SQL_SELECTSTMT_H_
#define OCEANBASE_SQL_SELECTSTMT_H_

#include "ob_stmt.h"
#include "ob_raw_expr.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_array.h"
#include "common/ob_vector.h"
<<<<<<< HEAD
//delete by qx [query optimization] 20170308 :b
//add by dhc [query optimization] 20170227 :b
//#include "ob_optimizer_relation.h"
//add :e
#include "common/ob_list.h"
//delete :e
=======
>>>>>>> refs/remotes/origin/master

namespace oceanbase
{
  namespace sql
  {
<<<<<<< HEAD

    //add by qx [query optimization] :b
    class ObBaseRelStatInfo;
    class ObOptimizerRelation;
    //add :e

    //add dhc [query optimization] :b
    class ObFromItemJoinMethodHelper;
    //add :e

=======
>>>>>>> refs/remotes/origin/master
    struct SelectItem
    {
      uint64_t   expr_id_;
      bool       is_real_alias_;
      common::ObString alias_name_;
      common::ObString expr_name_;
      common::ObObjType type_;
    };

    struct OrderItem
    {
      enum OrderType
      {
        ASC,
        DESC
      };

      uint64_t   expr_id_;
      OrderType  order_type_;
    };

    struct JoinedTable
    {
      enum JoinType
      {
        T_FULL,
        T_LEFT,
        T_RIGHT,
        T_INNER,
<<<<<<< HEAD
        //add wanglei [semi join] 20170417:b
        T_SEMI,
        T_SEMI_LEFT,
        T_SEMI_RIGHT,
        //add wanglei [semi join] 20170417:e
      };
      //add by dhc [query optimization] 20170220:b
      /**
       * @brief 支持的两表连接算法
       */
      enum JoinOperator{
        SEMI_JOIN,
        HASH_JOIN,
        BLOOMFILTER_JOIN,
        MERGE_JOIN,
      };
      //add:e
=======
      };
>>>>>>> refs/remotes/origin/master

      int add_table_id(uint64_t tid) { return table_ids_.push_back(tid); }
      int add_join_type(JoinType type) { return join_types_.push_back(type); }
      int add_expr_id(uint64_t eid) { return expr_ids_.push_back(eid); }
<<<<<<< HEAD
      int add_join_exprs(ObVector<uint64_t>& exprs);
      void set_joined_tid(uint64_t tid) { joined_table_id_ = tid; }
      //add by dhc [query optimization] 20170220:b
      int add_optimized_join_operator(JoinOperator type) { return optimized_join_operator_.push_back(type); }
      //add:e

      // add lxb [logical optimizer] 20170525
      common::ObArray<uint64_t>& get_table_ids() { return table_ids_; }
      common::ObArray<uint64_t>& get_expr_ids() { return expr_ids_; }
      common::ObArray<uint64_t>& get_join_types() { return join_types_; }      
      
=======
      void set_joined_tid(uint64_t tid) { joined_table_id_ = tid; }

>>>>>>> refs/remotes/origin/master
      uint64_t   joined_table_id_;
      common::ObArray<uint64_t>  table_ids_;
      common::ObArray<uint64_t>  join_types_;
      common::ObArray<uint64_t>  expr_ids_;
<<<<<<< HEAD
      //add by dhc [query optimization] 20170220:b
      /**
       * @brief 记录决策后的两表连接算法
       */
      common::ObArray<uint64_t>  optimized_join_operator_;
      //add:e
=======
>>>>>>> refs/remotes/origin/master
    };

    struct FromItem
    {
<<<<<<< HEAD
      //add by dhc [query optimization] 20170227 :b
      FromItem()
      {
        table_id_ = OB_INVALID_ID;
        is_joined_ = false;
        from_item_rel_opt_ = NULL;
      }
      ObOptimizerRelation* from_item_rel_opt_; ///< query optimizer FromItem relation info
      //add :e
      uint64_t   table_id_;
      // false: it is the real table id
      // true: it is the joined table id //slwang note: new gen_table_id
=======
      uint64_t   table_id_;
      // false: it is the real table id
      // true: it is the joined table id
>>>>>>> refs/remotes/origin/master
      bool      is_joined_;
    };
  }

  namespace common
  {
    template <>
      struct ob_vector_traits<oceanbase::sql::SelectItem>
      {
        typedef oceanbase::sql::SelectItem* pointee_type;
        typedef oceanbase::sql::SelectItem value_type;
        typedef const oceanbase::sql::SelectItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };

    template <>
      struct ob_vector_traits<oceanbase::sql::OrderItem>
      {
        typedef oceanbase::sql::OrderItem* pointee_type;
        typedef oceanbase::sql::OrderItem value_type;
        typedef const oceanbase::sql::OrderItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };

    template <>
      struct ob_vector_traits<oceanbase::sql::FromItem>
      {
        typedef oceanbase::sql::FromItem* pointee_type;
        typedef oceanbase::sql::FromItem value_type;
        typedef const oceanbase::sql::FromItem const_value_type;
        typedef value_type* iterator;
        typedef const value_type* const_iterator;
        typedef int32_t difference_type;
      };
  }

  namespace sql
  {
    class ObSelectStmt : public ObStmt
    {
    public:
      enum SetOperator
      {
        UNION,
        INTERSECT,
        EXCEPT,
        NONE,
      };

<<<<<<< HEAD
      //add by dhc [query optimization] 20170328:b
      /**
       * @brief 支持的两表连接算法
       */
      enum JoinOperator{
        MERGE_JOIN,
        SEMI_JOIN,
        HASH_JOIN,
        BLOOMFILTER_JOIN,
      };
      //add:e

=======
>>>>>>> refs/remotes/origin/master
      ObSelectStmt(common::ObStringBuf* name_pool);
      virtual ~ObSelectStmt();

      int32_t get_select_item_size() const { return select_items_.size(); }
      int32_t get_from_item_size() const { return from_items_.size(); }
      int32_t get_joined_table_size() const { return joined_tables_.size(); }
      int32_t get_group_expr_size() const { return group_expr_ids_.size(); }
      int32_t get_agg_fun_size() const { return agg_func_ids_.size(); }
      int32_t get_having_expr_size() const { return having_expr_ids_.size(); }
      int32_t get_order_item_size() const { return order_items_.size(); }
      void assign_distinct() { is_distinct_ = true; }
      void assign_all() { is_distinct_ = false; }
      void assign_set_op(SetOperator op) { set_op_ = op; }
      void assign_set_distinct() { is_set_distinct_ = true; }
      void assign_set_all() { is_set_distinct_ = false; }
      void assign_left_query_id(uint64_t lid) { left_query_id_ = lid; }
      void assign_right_query_id(uint64_t rid) { right_query_id_ = rid; }
      int check_alias_name(ResultPlan& result_plan, const common::ObString& sAlias) const;
      uint64_t get_alias_expr_id(common::ObString& alias_name);
      uint64_t generate_joined_tid() { return gen_joined_tid_--; }
      uint64_t get_left_query_id() { return left_query_id_; }
      uint64_t get_right_query_id() { return right_query_id_; }
      uint64_t get_limit_expr_id() const { return limit_count_id_; }
      uint64_t get_offset_expr_id() const { return limit_offset_id_; }
      bool is_distinct() { return is_distinct_; }
      bool is_set_distinct() { return is_set_distinct_; }
      bool is_for_update() { return for_update_; }
      bool has_limit()
      {
        return (limit_count_id_ != common::OB_INVALID_ID || limit_offset_id_ != common::OB_INVALID_ID);
      }
      SetOperator get_set_op() { return set_op_; }
      JoinedTable* get_joined_table(uint64_t table_id);

      const SelectItem& get_select_item(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < select_items_.size());
        return select_items_[index];
      }

      const FromItem& get_from_item(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < from_items_.size());
        return from_items_[index];
      }
<<<<<<< HEAD
      
      // add by lxb on 2017/02/12
      common::ObVector<FromItem>& get_from_items()
      {
        return from_items_;
      }
      
      // add by lxb on 2017/02/17
      common::ObVector<SelectItem>& get_select_items()
      {
        return select_items_;
      }

      //add dhc [query_optimizer] 20170607 :b
      FromItem& get_from_item_for_update(int32_t index)
      {
        OB_ASSERT(0 <= index && index < from_items_.size());
        return from_items_[index];
      }
      void set_select_stmt_rel_info(ObOptimizerRelation* select_stmt_rel_info)
      {
        select_stmt_rel_info_ = select_stmt_rel_info;
      }
      ObOptimizerRelation* get_select_stmt_rel_info()
      {
        return select_stmt_rel_info_;
      }
      //add dhc :e
=======
>>>>>>> refs/remotes/origin/master

      const OrderItem& get_order_item(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < order_items_.size());
        return order_items_[index];
      }
<<<<<<< HEAD
	  //add dhc, [optimize group_order by index,query_optimizer]20170419:b
      int64_t& get_order_item_flag(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < order_items_indexed_flags_.size());
        return order_items_indexed_flags_[index];
      }
	  //add:e
=======

>>>>>>> refs/remotes/origin/master
      uint64_t get_group_expr_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < group_expr_ids_.size());
        return group_expr_ids_[index];
      }
<<<<<<< HEAD
	  //add dhc, [optimize group_order by index/query optimizer]20170419:b
      int64_t& get_group_expr_flag(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < group_expr_indexed_flags_.size());
        return group_expr_indexed_flags_[index];
      }
	  //add:e
=======

>>>>>>> refs/remotes/origin/master
      uint64_t get_agg_expr_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < agg_func_ids_.size());
        return agg_func_ids_[index];
      }

      uint64_t get_having_expr_id(int32_t index) const
      {
        OB_ASSERT(0 <= index && index < having_expr_ids_.size());
        return having_expr_ids_[index];
      }

      common::ObVector<uint64_t>& get_having_exprs()
      {
        return having_expr_ids_;
      }

      int add_group_expr(uint64_t expr_id)
      {
<<<<<<< HEAD
        group_expr_indexed_flags_.push_back(0);//add dhc, [optimize group_order by index,query_optimizer]20170419
=======
>>>>>>> refs/remotes/origin/master
        return group_expr_ids_.push_back(expr_id);
      }

      int add_agg_func(uint64_t expr_id)
      {
        return agg_func_ids_.push_back(expr_id);
      }

      int add_from_item(uint64_t tid, bool is_joined = false)
      {
        int ret = OB_SUCCESS;
        if (tid != OB_INVALID_ID)
        {
          FromItem item;
          item.table_id_ = tid;
          item.is_joined_ = is_joined;
          ret = from_items_.push_back(item);
        }
        else
        {
          ret = OB_ERR_ILLEGAL_ID;
        }
        return ret;
      }

      int add_order_item(OrderItem& order_item)
      {
<<<<<<< HEAD
        order_items_indexed_flags_.push_back(0);//add dhc, [optimize group_order by index,query_optimizer]20170419
=======
>>>>>>> refs/remotes/origin/master
        return order_items_.push_back(order_item);
      }

      int add_joined_table(JoinedTable* pJoinedTable)
      {
        return joined_tables_.push_back(pJoinedTable);
      }

      int add_having_expr(uint64_t expr_id)
      {
        return having_expr_ids_.push_back(expr_id);
      }

      void set_limit_offset(const uint64_t& limit, const uint64_t& offset)
      {
        limit_count_id_ = limit;
        limit_offset_id_ = offset;
      }

      void set_for_update(bool for_update)
      {
        for_update_ = for_update;
      }

      int check_having_ident(
          ResultPlan& result_plan,
          ObString& column_name,
          TableItem* table_item,
          ObRawExpr*& ret_expr) const;

      int add_select_item(
          uint64_t eid,
          bool is_real_alias,
          const common::ObString& alias_name,
          const common::ObString& expr_name,
          const common::ObObjType& type);

      int copy_select_items(ObSelectStmt* select_stmt);
      void print(FILE* fp, int32_t level, int32_t index = 0);
<<<<<<< HEAD
      /**
       * @brief add_raw_var_expr
       * add a variable expression
       * @param raw_expr variable expression
       * @return error code
       */
      int add_raw_var_expr(const ObRawExpr *raw_expr) { return var_expr_list_.push_back(raw_expr); }
      /**
       * @brief get_raw_var_expr
       * get variable expression list
       * @return variable expression list
       */
      const ObIArray<const ObRawExpr *> & get_raw_var_expr() const { return var_expr_list_; }

      //add qx qx [query optimization] 20170228 :b
      oceanbase::common::ObList<ObOptimizerRelation*> * get_rel_opt_list() {return &rel_opt_list;}
      common::hash::ObHashMap<uint64_t,ObBaseRelStatInfo *, common::hash::NoPthreadDefendMode> * get_table_id_statInfo_map(){return &table_id_statInfo_map_;}
      //add :e

      //add dhc [query optimization] 20170328 :b
      oceanbase::common::ObList<ObOptimizerRelation*> * get_subquery_rel_opt_list() {return &subquery_rel_opt_list_;}
      oceanbase::common::ObList<ObFromItemJoinMethodHelper*> * get_from_item_method_list() {return &from_item_method_list_;}
      oceanbase::common::ObList<ObOptimizerRelation*> * get_unaryRef_expr_rel_info_list() {return &unaryRef_expr_rel_info_list_;}
      oceanbase::common::ObList<ObOptimizerRelation*> * get_joined_from_item_rel_info_list() {return &joined_from_item_rel_info_list_;}
      oceanbase::common::ObList<int> * get_from_item_appear_order_list() {return &from_item_appear_order_list_;}
      int remove_from_item(const int32_t index)
      {
          int ret = OB_SUCCESS;
          ret = from_items_.remove(index);
          return ret;
      }
      int remove_joined_table(uint64_t table_id);
      //add :e

	  
    // add by lxb [logical optimizer] on 20170803
    int remove_from_item_by_idx(const int32_t index)
    {
        int ret = OB_SUCCESS;
        ret = from_items_.remove(index);
        return ret;
    }
    
    // add by lxb [logical optimizer] on 20170803
    
	  common::ObVector<TableItem>& get_table_items() 
      {
        return table_items_;
      }

      common::ObVector<ColumnItem>& get_column_items() 
      {
        return column_items_;
      }
=======
>>>>>>> refs/remotes/origin/master

    private:
      /* These fields are only used by normal select */
      bool    is_distinct_;
      common::ObVector<SelectItem>   select_items_;
      common::ObVector<FromItem>     from_items_;
      common::ObVector<JoinedTable*> joined_tables_;
      common::ObVector<uint64_t>     group_expr_ids_;
<<<<<<< HEAD
      common::ObVector<int64_t>      group_expr_indexed_flags_;//indexed flag, 0 false : 1 true, default false //add liumz, [optimize group_order by index]20170419
=======
>>>>>>> refs/remotes/origin/master
      common::ObVector<uint64_t>     having_expr_ids_;
      common::ObVector<uint64_t>     agg_func_ids_;

      /* These fields are only used by set select */
      SetOperator set_op_;
      bool        is_set_distinct_;
      uint64_t    left_query_id_;
      uint64_t    right_query_id_;

      /* These fields are used by both normal select and set select */
      common::ObVector<OrderItem>  order_items_;
<<<<<<< HEAD
	  common::ObVector<int64_t>    order_items_indexed_flags_;//indexed flag, 0 false : 1 true, default false //add liumz, [optimize group_order by index]20170419
      
=======
>>>>>>> refs/remotes/origin/master

      /* -1 means no limit */
      uint64_t    limit_count_id_;
      uint64_t    limit_offset_id_;

      /* FOR UPDATE clause */
      bool      for_update_;

      uint64_t    gen_joined_tid_;
<<<<<<< HEAD

      //add zt 20151105:b
      common::ObArray<const ObRawExpr *> var_expr_list_;
      //add zt 20151105:e
      //add by qx [query optimization] 20170217 :b
      oceanbase::common::ObList<ObOptimizerRelation*> rel_opt_list;  ///< query optimizer relation info list
      common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> table_id_statInfo_map_;
      //add :e

      //add by dhc [query optimization] 20170328 :b
      oceanbase::common::ObList<ObOptimizerRelation*> subquery_rel_opt_list_;  ///< subquery relation info list
      oceanbase::common::ObList<ObFromItemJoinMethodHelper*> from_item_method_list_;///< from item join method info list
      oceanbase::common::ObList<int> from_item_appear_order_list_;///< from_item appear order
      oceanbase::common::ObList<ObOptimizerRelation*> unaryRef_expr_rel_info_list_;  ///< unaryRef_expr relation info list
      oceanbase::common::ObList<ObOptimizerRelation*> joined_from_item_rel_info_list_;  ///< joined from item relation info list
      ObOptimizerRelation* select_stmt_rel_info_;
      // add e
=======
>>>>>>> refs/remotes/origin/master
    };
  }
}

#endif //OCEANBASE_SQL_SELECTSTMT_H_
