/**
 * Copyright (C) 2013-2016 ECNU_DaSE.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_logical_plan.h
 * @brief logical plan class definition
 *
 * modified by zhutao:modified already delete
 *
 * @version __DaSE_VERSION
 * @author zhutao <zhutao@stu.ecnu.edu.cn>
 * @author wangdonghui <zjnuwangdonghui@163.com>
 * @date 2016_07_27
 */

#ifndef OCEANBASE_SQL_LOGICALPLAN_H_
#define OCEANBASE_SQL_LOGICALPLAN_H_
#include "parse_node.h"
#include "ob_raw_expr.h"
#include "ob_stmt.h"
#include "ob_select_stmt.h"
#include "ob_result_set.h"
#include "common/ob_string.h"
#include "common/ob_string_buf.h"
#include "common/ob_vector.h"
#include "common/ob_stack_allocator.h"
namespace oceanbase
{
  namespace sql
  {
    enum ObCurTimeType {
      CUR_TIME,
      CUR_TIME_UPS,
      NO_CUR_TIME
    };
    class ObSQLSessionInfo;
    class ObLogicalPlan
    {
    public:
      explicit ObLogicalPlan(oceanbase::common::ObStringBuf* name_pool);
      virtual ~ObLogicalPlan();

      oceanbase::common::ObStringBuf* get_name_pool() const
      {
        return name_pool_;
      }

      ObBasicStmt* get_query(uint64_t query_id) const;

      ObBasicStmt* get_main_stmt()
      {
       ObBasicStmt *stmt = NULL;
        if (stmts_.size() > 0)
          stmt = stmts_[0];
        return stmt;
      }

      ObSelectStmt* get_select_query(uint64_t query_id) const;

      ObSqlRawExpr* get_expr(uint64_t expr_id) const;
      //add wanglei [semi join] 20170417:b
      int32_t get_expr_list_num()const;
      ObSqlRawExpr* get_expr_for_something(int32_t no) const;
      oceanbase::common::ObVector<ObSqlRawExpr*> & get_expr_list();
      //add wanglei [semi join] 20170417:e
      int add_query(ObBasicStmt* stmt)
      {
        int ret = common::OB_SUCCESS;
        if ((!stmt) || (stmts_.push_back(stmt) != common::OB_SUCCESS))
        {
          TBSYS_LOG(WARN, "fail to allocate space for stmt. %p", stmt);
          ret = common::OB_ERROR;
        }
        return ret;
      }

      int add_expr(ObSqlRawExpr* expr)
      {
        int ret = common::OB_SUCCESS;
        if ((!expr) || (exprs_.push_back(expr) != common::OB_SUCCESS))
        {
          TBSYS_LOG(WARN, "fail to allocate space for expr. %p", expr);
          ret = common::OB_ERROR;
        }
        return ret;
      }

      // Just a storage, only need to add raw expression
      int add_raw_expr(ObRawExpr* expr)
      {
        int ret = common::OB_SUCCESS;
        if ((!expr) || (raw_exprs_store_.push_back(expr) != common::OB_SUCCESS))
        {
          TBSYS_LOG(WARN, "fail to allocate space for raw expr. %p", expr);
          ret = common::OB_ERROR;
        }
        return ret;
      }

      //add zt 20151102:b
//      int32_t get_raw_expr_count() const
//      {
//        return raw_exprs_store_.size();
//      }

//      const ObRawExpr* get_raw_expr(int32_t idx) const
//      {
//        return raw_exprs_store_.at(idx);
//      }
      //add zt 20151102:e

      int fill_result_set(ObResultSet& result_set, ObSQLSessionInfo *session_info, common::ObIAllocator &alloc);

      uint64_t generate_table_id()
      {
        return new_gen_tid_--;
      }

      uint64_t generate_column_id()
      {
        return new_gen_cid_--;
      }

      // It will reserve 10 id for the caller
      // In fact is for aggregate functions only,
      // because we need to push part aggregate to tablet and keep top aggregate on all
      uint64_t generate_range_column_id()
      {
        uint64_t ret_cid = new_gen_cid_;
        new_gen_cid_ -= 10;
        return ret_cid;
      }

      uint64_t generate_expr_id()
      {
        return new_gen_eid_++;
      }

      uint64_t generate_query_id()
      {
        return new_gen_qid_++;
      }

      int64_t generate_when_number()
      {
        return new_gen_wid_++;
      }

      int64_t inc_question_mark()
      {
        return question_marks_count_++;
      }

      int64_t get_question_mark_size() const
      {
        return question_marks_count_;
      }

      void set_cur_time_fun()
      {
        if (NO_CUR_TIME == cur_time_fun_type_)
        {
          cur_time_fun_type_ = CUR_TIME ;
        }
      }

      void set_cur_time_fun_ups()
      {
        cur_time_fun_type_ = CUR_TIME_UPS;
      }

      const ObCurTimeType get_cur_time_fun_type()
      {
        return cur_time_fun_type_;
      }

      int32_t get_stmts_count() const
      {
        return stmts_.size();
      }

      ObBasicStmt* get_stmt(int32_t index) const
      {
        OB_ASSERT(index >= 0 && index < get_stmts_count());
        return stmts_.at(index);
      }

      /*
       * get stmt by query_id
       * add by wangyanzhao on 2017/9/8
       */
      ObBasicStmt* get_stmt_by_id(uint64_t query_id) const
      {
        ObBasicStmt *stmt = NULL;
        int32_t num = stmts_.size();
        for (int32_t i = 0; i < num; i++)
        {
          if (stmts_[i]->get_query_id() == query_id)
          {
            stmt = stmts_[i];
            break;
          }
        }
        OB_ASSERT(NULL != stmt);
        return stmt;
      }
      
      /*
       * delete expr by id
       * add by lxb on 2016/12/25
       */ 
      void delete_expr_by_id(uint64_t expr_id) ;
      
      /*
       * delete stmt by query id
       * add by lxb on 2016/12/25
       */
      void delete_stmt_by_query_id(uint64_t query_id);
      
      void print(FILE* fp = stderr, int32_t level = 0) const;
      
      // add by lxb on 2017/02/16 for logical optimizer
      int64_t generate_alias_table_id()
      {
        return new_gen_aid_++;
      }
      
      //add slwang [exists related subquery] 20170626:b
      int32_t get_exprs_count() const
      {
        return exprs_.size();
      }
      
      //add 20170626:e
      
      // added by wangyanzhao 2017/1/13
	  int remove_expr(uint64_t expr_id)
	  {
	  	int ret = common::OB_ERROR;
		int32_t num = exprs_.size();
        for (int32_t i = 0; i < num; i++)
        {
          if (exprs_[i]->get_expr_id() == expr_id)
          {
            ret = exprs_.remove(i);
            break;
          }
        }
        return ret;
	  }

	  oceanbase::common::ObVector<ObSqlRawExpr*>& get_exprs()
	  {
        return exprs_;
	  }

	  oceanbase::common::ObVector<ObRawExpr*>& get_raw_exprs()
	  {
        return raw_exprs_store_;
	  }
	  // ended by wangyanzhao


      void set_query_rel_opt(ObOptimizerRelation* query_rel_opt)
      {
        query_rel_info_ = query_rel_opt;
      }

    protected:
      oceanbase::common::ObStringBuf* name_pool_;

    private:
      oceanbase::common::ObVector<ObBasicStmt*> stmts_;
      oceanbase::common::ObVector<ObSqlRawExpr*> exprs_;
      oceanbase::common::ObVector<ObRawExpr*> raw_exprs_store_;
      int64_t   question_marks_count_;
      ObCurTimeType cur_time_fun_type_;
      uint64_t  new_gen_tid_;
      uint64_t  new_gen_cid_;
      uint64_t  new_gen_qid_;
      uint64_t  new_gen_eid_;
      int64_t   new_gen_wid_;   // when number
      
      // add by lxb on 2017/02/16 for logical optimizer
      int64_t new_gen_aid_; // generate alias table
      ObOptimizerRelation* query_rel_info_; //add dhc [query_optimizer] 20170525
    };
  }
}

#endif //OCEANBASE_SQL_LOGICALPLAN_H_
