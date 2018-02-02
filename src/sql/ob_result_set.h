/**
 * Copyright (C) 2013-2016 ECNU_DaSE.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_result_set.cpp
 * @brief build logic plan
 *
 * modified by zhujun：support procedure
 * modified by zhutao: delete and add some function for procedure
 *
 * @version __DaSE_VERSION
 * @author zhujun <51141500091@ecnu.edu.cn>
 * @author zhutao <zhutao@stu.ecnu.edu.cn>
 * @date 2016_07_29
 */

/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_result_set.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_RESULT_SET_H
#define _OB_RESULT_SET_H 1
#include "common/ob_define.h"
#include "common/ob_row.h"
#include "common/ob_array.h"
#include "common/ob_se_array.h"
#include "common/ob_string_buf.h"
#include "sql/ob_phy_operator.h"
#include "sql/ob_physical_plan.h"
#include "common/utility.h"
#include "common/ob_stack_allocator.h"
#include "sql/ob_basic_stmt.h"
#include "obmysql/ob_mysql_global.h" // for EMySQLFieldType
#include "common/page_arena.h"

namespace oceanbase
{
  namespace sql
  {
    class ObPsStoreItem;
    class ObSQLSessionInfo;
    struct ObPsSessionInfo;
    struct ObPsStoreItemValue;
    // query result set
    class ObResultSet
    {
      public:
        struct Field
        {
          common::ObString tname_; // table name for display
          common::ObString org_tname_; // original table name
          common::ObString cname_;     // column name for display
          common::ObString org_cname_; // original column name
          common::ObObj type_;      // value type
          int64_t to_string(char *buffer, int64_t length) const;
          int deep_copy(Field &other, common::ObStringBuf *str_buf);
        };
        //        common::ObString proc_sql_;//add by zz 2014-12-27, delete by zt 20151117, wtf
        //        ObPhyOperator *ps_;//add by zz to store operator, delete by zt 20151117, wtf
      public:
        ObResultSet();
        ~ObResultSet();
        /// open and execute the execution plan
        /// @note SHOULD be called for all statement even if there is no result rows
        int open();
        /// get the next result row
        /// @return OB_ITER_END when no more data available
        int get_next_row(const common::ObRow *&row);
        /// close the result set after get all the rows
        int close();
        /// get number of rows affected by INSERT/UPDATE/DELETE
        int64_t get_affected_rows() const;
        /// get warning count during the execution
        int64_t get_warning_count() const;
        /// get statement id
        uint64_t get_statement_id() const;
        int64_t get_sql_id() const {return sql_id_;};
        /// get the server's error message
        const char* get_message() const;
        /// get the server's error code
        const int get_errcode() const;
        /**
         * get the row description
         * the row desc should have been valid after open() and before close()
         * @pre call open() first
         */
        int get_row_desc(const common::ObRowDesc *&row_desc) const;
        /// get the field columns
        const common::ObIArray<Field> & get_field_columns() const;
        /// get the param columns
        const common::ObIArray<Field> & get_param_columns() const;
        /// get the placeholder of params
        common::ObIArray<common::ObObj*> & get_params();
        const common::ObIArray<obmysql::EMySQLFieldType> & get_params_type() const;
        /// whether the result is with rows (true for SELECT statement)
        bool is_with_rows() const;
        /// get physical plan
        ObPhysicalPlan* get_physical_plan();
        /// to string
        int64_t to_string(char* buf, const int64_t buf_len) const;
        /// whether the statement is a prepared statment
        bool is_prepare_stmt() const;
        /// whether the statement is SHOW WARNINGS
        bool is_show_warnings() const;
        bool is_compound_stmt() const;
        ObBasicStmt::StmtType get_stmt_type() const;
        ObBasicStmt::StmtType get_inner_stmt_type() const;
        int64_t get_query_string_id() const;
        ////////////////////////////////////////////////////////////////
        // the following methods are used by the ob_sql module internally
        /// add a field columns
        int init();
        int reset();
        int add_field_column(const Field & field);
        int add_param_column(const Field & field);
        int pre_assign_params_room(const int64_t& size, common::ObIAllocator &alloc);

        //        int pre_assign_cur_time_room(common::ObObj *place_holder);  //add zt 20151121

        int pre_assign_cur_time_room(common::ObIAllocator &alloc);
        int fill_params(const common::ObIArray<obmysql::EMySQLFieldType>& types,
                        const common::ObIArray<common::ObObj>& values);
        int from_prepared(const ObResultSet& stored_result_set);
        int to_prepare(ObResultSet& other);
        int from_store(ObPsStoreItemValue *item, ObPsSessionInfo *info);
        const common::ObString& get_statement_name() const;
        void set_statement_id(const uint64_t stmt_id);
        void set_statement_name(const common::ObString name);
        void set_param_columns(common::ObArray<Field> &columns)
        {
          param_columns_ = columns;
        }
        void set_message(const char* message);
        void set_errcode(int code);
        void set_affected_rows(const int64_t& affected_rows);
        void set_warning_count(const int64_t& warning_count);
        void set_physical_plan(ObPhysicalPlan *physical_plan, bool did_own);
        //add lbzhong [auto_increment] 20161218:b
        void set_auto_increment(const bool auto_increment) { auto_increment_ = auto_increment; }
        bool is_auto_increment() const { return auto_increment_; }
        void set_auto_value(const int64_t auto_value) { auto_value_ = auto_value; }
        int64_t get_auto_value() const { return auto_value_; }
        //add:e
        void fileds_clear();
        void set_stmt_type(ObBasicStmt::StmtType stmt_type);
        void set_inner_stmt_type(ObBasicStmt::StmtType stmt_type);
        void set_compound_stmt(bool compound);
        int get_param_idx(int64_t param_addr, int64_t &idx);
        void set_session(ObSQLSessionInfo *s);
        ObSQLSessionInfo* get_session();
        void set_ps_transformer_allocator(common::ObArenaAllocator *allocator);
        void set_query_string_id(int64_t query_string_id);
        int set_cur_time(const common::ObObj& cur_time);
        int set_cur_time(const common::ObPreciseDateTime& cur_time);
        const common::ObObj*  get_cur_time_place() const;
        void set_params_type(const common::ObIArray<obmysql::EMySQLFieldType> & params_type);
        void set_plan_from_assign(bool flag)
        {
          plan_from_assign_ = flag;
        }

        void change_phy_plan(ObPhysicalPlan *plan, bool did_own);
        void set_sql_id(int64_t sql_id) {sql_id_ = sql_id;}

        //add zt 20151201:b
        //for postfix_expr to get procedure obj, read array variables
        /**
         * @brief set_running_procedure
         * set running procedure
         * @param proc SpProcedure object pointer
         */
        void set_running_procedure(SpProcedure *proc) { proc_ = proc;}
        /**
         * @brief get_running_procedure
         * get running procedure
         * @return SpProcedure object pointer
         */
        const SpProcedure* get_running_procedure() const{ return proc_; }
        /**
         * @brief get_stmt_hash
         * get statement hash code
         * @return hash code
         */
        int64_t get_stmt_hash() const { return stmt_hash_code_; }
        /**
         * @brief set_stmt_hash
         * set statement hash code
         * @param hc hash code
         */
        void set_stmt_hash(int64_t hc) { stmt_hash_code_ = hc; }
        /**
         * @brief set_no_group
         * set no group execution flag
         * @param no_group bool value
         */
        void set_no_group(bool no_group) { no_group_ = no_group; }
        /**
         * @brief get_no_group
         * get no group execution flag
         * @return flag
         */
        bool get_no_group() const { return no_group_; }
        //add by qx 20170317 :b
        /**
         * @brief set_long_trans
         * set long transcation flag
         * @param long_trans
         */
        void set_long_trans(bool long_trans) {long_trans_ = long_trans;}
        /**
         * @brief get_long_trans
         * get long transcation flag
         * @return
         */
        bool get_long_trans() const {return long_trans_;}
        //add :e
        //add zt 20151201:e
        //add by wdh 20160822 :b
        void set_cur_schema_version(int64_t cur_schema_version) {cur_schema_version_ = cur_schema_version;}
        int64_t get_cur_schema_version() const {return cur_schema_version_;}
        //add :e
      private:
        // types and constants
        static const int64_t MSG_SIZE = 512;
        static const int64_t SMALL_BLOCK_SIZE = 8*1024LL;
        static const int64_t COMMON_PARAM_NUM = 12;
      private:
        // disallow copy
        ObResultSet(const ObResultSet &other);
        ObResultSet& operator=(const ObResultSet &other);
        // function members
      private:
        // data members
        uint64_t statement_id_;
        int64_t sql_id_;
        int64_t affected_rows_; // number of rows affected by INSERT/UPDATE/DELETE
        int64_t warning_count_;
        common::ObString statement_name_;
        char message_[MSG_SIZE]; // null terminated message string
        common::ObArenaAllocator block_allocator_;
        common::ObSEArray<Field, common::OB_PREALLOCATED_NUM> field_columns_;
        common::ObSEArray<Field, COMMON_PARAM_NUM> param_columns_;
        common::ObSEArray<common::ObObj*, COMMON_PARAM_NUM> params_;
        common::ObSEArray<obmysql::EMySQLFieldType, COMMON_PARAM_NUM> params_type_;
        ObPhysicalPlan *physical_plan_;
        bool own_physical_plan_; // whether the physical plan is mine
        bool plan_from_assign_;
        ObBasicStmt::StmtType stmt_type_;
        // for a prepared SELECT, stmt_type_ is T_PREPARE
        // but in perf stat we want inner info, i.e. SELECT.
        ObBasicStmt::StmtType inner_stmt_type_;
        bool compound_;
        int errcode_;
        ObSQLSessionInfo *my_session_; // The session who owns this result set
        common::ObArenaAllocator *ps_trans_allocator_;
        int64_t query_string_id_;
        common::ObObj *cur_time_; // only used when the sql contains fun like current_time

        /**
         * The pointer to used to the located the running procedure, then I
         * can read variables or array from that. This is important when doing postfix_exprssion
         * calculation. However, if we only read variables or array from ObSqlSession, Such desgin
         * is not necessary.
         * @brief proc_
         */
        /// procedure physical plan
        SpProcedure *proc_; //add zt: 20151201
        int64_t stmt_hash_code_;  ///<  statement hash code_
        bool no_group_;  ///< no group excution flag
        int64_t cur_schema_version_; // add by wdh 20160822 [dev compile] used in cache manage
        bool long_trans_;   ///< long transcation flag  add by qx 20170317

        //add lbzhong [auto_increment] 20161218:b
        bool auto_increment_;
        int64_t auto_value_;
        //add:e
    };

    inline int64_t ObResultSet::Field::to_string(char *buffer, int64_t len) const
    {
      int64_t pos;
      pos = snprintf(buffer, len, "tname: %.*s, org_tname: %.*s, "
                     "cname: %.*s, org_cname, %.*s, type: %s",
                     tname_.length(), tname_.ptr(),
                     org_tname_.length(), org_tname_.ptr(),
                     cname_.length(), cname_.ptr(),
                     org_cname_.length(), org_cname_.ptr(), to_cstring(type_));
      return pos;
    }

    inline ObResultSet::ObResultSet()
      :statement_id_(common::OB_INVALID_ID),
       sql_id_(0),
       affected_rows_(0), warning_count_(0),
       block_allocator_(common::ObModIds::OB_SQL_RESULT_SET_DYN),
       field_columns_(SMALL_BLOCK_SIZE, common::ModulePageAllocator(block_allocator_)),
       param_columns_(SMALL_BLOCK_SIZE, common::ModulePageAllocator(block_allocator_)),
       params_(SMALL_BLOCK_SIZE, common::ModulePageAllocator(block_allocator_)),
       params_type_(SMALL_BLOCK_SIZE, common::ModulePageAllocator(block_allocator_)),
       physical_plan_(NULL),
       own_physical_plan_(false),
       plan_from_assign_(false),
       stmt_type_(ObBasicStmt::T_NONE),
       compound_(false),
       errcode_(0),
       my_session_(NULL),
       ps_trans_allocator_(NULL),
       query_string_id_(0),
       cur_time_(NULL),
       proc_(NULL), //add zt 20151202
       //add lbzhong [auto_increment] 20161218:b
        auto_increment_(false), auto_value_(OB_INVALID_AUTO_INCREMENT_VALUE)
       //add:e
    {
      message_[0] = '\0';
    }

    inline int64_t ObResultSet::get_affected_rows() const
    {
      return affected_rows_;
    }

    inline int64_t ObResultSet::get_warning_count() const
    {
      return warning_count_;
    }

    inline uint64_t ObResultSet::get_statement_id() const
    {
      return statement_id_;
    }

    inline const common::ObString& ObResultSet::get_statement_name() const
    {
      return statement_name_;
    }

    inline const char* ObResultSet::get_message() const
    {
      return message_;
    }

    inline const int ObResultSet::get_errcode() const
    {
      return errcode_;
    }

    inline ObPhysicalPlan* ObResultSet::get_physical_plan()
    {
      return physical_plan_;
    }

    inline void ObResultSet::set_statement_id(const uint64_t stmt_id)
    {
      statement_id_ = stmt_id;
    }

    inline void ObResultSet::set_message(const char* message)
    {
      snprintf(message_, MSG_SIZE, "%s", message);
    }

    inline void ObResultSet::set_errcode(int code)
    {
      errcode_ = code;
    }

    inline int ObResultSet::add_field_column(const ObResultSet::Field & field)
    {
      return field_columns_.push_back(field);
    }

    inline int ObResultSet::add_param_column(const ObResultSet::Field & field)
    {
      return param_columns_.push_back(field);
    }

    inline const common::ObIArray<ObResultSet::Field> & ObResultSet::get_field_columns() const
    {
      return field_columns_;
    }

    inline const common::ObIArray<ObResultSet::Field> & ObResultSet::get_param_columns() const
    {
      return param_columns_;
    }

    inline common::ObIArray<common::ObObj*> & ObResultSet::get_params()
    {
      return params_;
    }

    inline const common::ObIArray<obmysql::EMySQLFieldType> & ObResultSet::get_params_type() const
    {
      return params_type_;
    }

    inline bool ObResultSet::is_with_rows() const
    {
      return (field_columns_.count() > 0 && !is_prepare_stmt());
    }

    inline void ObResultSet::set_affected_rows(const int64_t& affected_rows)
    {
      affected_rows_ = affected_rows;
    }

    inline void ObResultSet::set_warning_count(const int64_t& warning_count)
    {
      warning_count_ = warning_count;
    }

    inline int64_t ObResultSet::to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      common::databuff_printf(buf, buf_len, pos, "stmt_type=%d ",
                              stmt_type_);
      common::databuff_printf(buf, buf_len, pos, "is_with_rows=%c ",
                              this->is_with_rows()?'Y':'N');
      common::databuff_printf(buf, buf_len, pos, "affected_rows=%ld ",
                              affected_rows_);
      common::databuff_printf(buf, buf_len, pos, "warning_count=%ld ",
                              warning_count_);
      common::databuff_printf(buf, buf_len, pos, "field_count=%ld ",
                              field_columns_.count());
      common::databuff_printf(buf, buf_len, pos, "message=%s ",
                              message_);
      common::databuff_printf(buf, buf_len, pos, "prepared_field_count=%ld ",
                              param_columns_.count());
      common::databuff_printf(buf, buf_len, pos, "prepared_param_count=%ld ",
                              params_.count());
      common::databuff_printf(buf, buf_len, pos, "stmt_id=%lu ",
                              statement_id_);
      common::databuff_printf(buf, buf_len, pos, "stmt_name=%.*s",
                              statement_name_.length(), statement_name_.ptr());
      common::databuff_printf(buf, buf_len, pos, "sql_id=%lu ",
                              sql_id_);
      return pos;
    }

    inline int ObResultSet::get_next_row(const common::ObRow *&row)
    {
      OB_ASSERT(physical_plan_);
      errcode_ = physical_plan_->get_main_query()->get_next_row(row);
      return errcode_;
    }

    inline int ObResultSet::close()
    {
      int ret = common::OB_SUCCESS;
      if (OB_UNLIKELY(NULL == physical_plan_))
      {
        if (ObBasicStmt::T_PREPARE != stmt_type_)
        {
          TBSYS_LOG(WARN, "physical_plan not init, stmt_type=%d", stmt_type_);
          ret = common::OB_NOT_INIT;
        }
      }
      else
      {
        ObPhyOperator *main_query = physical_plan_->get_main_query();
        ObPhyOperator *pre_query = physical_plan_->get_pre_query();
        int tmp_ret = OB_SUCCESS;
        if (NULL != pre_query)
        {
          if (OB_SUCCESS != (tmp_ret = pre_query->close()))
          {
            ret = tmp_ret;
            TBSYS_LOG(WARN, "failed to close pre_query, ret=%d", ret);
          }
        }

        if (NULL == main_query)
        {
          ret = OB_ERR_UNEXPECTED;
          TBSYS_LOG(ERROR, "main query must not be NULL");
        }
        else if (OB_SUCCESS != (tmp_ret = main_query->close()))
        {
          ret = tmp_ret;
          TBSYS_LOG(WARN, "failed to close main_query, ret=%d", ret);
        }

      }

      set_errcode(ret);
      return ret;
    }
    inline void ObResultSet::set_physical_plan(ObPhysicalPlan *physical_plan, bool did_own)
    {
      if (NULL != physical_plan_)
      {
        TBSYS_LOG(WARN, "physical_plan_ is not NULL, %p", physical_plan_);
      }
      physical_plan_ = physical_plan;
      own_physical_plan_ = did_own;
    }

    //add zt 20151121:b
    /**
     * @brief ObResultSet::change_phy_plan
     * change physical plan
     * @param plan physical plan
     * @param did_own own physical plan flag, bool value
     */
    inline void ObResultSet::change_phy_plan(ObPhysicalPlan *plan, bool did_own)
    {
      physical_plan_ = plan;
      own_physical_plan_ = did_own;
    }
    //add zt 20151121:e

    inline void ObResultSet::fileds_clear()
    {
      affected_rows_ = 0;
      warning_count_ = 0;
      message_[0] = '\0';
      field_columns_.clear();
      param_columns_.clear();
    }

    inline int ObResultSet::get_row_desc(const common::ObRowDesc *&row_desc) const
    {
      return physical_plan_->get_main_query()->get_row_desc(row_desc);
    }

    inline bool ObResultSet::is_prepare_stmt() const
    {
      return ObBasicStmt::T_PREPARE == stmt_type_;
    }

    inline void ObResultSet::set_stmt_type(ObBasicStmt::StmtType stmt_type)
    {
      stmt_type_ = stmt_type;
    }

    inline void ObResultSet::set_inner_stmt_type(ObBasicStmt::StmtType stmt_type)
    {
      inner_stmt_type_ = stmt_type;
    }

    inline void ObResultSet::set_compound_stmt(bool compound)
    {
      compound_ = compound;
    }

    inline bool ObResultSet::is_show_warnings() const
    {
      return ObBasicStmt::T_SHOW_WARNINGS == stmt_type_;
    }

    inline void ObResultSet::set_session(ObSQLSessionInfo *s)
    {
      my_session_ = s;
    }

    inline ObSQLSessionInfo* ObResultSet::get_session()
    {
      return my_session_;
    }

    inline ObBasicStmt::StmtType ObResultSet::get_stmt_type() const
    {
      return stmt_type_;
    }

    inline ObBasicStmt::StmtType ObResultSet::get_inner_stmt_type() const
    {
      return inner_stmt_type_;
    }

    inline bool ObResultSet::is_compound_stmt() const
    {
      return compound_;
    }

    inline void ObResultSet::set_ps_transformer_allocator(common::ObArenaAllocator *allocator)
    {
      ps_trans_allocator_ = allocator;
    }

    inline void ObResultSet::set_query_string_id(int64_t query_string_id)
    {
      query_string_id_ = query_string_id;
    }

    inline int64_t ObResultSet::get_query_string_id() const
    {
      return query_string_id_;
    }

    inline int ObResultSet::set_cur_time(const common::ObObj& cur_time)
    {
      int ret = OB_SUCCESS;
      if (NULL == cur_time_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "cur_time_ must not be NULL");
      }
      else if (ObPreciseDateTimeType != cur_time.get_type())
      {
        ret = OB_OBJ_TYPE_ERROR;
        TBSYS_LOG(ERROR, "cur_time type[%d] is not ObPreciseDateTimeType[%d]",
            cur_time.get_type(), ObPreciseDateTimeType);
      }
      else
      {
        *cur_time_ = cur_time;
      }
      return ret;
    }

    inline int ObResultSet::set_cur_time(const common::ObPreciseDateTime& cur_time)
    {
      int ret = OB_SUCCESS;
      if (NULL != cur_time_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(ERROR, "cur_time_ must not be NULL");
      }
      else
      {
        cur_time_->set_precise_datetime(cur_time);
      }
      return ret;
    }

    inline const common::ObObj* ObResultSet::get_cur_time_place() const
    {
      return cur_time_;
    }

    inline void ObResultSet::set_params_type(
        const common::ObIArray<obmysql::EMySQLFieldType> & params_type)
    {
      params_type_ = params_type;
    }

  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_RESULT_SET_H */
