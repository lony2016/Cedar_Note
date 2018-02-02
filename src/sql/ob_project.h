/**
<<<<<<< HEAD
 * Copyright (C) 2013-2016 ECNU_DaSE.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_project.h
 * @brief for operations of project
 *
 * modified by maoxiaoxiao:add functions to get next row if table has index and reset iterator
 *
 * @version __DaSE_VERSION
 * @author maoxiaoxiao <51151500034@ecnu.edu.cn>
 *
 * @date 2016_07_29
 */

/**
=======
>>>>>>> refs/remotes/origin/master
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_project.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_PROJECT_H
#define _OB_PROJECT_H 1
#include "ob_single_child_phy_operator.h"
#include "ob_sql_expression.h"
#include "common/page_arena.h"
#include "common/ob_se_array.h"
<<<<<<< HEAD
//add maoxx
#include "common/ob_row_store.h"
#include "ob_multiple_get_merge.h"
#include "ob_filter.h"
//add e
=======
>>>>>>> refs/remotes/origin/master

namespace oceanbase
{
  namespace sql
  {
    class ObProject: public ObSingleChildPhyOperator
    {
      public:
        ObProject();
        virtual ~ObProject();
        virtual void reset();
        virtual void reuse();
        int add_output_column(const ObSqlExpression& expr);
        inline int64_t get_output_column_size() const;
        inline int64_t get_rowkey_cell_count() const;
        inline void set_rowkey_cell_count(const int64_t count);
        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
<<<<<<< HEAD
        //add maoxx
        /**
         * @brief get_next_row_with_index
         * get next row if the table has index
         * @param row
         * @param pre_data_row_store
         * @param post_data_row_store
         * @return OB_SUCCESS or other ERROR
         */
        int get_next_row_with_index(const common::ObRow *&row, common::ObRowStore *pre_data_row_store, common::ObRowStore *post_data_row_store);
        //add e
=======
>>>>>>> refs/remotes/origin/master
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        void assign(const ObProject &other);
        const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> >  &get_output_columns() const;
        virtual ObPhyOperatorType get_type() const;
<<<<<<< HEAD
        //add maoxx
        /**
         * @brief reset_iterator
         */
        void reset_iterator();
        //add e

=======
>>>>>>> refs/remotes/origin/master
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;

      private:
        int cons_row_desc();
        // disallow copy
        ObProject(const ObProject &other);
        ObProject& operator=(const ObProject &other);
      protected:
        // data members 
        common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > columns_;
        common::ObRowDesc row_desc_;
        common::ObRow row_;
        int64_t rowkey_cell_count_;
    };

    inline int64_t ObProject::get_output_column_size() const
    {
      return columns_.count();
    }

    inline const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> >  & ObProject::get_output_columns() const
    {
      return columns_;
    }

    inline int64_t ObProject::get_rowkey_cell_count() const
    {
      return rowkey_cell_count_;
    }

    inline void ObProject::set_rowkey_cell_count(const int64_t count)
    {
      rowkey_cell_count_ = count;
    }


  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_PROJECT_H */
