/**
* Copyright (C) 2013-2016 DaSE .
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* @file ob_procedure_while.h
* @brief this class present a procedure "while" loop
*
* Created by zhujun: support procedure
*
* @version CEDAR 0.2 
* @author zhujun <51141500091@ecnu.edu.cn>
* @date 2014_11_23
*/
#ifndef OCEANBASE_SQL_OB_PROCEDURE_WHILE_H
#define OCEANBASE_SQL_OB_PROCEDURE_WHILE_H
#include "ob_multi_children_phy_operator.h"
#include "ob_sql_session_info.h"
#include "common/dlist.h"
namespace oceanbase
{
	namespace sql
	{
		class ObPhysicalPlan;
        /**
         * @brief The ObProcedureWhile class
         */
		class ObProcedureWhile : public ObMultiChildrenPhyOperator
		{
		public:
			ObProcedureWhile();
			virtual ~ObProcedureWhile();
			virtual void reset();
			virtual void reuse();
			virtual int open();
			virtual int close();
			virtual ObPhyOperatorType get_type() const
			{
				return PHY_PROCEDURE_WHILE;
			}
			virtual int64_t to_string(char* buf, const int64_t buf_len) const;
			virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
			virtual int get_next_row(const common::ObRow *&row);
			virtual int set_child(int32_t child_idx, ObPhyOperator &child_operator);
			virtual int32_t get_child_num() const;
            /**
             * @brief set the "while" loop expression id
             * @param expr
             * @return
             */
			int set_expr(ObSqlExpression& expr);
		private:
			//disallow copy
			ObProcedureWhile(const ObProcedureWhile &other);
			ObProcedureWhile& operator=(const ObProcedureWhile &other);
			//function members

		private:
			//data members
            ObSqlExpression expr_;///< "while" loop expression

            int32_t child_num_;	///then statements size

		};



	}
}

#endif



