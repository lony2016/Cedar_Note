/**
* Copyright (C) 2013-2016 DaSE .
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* @file ob_cursor_fetch_into.cpp
* @brief this class  present a "cursor fetch into" physical plan in oceanbase
*
* Created by zhujun: support procedure
*
* @version CEDAR 0.2 
* @author zhujun <513401199201286431@ecnu.edu.cn>
* @date 2014_11_23
*/
#include "ob_cursor_fetch_into.h"
#include "ob_result_set.h"
#include "ob_physical_plan.h"
#include "parse_malloc.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObCursorFetchInto::ObCursorFetchInto()
{
}

ObCursorFetchInto::~ObCursorFetchInto()
{
}

int ObCursorFetchInto::add_variable(ObString &var)
{
	variables_.push_back(var);
	return OB_SUCCESS;
}

void ObCursorFetchInto::reset()
{

}
void ObCursorFetchInto::reuse()
{

}
int ObCursorFetchInto::close()
{
	return OB_SUCCESS;
}

int ObCursorFetchInto::get_row_desc(const common::ObRowDesc *&row_desc) const
{
	int ret = OB_SUCCESS;
	if (OB_UNLIKELY(NULL == child_op_))
	{
		ret = OB_NOT_INIT;
		TBSYS_LOG(ERROR, "child_op_ is NULL");
	}
	else
	{
		ret = child_op_->get_row_desc(row_desc);
	}
	return ret;
}
int ObCursorFetchInto::get_next_row(const common::ObRow *&row)
{
	int ret = OB_SUCCESS;
	if (NULL == child_op_)
	{
		ret = OB_ERR_UNEXPECTED;
		TBSYS_LOG(ERROR, "child_op_ is NULL");
	}
	else
	{
        ret = child_op_->get_next_row(row);
	}
	return ret;
}

int ObCursorFetchInto::open()
{
	TBSYS_LOG(INFO, "zz:ObCursorFetchInto::open()");
	int ret = OB_SUCCESS;
	if (child_op_==NULL)
	{
		ret = OB_ERR_GEN_PLAN;
		TBSYS_LOG(ERROR, "child_op_ is NULL");
	}
	else
	{
		uint64_t table_id = OB_INVALID_ID;
		uint64_t column_id = OB_INVALID_ID;
		const ObRow *row;
        if((ret = child_op_->open()) != OB_SUCCESS)
		{
			TBSYS_LOG(WARN, "zz:child_op_ open failed");
		}
        else if((ret = child_op_->get_next_row(row)) != OB_SUCCESS)//取出一行
		{
			TBSYS_LOG(WARN, "zz:get_next_row failed");
		}
		else
		{
			ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
			for(int64_t i=0;i<variables_.count();i++)
			{
				const ObObj *cell = NULL;
				if(OB_SUCCESS !=(ret=row->raw_get_cell(i, cell, table_id, column_id)))//取出一列
				{
					TBSYS_LOG(WARN, "zz:raw_get_cell %ld failed",i);
					TBSYS_LOG(USER_ERROR, "declare cursor not have enough cell.");
				}
				else
				{
					ObString var_name=variables_.at(i);
					ObObj val;
					if(!session->variable_exists(var_name))
					{
						 ret=OB_ERR_VARIABLE_UNKNOWN;
						 TBSYS_LOG(USER_ERROR, "Variable %.*s does not declare", var_name.length(), var_name.ptr());
					}
					else
					{
                        if((ret = session->get_variable_value(var_name,val)) != OB_SUCCESS)
						{
							TBSYS_LOG(WARN, "get_variable_value  ERROR");
						}
                        else if(val.get_type() != cell->get_type())
						{
							ret=OB_ERR_ILLEGAL_TYPE;
							TBSYS_LOG(WARN, "variable %.*s data type is %d table column type is %d", var_name.length(), var_name.ptr(),val.get_type(),cell->get_type());
							TBSYS_LOG(USER_ERROR, "declare variable %.*s data type not suite table column", var_name.length(), var_name.ptr());
						}
                        else if((ret = session->replace_variable(var_name,*cell)) != OB_SUCCESS)
						{
							TBSYS_LOG(WARN, "replace_variable  ERROR");
						}
						else
						{
							TBSYS_LOG(INFO, "fetch into var %.*s value= %s",var_name.length(),var_name.ptr(),to_cstring(*cell));
						}
					}

				}
			}
            child_op_->close();
		}
	}
	return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObCursorFetchInto, PHY_CURSOR_FETCH_INTO);
  }
}

int64_t ObCursorFetchInto::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ObCursorFetchInto (var list size=%ld)\n", variables_.count());
  return pos;
}
