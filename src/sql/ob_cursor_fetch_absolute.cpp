/**
* Copyright (C) 2013-2016 DaSE .
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* @file ob_cursor_fetch_absolute.cpp
* @brief this class  present a "cursor fetch absolute" physical plan in oceanbase
*
* Created by zhounan: support curosr
*
* @version CEDAR 0.2 
* @author zhounan <zn4work@gmail.com>
* @date 2014_11_23
*/
#include"ob_cursor.h"
#include "ob_cursor_fetch_absolute.h"
#include "ob_result_set.h"
#include "ob_physical_plan.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

ObFetchAbsolute::ObFetchAbsolute()
{
}

ObFetchAbsolute::~ObFetchAbsolute()
{
}

void ObFetchAbsolute::reset()
{
  ObSingleChildPhyOperator::reset();
}

void ObFetchAbsolute::reuse()
{
  ObSingleChildPhyOperator::reuse();
}

int ObFetchAbsolute::get_row_desc(const common::ObRowDesc *&row_desc) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == child_op_))
  {
    ObResultSet *result_set = NULL;
    ObPhysicalPlan *physical_plan = NULL;
    ObPhyOperator *main_query = NULL;
    if ((result_set = my_phy_plan_->get_result_set()->get_session()->get_plan(cursor_name_)) == NULL
      || (physical_plan = result_set->get_physical_plan()) == NULL
      || (main_query = physical_plan->get_main_query()) == NULL)
    {
      ret = OB_NOT_INIT;
      TBSYS_LOG(ERROR, "Stored session plan can not be fount or not correct");
    }
    else
    {
      ret = main_query->get_row_desc(row_desc);
    }
  }
  else
  {
    ret = child_op_->get_row_desc(row_desc);
  }
  return ret;
}

int ObFetchAbsolute::open()
{
  int ret = OB_SUCCESS;
  ObResultSet *my_result_set = NULL;
   // get stored executing plan
  ObResultSet *result_set = NULL;
  ObPhysicalPlan *physical_plan = NULL;
  ObPhyOperator *main_query = NULL;
  ObSQLSessionInfo *session = my_phy_plan_->get_result_set()->get_session();
  if ((result_set = session->get_plan(cursor_name_)) == NULL
     || (physical_plan = result_set->get_physical_plan()) == NULL
     || (main_query = physical_plan->get_main_query()) == NULL)
   {
     ret = OB_NOT_INIT;
     TBSYS_LOG(ERROR, "Stored session plan can not be fount or not correct, result_set=%p main_query=%p phy_plan=%p",
               result_set, main_query, physical_plan);
   }
   else
   {
	 my_result_set = my_phy_plan_->get_result_set();
	 ret =  my_result_set->from_prepared(*result_set);
	 TBSYS_LOG(WARN, "my result_set get prepared, ret=%d", ret);
	 my_result_set->set_physical_plan(my_phy_plan_,true);
	 if(child_op_ == NULL)
	 	 {
	 		 if ((ret = set_child(0, *main_query)) != OB_SUCCESS)
	 		 {
	 			 TBSYS_LOG(ERROR, "Find stored executing plan failed");
	 		 }
	 	 }
	 	 else
	 	 {
	 		 TBSYS_LOG(INFO, "fetch child has been set");
	 	 }
   }
  return ret;
}

int ObFetchAbsolute::close()
{
    int ret = OB_SUCCESS;
    return ret;
 // return ObSingleChildPhyOperator::close();
}

int ObFetchAbsolute::get_next_row(const common::ObRow *&row)
{
  int ret = OB_SUCCESS;
  if (NULL == child_op_)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(ERROR, "child_op_ must not NULL");
  }
  else
  {
    ObCursor* cursor_op = static_cast<ObCursor*>(child_op_);
    if(1 == cursor_op->get_is_opened())
    {
      if(fetch_count_<=0)
      {
    	 ret = OB_ITER_END;
         my_phy_plan_->get_result_set()->set_message("row number cannot <= 0");
         TBSYS_LOG(USER_ERROR, "row number cannot <= 0");
      }
      else
      {
	     ObCursor* cursor_op = static_cast<ObCursor*>(child_op_);
         ret = cursor_op->get_ab_row(fetch_count_,row);
         if(ret == OB_ITER_END)
         {
             my_phy_plan_->get_result_set()->set_message("fetch out of range");
             TBSYS_LOG(USER_ERROR, "fetch out of range");
         }
      }
    }
    else
    {
       my_phy_plan_->get_result_set()->set_message("fetch error: cursor is not opened");
       TBSYS_LOG(USER_ERROR, "fetch error: cursor is not opened");
       ret = OB_ITER_END;
    }
  }
  return ret;
}

namespace oceanbase{
  namespace sql{
    REGISTER_PHY_OPERATOR(ObFetchAbsolute, PHY_CURSOR_FETCH_ABSOLUTE);
  }
}

int64_t ObFetchAbsolute::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos,"Open (cursor_name=%.*s)\n", cursor_name_.length(), cursor_name_.ptr());
  return pos;
}



