/**
 * Copyright (C) 2013-2016 ECNU_DaSE.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_raw_expr.cpp
 * @brief raw expression relation class definition
 *
 * modified by zhutao:add some functions and a class for procedure
 *
 * @version __DaSE_VERSION
 * @author zhutao <zhutao@stu.ecnu.edu.cn>
 * @author wangdonghui <zjnuwangdonghui@163.com>
 * @date 2016_07_29
 */

#include "ob_raw_expr.h"
#include "ob_transformer.h"
#include "type_name.c"
#include "ob_prepare.h"
#include "ob_result_set.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;

bool ObRawExpr::is_const() const
{
  return (type_ >= T_INT && type_ <= T_NULL);
}

bool ObRawExpr::is_column() const
{
  return (type_ == T_REF_COLUMN);
}

bool ObRawExpr::is_equal_filter() const
{
  bool ret = false;
  if (type_ == T_OP_EQ || type_ == T_OP_IS)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->is_const()
      || binary_expr->get_second_op_expr()->is_const())
      ret = true;
  }
  return ret;
}

bool ObRawExpr::is_range_filter() const
{
  bool ret = false;
  if (type_ >= T_OP_LE && type_ <= T_OP_GT)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->is_const()
      || binary_expr->get_second_op_expr()->is_const())
      ret = true;
  }
  else if (type_ == T_OP_BTW)
  {
    ObTripleOpRawExpr *triple_expr = dynamic_cast<ObTripleOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (triple_expr->get_second_op_expr()->is_const()
      && triple_expr->get_third_op_expr()->is_const())
      ret = true;
  }
  return ret;
}

bool ObRawExpr::is_join_cond() const
{
  bool ret = false;
  if (type_ == T_OP_EQ)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN
      && binary_expr->get_second_op_expr()->get_expr_type() == T_REF_COLUMN)
      ret = true;
  }
  else if(is_semi_join_cond()){
    ret = true;
  }
  return ret;
}

//add by qx [query optimization] 20170314 :b
bool ObRawExpr::is_join_cond_opt() const
{
  bool ret = false;
  if (type_ >= T_OP_EQ && type_ <= T_OP_NE)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr == NULL)
    {
      TBSYS_LOG(WARN,"dynamic_cast is fail, type = %d",this->get_expr_type());
    }
    else if (binary_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN
             && binary_expr->get_second_op_expr()->get_expr_type() == T_REF_COLUMN)
    {
      ret = true;
    }
  }
  //not
  else if (type_ == T_OP_NOT)
  {
    ObUnaryOpRawExpr *unary_expr = dynamic_cast<ObUnaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (unary_expr == NULL)
    {
    }
    else if (unary_expr->get_op_expr()->is_join_cond_opt())
    {
      ret = true;
    }
  }
  //or / and
  else if (type_ == T_OP_AND || type_ == T_OP_OR)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr*>(const_cast<ObRawExpr *>(this));
    if (binary_expr != NULL)
    {
    }
    else if (binary_expr->get_first_op_expr()->is_join_cond_opt() ||
             binary_expr->get_second_op_expr()->is_join_cond_opt())
    {
      ret = true;
    }
  }
  return ret;
}
bool ObRawExpr::is_semi_join_cond() const
{
  bool ret = false;
  if (type_ == T_OP_LEFT_SEMI || type_ == T_OP_LEFT_ANTI_SEMI)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->get_expr_type() == T_REF_COLUMN
      && binary_expr->get_second_op_expr()->get_expr_type() == T_REF_COLUMN)
      ret = true;
  }
  return ret;
}

bool ObRawExpr::is_one_side_const() const
{
  bool ret = false;
  if (type_ >= T_OP_LE && type_ <= T_OP_GT)
  {
    ObBinaryOpRawExpr *binary_expr = dynamic_cast<ObBinaryOpRawExpr *>(const_cast<ObRawExpr *>(this));
    if (binary_expr->get_first_op_expr()->is_const()
      || binary_expr->get_second_op_expr()->is_const())
      ret = true;
  }
  return ret;
}

bool ObRawExpr::is_sub_query() const
{
  return (type_ == T_REF_QUERY);
}


//add :e
bool ObRawExpr::is_aggr_fun() const
{
  bool ret = false;
  if (type_ >= T_FUN_MAX && type_ <= T_FUN_AVG)
    ret = true;
  return ret;
}

int ObConstRawExpr::set_value_and_type(const common::ObObj& val)
{
  int ret = OB_SUCCESS;
  switch(val.get_type())
  {
    case ObNullType:   // 空类型
      this->set_expr_type(T_NULL);
      this->set_result_type(ObNullType);
      break;
    case ObIntType:
      this->set_expr_type(T_INT);
      this->set_result_type(ObIntType);
      break;
    case ObFloatType:              // @deprecated
      this->set_expr_type(T_FLOAT);
      this->set_result_type(ObFloatType);
      break;
    case ObDoubleType:             // @deprecated
      this->set_expr_type(T_DOUBLE);
      this->set_result_type(ObDoubleType);
      break;
    case ObPreciseDateTimeType:    // =5
    case ObCreateTimeType:
    case ObModifyTimeType:
      this->set_expr_type(T_DATE);
      this->set_result_type(ObPreciseDateTimeType);
      break;
    case ObVarcharType:
      this->set_expr_type(T_STRING);
      this->set_result_type(ObVarcharType);
      break;
    case ObBoolType:
      this->set_expr_type(T_BOOL);
      this->set_result_type(ObBoolType);
      break;
      //add fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
    case ObDecimalType:
      this->set_expr_type(T_DECIMAL);
      this->set_result_type(ObDecimalType);
      break;
      //add:e
    default:
      ret = OB_NOT_SUPPORTED;
      TBSYS_LOG(WARN, "obj type not support, type=%d", val.get_type());
      break;
  }
  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (ObExtendType != val.get_type())
    {
      value_ = val;
    }
  }
  return ret;
}

void ObConstRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s : ", get_type_name(get_expr_type()));
  switch(get_expr_type())
  {
    case T_INT:
    {
      int64_t i = 0;
      value_.get_int(i);
      fprintf(fp, "%ld\n", i);
      break;
    }
    case T_STRING:
    case T_BINARY:
    {
      ObString str;
      value_.get_varchar(str);
      fprintf(fp, "%.*s\n", str.length(), str.ptr());
      break;
    }
    case T_DATE:
    {
      ObDateTime d = static_cast<ObDateTime>(0L);
      value_.get_datetime(d);
      fprintf(fp, "%ld\n", d);
      break;
    }
    case T_FLOAT:
    {
      float f = 0.0f;
      value_.get_float(f);
      fprintf(fp, "%f\n", f);
      break;
    }
    case T_DOUBLE:
    {
      double d = 0.0f;
      value_.get_double(d);
      fprintf(fp, "%lf\n", d);
      break;
    }
    case T_DECIMAL:
    {
      ObString str;
      value_.get_varchar(str);
      fprintf(fp, "%.*s\n", str.length(), str.ptr());
      break;
    }
    case T_BOOL:
    {
      bool b = false;
      value_.get_bool(b);
      fprintf(fp, "%s\n", b ? "TRUE" : "FALSE");
      break;
    }
    case T_NULL:
    {
      fprintf(fp, "NULL\n");
      break;
    }
    case T_UNKNOWN:
    {
      fprintf(fp, "UNKNOWN\n");
      break;
    }
    default:
      fprintf(fp, "error type!\n");
      break;
  }
}

int ObConstRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  UNUSED(logical_plan);
  UNUSED(physical_plan);
  UNUSED(transformer);
  float f = 0.0f;
  double d = 0.0;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  switch (item.type_)
  {
    case T_STRING:
    case T_BINARY:
      ret = value_.get_varchar(item.string_);
      break;
    case T_FLOAT:
      ret = value_.get_float(f);
      item.value_.float_ = f;
      break;
    case T_DOUBLE:
      ret = value_.get_double(d);
      item.value_.double_ = d;
      break;
    case T_DECIMAL:
      //modify xsl ECNU_DECIMAL 2016_12
      //modify fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
      //ret = value_.get_varchar(item.string_);
        ret = value_.get_decimal_v2(item.dec);
        item.len = value_.get_nwords();
      //modify:e
      //modify:e
      break;
    case T_INT:
      ret = value_.get_int(item.value_.int_);//slwang note:value_是ObConstRawExpr的成员变量，是一个ObObj对象
                                             //get_int实质上是把ObObj中的value_中的int_val值（这才是实际存储的值）传给了item下的value_中的int_
      break;
    case T_BOOL:
      ret = value_.get_bool(item.value_.bool_);
      break;
    case T_DATE:
      ret = value_.get_precise_datetime(item.value_.datetime_);
      break;
    case T_QUESTIONMARK:
      ret = value_.get_int(item.value_.int_);
      break;
    case T_SYSTEM_VARIABLE:
    case T_TEMP_VARIABLE:
      ret = value_.get_varchar(item.string_);
      break;
    case T_NULL:
      break;
    default:
      TBSYS_LOG(WARN, "unexpected expression type %d", item.type_);
      ret = OB_ERR_EXPR_UNKNOWN;
      break;
  }
  if (OB_SUCCESS == ret)
  {
    ret = inter_expr.add_expr_item(item);
  }
  return ret;
}

//add zt 20151125:b
int ObArrayRawExpr::fill_sql_expression(
    ObSqlExpression &inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item, idx_item;
  UNUSED(transformer);
  UNUSED(logical_plan);
  UNUSED(physical_plan);
//  if( OB_SUCCESS != (ret = idx_expr_->fill_sql_expression(
//                             inter_expr,
//                             transformer,
//                             logical_plan,
//                             physical_plan)))
//  {
//    TBSYS_LOG(WARN, "fill expression for the array idx fail");
//  }

  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.string_ = array_name_;

  idx_item.data_type_ = ObIntType; //idx must be int value
  if( ObIntType == idx_value_.get_type() )
  {
    idx_item.type_ = T_INT;
    idx_value_.get_int(idx_item.value_.int_);
  }
  else if ( ObVarcharType == idx_value_.get_type() )
  {
    idx_item.type_ = T_TEMP_VARIABLE;
    idx_value_.get_varchar(idx_item.string_);
  }

  if( OB_SUCCESS == ret )
  {
    //comment: it is more reasonable to push idx_item first, then item
    //Thus, idx value would be calculated first, then the array value
    //However, I want to control the serialization of array var,
    //Maybe the array val could computed before serialization
    if( OB_SUCCESS != (ret = inter_expr.add_expr_item(item)) )
    {
      TBSYS_LOG(WARN, "add idx item fail");
    }
    else
    {
      ret = inter_expr.add_expr_item(idx_item);
    }
  }
  return ret;
}

void ObArrayRawExpr::print(FILE *fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s : ", get_type_name(get_expr_type()));
  fprintf(fp, "%.*s[%s]\n", array_name_.length(), array_name_.ptr(), to_cstring(idx_value_));
//  idx_expr_->print(fp, level+1);
}
//add zt 20151125:e

void ObCurTimeExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
}

int ObCurTimeExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  UNUSED(physical_plan);
  UNUSED(transformer);

  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type(); //T_CUR_TIME
  item.data_type_ = ObPreciseDateTimeType;
  item.value_.int_ = logical_plan->get_cur_time_fun_type(); //just place holder
  ret = inter_expr.add_expr_item(item);

  return ret;
}

void ObUnaryRefRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObUnaryRefRawExpr> %s : %lu </ObUnaryRefRawExpr>\n", get_type_name(get_expr_type()), id_);
}

int ObUnaryRefRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  if (transformer == NULL || logical_plan == NULL || physical_plan == NULL)
  {
    TBSYS_LOG(ERROR, "transformer error");
    ret = OB_ERROR;
  }
  else
  {
    ErrStat err_stat;
    int32_t index = OB_INVALID_INDEX;
    ret = transformer->gen_physical_select(logical_plan, physical_plan, err_stat, id_, &index);
    item.value_.int_ = index;
  }
  if (ret == OB_SUCCESS && OB_INVALID_INDEX == item.value_.int_)
  {
    TBSYS_LOG(ERROR, "generating physical plan for sub-query error");
    ret = OB_ERROR;
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObBinaryRefRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  if (first_id_ == OB_INVALID_ID)
    fprintf(fp, "<ObBinaryRefRawExpr> %s : [table_id, column_id] = [NULL, %lu] </ObBinaryRefRawExpr>\n",
            get_type_name(get_expr_type()), second_id_);
  else
    fprintf(fp, "<ObBinaryRefRawExpr> %s : [table_id, column_id] = [%lu, %lu] </ObBinaryRefRawExpr>\n",
            get_type_name(get_expr_type()), first_id_, second_id_);
}

int ObBinaryRefRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  UNUSED(transformer);
  UNUSED(logical_plan);
  UNUSED(physical_plan);
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();

  if (ret == OB_SUCCESS && get_expr_type() == T_REF_COLUMN)
  {
    item.value_.cell_.tid = first_id_;
    item.value_.cell_.cid = second_id_;
  }
  else
  {
    // No other type
    ret = OB_ERROR;
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObUnaryOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObUnaryOpRawExpr>\n");
  
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  
  expr_->print(fp, level + 1);
  
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "</ObUnaryOpRawExpr>\n");
}

int ObUnaryOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = 1; /* One operator */

  ret = expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObBinaryOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObBinaryOpRawExpr>\n");
  
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  
  first_expr_->print(fp, level + 1);
  second_expr_->print(fp, level + 1);
  
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "</ObBinaryOpRawExpr>\n");
}

void ObBinaryOpRawExpr::set_op_exprs(ObRawExpr *first_expr, ObRawExpr *second_expr)
{
  ObItemType exchange_type = T_MIN_OP;
  switch (get_expr_type())
  {
    case T_OP_LE:
      exchange_type = T_OP_GE;
      break;
    case T_OP_LT:
      exchange_type = T_OP_GT;
      break;
    case T_OP_GE:
      exchange_type = T_OP_LE;
      break;
    case T_OP_GT:
      exchange_type = T_OP_LT;
      break;
    case T_OP_EQ:
    case T_OP_NE:
      exchange_type = get_expr_type();
      break;
    default:
      exchange_type = T_MIN_OP;
      break;
  }
  if (exchange_type != T_MIN_OP
    && first_expr && first_expr->is_const()
    && second_expr && second_expr->is_column())
  {
    set_expr_type(exchange_type);
    first_expr_ = second_expr;
    second_expr_ = first_expr;
  }
  else
  {
    first_expr_ = first_expr;
    second_expr_ = second_expr;
  }
}

int ObBinaryOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  bool dem_1_to_2 = false;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = 2; /* Two operators */

  // all form with 1 dimension and without sub-select will be changed to 2 dimensions
  // c1 in (1, 2, 3) ==> (c1) in ((1), (2), (3))
  if ((ret = first_expr_->fill_sql_expression(
                             inter_expr,
                             transformer,
                             logical_plan,
                             physical_plan)) == OB_SUCCESS
    && (get_expr_type() == T_OP_IN || get_expr_type() == T_OP_NOT_IN))
  {
    if (!first_expr_ || !second_expr_)
    {
      ret = OB_ERR_EXPR_UNKNOWN;
    }
    else if (first_expr_->get_expr_type() != T_OP_ROW
      && first_expr_->get_expr_type() != T_REF_QUERY
      && second_expr_->get_expr_type() == T_OP_ROW)//slwang note: 表达式：c1 in (1, 2, 3) c1是T_REF_COLUMN;(1,2,3)是T_OP_ROW
    {
      dem_1_to_2 = true;
      ExprItem dem2;
      dem2.type_ = T_OP_ROW;
      dem2.data_type_ = ObIntType;
      dem2.value_.int_ = 1;
      ret = inter_expr.add_expr_item(dem2);
    }
    if (OB_LIKELY(ret == OB_SUCCESS))
    {
      ExprItem left_item;
      left_item.type_ = T_OP_LEFT_PARAM_END;
      left_item.data_type_ = ObIntType;
      switch (first_expr_->get_expr_type())
      {
        case T_OP_ROW:
        case T_REF_QUERY:
          left_item.value_.int_ = 2;
          break;
        default:
          left_item.value_.int_ = 1;
          break;
      }
      if (dem_1_to_2)
        left_item.value_.int_ = 2;
      ret = inter_expr.add_expr_item(left_item);
    }
  }
  if (ret == OB_SUCCESS)
  {
    if (!dem_1_to_2)
    {
      ret = second_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
    }
    else
    {
      ExprItem dem2;
      dem2.type_ = T_OP_ROW;
      dem2.data_type_ = ObIntType;
      dem2.value_.int_ = 1;//slwang note
      ObMultiOpRawExpr *row_expr = dynamic_cast<ObMultiOpRawExpr*>(second_expr_);
      if (row_expr != NULL)
      {
        ExprItem row_item;
        row_item.type_ = row_expr->get_expr_type();
        row_item.data_type_ = row_expr->get_result_type();
        row_item.value_.int_ = row_expr->get_expr_size();
        for (int32_t i = 0; ret == OB_SUCCESS && i < row_expr->get_expr_size(); i++)
        {
          if ((ret = row_expr->get_op_expr(i)->fill_sql_expression(
                                                   inter_expr,
                                                   transformer,
                                                   logical_plan,
                                                   physical_plan)) != OB_SUCCESS
            || (ret = inter_expr.add_expr_item(dem2)) != OB_SUCCESS)
          {
            break;
          }
        }
        if (ret == OB_SUCCESS)
          ret = inter_expr.add_expr_item(row_item);
      }
      else
      {
        ret = OB_ERR_EXPR_UNKNOWN;
      }
    }
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObTripleOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObTripleOpRawExpr>\n");
  
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  first_expr_->print(fp, level + 1);
  second_expr_->print(fp, level + 1);
  third_expr_->print(fp, level + 1);
  
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "</ObTripleOpRawExpr>\n");
}

void ObTripleOpRawExpr::set_op_exprs(
    ObRawExpr *first_expr,
    ObRawExpr *second_expr,
    ObRawExpr *third_expr)
{
  first_expr_ = first_expr;
  second_expr_ = second_expr;
  third_expr_ = third_expr;
}

int ObTripleOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = 3; /* thress operators */

  if (ret == OB_SUCCESS)
    ret = first_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = second_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = third_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObMultiOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObMultiOpRawExpr>\n");
  
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  for (int32_t i = 0; i < exprs_.size(); i++)
  {
    exprs_[i]->print(fp, level + 1);
  }
  
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "</ObMultiOpRawExpr>\n");
}

int ObMultiOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = get_expr_type();
  item.data_type_ = get_result_type();
  item.value_.int_ = exprs_.size();

  for (int32_t i = 0; ret == OB_SUCCESS && i < exprs_.size(); i++)
  {
    ret = exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  }
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObCaseOpRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  if (arg_expr_)
    arg_expr_->print(fp, level + 1);
  for (int32_t i = 0; i < when_exprs_.size() && i < then_exprs_.size(); i++)
  {
    when_exprs_[i]->print(fp, level + 1);
    then_exprs_[i]->print(fp, level + 1);
  }
  if (default_expr_)
  {
    default_expr_->print(fp, level + 1);
  }
  else
  {
    for(int i = 0; i < level; ++i) fprintf(fp, "    ");
    fprintf(fp, "DEFAULT : NULL\n");
  }
}

int ObCaseOpRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  if (arg_expr_ == NULL)
    item.type_ = T_OP_CASE;
  else
    item.type_ = T_OP_ARG_CASE;
  item.data_type_ = get_result_type();
  item.value_.int_ = (arg_expr_ == NULL ? 0 : 1) + when_exprs_.size() + then_exprs_.size();
  item.value_.int_ += (default_expr_ == NULL ? 0 : 1);

  if (ret == OB_SUCCESS && arg_expr_ != NULL)
    ret = arg_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  for (int32_t i = 0; ret == OB_SUCCESS && i < when_exprs_.size() && i < then_exprs_.size(); i++)
  {
    ret = when_exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
    if (ret != OB_SUCCESS)
      break;
    ret = then_exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  }
  if (ret == OB_SUCCESS && default_expr_ != NULL)
    ret = default_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item(item);
  return ret;
}

void ObAggFunRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s\n", get_type_name(get_expr_type()));
  if (distinct_)
  {
    for(int i = 0; i < level; ++i) fprintf(fp, "    ");
    fprintf(fp, "DISTINCT\n");
  }
  if (param_expr_)
    param_expr_->print(fp, level + 1);
}

int ObAggFunRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  inter_expr.set_aggr_func(get_expr_type(), distinct_);
  if (param_expr_)
    ret = param_expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  return ret;
}

void ObSysFunRawExpr::print(FILE* fp, int32_t level) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "%s : %.*s\n", get_type_name(get_expr_type()), func_name_.length(), func_name_.ptr());
  for (int32_t i = 0; i < exprs_.size(); i++)
  {
    exprs_[i]->print(fp, level + 1);
  }
}

int ObSysFunRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan) const
{
  int ret = OB_SUCCESS;
  ExprItem item;
  item.type_ = T_FUN_SYS;
  item.string_ = func_name_;
  item.value_.int_ = exprs_.size();
  for (int32_t i = 0; ret == OB_SUCCESS && i < exprs_.size(); i++)
  {
    ret = exprs_[i]->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
    if (ret != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Add parameters of system function failed, param %d", i + 1);
      break;
    }
  }
  if (ret == OB_SUCCESS && (ret = inter_expr.add_expr_item(item)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add system function %.*s failed", func_name_.length(), func_name_.ptr());
  }
  return ret;
}

ObSqlRawExpr::ObSqlRawExpr()
{
  expr_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  column_id_ = OB_INVALID_ID;
  is_apply_ = false;
  contain_aggr_ = false;
  contain_alias_ = false;
  is_columnlized_ = false;
  expr_ = NULL;

  //add dhc [join_without_pushdown_is_null/query_optimizer] 20151214:b
  can_push_down_with_outerjoin_ = true;
  //add duyr 20151214:e
}

ObSqlRawExpr::ObSqlRawExpr(
    uint64_t expr_id, uint64_t table_id, uint64_t column_id, ObRawExpr* expr)
{
  table_id_ = table_id;
  expr_id_ = expr_id;
  column_id_ = column_id;
  is_apply_ = false;
  contain_aggr_ = false;
  contain_alias_ = false;
  is_columnlized_ = false;
  expr_ = expr;

  //add dhc [join_without_pushdown_is_null/query_optimizer] 20151214:b
  can_push_down_with_outerjoin_ = true;
  //add duyr 20151214:e
}

int ObSqlRawExpr::fill_sql_expression(
    ObSqlExpression& inter_expr,
    ObTransformer *transformer,
    ObLogicalPlan *logical_plan,
    ObPhysicalPlan *physical_plan)
{
  int ret = OB_SUCCESS;
  if (!(transformer == NULL && logical_plan == NULL && physical_plan == NULL)
    && !(transformer != NULL && logical_plan != NULL && physical_plan != NULL))
  {
    TBSYS_LOG(WARN,"(ObTransformer, ObLogicalPlan, ObPhysicalPlan) should be set together");
  }

  inter_expr.set_tid_cid(table_id_, column_id_);
  if (ret == OB_SUCCESS)
    ret = expr_->fill_sql_expression(inter_expr, transformer, logical_plan, physical_plan);
  if (ret == OB_SUCCESS)
    ret = inter_expr.add_expr_item_end();
  return ret;
}

void ObSqlRawExpr::print(FILE* fp, int32_t level, int32_t index) const
{
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObSqlRawExpr %d Begin>\n", index);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "expr_id = %lu\n", expr_id_);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  if (table_id_ == OB_INVALID_ID)
    fprintf(fp, "(table_id : column_id) = (NULL : %lu)\n", column_id_);
  else
    fprintf(fp, "(table_id : column_id) = (%lu : %lu)\n", table_id_, column_id_);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "is_apply_: %s, contain_aggr_: %s, contain_alias_: %s, is_columnlized_: %s\n", 
  	is_apply_?"True":"False", contain_aggr_?"True":"False", contain_alias_?"True":"False", is_columnlized_?"True":"False");
  
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "tables_set_: [");
  for(int32_t i = 0; i < 32 * 8; ++i)
  {
    if(tables_set_.has_member(i))
    {
      fprintf(fp, "%d, ", i);
    }
  }
  fprintf(fp, "]\n");
  
  expr_->print(fp, level + 1);
  for(int i = 0; i < level; ++i) fprintf(fp, "    ");
  fprintf(fp, "<ObSqlRawExpr %d End>\n", index);
}

/** optimize sql expression */
// add by lxb on 2017/02/15 for logical optimizer
int ObSqlRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  common::ObBitSet<>& bit_set = get_tables_set();
  bit_set.clear();
  expr_->optimize_sql_expression(main_stmt, bit_set, table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObBinaryOpRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  first_expr_->optimize_sql_expression(main_stmt, bit_set, table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, type);
  second_expr_->optimize_sql_expression(main_stmt, bit_set, table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObBinaryRefRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  if (type == 1) 
  {
    if (first_id_ == table_id) 
    {
      real_table_id = table_id_hashmap.find(second_id_)->second;
      set_first_ref_id(real_table_id);
      set_second_ref_id(column_id_hashmap.find(second_id_)->second);
      
      // the index in tables_hash_ of subquery has deleted before.
      // bit_set.del_member(main_stmt->get_table_bit_index(table_id)); no need delete bit_set generated by table_id.
      // add bit_set generated by sub_table_id.
      bit_set.add_member(main_stmt->get_table_bit_index(real_table_id)); 
    }
    else 
    {
      bit_set.add_member(main_stmt->get_table_bit_index(first_id_));
    }
  }
  else if (type == 2) 
  {
    uint64_t first_ref_id = get_first_ref_id();
    set_first_ref_id(alias_table_hashmap[first_ref_id]);
    bit_set.add_member(main_stmt->get_table_bit_index(first_ref_id));
  }
  else if (type == 3) 
  {
    if(first_id_ == table_id)
    {
      set_first_ref_id(real_table_id);
      bit_set.add_member(main_stmt->get_table_bit_index(real_table_id));
    }else 
    {
      bit_set.add_member(main_stmt->get_table_bit_index(first_id_));
    }
  }
  else if (type == 4) 
  {
    if(first_id_ != table_id)
    {
      real_table_id = first_id_;
    }
    bit_set.add_member(main_stmt->get_table_bit_index(first_id_));
  }
  //add slwang [bugfix exists subquery] 20171104:b
  else if(type == 5)
  {
        if(first_id_ == table_id)//table_id代表重名的表的原table_id
        {
            set_first_ref_id(real_table_id);//real_table_id为new gen_id因为重名，而将新产生的gen_id替换表达式中的原表的id
            bit_set.add_member(main_stmt->get_table_bit_index(real_table_id));
        }
        else//必须这样做，因为调用此函数时，先将此sql_expr的bit_set清空了
        {
            bit_set.add_member(main_stmt->get_table_bit_index(first_id_));
        }
  }
  //add [bugfix exists subquery] 20171104:b
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObConstRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  UNUSED(main_stmt);
  UNUSED(bit_set);
  UNUSED(table_id_hashmap);
  UNUSED(column_id_hashmap);
  UNUSED(table_id);
  UNUSED(real_table_id);
  UNUSED(alias_table_hashmap);
  UNUSED(type);
  
  return ret;
}

// add by lxb on 2017/03/21 for logical optimizer
int ObArrayRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  UNUSED(main_stmt);
  UNUSED(bit_set);
  UNUSED(table_id_hashmap);
  UNUSED(column_id_hashmap);
  UNUSED(table_id);
  UNUSED(real_table_id);
  UNUSED(alias_table_hashmap);
  UNUSED(type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObCurTimeExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  UNUSED(main_stmt);
  UNUSED(bit_set);
  UNUSED(table_id_hashmap);
  UNUSED(column_id_hashmap);
  UNUSED(table_id);
  UNUSED(real_table_id);
  UNUSED(alias_table_hashmap);
  UNUSED(type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObUnaryRefRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  UNUSED(main_stmt);
  UNUSED(bit_set);
  UNUSED(table_id_hashmap);
  UNUSED(column_id_hashmap);
  UNUSED(table_id);
  UNUSED(real_table_id);
  UNUSED(alias_table_hashmap);
  UNUSED(type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObUnaryOpRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  expr_->optimize_sql_expression(main_stmt, bit_set, table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObTripleOpRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  first_expr_->optimize_sql_expression(main_stmt, bit_set, table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, type);
  second_expr_->optimize_sql_expression(main_stmt, bit_set, table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, type);
  third_expr_->optimize_sql_expression(main_stmt, bit_set, table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObMultiOpRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  UNUSED(main_stmt);
  UNUSED(bit_set);
  UNUSED(table_id_hashmap);
  UNUSED(column_id_hashmap);
  UNUSED(table_id);
  UNUSED(real_table_id);
  UNUSED(alias_table_hashmap);
  UNUSED(type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObCaseOpRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  UNUSED(main_stmt);
  UNUSED(bit_set);
  UNUSED(table_id_hashmap);
  UNUSED(column_id_hashmap);
  UNUSED(table_id);
  UNUSED(real_table_id);
  UNUSED(alias_table_hashmap);
  UNUSED(type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObAggFunRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  UNUSED(main_stmt);
  UNUSED(bit_set);
  UNUSED(table_id_hashmap);
  UNUSED(column_id_hashmap);
  UNUSED(table_id);
  UNUSED(real_table_id);
  UNUSED(alias_table_hashmap);
  UNUSED(type);
  
  return ret;
}

// add by lxb on 2017/02/15 for logical optimizer
int ObSysFunRawExpr::optimize_sql_expression(
    ObSelectStmt *&main_stmt,
    common::ObBitSet<> &bit_set,
    std::map<uint64_t, uint64_t> table_id_hashmap, 
    std::map<uint64_t, uint64_t> column_id_hashmap,
    uint64_t table_id,
    uint64_t &real_table_id,
    std::map<uint64_t, uint64_t> alias_table_hashmap,
    int type)
{
  int ret = OB_SUCCESS;
  
  UNUSED(main_stmt);
  UNUSED(bit_set);
  UNUSED(table_id_hashmap);
  UNUSED(column_id_hashmap);
  UNUSED(table_id);
  UNUSED(real_table_id);
  UNUSED(alias_table_hashmap);
  UNUSED(type);
  
  return ret;
}
