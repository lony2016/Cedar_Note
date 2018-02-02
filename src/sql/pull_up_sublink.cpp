#include "pull_up_sublink.h"
#include <stdio.h>
#include <string.h>
#include "ob_logical_plan.h"
#include "ob_stmt.h"
#include "ob_select_stmt.h"
#include "ob_result_set.h"
#include "parse_malloc.h"
#include "ob_multi_logic_plan.h"
#include "ob_explain_stmt.h"
#include "ob_basic_stmt.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

#define CREATE_RAW_EXPR(expr, type_name, result_plan)    \
({    \
  ObMultiLogicPlan *multi_logic_plan = NULL; \
  multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan->plan_tree_); \
  ObLogicalPlan* logical_plan = multi_logic_plan->at(0); \
  ObStringBuf* name_pool = static_cast<ObStringBuf*>(result_plan->name_pool_);  \
  expr = (type_name*)parse_malloc(sizeof(type_name), name_pool);   \
  if (expr != NULL) \
  { \
    expr = new(expr) type_name();   \
    if (OB_SUCCESS != logical_plan->add_raw_expr(expr))    \
    { \
      expr = NULL;  /* no memory leak, bulk dealloc */ \
    } \
  } \
  if (expr == NULL)  \
  { \
    result_plan->err_stat_.err_code_ = OB_ERR_PARSER_MALLOC_FAILED; \
    TBSYS_LOG(WARN, "out of memory"); \
    snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG,  \
        "Fail to malloc new raw expression"); \
  } \
  expr; \
})


int oceanbase::sql::get_select_stmt_by_expr(
  ObLogicalPlan *logic_plan,
  ObSqlRawExpr *sql_expr,
  ObSelectStmt *&output_stmt
  )
{
  for (int32_t i = 0; i < logic_plan->get_stmts_count(); i++)
  {
    ObBasicStmt *stmt = logic_plan->get_stmt(i);
    if (stmt->get_stmt_type() == ObBasicStmt::T_EXPLAIN)
    {
      continue;
    }

    else if (stmt->get_stmt_type() == ObBasicStmt::T_SELECT)
    {
      ObSelectStmt *select_stmt = static_cast<ObSelectStmt*>(stmt);
      common::ObVector<uint64_t>& where_exprs = select_stmt->get_where_exprs();
      for (int32_t j = 0; j < where_exprs.size(); j++)
      {
        if (where_exprs.at(j) == sql_expr->get_expr_id())
        {
          output_stmt = select_stmt;
          return OB_SUCCESS;
        }
      }
    }
    else
    {
      // invalid stmt
      // TODO: add support to update, delete, insert
      continue;
    }
  }

  return OB_ERROR;

}


int oceanbase::sql::pull_up_sublinks_recurse(
  ResultPlan *result_plan,
  ObLogicalPlan *logic_plan,
  ObRawExpr *raw_expr,
  ObSqlRawExpr *sql_expr
  )
{
  if(!logic_plan && !raw_expr)
    return 1;

  if(raw_expr->get_expr_type() == T_OP_IN || raw_expr->get_expr_type() == T_OP_NOT_IN)
  {
    ObBinaryOpRawExpr *binary_expr = static_cast<ObBinaryOpRawExpr*>(raw_expr);
    ObRawExpr * right_expr = binary_expr->get_second_op_expr();
    ObBinaryRefRawExpr *left_expr = static_cast<ObBinaryRefRawExpr*>(binary_expr->get_first_op_expr());
    // check whether it is a subquery
    if(right_expr->get_expr_type() == T_REF_QUERY)
    {
      // left only has 1 column
      if(left_expr->get_expr_type() == T_REF_COLUMN)
      {
        // generate new table item
        uint64_t subquery_id = static_cast<ObUnaryRefRawExpr*>(binary_expr->get_second_op_expr())->get_ref_id();
        ObSelectStmt *sub_select_stmt = static_cast<ObSelectStmt*>(logic_plan->get_stmt_by_id(subquery_id));
        const SelectItem & sub_select_item = sub_select_stmt->get_select_item(0);
        uint64_t  sub_select_expr_id = sub_select_item.expr_id_;
        ObRawExpr* sub_select_raw_expr = logic_plan->get_expr(sub_select_expr_id)->get_expr();

        TBSYS_LOG(DEBUG, "pull up IN sublink: subselect expr type: %d", T_REF_COLUMN);
        // if select item in subselect is not column ref, don't process
        if(sub_select_raw_expr->get_expr_type() != T_REF_COLUMN)
        {
          return 1;
        }

        ObSelectStmt *select_stmt;
        int ret = get_select_stmt_by_expr(logic_plan, sql_expr, select_stmt);
        if (ret != OB_SUCCESS)
        {
          // invalid stmt
          // TODO: add support to update, delete, insert
          return 1;
        }

        char generated_name[25];
        sprintf(generated_name, "generated_table%d", select_stmt->get_table_size());
        char *new_name = parse_strdup(generated_name, logic_plan->get_name_pool());
        // TODO: 内存分配方式是否恰当?
        ObString table_name(25, ObString::obstr_size_t(strlen(generated_name)), new_name);
        ObString alias_name;
        uint64_t table_id;
        ret = select_stmt->add_table_item(
          *result_plan,
          table_name,
          alias_name,
          table_id,
          TableItem::GENERATED_TABLE,
          subquery_id,
          true
          );


        // generate new column item
        ObBinaryRefRawExpr *sub_select_expr = static_cast<ObBinaryRefRawExpr*>(sub_select_raw_expr);
        uint64_t sub_select_table_id = sub_select_expr->get_first_ref_id();
        uint64_t sub_select_column_id = sub_select_expr->get_second_ref_id();
        ColumnItem *sub_select_column_item = sub_select_stmt->get_column_item_by_id(sub_select_table_id, sub_select_column_id);
        ColumnItem column_item;
        column_item.table_id_ = table_id;
        //there is only one column in sublink's select clause, so it is the min column id
        column_item.column_id_ = OB_APP_MIN_COLUMN_ID;
        column_item.query_id_ = 0;
        // get name of new column item
        if(sub_select_item.is_real_alias_) {
          // is alias, use alias name
          column_item.column_name_ = sub_select_item.alias_name_;
        }
        else {
          // otherwise, use column name in subquery
          column_item.column_name_ = sub_select_column_item->column_name_;
        }
        column_item.data_type_ = sub_select_column_item->data_type_;
        column_item.is_name_unique_ = false;
        column_item.is_group_based_ = sub_select_column_item->is_group_based_;

        ret = select_stmt->add_column_item(column_item);

        // add from item
        ret = select_stmt->add_from_item(table_id);

        // change father select's columnitem.is_name_unique_ to false
        ObVector<ColumnItem>& f_columns = select_stmt->get_column_items();
        for(int i = 0; i < f_columns.size(); ++i)
        {
          if(sub_select_column_id == f_columns.at(i).column_id_)
          {
            f_columns.at(i).is_name_unique_ = false;
            break;
          }
        }

        // change T_OP_IN to T_OP_LEFT_SEMI
        if(raw_expr->get_expr_type() == T_OP_IN)
        {
          raw_expr->set_expr_type(T_OP_LEFT_SEMI);
        }
        else if (raw_expr->get_expr_type() == T_OP_NOT_IN)
        {
          raw_expr->set_expr_type(T_OP_LEFT_ANTI_SEMI);
        }
        raw_expr->set_result_type(ObBoolType);

        ObBinaryRefRawExpr *right_expr = NULL;
        if (CREATE_RAW_EXPR(right_expr, ObBinaryRefRawExpr, result_plan) == NULL)
          return 0;
        right_expr->set_expr_type(T_REF_COLUMN);
        right_expr->set_result_type(column_item.data_type_);
        right_expr->set_first_ref_id(column_item.table_id_);
        right_expr->set_second_ref_id(column_item.column_id_);
        // add new right expr to T_OP_LEFT_SEMI
        binary_expr->set_op_exprs(binary_expr->get_first_op_expr(), right_expr);

        // change sql_expr's tables_sets
        sql_expr->get_tables_set().add_member(select_stmt->get_table_bit_index(table_id));


      }
      else if(left_expr->get_expr_type() == T_REF_QUERY)
      {

      }
      else if(left_expr->get_expr_type() == T_OP_ROW)
      {

      }
      else
      {

      }
    }
  }


  else if (raw_expr->get_expr_type() == T_OP_AND || raw_expr->get_expr_type() == T_OP_OR)
  {
    ObBinaryOpRawExpr * binary_expr = static_cast<ObBinaryOpRawExpr*>(raw_expr);
    pull_up_sublinks_recurse(result_plan, logic_plan, binary_expr->get_first_op_expr(), sql_expr);
    pull_up_sublinks_recurse(result_plan, logic_plan, binary_expr->get_second_op_expr(), sql_expr);
  }
  else
  {

  }

  return 0;
}

int oceanbase::sql::pull_up_sublinks(ResultPlan *result_plan, ObLogicalPlan *logic_plan)
{
  int ret = OB_SUCCESS;
  common::ObVector<ObSqlRawExpr*>& sql_exprs = logic_plan->get_exprs();
  int32_t num = sql_exprs.size();
  for (int32_t i = 0; i < num; i++)
  {
    pull_up_sublinks_recurse(result_plan, logic_plan, sql_exprs[i]->get_expr(), sql_exprs[i]);
  }
  return ret;
}
