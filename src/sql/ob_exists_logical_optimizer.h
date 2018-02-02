#ifndef OB_EXISTS_LOGICAL_OPTIMIZER_H
#define OB_EXISTS_LOGICAL_OPTIMIZER_H

#include "sql/ob_result_set.h"
/* parse_node.h包含了 "ob_item_type.h" */
#include "sql/parse_node.h"
#include "sql/ob_multi_logic_plan.h"

namespace oceanbase
{
  namespace sql
  {
    class ObExistsLogicalOptimizer
    {
      public:
        ObExistsLogicalOptimizer(){}
        ~ObExistsLogicalOptimizer(){}
        /*optimize entrance slwang*/
        static int optimize_exists(ResultPlan &result_plan, ObResultSet &result);

        static bool is_exists_query(ObLogicalPlan* logical_plan, uint64_t& sub_query_id, uint64_t& exists_expr_id);

        //use recursion
        static int pull_up_exists_subquery(ResultPlan &result_plan, ObLogicalPlan *&logical_plan);
        static int pull_up_exists_recursion(ResultPlan &result_plan, ObLogicalPlan *&logical_plan, ObRawExpr *raw_expr, ObSqlRawExpr *sql_raw_expr, int32_t &index);

        //
        static int pull_up_exists_logical_plan(ResultPlan &result_plan, ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id, uint64_t exists_expr_id, int32_t &index);

        static int pull_up_exists_from_items(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id);

        static int pull_up_exists_table_items(ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id);

        static int pull_up_exists_column_items(ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id);

        static int delete_sub_select_exprs(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, int32_t index);

        static int pull_up_exists_where_exprs(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id, uint64_t exists_expr_id, int32_t &index);

        //add slwang[bugfix exists alias name ] 20171104
        static int find_exists_tables_alias(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt);
        //add 20171104:e

        //add slwang[bugfix exists alias name ] 20171107:b
        static int if_can_pull_up(ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt);
        //add 20171107:e

        static int set_exists_tables_alias(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t table_alias_id);
        //wangyaozhao has this method
        //static int get_select_stmt_by_expr(ObLogicalPlan *logic_plan,ObSqlRawExpr *sql_expr,ObSelectStmt *&output_stmt);


    };
  }
}


#endif // OB_EXISTS_LOGICAL_OPTIMIZER_H
