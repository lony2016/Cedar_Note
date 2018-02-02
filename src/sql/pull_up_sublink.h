#ifndef OCEANBASE_PULL_UP_SUBLINK_H_
#define OCEANBASE_PULL_UP_SUBLINK_H_
#include "common/ob_define.h"
#include "parse_node.h"
#include "ob_raw_expr.h"
#include "ob_logical_plan.h"
#include "ob_select_stmt.h"

namespace oceanbase
{
  namespace sql
  {

    int pull_up_sublinks_recurse(ResultPlan *result_plan, ObLogicalPlan *logic_plan, ObRawExpr *raw_expr, ObSqlRawExpr *sql_expr);
    int pull_up_sublinks(ResultPlan *result_plan, ObLogicalPlan *logic_plan);
    int get_select_stmt_by_expr(ObLogicalPlan *logic_plan,ObSqlRawExpr *sql_expr,ObSelectStmt *&output_stmt);
  }
}

#endif //OCEANBASE_PULL_UP_SUBLINK_H_

