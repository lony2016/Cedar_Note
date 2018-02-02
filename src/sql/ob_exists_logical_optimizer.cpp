#include "sql/ob_exists_logical_optimizer.h"
//#include "sql/ob_multi_logic_plan.h"
#include "sql/ob_raw_expr.h"
#include "common/ob_define.h"
#include "ob_optimizer_logical.h"
#include "sql/ob_explain_stmt.h"
//add slwang 20171108:b
#include "pull_up_sublink.h"
//add 20171108:e

#include<stdio.h>
//add slwang 20171104
#include<vector>

//using namespace std;

namespace oceanbase
{
  namespace sql
  {

    /*optimize entrance slwang*/
    //start
    int ObExistsLogicalOptimizer::optimize_exists(ResultPlan& result_plan, ObResultSet& result)
    {
      int ret = OB_SUCCESS;

      ObMultiLogicPlan* multi_logic_plan = static_cast<ObMultiLogicPlan*>(result_plan.plan_tree_);
      ObLogicalPlan* logical_plan = multi_logic_plan->at(0);
      //add by slwang
      FILE* fp1 = NULL;
      fp1 = fopen("/home/wangshuanglong/logic_plan_exists.txt", "at+");
      logical_plan->print(fp1, 0);
      fclose(fp1);
      //add e

      for(int32_t i = 0; ret == OB_SUCCESS && i < multi_logic_plan->size(); ++i)
      {
          logical_plan = multi_logic_plan->at(i);
          uint64_t sub_query_id = OB_INVALID_ID;
          uint64_t exists_expr_id = OB_INVALID_ID;

          if(logical_plan)
          {
              if(!(is_exists_query(logical_plan, sub_query_id, exists_expr_id)))
              {
                 TBSYS_LOG(DEBUG, "Sql is not a exists statement, don't need to be optimized of exists");
              }
              else
              {
                  //start exists optimizer
                  ret = pull_up_exists_subquery(result_plan, logical_plan);
              }
          }

      }

      //add by slwang
      FILE* fp2 = NULL;
      fp2 = fopen("/home/wangshuanglong/logic_optimize_exists.txt", "at+");
      logical_plan->print(fp2, 0);
      fclose(fp2);
      //add e
      UNUSED(result);
      return ret;
    }


    int ObExistsLogicalOptimizer::pull_up_exists_subquery(ResultPlan &result_plan, ObLogicalPlan *&logical_plan)
    {
        int ret = OB_SUCCESS;
        common::ObVector<ObSqlRawExpr*>& sql_raw_exprs = logical_plan->get_exprs();

        for (int32_t i = 0; i < sql_raw_exprs.size(); ++i)
        {
            ObRawExpr* raw_expr = sql_raw_exprs.at(i)->get_expr();
            if(ret == OB_SUCCESS)
            {
                ret = pull_up_exists_recursion(result_plan, logical_plan, raw_expr, sql_raw_exprs.at(i), i);
            }
        }
        return ret;
    }

    int ObExistsLogicalOptimizer::pull_up_exists_recursion(ResultPlan &result_plan, ObLogicalPlan *&logical_plan, ObRawExpr *raw_expr, ObSqlRawExpr *sql_raw_expr, int32_t &index)
    {
        int ret = OB_SUCCESS;
        if(!logical_plan && !raw_expr)
          return 1;//1 = OB_ERROR
//        TBSYS_LOG(INFO, "pull_up_exists_logical_plan exists_slwang raw_expr->get_expr_type() = %d", raw_expr->get_expr_type());
        if(raw_expr->get_expr_type() == T_OP_EXISTS)
        {
            uint64_t exists_expr_id = sql_raw_expr->get_expr_id();
            ObUnaryOpRawExpr* unary_op_raw_expr = dynamic_cast<ObUnaryOpRawExpr*>(raw_expr);
            ObUnaryRefRawExpr* unary_expr = dynamic_cast<ObUnaryRefRawExpr*>(unary_op_raw_expr->get_op_expr());
            if(unary_expr->get_expr_type() == T_REF_QUERY)
            {
                uint64_t sub_query_id = unary_expr->get_ref_id();
                ObSelectStmt *main_stmt;

                //modify slwang [code optimize] 20171108:b
                //According to exists_sql_expr, get select_stmt its belongs to
                //ret = get_select_stmt_by_expr(logical_plan, sql_raw_expr, main_stmt);
                ret = sql::get_select_stmt_by_expr(logical_plan, sql_raw_expr, main_stmt);//use wangyanzhao function
                //modify slwang [code optimize] 20171108:b
                if (ret != OB_SUCCESS)
                {
                  // invalid stmt
                  // TODO: add support to update, delete, insert
                  return 1;
                }
                ObSelectStmt* sub_stmt = logical_plan->get_select_query(sub_query_id);
                ret = pull_up_exists_logical_plan(result_plan, logical_plan, main_stmt, sub_stmt, sub_query_id, exists_expr_id, index);
            }

        }
//        else if(raw_expr->get_expr_type() == T_OP_NOT)
//        {
//            ObUnaryOpRawExpr* unary_op_raw_expr = dynamic_cast<ObUnaryOpRawExpr*>(raw_expr);
//            ObRawExpr* unary_expr = unary_op_raw_expr->get_op_expr();
//            pull_up_exists_recursion(result_plan, logical_plan, unary_expr, sql_raw_expr, index);
//        }
        else if(raw_expr->get_expr_type() == T_OP_OR)
        {
            //TODO:
            //pull_up_exists_recursion
        }
        UNUSED(result_plan);
        return ret;
    }

    int ObExistsLogicalOptimizer::pull_up_exists_logical_plan(ResultPlan &result_plan, ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id, uint64_t exists_expr_id, int32_t &index)
    {
        int ret = OB_SUCCESS;

//        TBSYS_LOG(INFO, "pull_up_exists_logical_plan exists_slwang sub_query_id = %lu , exists_expr_id = %lu", sub_query_id, exists_expr_id);

        if (ret == OB_SUCCESS &&  ObOptimizerLogical::is_simple_subquery(sub_stmt))
        {
        }
        else
        {
          ret = OB_SQL_CAN_NOT_PULL_UP;
        }

        //add slwang[bugfix exists] 20171107:b
        if(ret == OB_SUCCESS)
        {
            ret = if_can_pull_up(main_stmt, sub_stmt);
        }
        //add 20171107:e

        //add slwang[bugfix exists] 20171104:b
        if(ret == OB_SUCCESS)
        {
            ret = find_exists_tables_alias(logical_plan, main_stmt, sub_stmt);
        }
        //add slwang:e
        //pull up subquery from_items
        if(ret == OB_SUCCESS)
        {
            ret = pull_up_exists_from_items(logical_plan, main_stmt, sub_stmt, sub_query_id);
        }

        //pull up subquery table_items
        if(ret == OB_SUCCESS)
        {
            ret = pull_up_exists_table_items(main_stmt, sub_stmt, sub_query_id);
        }

        //pull up subquery colums_items
        if(ret == OB_SUCCESS)
        {
            ret = pull_up_exists_column_items(main_stmt, sub_stmt, sub_query_id);
        }

        //select items of exists' subquery don't need, directly delete them.
        if(ret == OB_SUCCESS)
        {
            ret = delete_sub_select_exprs(logical_plan, main_stmt, sub_stmt, index);
        }

        //pull up suquery where_exprs
        if(ret == OB_SUCCESS)
        {
            ret = pull_up_exists_where_exprs(logical_plan, main_stmt, sub_stmt, sub_query_id, exists_expr_id, index);
        }

        //delete subquery's SelectStmt
        if(ret == OB_SUCCESS)
        {
            logical_plan->delete_stmt_by_query_id(sub_query_id);
        }

        //add slwang 20171107:b
        if (ret == OB_SUCCESS)
        {
        }
        else
        {
            if(ret == OB_SQL_CAN_NOT_PULL_UP)
            {
                TBSYS_LOG(DEBUG, "exists's subquery can not be pull up");
                ret = OB_NOT_SUPPORTED;
//                snprintf(result_plan->err_stat_.err_msg_, MAX_ERROR_MSG, "Not supported feature or function");
            }
            else
            {
                ret = OB_NOT_SUPPORTED;
                TBSYS_LOG(ERROR, "pull up exists sub query fail");

            }
        }
        //add 20171107:e
        UNUSED(result_plan);

        return ret;
    }

    
    /*Check whether the query is a exists statement */
    bool ObExistsLogicalOptimizer::is_exists_query(ObLogicalPlan* logical_plan, uint64_t& sub_query_id, uint64_t& exists_expr_id)
    {
      bool is_exists = false;
      int exprs_count = logical_plan->get_exprs_count();//slwang note : TO MODIFY: Take vector <ObSqlRawExpr> to loop
      for(int32_t i=1;i<=exprs_count;++i)
      {
         ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(i));//vector
         ObRawExpr* raw_expr = sql_raw_expr->get_expr();

         ObItemType type = raw_expr->get_expr_type();
         if(type == T_OP_EXISTS)
         {
           is_exists = true;
           exists_expr_id = sql_raw_expr->get_expr_id();
           ObUnaryOpRawExpr* unary_op_raw_expr = dynamic_cast<ObUnaryOpRawExpr*>(raw_expr);
           ObUnaryRefRawExpr* unary_expr = dynamic_cast<ObUnaryRefRawExpr*>(unary_op_raw_expr->get_op_expr());
           sub_query_id = unary_expr->get_ref_id();
           break;
         }
      }
      return is_exists;
    }

    //判断是否可以提升
    int ObExistsLogicalOptimizer::if_can_pull_up(ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt)
    {
        int ret = OB_SUCCESS;
        //判断主查询
        int32_t main_from_size = main_stmt->get_from_item_size();
        //判断主查询是否有left join或者right join
        for(int32_t i=0; i < main_from_size; ++i )
        {
            FromItem& main_from_item = const_cast<FromItem&>(main_stmt->get_from_item(i));
            bool main_is_joined = main_from_item.is_joined_;
            if(main_is_joined)
            {
                JoinedTable* main_joined_table = main_stmt->get_joined_table(main_from_item.table_id_);
                ObArray<uint64_t>& main_join_types = main_joined_table->get_join_types();
                for(int32_t i = 0; i < main_join_types.count(); ++i)
                {
                    uint64_t join_type = main_join_types.at(i);
                    if(join_type == JoinedTable::T_LEFT || join_type == JoinedTable::T_FULL || join_type == JoinedTable::T_RIGHT)
                    {
                        ret = OB_SQL_CAN_NOT_PULL_UP;
                        TBSYS_LOG(DEBUG, "slwang EXISTS SQL can not be pulled up because of out join");
                        return ret;
                    }
                }
            }

        }

        //判断子链接
        int32_t sub_from_size = sub_stmt->get_from_item_size();
        for (int32_t i = 0; i < sub_from_size; ++i)
        {
            FromItem& sub_from_item = const_cast<FromItem&>(sub_stmt->get_from_item(i));
            bool sub_is_joined = sub_from_item.is_joined_;
            if(sub_is_joined)
            {
                JoinedTable* sub_joined_table = sub_stmt->get_joined_table(sub_from_item.table_id_);
                ObArray<uint64_t>& sub_join_types = sub_joined_table->get_join_types();
                for(int32_t i = 0; i < sub_join_types.count(); ++i)
                {
                    uint64_t join_type = sub_join_types.at(i);
                    if(join_type == JoinedTable::T_LEFT || join_type == JoinedTable::T_FULL || join_type == JoinedTable::T_RIGHT)
                    {
                        ret = OB_SQL_CAN_NOT_PULL_UP;
                        TBSYS_LOG(DEBUG, "EXISTS SQL can not be pulled up because of out join");
                        return ret;
                    }
                }
            }
        }

        return ret;
    }

    /*modify sub_stmt has main stmt'table name,modify sub stmt'table name to alias name*/
    int ObExistsLogicalOptimizer::find_exists_tables_alias(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt)
    {
        int ret = OB_SUCCESS;
        //1.找到子查询中跟父查询中有重名的表的id,存储起来
        ObVector<TableItem>& main_table_items = main_stmt->get_table_items();
        ObVector<TableItem>& sub_table_items = sub_stmt->get_table_items();
        std::vector<uint64_t> table_alias_ids;
        for(int32_t i=0; i < sub_table_items.size(); ++i)
        {
            TableItem& sub_table_item = sub_table_items.at(i);
            bool flag = false;
            for(int32_t j=0; j < main_table_items.size();++j)
            {
                TableItem& main_table_item = main_table_items.at(j);
                if(main_table_item.table_id_ == sub_table_item.table_id_)//再加上ref_id是否相等就可以就把父查询和子查询都给更改了别名
                {
                    flag = true;
                    break;
                }
            }
            if(flag)
            {
                table_alias_ids.push_back(sub_table_item.table_id_);
            }
        }
        //遍历重名的表
        for(int32_t i=0; i < (int32_t)(table_alias_ids.size()); ++i)
        {
            uint64_t table_alias_id = table_alias_ids.at(i);
            set_exists_tables_alias(logical_plan, main_stmt, sub_stmt, table_alias_id);
        }
        return ret;
    }

    int ObExistsLogicalOptimizer::set_exists_tables_alias(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t table_alias_id)
    {
        int ret = OB_SUCCESS;

        //开始别名的调整
        uint64_t ref_id = table_alias_id;
        uint64_t gen_id = logical_plan->generate_table_id();
        //1.调整table_hash
        // remove_column_desc导致bit index减一，再执行add_column_desc，则bit index加1，减一和加一导致还是原来的值，就会出错
        ObRowDesc* table_hash = sub_stmt->get_table_hash();
        table_hash->add_column_desc(gen_id, OB_INVALID_ID);

        //2.更改from item
        int32_t from_item_size = sub_stmt->get_from_item_size();
        bool flag = false;//用来标记每个from item是否存在与父查询相同的表
        for(int32_t i = 0; i < from_item_size; ++i)
        {
            FromItem& from_item = const_cast<FromItem&>(sub_stmt->get_from_item(i));
            flag = false;
            if(from_item.is_joined_)
            {
                uint64_t joined_table_id = from_item.table_id_;
                JoinedTable* joined_table = sub_stmt->get_joined_table(joined_table_id);
                common::ObArray<uint64_t>& joined_table_ids = joined_table->get_table_ids();
                common::ObArray<uint64_t>& joined_expr_ids = joined_table->get_expr_ids();
                //修改table id
                for (int64_t j = 0; j < joined_table_ids.count(); ++j)
                {
                    if(joined_table_ids.at(j) == ref_id)
                    {
                        joined_table_ids.at(j) = gen_id;
                        flag = true;
//                        break;
                    }
                }
                //修改joined table的连接条件的sql_expr 的相关信息
                if (flag == true)
                {
                    for (int64_t j = 0; j < joined_expr_ids.count(); ++j)
                    {
                        ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(joined_expr_ids.at(j)));
                        std::map<uint64_t, uint64_t> table_id_hashmap;//exists的优化用不上
                        std::map<uint64_t, uint64_t> column_id_hashmap;//exists的优化用不上
                        std::map<uint64_t, uint64_t> alias_table_hashmap;//exists的优化用不上
                        //modify sql_expr tables_set
                        sql_raw_expr->optimize_sql_expression(sub_stmt, table_id_hashmap, column_id_hashmap, ref_id, gen_id, alias_table_hashmap, 5);//5是为了走此方法中的exists优化模块
                    }
                }
            }
            else
            {
                if(from_item.table_id_ == ref_id)
                {
                    from_item.table_id_ = gen_id;
                    flag = true;
                }
            }

            //如果from item中不存在此表，则直接进行下一个循环;否则，继续往下更改相关信息
            if (flag == false)
            {
              continue; // 如果from item中不存在此表，则直接进行下一个循环
            }

            //2. midify table items
            uint64_t gen_idx = logical_plan->generate_alias_table_id();//产生用于产生别名的索引
            ObString new_alias_name;
            ob_write_string(*logical_plan->get_name_pool(),
                            ObString::link_string("alias", gen_idx),
                            new_alias_name);
            int32_t sub_table_size = sub_stmt->get_table_size();
            for(int32_t i=0;i < sub_table_size; ++i)
            {
                TableItem& sub_table_item = sub_stmt->get_table_item(i);
                if(sub_table_item.table_id_ == ref_id)
                {
                    sub_table_item.table_id_ = gen_id;
                    sub_table_item.alias_name_ = new_alias_name;
                    sub_table_item.type_ = TableItem::ALIAS_TABLE;
                }
            }

            //3.modify column item
            int32_t sub_column_size = sub_stmt->get_column_size();
            for(int32_t i = 0; i < sub_column_size; ++i)
            {
                ColumnItem* sub_column_item = const_cast<ColumnItem*>(sub_stmt->get_column_item(i));
                if(sub_column_item->table_id_ == ref_id)
                {
                    sub_column_item->table_id_ = gen_id;
                    sub_stmt->get_table_item_by_id(gen_id)->has_scan_columns_ = true;
                }
            }

            //4.exists select item don't need to pull up, so no need to handle

            //5. modify where conditions
            int32_t sub_condition_size = sub_stmt->get_condition_size();
            for(int32_t i = 0; i< sub_condition_size; ++i)
            {
                uint64_t expr_id = sub_stmt->get_condition_id(i);
                ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
                std::map<uint64_t, uint64_t> table_id_hashmap;//exists的优化用不上
                std::map<uint64_t, uint64_t> column_id_hashmap;//exists的优化用不上
                std::map<uint64_t, uint64_t> alias_table_hashmap;//exists的优化用不上
                sql_raw_expr->optimize_sql_expression(sub_stmt,
                    table_id_hashmap, column_id_hashmap, ref_id, gen_id, alias_table_hashmap, 5);
            }
        }

        UNUSED(main_stmt);

        return ret;
    }

    int ObExistsLogicalOptimizer::pull_up_exists_from_items(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id)
    {
        int ret = OB_SUCCESS;

        ObVector<FromItem>& from_items = main_stmt->get_from_items();

        int32_t sub_from_size = sub_stmt->get_from_item_size();
        for(int32_t i = 0; i < sub_from_size; ++i )
        {
            FromItem& sub_from_item = const_cast<FromItem&>(sub_stmt->get_from_item(i));
            bool sub_is_joined = sub_from_item.is_joined_;
            if(sub_is_joined)//joined_table
            {
                JoinedTable* sub_joined_table = sub_stmt->get_joined_table(sub_from_item.table_id_);
                ObArray<uint64_t>& sub_joined_table_ids = sub_joined_table->get_table_ids();
                ObRowDesc* table_hash = main_stmt->get_table_hash();
                for (int64_t j = 0; j < sub_joined_table_ids.count(); ++j)
                {
                  //If the table exist in main stmt, it don't need to add.
                  int64_t idx = table_hash->get_idx(sub_joined_table_ids.at(j), OB_INVALID_ID);
                  if (idx == OB_INVALID_INDEX) //table_id don't exists in table_hash
                  {
                      //add sub_table_id into main_stmt from items
                      FromItem new_from_item;
                      new_from_item.table_id_ = sub_joined_table_ids.at(j);
                      new_from_item.is_joined_ = false;
                      from_items.push_back(new_from_item);

                      //add main stmt table_hash
                      table_hash->add_column_desc(sub_joined_table_ids.at(j), OB_INVALID_ID);
                  }
                }
                //add sub_joined_table_expr into where_exprs
                ObVector<uint64_t>& main_where_exprs_id = main_stmt->get_where_exprs();
                ObArray<uint64_t>& sub_joined_expr_ids = sub_joined_table->get_expr_ids();
                for(int64_t k = 0; k < sub_joined_expr_ids.count(); ++k)
                {
                    uint64_t joined_expr_id = sub_joined_expr_ids.at(k);
                    main_where_exprs_id.push_back(joined_expr_id);
                }
            }
            else
            {
                ObRowDesc* table_hash = main_stmt->get_table_hash();


                int64_t idx = table_hash->get_idx(sub_from_item.table_id_, OB_INVALID_ID);
                if (idx == OB_INVALID_INDEX) //table_id don't exists in table_hash
                {
                    //add table_id into main_stmt from items
                    from_items.push_back(sub_from_item);

                    //add main stmt table_hash
                    table_hash->add_column_desc(sub_from_item.table_id_, OB_INVALID_ID);
                }

            }
        }
        UNUSED(logical_plan);
        UNUSED(sub_query_id);
        return ret;
    }

    int ObExistsLogicalOptimizer::pull_up_exists_table_items(ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id)
    {
        int ret = OB_SUCCESS;
        ObVector<TableItem>& table_items = main_stmt->get_table_items();
        ObVector<TableItem>& sub_table_items = sub_stmt->get_table_items();
        int32_t table_items_size = main_stmt->get_table_size();
        int32_t sub_table_items_size = sub_stmt->get_table_size();
        for(int32_t i = 0; i < sub_table_items_size; ++i)
        {
            bool flag = false;
            TableItem& sub_table_item = sub_table_items.at(i);
            for(int32_t j = 0; j < table_items_size; ++j)
            {
                TableItem& table_item = table_items.at(j);
                if(sub_table_item.table_id_ == table_item.table_id_ )
                {
                    flag = true;
                }
            }
            if(!flag)
            {
                table_items.push_back(sub_table_item);
            }
        }
        UNUSED(sub_query_id);
        return ret;
    }

    int ObExistsLogicalOptimizer::pull_up_exists_column_items(ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id)
    {
        int ret = OB_SUCCESS;
        ObVector<ColumnItem>& main_column_items = main_stmt->get_column_items();
        int32_t main_column_size = main_stmt->get_column_size();
        int32_t sub_column_size = sub_stmt->get_column_size();
        for(int32_t i = 0; i < sub_column_size; ++i )
        {
            bool flag = false;
            ColumnItem* sub_column_item = const_cast<ColumnItem*>(sub_stmt->get_column_item(i));
//            TBSYS_LOG(INFO, "pull_up_exists_column_items subColumnName:%.*s, subtableId:%lu", sub_column_item->column_name_.length(), sub_column_item->column_name_.ptr(), sub_column_item->table_id_ );
            for(int32_t j = 0; j < main_column_size; ++j)
            {
                ColumnItem* main_column_item = const_cast<ColumnItem*>(main_stmt->get_column_item(j));
                //sub_column_item already exists in main_column_items
                if((sub_column_item->column_name_ == main_column_item->column_name_)
                          && (sub_column_item->table_id_ == main_column_item->table_id_))
                {
                    flag = true;
                    break;
                }
            }
            if(!flag)
            {
                //TODO:some columns in select_item , but where_conditions do not use, do not need to pull up select_item


                //modify sub_column_item's query_id equals main_stmt's query_id
                //sub_column_item->query_id_ = main_stmt->get_query_id();//注释掉 20170910
                main_column_items.push_back(*sub_column_item);

                //if column id has scan, table item's has_scan_columns_ should set true.
                main_stmt->get_table_item_by_id(sub_column_item->table_id_)->has_scan_columns_ = true;
            }
        }
        UNUSED(sub_query_id);
        return ret;
    }

    //optimize exists, don't need sub_select_items_exprs
    int ObExistsLogicalOptimizer::delete_sub_select_exprs(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, int32_t index)
    {
        int ret = OB_SUCCESS;
        UNUSED(index);
        //no need sub_select_items's expr，directly delete it
        int32_t sub_select_items_size = sub_stmt->get_select_item_size();
        for(int32_t i = 0; i < sub_select_items_size; ++i)
        {
            SelectItem& sub_select_item = const_cast<SelectItem&>(sub_stmt->get_select_item(i));
            uint64_t sub_select_expr_id = sub_select_item.expr_id_;
            logical_plan->delete_expr_by_id(sub_select_expr_id);
        }
        UNUSED(main_stmt);
        return ret;
    }

    int ObExistsLogicalOptimizer::pull_up_exists_where_exprs(ObLogicalPlan*& logical_plan, ObSelectStmt*& main_stmt, ObSelectStmt* sub_stmt, uint64_t sub_query_id, uint64_t exists_expr_id, int32_t &index)
    {
        int ret = OB_SUCCESS;
        //where expr
        //delete T_OP_EXISTS expr
        ObVector<uint64_t>& main_where_exprs_id = main_stmt->get_where_exprs();
        for(int32_t i = 0; i < main_where_exprs_id.size(); ++i)
        {
            if(main_where_exprs_id.at(i) == exists_expr_id)
            {
                main_where_exprs_id.remove(i);//delete T_OP_EXISTS expr
            }
        }

        int32_t sub_condition_size = sub_stmt->get_condition_size();
        for(int32_t i = 0; i < sub_condition_size; ++i)
        {
            uint64_t sub_where_expr_id = sub_stmt->get_condition_id(i);
            //sub_stmt where_exprs need insert before main_stmt where_expr
            //main_where_exprs_id.push_back(sub_where_expr_id);
            //delete [bugfix exists] 20171107:b
            //main_where_exprs_id.insert(main_where_exprs_id.begin()+i, sub_where_expr_id);
            //delete 20171107:e

            ObSqlRawExpr* sub_sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(sub_where_expr_id));
            ObRawExpr* sub_raw_expr = sub_sql_raw_expr->get_expr();
            ObItemType sub_item_type = sub_raw_expr->get_expr_type();

            if(sub_item_type == T_OP_EQ)
            {
                ObBinaryOpRawExpr* binary_op_expr = dynamic_cast<ObBinaryOpRawExpr*>(sub_raw_expr);
                ObBinaryRefRawExpr* first_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_expr->get_first_op_expr());
                ObBinaryRefRawExpr* second_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_expr->get_second_op_expr());
                if((first_expr != NULL) && (second_expr != NULL))
                {
                    uint64_t first_table_id = first_expr->get_first_ref_id();
                    uint64_t second_table_id = second_expr->get_first_ref_id();

                    //change
                    ObVector<TableItem>& sub_table_items = sub_stmt->get_table_items();
                    bool flag = true;
                    //first_table_id
                    for(int32_t i = 0; i < sub_table_items.size(); ++i)
                    {
                        TableItem &sub_table_item = sub_table_items.at(i);
                        if(sub_table_item.table_id_ == first_table_id)
                        {
                            flag = false;
                            break;
                        }
                    }
                    if(flag)
                    {
                        sub_raw_expr->set_expr_type(T_OP_LEFT_SEMI);
                    }
                    else
                    {
                        flag = true;
                        //second_table_id
                        for(int32_t i = 0; i < sub_table_items.size(); ++i)
                        {
                            TableItem &sub_table_item = sub_table_items.at(i);
//                            TBSYS_LOG(INFO, "pull_up_exists_where_exprs slwang after for// flag = %d, second_table_id = %ld, sub_table_item.table_id_ = %ld", flag, second_table_id, sub_table_item.table_id_);
                            if(sub_table_item.table_id_ == second_table_id)
                            {
                                flag = false;
                                break;
                            }
                        }
                        if(flag)
                        {
                            sub_raw_expr->set_expr_type(T_OP_LEFT_SEMI);
                        }
                    }


                }
            }
        }
        //add slwang [bugfix exists] 20171107:b

        //需先遍历父查询中是否存在T_OP_LEFT_SEMI
        int32_t main_where_exprs_id_size = main_stmt->get_condition_size();
        uint64_t sub_insert_location = OB_INVALID_ID;//赋的初值没什么含义
        for(int32_t i = 0; i < main_where_exprs_id_size; ++i)
        {
            uint64_t opt_where_expr_id = main_stmt->get_condition_id(i);
            ObSqlRawExpr* main_sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(opt_where_expr_id));
            ObRawExpr* main_raw_expr = main_sql_raw_expr->get_expr();
            ObItemType main_item_type = main_raw_expr->get_expr_type();
            /*对于多层exists嵌套，需把子查询中的连接条件插入到其父查询连接条件的前面，
             * 但又不能插入到最外层的主查询的连接条件的前面，否则影响最外层查询的hint信息
            */
            if(main_item_type == T_OP_LEFT_SEMI)//必须插入到找到的第一个条件前面
            {
                sub_insert_location = i;//找到第一个满足条件插入位置就结束
                break;
            }
        }

        //子查询连接条件插入父查询开始
        if(sub_insert_location == OB_INVALID_ID)//父查询中还没有子查询的相关连接条件（T_OP_LEFT_SEMI）
        {
            for(int32_t i = 0; i < sub_condition_size; ++i)
            {
                uint64_t sub_where_expr_id = sub_stmt->get_condition_id(i);
                main_where_exprs_id.push_back(sub_where_expr_id);//直接将子查询中的连接条件插入到父查询的后面
            }

        }
        else//父查询中有T_OP_LEFT_SEMI
        {
            for(int32_t i = 0; i < sub_condition_size; ++i)
            {
                uint64_t sub_where_expr_id = sub_stmt->get_condition_id(i);
                //将内层的子查询的连接条件全部插入到上一层的T_OP_LEFT_SEMI的前面
                main_where_exprs_id.insert(main_where_exprs_id.begin() + sub_insert_location + i, sub_where_expr_id);
            }
        }

        //add 20171107:e

        //add slwang [bugfix exists] 20171105:b
        //子查询的where提升后，还需更新一遍提升后主查询的每个sql's bit_set
        ObVector<uint64_t>& optimize_where_exprs_ids = main_stmt->get_where_exprs();
        for(int32_t i = 0; i < optimize_where_exprs_ids.size(); ++i)
        {
            std::map<uint64_t, uint64_t> table_id_hashmap;//exists的优化用不上
            std::map<uint64_t, uint64_t> column_id_hashmap;//exists的优化用不上
            std::map<uint64_t, uint64_t> alias_table_hashmap;//exists的优化用不上
            uint64_t table_id = OB_INVALID_ID;//exists的优化用不上
            uint64_t real_table_id = OB_INVALID_ID;//exists的优化用不上
            uint64_t expr_id = main_stmt->get_condition_id(i);
            ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
            sql_raw_expr->optimize_sql_expression(main_stmt, table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, 3);
        }
        //add 20171105:e

        //delete exists expr
        logical_plan->delete_expr_by_id(exists_expr_id);

        //when T_OP_EXISTS expr delete, need to update index.
        --index;

        UNUSED(sub_query_id);

        return ret;
    }

  }
}
