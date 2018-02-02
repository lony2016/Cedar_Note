#include <map>
#include "sql/ob_optimizer_logical.h"
#include "sql/ob_select_stmt.h"
#include "sql/ob_explain_stmt.h"
#include "common/hash/ob_hashmap.h"
#include "common/ob_vector.h"
#include "parse_malloc.h"


namespace oceanbase 
{
  namespace sql 
  {
    
    /*
     * @brief decide whether sub query is a simple subquery or not  
     * @return 
     */
    bool ObOptimizerLogical::is_simple_subquery(ObSelectStmt *select_stmt)
    { 
      
      // make sure it's a valid subselect
      if(!(select_stmt->get_stmt_type() == ObSelectStmt::T_SELECT))
        return false;
      
      // can't pull up a query with for update
      if(select_stmt->is_for_update())
        return false;
      
      // can't pull up a query with setops
      if(select_stmt->get_set_op() != ObSelectStmt::NONE)
        return false;
      
      /*
       * can't pull up a subquery involving grouping aggregation, sorting,
       * limiting, for update, distinct 
       */
      if(select_stmt->has_limit() || 
         select_stmt->is_distinct() ||
//         select_stmt->is_set_distinct() ||
         select_stmt->is_show_stmt() ||
         select_stmt->has_limit() ||
         select_stmt->get_group_expr_size()>0 || 
         select_stmt->get_agg_fun_size()>0 ||
         select_stmt->get_having_expr_size()>0 ||
         select_stmt->get_order_item_size()>0 ||
         select_stmt->get_when_expr_size()>0 
         // select_stmt->get_has_range() || // 学校版本没有这个功能
         // select_stmt->has_sequence()
         )
        return false;
      
      /*
      // add by lxb [logical optimizer] 20170601 start
      if (select_stmt->get_anal_fun_size()>0 
          || select_stmt->get_partition_expr_size()>0 
          || select_stmt->get_order_item_for_rownum_size()>0) 
        return false;
      // add by lxb [logical optimizer] 20170601 end
      */
      
      // add by lxb [logical optimizer] 20170602 start
      if(select_stmt->get_query_hint().is_has_hint_ == true)
        return false;
      // add by lxb [logical optimizer] 20170602 end
      
      return true;
    }
    
    /*
     * @brief pull up subquery
     * @return 
     */
    int ObOptimizerLogical::pull_up_subqueries(
        ObLogicalPlan *&logical_plan, ObResultSet &result, ResultPlan &result_plan)
    {
      
      int ret = OB_SUCCESS;
      uint64_t query_id = common::OB_INVALID_ID;
      ObSelectStmt *main_stmt = NULL;
      
      // 场景：父查询和子查询含有相同的表，两边都需要取别名
      // 父查询取别名必须在子查询都提升之后才能取别名
      // 考虑：select * from (select sub1.* from t1, (select * from t1) sub1 where t1.c2=sub1.c2 ) sub2, 
      // t1 where t1.c1=sub2.c1;
      std::map<uint64_t, uint64_t> father_alias_table;
      
      if(logical_plan)
      {
        int32_t stmt_size = logical_plan->get_stmts_count();
        
        // if stmt size > 1, so has subquery
        if (stmt_size > 1) 
        {
          // 打印提升前的逻辑计划
          /*
          FILE* fp_start = NULL;
          fp_start = fopen("logic_start.txt", "wt");
          logical_plan->print(fp_start, 0);
          fclose(fp_start);
          */
          
          if(query_id == OB_INVALID_ID)
          {
            // explain stmt is different from select stmt
            ObBasicStmt *basic_stmt = logical_plan->get_main_stmt();
            if (basic_stmt->get_stmt_type() == ObBasicStmt::T_EXPLAIN)
            {
              ObExplainStmt *explain_stmt = dynamic_cast<ObExplainStmt*>(basic_stmt);
              main_stmt = dynamic_cast<ObSelectStmt*>(
                    logical_plan->get_query(explain_stmt->get_explain_query_id()));
            }
            else
            {
              main_stmt = dynamic_cast<ObSelectStmt*>(basic_stmt);
            }
          }
          
          if(main_stmt == NULL){
            ret = OB_ERR_ILLEGAL_ID;
            TBSYS_LOG(INFO, "Stmt is not select stmt, so it will not enter the logical optimizer");
          } 
          else if(OB_LIKELY(ret == OB_SUCCESS))
          {
            // add by lxb [logical optimizer] 20170602 start
            if(!main_stmt->is_for_update() 
               && main_stmt->get_stmt_type() == ObSelectStmt::T_SELECT
               && main_stmt->get_query_hint().is_has_hint_ == false) // add by lxb [logical optimizer] 20170602 end
            {
              TBSYS_LOG(DEBUG, "enter the logical optimizer");
              ret = pull_up_subquery_union(logical_plan, main_stmt, result_plan, father_alias_table);
              TBSYS_LOG(DEBUG, "leave the logical optimizer");
            }
            TBSYS_LOG(WARN, "leave the logical optimizer");
          }
          
          // 打印提升后的逻辑计划
          /*
          FILE* fp = NULL; 
          fp = fopen("logic_end.txt", "wt");
          logical_plan->print(fp, 0);
          fclose(fp);
          */
        }
      }
      
      UNUSED(result);
      
      return ret;
    }
    
    /*
     * pull_up_subquery_union
     */ 
    int ObOptimizerLogical::pull_up_subquery_union(ObLogicalPlan *&logical_plan, ObSelectStmt *&main_stmt, 
        ResultPlan &result_plan, std::map<uint64_t, uint64_t> &father_alias_table)
    {
      int ret = OB_SUCCESS;
      
      if (main_stmt->get_set_op() != ObSelectStmt::NONE) 
      {
        uint64_t left_query_id = main_stmt->get_left_query_id();
        uint64_t right_query_id = main_stmt->get_right_query_id();
        
        ObSelectStmt* left_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(left_query_id));
        ObSelectStmt* right_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(right_query_id));
        
        // 把父查询拆成隐式连接的形式，这样更有利于后续的编码，同样一个问题就是父查询不能要外连接
        // (t1, t2) left t3，如果t3跟t1和t2中的字段做join，则提升后t1, t2 left t3会报错
        // 首先判断父查询中有没有外连接，没有外连接才能把父查询拆成隐式连接的形式，如果有，则不拆
        ret = split_mainquery_from_items(logical_plan, left_stmt, result_plan);
        int32_t left_from_item_size = left_stmt->get_from_item_size();
        for (int32_t i = 0; i < left_from_item_size; ++i) 
        {
          FromItem& from_item = const_cast<FromItem&>(left_stmt->get_from_item(i));
          pull_up_subquery_recursion(logical_plan, left_stmt, from_item, result_plan, father_alias_table, i);
        }
        
        ret = split_mainquery_from_items(logical_plan, right_stmt, result_plan);
        int32_t right_from_item_size = right_stmt->get_from_item_size();
        for (int32_t i = 0; i < right_from_item_size; ++i) 
        {
          FromItem& from_item = const_cast<FromItem&>(right_stmt->get_from_item(i));
          pull_up_subquery_recursion(logical_plan, right_stmt, from_item, result_plan, father_alias_table, i);
        }
        
        // Old select items is the left stmt's select items.
        common::ObVector<SelectItem>& main_select_items = main_stmt->get_select_items();
        common::ObVector<SelectItem>& left_select_items = left_stmt->get_select_items();
        for (int32_t i = 0; i < main_select_items.size(); ++i) 
        {
          main_select_items.at(i) = left_select_items.at(i);
        }
      } 
      else 
      {
        ret = split_mainquery_from_items(logical_plan, main_stmt, result_plan);
        int32_t from_item_size = main_stmt->get_from_item_size();
        for (int32_t i = 0; i < from_item_size; ++i) 
        {
          FromItem& from_item = const_cast<FromItem&>(main_stmt->get_from_item(i));
          pull_up_subquery_recursion(logical_plan, main_stmt, from_item, result_plan, father_alias_table, i);
        }
      }
      
      ret = set_main_query_alias_table(logical_plan, main_stmt, result_plan, father_alias_table);
      
      return ret;
    }
    
    int ObOptimizerLogical::split_mainquery_from_items(ObLogicalPlan *&logical_plan, 
                         ObSelectStmt *&main_stmt, ResultPlan &result_plan)
    {
      int ret = OB_SUCCESS;
            
      // 判断有无外连接（如果有外连接的话，则不可以转换成隐式连接，因为table连接的顺序需要保持一致）
      bool flag = false;
      common::ObVector<FromItem>& from_items = main_stmt->get_from_items();
      common::ObVector<uint64_t>& where_exprs = main_stmt->get_where_exprs();
      // int32_t from_item_size = main_stmt->get_from_item_size();
      for (int32_t i = 0; i < main_stmt->get_from_item_size(); ++i) 
      {
        flag = false;
        FromItem& from_item = const_cast<FromItem&>(main_stmt->get_from_item(i));
        if (from_item.is_joined_) 
        {
          uint64_t joined_table_id = from_item.table_id_;
          JoinedTable* joined_table = main_stmt->get_joined_table(joined_table_id);
          int64_t joined_type_size = joined_table->get_join_types().count();
          for (int64_t j = 0; j < joined_type_size; ++j) 
          {
            if (joined_table->get_join_types().at(j) != JoinedTable::T_INNER) 
            {
              flag = true; // 如果存在外连接，则标记为true
              break;
            }
          }
          
          // 如果存在外连接，则往下遍历
          if (flag == true) 
          {
            continue;
          }
          else // 如果不存在外连接，则需要转换成为隐式连接，且要保证父查询中表连接顺序不变
          {
            common::ObArray<uint64_t>& joined_table_ids = joined_table->get_table_ids();
            common::ObArray<uint64_t>& joined_expr_ids = joined_table->get_expr_ids();
            // common::ObArray<int64_t>& joined_expr_nums = joined_table->get_expr_nums_per_join();
            for (int64_t j = joined_table_ids.count()-1; j >= 0 ; --j) 
            {
              // 由于父查询中可能存在外连接，则需要考虑顺序问题，如t1 inner (t2, t3), t4 left t5;
              FromItem new_from_item;
              new_from_item.table_id_ = joined_table_ids.at(j);
              new_from_item.is_joined_ = false;
              // new_from_item.from_item_rel_opt_ = NULL;
              // from_items.push_back(new_from_item);
              from_items.insert(from_items.begin()+i, new_from_item);
              
              if (j > 0)
              {
                // 学校版本不存在join on多个条件的情况
                /*
                int64_t joined_expr_ids_index = joined_table->get_expr_ids_index(j-1);
                for (int64_t k = 0; k < joined_expr_nums.at(j-1); ++k) 
                {
                  uint64_t expr_id = joined_expr_ids.at(joined_expr_ids_index + k);
                  where_exprs.push_back(expr_id);
                }
                */
                
                uint64_t expr_id = joined_expr_ids.at(j-1);
                where_exprs.push_back(expr_id);
              }
            }
            
            // 最后移除from item和join table
            int64_t count = joined_table_ids.count();
            main_stmt->remove_joined_table(from_item.table_id_);
            main_stmt->remove_from_item_by_idx(i+static_cast<int32_t>(count));
          }
          
        }
      }
      
      UNUSED(logical_plan);
      UNUSED(result_plan);
      return ret;
    }
    
    /*
     * set_main_query_alias_table
     */
    int ObOptimizerLogical::set_main_query_alias_table(ObLogicalPlan *&logical_plan, 
                 ObSelectStmt *&main_stmt, ResultPlan &result_plan,
                 std::map<uint64_t, uint64_t> &father_alias_table)
    {
      int ret = OB_SUCCESS;
      
      std::map<uint64_t, uint64_t> column_id_hashmap; // 没有实际的作用
      std::map<uint64_t, uint64_t> table_id_hashmap; // 没有实际的作用
      std::map<uint64_t, uint64_t>::iterator father_alias_it;
      
      // 开始别名的调整
      for (father_alias_it = father_alias_table.begin(); father_alias_it != father_alias_table.end(); ++father_alias_it) 
      {
        // 生成别名表id
        uint64_t ref_id = father_alias_it->first;
        uint64_t gen_aid = logical_plan->generate_table_id();
        father_alias_table[ref_id] = gen_aid;
        bool flag = false; // 用来判断查询中有没有与子查询相同的表
        
        // 调整table hash
        common::ObRowDesc* table_hash = main_stmt->get_table_hash();
        // remove_column_desc导致bit index减一，再执行add_column_desc，则bit index加1，减一和加一导致还是原来的值，就会出错
        // table_hash->remove_column_desc(ref_id, OB_INVALID_ID); 
        table_hash->add_column_desc(gen_aid, OB_INVALID_ID);
        // TBSYS_LOG(DEBUG, "father ref_id:[%ld], table_id:[%ld] -> bit index:[%d]", 
        //     ref_id, gen_aid, main_stmt->get_table_bit_index(gen_aid));
        
        // from item
        int32_t from_item_size = main_stmt->get_from_item_size();
        for (int32_t i = 0; i < from_item_size; ++i) 
        {
          FromItem& from_item = const_cast<FromItem&>(main_stmt->get_from_item(i));
          if(from_item.is_joined_)
          {
            uint64_t joined_table_id = from_item.table_id_;
            JoinedTable* joined_table = main_stmt->get_joined_table(joined_table_id);
            common::ObArray<uint64_t>& joined_table_ids = joined_table->get_table_ids();
            common::ObArray<uint64_t>& joined_expr_ids = joined_table->get_expr_ids();
            
            // 修改table id
            for (int64_t j = 0; j < joined_table_ids.count(); ++j) 
            {
              if(joined_table_ids.at(j) == ref_id)
              {
                joined_table_ids.at(j) = gen_aid;
                flag = true;
              }
            }
            // 修改expr id
            if (flag == true) 
            {
              for (int64_t j = 0; j < joined_expr_ids.count(); ++j) 
              {
                ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(joined_expr_ids.at(j)));
                sql_raw_expr->optimize_sql_expression(main_stmt, 
                    table_id_hashmap, column_id_hashmap, ref_id, gen_aid, father_alias_table, 3);
              }
            }
          }
          else
          {
            if(from_item.table_id_ == ref_id)
            {
              from_item.table_id_ = gen_aid;
              flag = true;
            }
          }
        }
        
        if (flag == false) 
        {
          continue; // 如果from item中不存在此表，则直接进行下一个循环
        }
        
        // table item
        uint64_t gen_idx = logical_plan->generate_alias_table_id();
        ObString new_alias_name;
        ob_write_string(*logical_plan->get_name_pool(), 
                        ObString::link_string("alias", gen_idx), 
                        new_alias_name);
        int32_t table_size = main_stmt->get_table_size();
        for (int32_t i = 0; i < table_size; ++i) 
        {
          TableItem& table_item = main_stmt->get_table_item(i);
          if (table_item.table_id_ == ref_id) 
          {
            table_item.table_id_ = gen_aid;
            table_item.alias_name_ = new_alias_name;
            table_item.type_ = TableItem::ALIAS_TABLE;
          }
        }
        
        // coloumn item
        int32_t column_size = main_stmt->get_column_size();
        for (int32_t i = 0; i < column_size; ++i)
        {
          ColumnItem* column_item = const_cast<ColumnItem*>(main_stmt->get_column_item(i));
          if (column_item->table_id_ == ref_id) 
          {
            column_item->table_id_ = gen_aid;
            main_stmt->get_table_item_by_id(gen_aid)->has_scan_columns_ = true;
          }
        }
        
        // select item
        int32_t select_size = main_stmt->get_select_item_size();
        for (int32_t i = 0; i < select_size; ++i)
        {
          SelectItem& select_item = const_cast<SelectItem&>(main_stmt->get_select_item(i));
          uint64_t expr_id = select_item.expr_id_;
          ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
          sql_raw_expr->optimize_sql_expression(main_stmt, 
              table_id_hashmap, column_id_hashmap, ref_id, gen_aid, father_alias_table, 3);
        }
        
        // where condition
        int32_t condition_size = main_stmt->get_condition_size();
        for (int32_t i = 0; i < condition_size; ++i) 
        {
          uint64_t expr_id = main_stmt->get_condition_id(i);
          ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
          sql_raw_expr->optimize_sql_expression(main_stmt, 
              table_id_hashmap, column_id_hashmap, ref_id, gen_aid, father_alias_table, 3);
        }
        
      }
      
      UNUSED(result_plan);
      return ret;
    }
    
    
    /*
     * pull_up_subquery_recursion
     */ 
    int ObOptimizerLogical::pull_up_subquery_recursion(ObLogicalPlan *&logical_plan, 
        ObSelectStmt *&main_stmt, FromItem& from_item, ResultPlan &result_plan,
        std::map<uint64_t, uint64_t> &father_alias_table, int32_t from_item_idx)
    {
      int ret = OB_SUCCESS;
      
      uint64_t table_id = from_item.table_id_;
      bool is_joined = from_item.is_joined_;
      JoinedTable* joined_table = NULL;
      uint64_t join_type = JoinedTable::T_INNER;
      
      if(is_joined) // the connection type of tables is explicit join ( inner join ... ), not , connect . 
      {
        joined_table = main_stmt->get_joined_table(table_id);
        // 直接在for里面获取table个数是因为后续会不断往里面加表
        // 而且表插入的位置是在子查询位置后面，而不会直接插入到join table尾部
        
        // 只要父查询中存在外连接，则子查询只能是单表
        for (int64_t i = 0; i < joined_table->table_ids_.count(); ++i) 
        {
          if (i > 0 && joined_table->get_join_types().at(i-1) != JoinedTable::T_INNER) 
          {
            join_type = joined_table->get_join_types().at(i-1);
            break;
          }
          else if (i == 0 && joined_table->get_join_types().at(0) != JoinedTable::T_INNER)
          {
            join_type = joined_table->get_join_types().at(0);
            break;
          }
        }
        
        for (int64_t i = 0; i < joined_table->table_ids_.count(); ++i) 
        {
          uint64_t joined_table_id = joined_table->table_ids_.at(i);
          TableItem* table_item = main_stmt->get_table_item_by_id(joined_table_id);
          
          // this table type represent subquery
          if (table_item->type_ == TableItem::GENERATED_TABLE) 
          {
            // this type of sql can not be pulled up : t1 left join (t2 inner join t3)
            // join table第一个位置如果是子查询，join type是外连接，也是不能被提升的，原因如下：
            // (t1, t2) left t3，如果t3跟t1和t2中的字段做join，则提升后t1, t2 left t3会报错
            // left支持的情况：全部都是显式连接，且表连接顺序不变，外连接后边的表必须是单表子查询，左边也必须为单表
            // 左边不能为多表的原因是：显式内连接join表太复杂了，没有规律性
            
            uint64_t ref_id = table_item->ref_id_;
            ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
            if(subquery_stmt == NULL)
            {
              ret = OB_ERR_ILLEGAL_ID;
              TBSYS_LOG(ERROR, "Get Stmt error");
              
            } 
            else if(OB_LIKELY(ret == OB_SUCCESS))
            {
              // pull up subquery execute
              ret = pull_up_simple_subquery(logical_plan, main_stmt, from_item, 
                  table_item, joined_table, join_type, result_plan, father_alias_table, from_item_idx);
            } 
            else 
            {
              // if it is not sub query, then continue execute.
            }
          }
        }
      }
      else
      {
        TableItem* table_item = main_stmt->get_table_item_by_id(table_id);
        // this table type represent subquery
        if (table_item->type_ == TableItem::GENERATED_TABLE) 
        {
          uint64_t ref_id = table_item->ref_id_;
          ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
          if(subquery_stmt == NULL)
          {
            ret = OB_ERR_ILLEGAL_ID;
            TBSYS_LOG(ERROR, "Get Stmt error");
            
          } 
          else if(OB_LIKELY(ret == OB_SUCCESS))
          {
            // pull up subquery execute
            ret = pull_up_simple_subquery(logical_plan, main_stmt, from_item, table_item, 
                joined_table, join_type, result_plan, father_alias_table, from_item_idx);
          } 
          else 
          {
            // if it is not sub query, then continue execute.
          }
        }
      }
      
      return ret;
    }
    
    /*
     * if_can_pull_up
     */
    int ObOptimizerLogical::if_can_pull_up(
        ObLogicalPlan *&logical_plan, ObSelectStmt* subquery_stmt, 
        JoinedTable *&joined_table, uint64_t join_type, bool is_joined, 
        uint64_t table_id, ObSelectStmt *&main_stmt)
    {
      int ret = OB_SUCCESS;
      
      int32_t sub_from_size = subquery_stmt->get_from_item_size();
      for (int32_t i = 0; i < sub_from_size; ++i) 
      {
        FromItem& sub_from_item = const_cast<FromItem&>(subquery_stmt->get_from_item(i));
        bool sub_is_joined = sub_from_item.is_joined_;
        
        if (is_joined) 
        {
          if (sub_is_joined) 
          {
            JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
            
            // To judge whether the sql can pull up, 10 define before.
            if (join_type != JoinedTable::T_INNER) // father join is out join, include left, right
            {
              ret = OB_SQL_CAN_NOT_PULL_UP; // 父查询是外连接，且子查询有多表是不能提升，t3 left (t1 left/inner/right t2)是不支持的
            }
            else 
            {
              // (t1 left t2) inner t3也不支持
              for (int64_t j = 0; j < sub_joined_table->get_join_types().count(); ++j)
              {
                if (sub_joined_table->get_join_types().at(j) != JoinedTable::T_INNER) 
                {
                  ret = OB_SQL_CAN_NOT_PULL_UP;
                  break;
                }
              }
            }
          }
          else 
          {
            if (join_type != JoinedTable::T_INNER) // 支持 t1 left (t2)
            {
              if (sub_from_size>1) 
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
              }
            }
          }
        }
        else 
        {
          // 如果父查询是隐式的内连接，则不管子查询的from item是join table（包括内连接）还是单表，都可以整个join table或单表提上去
          // 原因是OB构造的是一颗左升树
          // 如果子查询是内连接，则查询结果会有误，故需要过滤这种情况
          if (sub_is_joined) 
          {
            JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
            for (int64_t j = 0; j < sub_joined_table->get_join_types().count(); ++j)
            {
              if (sub_joined_table->get_join_types().at(j) != JoinedTable::T_INNER) 
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                break;
              }
            }
          }
        }
        // 如果子查询不能提升，则直接跳出循环
        if (ret != OB_SUCCESS) 
        {
          TBSYS_LOG(DEBUG, "SQL can not be pulled up because of out join");
          break;
        }
      }
      
      // 结果集为一些复杂的，带运算，带函数的，不能提升
      if (ret == OB_SUCCESS) 
      {
        // 判断子查询
        int32_t sub_select_item_size = subquery_stmt->get_select_item_size();
        for (int32_t i = 0; i < sub_select_item_size; ++i) 
        {
          SelectItem& sub_select_item = const_cast<SelectItem&>(subquery_stmt->get_select_item(i));
          ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_select_item.expr_id_);
          ObRawExpr* raw_expr = sql_raw_expr->get_expr();
          ObItemType item_type = raw_expr->get_expr_type();
          if (item_type != T_REF_COLUMN) 
          {
            ret = OB_SQL_CAN_NOT_PULL_UP;
            TBSYS_LOG(DEBUG, "SQL can not be pulled up because of select item");
            break;
          }
        }
        
        // 判断父查询
        if (ret == OB_SUCCESS) 
        {
          int32_t select_item_size = main_stmt->get_select_item_size();
          for (int32_t i = 0; i < select_item_size; ++i) 
          {
            SelectItem& select_item = const_cast<SelectItem&>(main_stmt->get_select_item(i));
            ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(select_item.expr_id_);
            ObRawExpr* raw_expr = sql_raw_expr->get_expr();
            ObItemType item_type = raw_expr->get_expr_type();
            if (item_type != T_REF_COLUMN) 
            {
              ret = OB_SQL_CAN_NOT_PULL_UP;
              TBSYS_LOG(DEBUG, "SQL can not be pulled up because of select item");
              break;
            }
          }
        }
      }
      
      if (ret == OB_SUCCESS) 
      {
        int32_t condition_size = subquery_stmt->get_condition_size();
        for (int32_t i = 0; i < condition_size; ++i) 
        {
          uint64_t condition_id = subquery_stmt->get_condition_id(i);
          ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(condition_id);
          ObRawExpr* raw_expr = sql_raw_expr->get_expr();
          ObItemType item_type = raw_expr->get_expr_type();
          if ((item_type >= T_OP_NEG && item_type <= T_OP_NOT_IN )
              || item_type == T_OP_CNN )
          {
            // 过滤左右两边为子查询的情况
            if ((item_type>=T_OP_EQ && item_type<=T_OP_NE)
                || item_type==T_OP_IN 
                || item_type==T_OP_NOT_IN) 
            {
              ObBinaryOpRawExpr* binary_op_raw_expr = dynamic_cast<ObBinaryOpRawExpr*>(raw_expr);
              if (binary_op_raw_expr == NULL) 
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                TBSYS_LOG(DEBUG, "SQL can not be pulled up because of condition"); 
                break;
              }
              
              ObBinaryRefRawExpr* left_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_raw_expr->get_first_op_expr());
              ObBinaryRefRawExpr* right_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_raw_expr->get_second_op_expr());

              if (left_expr == NULL || right_expr == NULL)
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                TBSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
                break;
              }
              else if (left_expr->get_expr_type() != T_REF_COLUMN
                  || right_expr->get_expr_type() == T_REF_QUERY) 
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                TBSYS_LOG(DEBUG, "SQL can not be pulled up because of condition"); 
                break;
              }
            }
          }
          else 
          {
            ret = OB_SQL_CAN_NOT_PULL_UP;
            TBSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
            break;
          }
        }
      }
      
      if (ret == OB_SUCCESS) 
      {
        int32_t condition_size = main_stmt->get_condition_size();
        for (int32_t i = 0; i < condition_size; ++i) 
        {
          uint64_t condition_id = main_stmt->get_condition_id(i);
          ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(condition_id);
          ObRawExpr* raw_expr = sql_raw_expr->get_expr();
          ObItemType item_type = raw_expr->get_expr_type();
          if ((item_type >= T_OP_NEG && item_type <= T_OP_NOT_IN )
              || item_type == T_OP_CNN )
          {
            // 过滤左右两边为子查询的情况
            if ((item_type>=T_OP_EQ && item_type<=T_OP_NE)
                || item_type==T_OP_IN 
                || item_type==T_OP_NOT_IN) 
            {
              ObBinaryOpRawExpr* binary_op_raw_expr = dynamic_cast<ObBinaryOpRawExpr*>(raw_expr);
              
              if (binary_op_raw_expr == NULL) 
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                TBSYS_LOG(DEBUG, "SQL can not be pulled up because of condition"); 
                break;
              }
              
              ObBinaryRefRawExpr* left_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_raw_expr->get_first_op_expr());
              ObBinaryRefRawExpr* right_expr = dynamic_cast<ObBinaryRefRawExpr*>(binary_op_raw_expr->get_second_op_expr());
              if (left_expr == NULL || right_expr == NULL)
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                TBSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
                break;
              } 
              else if (left_expr->get_expr_type() != T_REF_COLUMN 
                  || right_expr->get_expr_type() == T_REF_QUERY) 
              {
                ret = OB_SQL_CAN_NOT_PULL_UP;
                TBSYS_LOG(DEBUG, "SQL can not be pulled up because of condition"); 
                break;
              }
            }
          } 
          else 
          {
            ret = OB_SQL_CAN_NOT_PULL_UP;
            TBSYS_LOG(DEBUG, "SQL can not be pulled up because of condition");
            break;
          }
        }
      }
      
      UNUSED(joined_table);
      UNUSED(table_id);
      
      return ret;
    }
    
    /*
     * pull_up_from_items
     */ 
    int ObOptimizerLogical::pull_up_from_items(ObLogicalPlan *&logical_plan, 
        ObSelectStmt *&main_stmt, ObSelectStmt* subquery_stmt, FromItem &from_item,
        std::map<uint64_t, uint64_t> &alias_table_hashmap, std::map<uint64_t, uint64_t> &table_id_hashmap, 
        std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id, 
        std::map<uint64_t, uint64_t> &father_alias_table, int32_t from_item_idx)
    {
      int ret = OB_SUCCESS;
      
      bool is_joined = from_item.is_joined_;
      common::ObVector<FromItem>& from_items = main_stmt->get_from_items();
      
      // start pull up from item
      int32_t sub_from_size = subquery_stmt->get_from_item_size();
      // for (int32_t i = 0; i < sub_from_size; ++i) 
      for (int32_t i = sub_from_size-1; i >= 0 ; --i) // 目的就是先放最后一个item，保持原来的顺序 
      {
        FromItem& sub_from_item = const_cast<FromItem&>(subquery_stmt->get_from_item(i));
        bool sub_is_joined = sub_from_item.is_joined_;
        
        if (is_joined) // 父查询是显式连接
        {
          if (sub_is_joined) // 子查询是显式连接（优化后的架构不走这个分支）
          {
            JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
            common::ObArray<uint64_t>& sub_joined_table_ids = sub_joined_table->get_table_ids();
            common::ObRowDesc* table_hash = main_stmt->get_table_hash();
            
            // Add column desc first, this step has not pull up from item.
            for (int64_t j = 0; j < sub_joined_table_ids.count(); ++j)
            {
              // If the table exist in main stmt, it should change to alias table.
              int64_t idx = table_hash->get_idx(sub_joined_table_ids.at(j), OB_INVALID_ID);
              if (idx > 0) 
              {
                uint64_t alias_table_id = logical_plan->generate_table_id();
                alias_table_hashmap[sub_joined_table_ids.at(j)] = alias_table_id;
                
                // 父查询中需要去别名的表
                father_alias_table.insert(std::pair<uint64_t, uint64_t>(sub_joined_table_ids.at(j), sub_joined_table_ids.at(j)));
              } 
              else 
              {
                alias_table_hashmap[sub_joined_table_ids.at(j)] = sub_joined_table_ids.at(j);
              }
              table_hash->add_column_desc(alias_table_hashmap[sub_joined_table_ids.at(j)], OB_INVALID_ID);
            }
          }
          else // 子查询是隐式连接
          {
            common::ObRowDesc* table_hash = main_stmt->get_table_hash();
            
            // If the table exist in main stmt, it should change to alias table.
            int64_t idx = table_hash->get_idx(sub_from_item.table_id_, OB_INVALID_ID);
            if (idx > 0) 
            {
              uint64_t alias_table_id = logical_plan->generate_table_id();
              alias_table_hashmap[sub_from_item.table_id_] = alias_table_id;
              
              // 父查询中需要去别名的表
              father_alias_table.insert(std::pair<uint64_t, uint64_t>(sub_from_item.table_id_, sub_from_item.table_id_));
            } 
            else 
            {
              alias_table_hashmap[sub_from_item.table_id_] = sub_from_item.table_id_;
            }
            table_hash->add_column_desc(alias_table_hashmap[sub_from_item.table_id_], OB_INVALID_ID);
          }
        } 
        else // 父查询是隐式连接
        {
          if (sub_is_joined) // 子查询是显式连接
          {
            JoinedTable* sub_joined_table = subquery_stmt->get_joined_table(sub_from_item.table_id_);
            common::ObArray<uint64_t>& sub_joined_table_ids = sub_joined_table->get_table_ids();
            common::ObArray<uint64_t>& sub_joined_expr_ids = sub_joined_table->get_expr_ids();
            common::ObRowDesc* table_hash = main_stmt->get_table_hash();
            
            for (int64_t j = 0; j < sub_joined_table_ids.count(); ++j)
            {
              // If the table exist in main stmt, it should change to alias table.
              int64_t idx = table_hash->get_idx(sub_joined_table_ids.at(j), OB_INVALID_ID);
              if (idx > 0) 
              {
                uint64_t alias_table_id = logical_plan->generate_table_id();
                alias_table_hashmap[sub_joined_table_ids.at(j)] = alias_table_id;
                
                // 父查询中需要去别名的表
                father_alias_table.insert(std::pair<uint64_t, uint64_t>(sub_joined_table_ids.at(j), sub_joined_table_ids.at(j)));
              } 
              else 
              {
                alias_table_hashmap[sub_joined_table_ids.at(j)] = sub_joined_table_ids.at(j);
              }
              table_hash->add_column_desc(alias_table_hashmap[sub_joined_table_ids.at(j)], OB_INVALID_ID);
              sub_joined_table_ids.at(j) = alias_table_hashmap[sub_joined_table_ids.at(j)];
            }
            
            for (int64_t k = 0; k < sub_joined_expr_ids.count(); ++k) 
            {
              uint64_t expr_id = sub_joined_expr_ids.at(k);
              ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
              
              uint64_t real_table_id = 0;
              sql_raw_expr->optimize_sql_expression(main_stmt, 
                  table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, 2); // 对join条件中表替换成取别名的表
            }
            
            // 
            uint64_t joined_tid = main_stmt->generate_joined_tid();
            if(i == sub_from_size-1) // 当join table处于sub from item的最后一项时，直接用join id替换父查询的from item
            {
              from_item.table_id_ = joined_tid;
              from_item.is_joined_ = sub_from_item.is_joined_;
            }
            else // 当join table不处于sub from item的最后项时，新建from item放入父查询的from items
            {
              FromItem new_from_item;
              new_from_item.table_id_ = joined_tid;
              new_from_item.is_joined_ = sub_from_item.is_joined_;
              // new_from_item.from_item_rel_opt_ = NULL;
              from_items.insert(from_items.begin()+from_item_idx, new_from_item);
            }
            sub_joined_table->set_joined_tid(joined_tid);
            main_stmt->add_joined_table(sub_joined_table);
          }
          else 
          {
            common::ObRowDesc* table_hash = main_stmt->get_table_hash();
            
            // If the table exist in main stmt, it should change to alias table.
            int64_t idx = table_hash->get_idx(sub_from_item.table_id_, OB_INVALID_ID);
            if (idx > 0) 
            {
              uint64_t alias_table_id = logical_plan->generate_table_id();
              alias_table_hashmap[sub_from_item.table_id_] = alias_table_id;
              
              // 父查询中需要去别名的表
              father_alias_table.insert(std::pair<uint64_t, uint64_t>(sub_from_item.table_id_, sub_from_item.table_id_));
            } 
            else 
            {
              alias_table_hashmap[sub_from_item.table_id_] = sub_from_item.table_id_;
            }
            table_hash->add_column_desc(alias_table_hashmap[sub_from_item.table_id_], OB_INVALID_ID);

            if(i == sub_from_size-1)
            {
              from_item.table_id_ = alias_table_hashmap[sub_from_item.table_id_];
              from_item.is_joined_ = sub_from_item.is_joined_;
            }
            else 
            {
              FromItem new_from_item;
              new_from_item.table_id_ = alias_table_hashmap[sub_from_item.table_id_];
              new_from_item.is_joined_ = sub_from_item.is_joined_;
              // new_from_item.from_item_rel_opt_ = NULL;
              from_items.insert(from_items.begin()+from_item_idx, new_from_item);
            }
          }
        }
      }
      return ret;
    }
    
    /*
     * pull_up_table_items
     */ 
    int ObOptimizerLogical::pull_up_table_items(ObLogicalPlan *&logical_plan, 
        ObSelectStmt *&main_stmt, ObSelectStmt* subquery_stmt, 
        std::map<uint64_t, uint64_t> &alias_table_hashmap, uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      
      common::ObVector<TableItem>& table_items = main_stmt->get_table_items();
      int32_t table_item_size = main_stmt->get_table_size();
      for (int32_t i = 0; i < table_item_size; ++i) 
      {
        if (main_stmt->get_table_item(i).table_id_ == table_id) 
        {
          table_items.remove(i);
          break;
        }
      }
      
      int32_t sub_table_item_size = subquery_stmt->get_table_size();
      for (int32_t i = 0; i < sub_table_item_size; ++i) 
      {
        TableItem& sub_table_item = subquery_stmt->get_table_item(i);
        uint64_t sub_table_id = sub_table_item.table_id_;
        
        if (alias_table_hashmap[sub_table_id] == sub_table_id) 
        {
          table_items.push_back(sub_table_item);
        } 
        else // base table transform to alias table 
        {
          ObString new_alias_name;
          ob_write_string(*logical_plan->get_name_pool(), 
                          ObString::link_string("alias", logical_plan->generate_alias_table_id()), 
                          new_alias_name);
          
          sub_table_item.table_id_ = alias_table_hashmap[sub_table_id];
          sub_table_item.alias_name_ = new_alias_name;
          sub_table_item.type_ = TableItem::ALIAS_TABLE;
          table_items.push_back(sub_table_item);
        }
      }
      
      return ret;
    }
    
    /*
     * pull_up_column_items
     */ 
    int ObOptimizerLogical::pull_up_column_items(ObLogicalPlan *&logical_plan, 
                ObSelectStmt *&main_stmt, ObSelectStmt* subquery_stmt, 
                std::map<uint64_t, uint64_t> &alias_table_hashmap, 
                std::map<uint64_t, uint64_t> &table_id_hashmap, 
                std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      
      int32_t sub_column_size = subquery_stmt->get_column_size();
      common::ObVector<ColumnItem>& main_column_items = main_stmt->get_column_items();
      int32_t column_size = main_stmt->get_column_size();
      for (int32_t i = column_size-1; i >= 0; --i) // as the function remove below, i should decline, and it can avoid NULL exception .
      {
        ColumnItem* column_item = const_cast<ColumnItem*>(main_stmt->get_column_item(i));
        common::ObString column_name = column_item->column_name_;
        if(column_item->table_id_ == table_id)
        {
          int32_t sub_select_size = subquery_stmt->get_select_item_size();
          for (int32_t j = 0; j < sub_select_size; ++j) 
          {
            SelectItem& sub_select_item = const_cast<SelectItem&>(subquery_stmt->get_select_item(j));
            uint64_t sub_expr_id = sub_select_item.expr_id_;
            common::ObString sub_alias_name = sub_select_item.alias_name_;
            
            ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_expr_id);
            ObRawExpr* raw_expr = sql_raw_expr->get_expr();
            ObItemType item_type = raw_expr->get_expr_type();
            if(item_type == T_REF_COLUMN)
            {
              ObBinaryRefRawExpr* binary_ref_raw_expr = dynamic_cast<ObBinaryRefRawExpr*>(raw_expr);
              uint64_t first_ref_id = binary_ref_raw_expr->get_first_ref_id();
              uint64_t second_ref_id = binary_ref_raw_expr->get_second_ref_id();
              if(column_name.compare(sub_alias_name) == 0) // the column names in sub query can not same. 
              {
                column_id_hashmap[column_item->column_id_] = second_ref_id; // join condition column id
                table_id_hashmap[column_item->column_id_] = alias_table_hashmap[first_ref_id]; // join condition table id
                break;
              }
            }
          }
          
          main_column_items.remove(i);
        }
      }
      
      for (int32_t i = 0; i < sub_column_size; ++i)
      {
        ColumnItem* sub_column_item = const_cast<ColumnItem*>(subquery_stmt->get_column_item(i));
        sub_column_item->query_id_ = main_stmt->get_query_id();
        sub_column_item->table_id_ = alias_table_hashmap[sub_column_item->table_id_];
        
        main_column_items.push_back(*sub_column_item);
        // if column id has scan, table item's has_scan_columns_ should set true.
        main_stmt->get_table_item_by_id(sub_column_item->table_id_)->has_scan_columns_ = true;
      }
      
      return ret;
    }
    
    /*
     * pull_up_where_items
     */ 
    int ObOptimizerLogical::pull_up_where_items(ObLogicalPlan *&logical_plan, 
                ObSelectStmt *&main_stmt, ObSelectStmt* subquery_stmt, 
                std::map<uint64_t, uint64_t> &alias_table_hashmap,
                std::map<uint64_t, uint64_t> &table_id_hashmap, 
                std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      
      int32_t sub_condition_size = subquery_stmt->get_condition_size();
      common::ObVector<uint64_t>& main_where_exprs = main_stmt->get_where_exprs();
      for (int32_t j = 0; j < sub_condition_size; ++j) 
      {
        uint64_t sub_where_exprs_id = subquery_stmt->get_condition_id(j);
        // main_where_exprs.push_back(sub_where_exprs_id);
        // 考虑子链接提升后，隐式连接的顺序是按照条件的顺序确定的，则需要保证条件顺序与原来一致
        main_where_exprs.insert(main_where_exprs.begin()+j, sub_where_exprs_id); 
        
        
        ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(sub_where_exprs_id);
        uint64_t real_table_id = 0;
        sql_raw_expr->optimize_sql_expression(main_stmt, 
            table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, 2);
      }
      
      // main condition
      int32_t condition_size = main_stmt->get_condition_size();
      for (int32_t i = 0; i < condition_size; ++i) 
      {
        uint64_t real_table_id = 0;
        uint64_t expr_id = main_stmt->get_condition_id(i);
        ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
        sql_raw_expr->optimize_sql_expression(main_stmt, 
            table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, 1);
      }
      
      return ret;
    }
    
    /*
     * pull_up_column_items
     */ 
    int ObOptimizerLogical::pull_up_select_items(ObLogicalPlan *&logical_plan, 
                ObSelectStmt *&main_stmt, ObSelectStmt* subquery_stmt, 
                std::map<uint64_t, uint64_t> &alias_table_hashmap,
                std::map<uint64_t, uint64_t> &table_id_hashmap, 
                std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id)
    {
      int ret = OB_SUCCESS;
      
      // 删除子查询的select item对应的表达式
      int32_t sub_select_size = subquery_stmt->get_select_item_size();
      for (int32_t i = 0; i < sub_select_size; ++i) 
      {
        SelectItem& sub_select_item = const_cast<SelectItem&>(subquery_stmt->get_select_item(i));
        uint64_t sub_expr_id = sub_select_item.expr_id_;
        logical_plan->delete_expr_by_id(sub_expr_id);
      }
      
      // 由于select item中expr name和alias name都是存的是alias name，如果父查询取别名的话，下面条件
      // if(first_ref_id == table_id && (alias_name.compare(sub_alias_name)) == 0)会返回false，则需要进行调整
      // 修改父查询中select item对应的表名
      int32_t select_size = main_stmt->get_select_item_size();
      for (int32_t j = 0; j < select_size; ++j) 
      {
        SelectItem& select_item = const_cast<SelectItem&>(main_stmt->get_select_item(j));
        uint64_t expr_id = select_item.expr_id_;
        ObSqlRawExpr* sql_raw_expr = logical_plan->get_expr(expr_id);
        
        uint64_t real_table_id = 0;
        sql_raw_expr->optimize_sql_expression(main_stmt, 
            table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, 1);
      }
      
      return ret;
    }
    
    /*
     * pull_up_from_join_items
     */ 
    int ObOptimizerLogical::pull_up_from_join_items(ObLogicalPlan *&logical_plan, 
                ObSelectStmt *&main_stmt, ObSelectStmt* subquery_stmt, 
                std::map<uint64_t, uint64_t> &alias_table_hashmap,
                std::map<uint64_t, uint64_t> &table_id_hashmap, 
                std::map<uint64_t, uint64_t> &column_id_hashmap, uint64_t table_id, JoinedTable *&joined_table)
    {
      int ret = OB_SUCCESS;
      
      // common::ObVector<FromItem>& from_items = main_stmt->get_from_items();
      common::ObArray<uint64_t>& table_ids = joined_table->get_table_ids();
      for (int64_t i = 0; i < table_ids.count(); ++i) 
      {
        if(table_ids.at(i) == table_id)
        {
          uint64_t expr_id = 0;
          uint64_t real_table_id = 0; // the real table in join expr, when the subquery has multi table.
          // uint64_t sub_real_table_id = 0; // for the table which exist int father query.
          
          // 学校版本join on不存在多个条件的情况
          /*
          int64_t cur_expr_nums_idx = 0;
          int64_t cur_expr_nums = 0;
          if (i == 0) // 考虑join table中table ids与expr ids并不是一一对应的
          {
            cur_expr_nums_idx = 0;
            cur_expr_nums = joined_table->get_expr_nums_per_join().at(0);
          } 
          else 
          {
            cur_expr_nums_idx = joined_table->get_expr_ids_index(i-1);
            cur_expr_nums = joined_table->get_expr_nums_per_join().at(i-1);
          }
          
          // 考虑多表连接的情况
          for (int64_t j = 0; j < cur_expr_nums; ++j) 
          {
            expr_id = joined_table->get_expr_ids().at(cur_expr_nums_idx+j);
            // 找到子查询中与父查询join的表
            ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
            sql_raw_expr->optimize_sql_expression(main_stmt, 
                table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, 1);
          }
          */
          
          if (i == 0) // 考虑join table中table ids与expr ids并不是一一对应的
          {
            expr_id = joined_table->get_expr_ids().at(0);
          } 
          else 
          {
            expr_id = joined_table->get_expr_ids().at(i-1);
          }
          
          // 找到子查询中与父查询join的表
          ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
          sql_raw_expr->optimize_sql_expression(main_stmt, 
              table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, 1);
          
          // change table id in main joined item.
          table_ids.at(i) = real_table_id;
          
          // All the tables in subquery should be added to the main stmt's from items. 
          int32_t sub_from_size = subquery_stmt->get_from_item_size();
          for (int32_t j = 0; j < sub_from_size; ++j) // sub from item只有一个
          {
            FromItem& sub_from_item = const_cast<FromItem&>(subquery_stmt->get_from_item(j));
            bool sub_is_joined = sub_from_item.is_joined_;
            
            // 子查询要么已经合并成一个join table，要么是单表
            // （修正后的情况：要么父查询是隐式连接，要么这里是单表，单表见else部分）
            if (sub_is_joined) // （优化后的架构不走这个分支）
            {
              
            }
            else
            {
              // 对于单表的情况，real_table_id已经提升上去了
            }  
          }
          break;
        }
      }
      
      // 调整使用了子查询的join条件
      common::ObArray<uint64_t>& expr_ids = joined_table->get_expr_ids();
      for (int64_t i = 0; i < expr_ids.count(); ++i) 
      {
        uint64_t real_table_id = 0;
        uint64_t expr_id = expr_ids.at(i);
        ObSqlRawExpr* sql_raw_expr = const_cast<ObSqlRawExpr*>(logical_plan->get_expr(expr_id));
        sql_raw_expr->optimize_sql_expression(main_stmt, 
            table_id_hashmap, column_id_hashmap, table_id, real_table_id, alias_table_hashmap, 1);
      }
      
      return ret;
    }
    
    /*
     * pull up subquery execute
     */ 
    int ObOptimizerLogical::pull_up_simple_subquery(ObLogicalPlan *&logical_plan, 
          ObSelectStmt *&main_stmt, FromItem &from_item, TableItem *&table_item, 
          JoinedTable *&joined_table, uint64_t join_type, ResultPlan &result_plan,
          std::map<uint64_t, uint64_t> &father_alias_table, int32_t from_item_idx)
    {
      int ret = OB_SUCCESS;
      
      uint64_t table_id = table_item->table_id_; // if subquery, from_item's table_id equal table_item's table_id
      bool is_joined = from_item.is_joined_;
      uint64_t ref_id = table_item->ref_id_;
      ObSelectStmt* subquery_stmt = dynamic_cast<ObSelectStmt*>(logical_plan->get_query(ref_id));
      
      
      /** pull up subquery recursion **/
      ret = pull_up_subquery_union(logical_plan, subquery_stmt, result_plan, father_alias_table);
      
      /** 
       * 1. sub_table_id replace table_id, change column_desc. 
       *    tables_hash_(ObRowDesc) must be replace. 
      **/
      std::map<uint64_t, uint64_t> column_id_hashmap; // for main stmt's condition in function pull_up_column_items
      std::map<uint64_t, uint64_t> table_id_hashmap; // for main stmt's condition in function pull_up_column_items
      std::map<uint64_t, uint64_t> alias_table_hashmap; // base table transform to alias table in function pull_up_from_items
      
      if (ret==OB_SUCCESS 
          && is_simple_subquery(subquery_stmt) 
          && is_simple_subquery(main_stmt)) // 父查询也需要过在此做是否简单子查询判断
      {
      }
      else 
      {
        ret = OB_SQL_CAN_NOT_PULL_UP;
      }
      
      // decide whether the sub query can pull up or not
      if (ret == OB_SUCCESS) 
      {
        TBSYS_LOG(DEBUG, "execute function if_can_pull_up");
        ret = if_can_pull_up(
              logical_plan, subquery_stmt, joined_table, 
              join_type, is_joined, table_id, main_stmt);
      }
      
      // 如果把子查询转换成隐式连接的形式，则父查询不能有外连接，而且提升后，join table需要保持原来的顺序
      // 如：(t1, t2, t3) join t4 -> t1, t2, t3 join t4，
      // t4 join (t1, t2, t3) -> t1, t2, t4 join t3，这里其实是不需要考虑t4与哪张表做join，如果是外连接则需要考虑
      // 当join为外连接，且t4跟t1和t3中的字段join，则提升会出问题，所以也说明父查询不能是外连接。
      // 这种方式不考虑
      
      if (ret == OB_SUCCESS) 
      {
        TBSYS_LOG(DEBUG, "execute function pull_up_from_items");
        ret = pull_up_from_items(
              logical_plan, main_stmt, subquery_stmt, from_item, 
              alias_table_hashmap, table_id_hashmap, column_id_hashmap, 
              table_id, father_alias_table, from_item_idx);
      }
      
      if (ret == OB_SUCCESS) 
      {
        /** 2. sub_table_item replace table_item **/
        TBSYS_LOG(DEBUG, "execute function pull_up_table_items");
        ret = pull_up_table_items(logical_plan, main_stmt, subquery_stmt, alias_table_hashmap, table_id);
        
        /** 3. sub_table_id replace column_item.TableRef **/
        if (ret == OB_SUCCESS) 
        {
          TBSYS_LOG(DEBUG, "execute function pull_up_column_items");
          ret = pull_up_column_items(logical_plan, main_stmt, subquery_stmt, alias_table_hashmap, table_id_hashmap, column_id_hashmap, table_id);
        }
        
        /** 4. sub_select_item.sub_expr_id replace select_item.expr_id **/
        if (ret == OB_SUCCESS) 
        {
          TBSYS_LOG(DEBUG, "execute function pull_up_select_items");
          ret = pull_up_select_items(logical_plan, main_stmt, subquery_stmt, alias_table_hashmap, table_id_hashmap, column_id_hashmap, table_id);
        } 
        
        /** 
         * 5. main conditions add the conditions of sub query,
         *    sub_table_id replace ObRawExpr's table_id. 
         **/
        if (ret == OB_SUCCESS) 
        {
          TBSYS_LOG(DEBUG, "execute function pull_up_where_items");
          ret = pull_up_where_items(logical_plan, main_stmt, subquery_stmt, alias_table_hashmap, table_id_hashmap, column_id_hashmap, table_id);
        }
        
        // consider the sub_table_id of join condition
        // (if has the same column name, it choose the first one, and the sequence of the column is same)
        // join condition should repalce table_id
        if (ret == OB_SUCCESS) 
        {
          if(is_joined) // 父查询是显式连接的情况下
          {
            TBSYS_LOG(DEBUG, "execute function pull_up_from_join_items");
            ret = pull_up_from_join_items(logical_plan, main_stmt, subquery_stmt, alias_table_hashmap, table_id_hashmap, column_id_hashmap, table_id, joined_table);
          }
        }
        
        /** 6.delete sub query stmt **/
        if (ret == OB_SUCCESS) 
        {
          TBSYS_LOG(DEBUG, "execute function delete_stmt_by_query_id");
          logical_plan->delete_stmt_by_query_id(ref_id);
        }
      }
      
      if (ret == OB_SUCCESS) 
      {
      }
      else 
      {
        if(ret == OB_SQL_CAN_NOT_PULL_UP)
        {
          TBSYS_LOG(DEBUG, "sub query can not be pull up");
        }
        else
        {
          TBSYS_LOG(ERROR, "pull up sub query fail");
        }
      }
      
      return ret;
    }
  }
}


