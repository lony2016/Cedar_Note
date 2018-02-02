/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_sql_read_strategy.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@alipay.com>
 *
 */

#include "ob_sql_read_strategy.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
//add wanglei [semi join in expr] 20161130:b
struct ObSqlReadStrategy::Comparer
{
    Comparer(const common::ObRowkeyInfo & rowkey_info):compare_rowkey_info_(rowkey_info)
    {
        // compare_rowkey_info_ =  rowkey_info;
    }
    bool  operator()(const sql::ObSqlReadStrategy::RowKey_Objs objs1,const sql::ObSqlReadStrategy::RowKey_Objs objs2)
    {

        int  ret = 0  ;
        int i ;
        for ( i = 0; i < compare_rowkey_info_.get_size();i++)
        {
            if ( (ret = objs1.row_key_objs_[i].compare(objs2.row_key_objs_[i])) != 0)  //不等
            {
                break;
            }
            else  if ( objs1.row_key_objs_[i].is_max_value() || objs1.row_key_objs_[i].is_min_value() )
            {
                break ;
            }
        }
        return ret < 0 ? true : false ;
    }
private:
    const common::ObRowkeyInfo &compare_rowkey_info_;
};
int ObSqlReadStrategy::compare_range(const sql::ObSqlReadStrategy::RowKey_Objs objs1,const sql::ObSqlReadStrategy::RowKey_Objs objs2) const
{
    int  ret = OB_SUCCESS  ;
    if ( NULL!=rowkey_info_)
    {
        for ( int  i = 0; i < rowkey_info_->get_size();i++)
        {
            if ( (ret = objs1.row_key_objs_[i].compare(objs2.row_key_objs_[i])) != 0)
            {
                break;
            }
            else  if ( objs1.row_key_objs_[i].is_max_value() || objs1.row_key_objs_[i].is_min_value() )
            {
                break ;
            }
        }
    }
    else
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "rowkey_info_shoud not be null, ret= %d", ret);
    }

    return ret  ;
}
int ObSqlReadStrategy::eraseDuplicate(pRowKey_Objs start_rowkeys, pRowKey_Objs end_rowkeys, bool forward, int64_t position)
{
    int ret = OB_SUCCESS;
    if ( NULL == start_rowkeys || NULL == end_rowkeys || range_count_ <=0 )
    {
        ret = OB_ERR_UNEXPECTED ;
        TBSYS_LOG(ERROR, "param error   range_count_= %ld, ret =  %d",range_count_, ret);
    }
    else if ( position < range_count_ && position >=1 && !forward )   //
    {
        for ( int64_t i = position - 1 ; i < range_count_ - 1  ; i++ )
        {
            memcpy(start_rowkeys + i , start_rowkeys + (i + 1) ,  sizeof(RowKey_Objs));
            memcpy(end_rowkeys + i , end_rowkeys + (i + 1) ,  sizeof(RowKey_Objs));
        }
        range_count_ -- ;
        if ( range_count_ <= 0 )
        {
            ret = OB_ERR_UNEXPECTED ;
            TBSYS_LOG(ERROR, "IN duplicate ranges , range_count_ = %ld error  %d", range_count_,ret);
        }
    }
    else
    {
        ret = OB_ERR_UNEXPECTED ;
        TBSYS_LOG(WARN, "UNEXPECTED ,check your param !  ret =  %d", ret);
    }
    return ret;
}
int  ObSqlReadStrategy::release_rowkey_objs()
{
    int ret = OB_SUCCESS;
    for ( int i = 0 ;i < OB_MAX_ROWKEY_COLUMN_NUMBER*range_count_cons_; i++ )
    {
        if (mutiple_start_key_mem_hold_[i] != NULL && mutiple_end_key_mem_hold_[i] != NULL && mutiple_start_key_mem_hold_[i] == mutiple_end_key_mem_hold_[i])
        {
            ob_free(mutiple_start_key_mem_hold_[i]);
            mutiple_start_key_mem_hold_[i] = NULL;
            mutiple_end_key_mem_hold_[i] = NULL;
        }
        else
        {
            if (mutiple_start_key_mem_hold_[i] != NULL)
            {
                ob_free(mutiple_start_key_mem_hold_[i]);
                mutiple_start_key_mem_hold_[i] = NULL;
            }
            if (mutiple_end_key_mem_hold_[i] != NULL)
            {
                ob_free(mutiple_end_key_mem_hold_[i]);
                mutiple_end_key_mem_hold_[i] = NULL;
            }
        }
    }
    if ( range_count_cons_ >= 1 )
    {
        if (NULL != mutiple_start_key_objs_)
        {
            ob_free(mutiple_start_key_objs_ );
            mutiple_start_key_objs_ = NULL;
        }
        if (NULL != mutiple_end_key_objs_)
        {
            ob_free(mutiple_end_key_objs_ );
            mutiple_end_key_objs_ = NULL;
        }
        if( NULL != mutiple_start_key_mem_hold_)
        {
            ob_free(mutiple_start_key_mem_hold_ );
            mutiple_start_key_mem_hold_ = NULL;
        }
        if( NULL != mutiple_end_key_mem_hold_)
        {
            ob_free(mutiple_end_key_mem_hold_ );
            mutiple_end_key_mem_hold_ = NULL;
        }
        range_count_ = 0;
        range_count_cons_ = 0;
        idx_key_ = 0;
    }
    return ret ;
}
//add wanglei [semi join in expr] 20161130:e
ObSqlReadStrategy::ObSqlReadStrategy()
  :simple_in_filter_list_(common::OB_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_READ_STRATEGY)),
   simple_cond_filter_list_(common::OB_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_READ_STRATEGY)),
   rowkey_info_(NULL)
{
  memset(start_key_mem_hold_, 0, sizeof(start_key_mem_hold_));
  memset(end_key_mem_hold_, 0, sizeof(end_key_mem_hold_));
  //add wanglei [semi join in expr] 20161130:b
  range_count_ = 0;
  range_count_cons_ = 0;
  idx_key_ = 0 ;
  mutiple_start_key_objs_ = NULL;
  mutiple_end_key_objs_ = NULL;
  mutiple_start_key_mem_hold_= NULL ;
  mutiple_end_key_mem_hold_= NULL;
  in_sub_query_idx_ = OB_INVALID_INDEX;
  //add wanglei [semi join in expr] 20161130:e
}

ObSqlReadStrategy::~ObSqlReadStrategy()
{
  this->destroy();
}

void ObSqlReadStrategy::reset()
{
  simple_in_filter_list_.clear();
  simple_cond_filter_list_.clear();
  rowkey_info_ = NULL;
  memset(start_key_mem_hold_, 0, sizeof(start_key_mem_hold_));
  memset(end_key_mem_hold_, 0, sizeof(end_key_mem_hold_));
  //add wanglei [semi join in expr] 20170416:b
  release_rowkey_objs() ;
  idx_key_ = 0;
  range_count_ = 0;
  range_count_cons_ = 0;
  //add wanglei [semi join in expr] 20170416:e
}

int ObSqlReadStrategy::find_single_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found)
{
  static const bool single_row_only = true;
  bool found_start = false;
  bool found_end = false;
  int ret = find_closed_column_range(real_val, idx, column_id, found_start, found_end, single_row_only);
  found = (found_start && found_end);
  return ret;
}

int ObSqlReadStrategy::find_scan_range(ObNewRange &range, bool &found, bool single_row_only)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  uint64_t column_id = OB_INVALID_ID;
  bool found_start = false;
  bool found_end = false;
  OB_ASSERT(NULL != rowkey_info_);
  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    start_key_objs_[idx].set_min_value();
    end_key_objs_[idx].set_max_value();
  }

  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    if (OB_SUCCESS != (ret = rowkey_info_->get_column_id(idx, column_id)))
    {
      TBSYS_LOG(WARN, "fail to get column id ret=%d, idx=%ld, column_id=%ld", ret, idx, column_id);
      break;
    }
    else
    {
      if (OB_SUCCESS != (ret = find_closed_column_range(true, idx, column_id, found_start, found_end, single_row_only)))
      {
        TBSYS_LOG(WARN, "fail to find closed column range for column %lu", column_id);
        break;
      }
    }
    if (!found_start || !found_end)
    {
      break; // no more search
    }
  }
  range.start_key_.assign(start_key_objs_, rowkey_info_->get_size());
  range.end_key_.assign(end_key_objs_, rowkey_info_->get_size());

  if (0 == idx && (!found_start) && (!found_end))
  {
    found = false;
  }
  return ret;
}

int ObSqlReadStrategy::find_closed_column_range(bool real_val, int64_t idx, uint64_t column_id, bool &found_start, bool &found_end, bool single_row_only)
{
  int ret = OB_SUCCESS;
  int i = 0;
  uint64_t cond_cid = OB_INVALID_ID;
  int64_t cond_op = T_MIN_OP;
  ObObj cond_val;
  ObObj cond_start;
  ObObj cond_end;
  const ObRowkeyColumn *column = NULL;
  found_end = false;
  found_start = false;
  for (i = 0; i < simple_cond_filter_list_.count(); i++)
  {
    if (simple_cond_filter_list_.at(i).is_simple_condition(real_val, cond_cid, cond_op, cond_val))
    {
      if ((cond_cid == column_id) &&
          (NULL != (column = rowkey_info_->get_column(idx))))
      {
        ObObjType target_type = column->type_;
        ObObj expected_type;
        ObObj promoted_obj;
        const ObObj *p_promoted_obj = NULL;
        ObObjType source_type = cond_val.get_type();
        expected_type.set_type(target_type);
        ObString string;
        char *varchar_buff = NULL;
        //modify xsl DECIMAL
        if ((target_type == ObVarcharType && source_type != ObVarcharType) || (target_type == ObDecimalType))
        {
            //modify e
          if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
          {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
          }
          else
          {
            string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
            promoted_obj.set_varchar(string);
            //add xsl DECIMAL
            if(target_type == ObDecimalType)
            {
                ret = obj_cast_for_rowkey(cond_val, expected_type, promoted_obj, p_promoted_obj);
            }
            else
            {
                ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj);
            }
            //add e
            //modify xsl DECIMAL
            if (OB_SUCCESS != ret/*(ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj))*/)
            //modify e
            {
                TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
                ob_free(varchar_buff);
                varchar_buff = NULL;
                break;
            }
            else
            {
              switch (cond_op)
              {
                case T_OP_LT:
                case T_OP_LE:
                  if (end_key_objs_[idx].is_max_value())
                  {
                    end_key_objs_[idx] = *p_promoted_obj;
                    end_key_mem_hold_[idx] = varchar_buff;
                    found_end = true;
                  }
                  else if (*p_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                    else
                    {
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                  }
                  else
                  {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                  break;
                case T_OP_GT:
                case T_OP_GE:
                  if (start_key_objs_[idx].is_min_value())
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    found_start = true;
                    start_key_mem_hold_[idx] = varchar_buff;
                  }
                  else if (*p_promoted_obj > start_key_objs_[idx])
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    if (start_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(start_key_mem_hold_[idx]);
                      start_key_mem_hold_[idx] = varchar_buff;
                    }
                    else
                    {
                      start_key_mem_hold_[idx] = varchar_buff;
                    }
                  }
                  else
                  {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                  break;
                case T_OP_EQ:
                case T_OP_IS:
                  if (start_key_objs_[idx].is_min_value() && end_key_objs_[idx].is_max_value())
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    end_key_objs_[idx] = *p_promoted_obj;
                    // when free, we compare this two address, if equals, then release once
                    start_key_mem_hold_[idx] = varchar_buff;
                    end_key_mem_hold_[idx] = varchar_buff;
                    found_start = true;
                    found_end = true;
                  }
                  else if (start_key_objs_[idx] == end_key_objs_[idx])
                  {
                    if (*p_promoted_obj != start_key_objs_[idx])
                    {
                      TBSYS_LOG(WARN, "two different equal condition on the sanme column, column_id=%lu", column_id);
                    }
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                  else
                  {
                    // actually, if the eq condition is not between the previous range, we also can set range using eq condition,in this case,
                    // the scan range is actually a single-get, filter will filter-out the record,
                    // so, here , we set range to a single-get scan uniformly,contact to lide.wd@taobao.com
                    //if (*p_promoted_obj >= start_key_objs_[idx] && *p_promoted_obj <= end_key_objs_[idx])
                    //{
                      start_key_objs_[idx] = *p_promoted_obj;
                      end_key_objs_[idx] = *p_promoted_obj;
                      if (start_key_mem_hold_[idx] != NULL)
                      {
                        ob_free(start_key_mem_hold_[idx]);
                      }
                      start_key_mem_hold_[idx] = varchar_buff;
                      if (end_key_mem_hold_[idx] != NULL)
                      {
                        ob_free(end_key_mem_hold_[idx]);
                      }
                      end_key_mem_hold_[idx] = varchar_buff;
                    //}
                  }
                  break;
                default:
                  ob_free(varchar_buff);
                  varchar_buff = NULL;
                  TBSYS_LOG(WARN, "unexpected cond op: %ld", cond_op);
                  ret = OB_ERR_UNEXPECTED;
                  break;
              }
            }
          }
        }
        /*
        //add xsl ECNU_DECIMAL 2017_3
        else if (target_type == ObDecimalType || source_type == ObDecimalType)
                {
                    //p_promoted_obj = (ObObj*)ob_malloc(sizeof(ObDecimal), ObModIds::OB_SQL_READ_STRATEGY);
                    p_promoted_obj = &cond_val;
                    switch (cond_op)
                    {
                      case T_OP_LT:
                      case T_OP_LE:
                        if (end_key_objs_[idx].is_max_value())
                        {
                          end_key_objs_[idx] = *p_promoted_obj;
                          found_end = true;
                        }
                        else
                        {
                          if (*p_promoted_obj < end_key_objs_[idx])
                          {
                            end_key_objs_[idx] = *p_promoted_obj;
                            if (end_key_mem_hold_[idx] != NULL)
                            {
                              ob_free(end_key_mem_hold_[idx]);
                            }
                          }
                        }
                        break;
                      case T_OP_GT:
                      case T_OP_GE:
                        if (start_key_objs_[idx].is_min_value())
                        {
                          start_key_objs_[idx] = *p_promoted_obj;
                          found_start = true;
                        }
                        else
                        {
                          if (*p_promoted_obj > start_key_objs_[idx])
                          {
                            start_key_objs_[idx] = *p_promoted_obj;
                            if (start_key_mem_hold_[idx] != NULL)
                            {
                              ob_free(start_key_mem_hold_[idx]);
                            }
                          }
                        }
                        break;
                      case T_OP_EQ:
                      case T_OP_IS:
                        if (start_key_objs_[idx].is_min_value() && end_key_objs_[idx].is_max_value())
                        {
                          start_key_objs_[idx] = *p_promoted_obj;
                          end_key_objs_[idx] = *p_promoted_obj;
                          found_start = true;
                          found_end = true;
                        }
                        else if (start_key_objs_[idx] == end_key_objs_[idx])
                        {
                          if (*p_promoted_obj != start_key_objs_[idx])
                          {
                            TBSYS_LOG(WARN, "two different equal condition on the same column, column_id=%lu"
                                " start_key_objs_[idx]=%s, *p_promoted_obj=%s",
                                column_id, to_cstring(start_key_objs_[idx]), to_cstring(*p_promoted_obj));
                          }
                        }
                        else
                        {
                          // actually, if the eq condition is not between the previous range, we also can set range using eq condition,in this case,
                          // the scan range is actually a single-get, filter will filter-out the record,
                          // so, here , we set range to a single-get scan uniformly,contact to lide.wd@taobao.com
                          //if (*p_promoted_obj >= start_key_objs_[idx] && *p_promoted_obj <= end_key_objs_[idx])
                          //{
                            start_key_objs_[idx] = *p_promoted_obj;
                            end_key_objs_[idx] = *p_promoted_obj;
                            if (start_key_mem_hold_[idx] != NULL)
                            {
                              ob_free(start_key_mem_hold_[idx]);
                            }
                            if (end_key_mem_hold_[idx] != NULL)
                            {
                              ob_free(end_key_mem_hold_[idx]);
                            }
                          //}
                        }
                        break;
                      default:
                        TBSYS_LOG(WARN, "unexpected cond op: %ld", cond_op);
                        ret = OB_ERR_UNEXPECTED;
                        break;
                    }
                }

        //add e
        */
        else
        {
          if (OB_SUCCESS != (ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj)))
          {
            TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
            break;
          }
          else
          {
            switch (cond_op)
            {
              case T_OP_LT:
              case T_OP_LE:
                if (end_key_objs_[idx].is_max_value())
                {
                  end_key_objs_[idx] = *p_promoted_obj;
                  found_end = true;
                }
                else
                {
                  if (*p_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                    }
                  }
                }
                break;
              case T_OP_GT:
              case T_OP_GE:
                if (start_key_objs_[idx].is_min_value())
                {
                  start_key_objs_[idx] = *p_promoted_obj;
                  found_start = true;
                }
                else
                {
                  if (*p_promoted_obj > start_key_objs_[idx])
                  {
                    start_key_objs_[idx] = *p_promoted_obj;
                    if (start_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(start_key_mem_hold_[idx]);
                    }
                  }
                }
                break;
              case T_OP_EQ:
              case T_OP_IS:
                if (start_key_objs_[idx].is_min_value() && end_key_objs_[idx].is_max_value())
                {
                  start_key_objs_[idx] = *p_promoted_obj;
                  end_key_objs_[idx] = *p_promoted_obj;
                  found_start = true;
                  found_end = true;
                }
                else if (start_key_objs_[idx] == end_key_objs_[idx])
                {
                  if (*p_promoted_obj != start_key_objs_[idx])
                  {
                    TBSYS_LOG(WARN, "two different equal condition on the same column, column_id=%lu"
                        " start_key_objs_[idx]=%s, *p_promoted_obj=%s",
                        column_id, to_cstring(start_key_objs_[idx]), to_cstring(*p_promoted_obj));
                  }
                }
                else
                {
                  // actually, if the eq condition is not between the previous range, we also can set range using eq condition,in this case,
                  // the scan range is actually a single-get, filter will filter-out the record,
                  // so, here , we set range to a single-get scan uniformly,contact to lide.wd@taobao.com
                  //if (*p_promoted_obj >= start_key_objs_[idx] && *p_promoted_obj <= end_key_objs_[idx])
                  //{
                    start_key_objs_[idx] = *p_promoted_obj;
                    end_key_objs_[idx] = *p_promoted_obj;
                    if (start_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(start_key_mem_hold_[idx]);
                    }
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                    }
                  //}
                }
                break;
              default:
                TBSYS_LOG(WARN, "unexpected cond op: %ld", cond_op);
                ret = OB_ERR_UNEXPECTED;
                break;
            }
          }
        }
        if (single_row_only && cond_op != T_OP_EQ)
        {
          found_end = found_start = false;
        }
      }
    }
    else if ((!single_row_only) && simple_cond_filter_list_.at(i).is_simple_between(real_val, cond_cid, cond_op, cond_start, cond_end))
    {
      if (cond_cid == column_id)
      {
        OB_ASSERT(T_OP_BTW == cond_op);
        column = rowkey_info_->get_column(idx);
        ObObjType target_type;
        if (column == NULL)
        {
          TBSYS_LOG(WARN, "get column from rowkey_info failed, column = NULL");
        }
        else
        {
          target_type = column->type_;
          ObObj expected_type;
          expected_type.set_type(target_type);
          ObObj start_promoted_obj;
          ObString start_string;
          ObString end_string;
          char *varchar_buff = NULL;
          const ObObj *p_start_promoted_obj = NULL;
          ObObj end_promoted_obj;
          const ObObj *p_end_promoted_obj = NULL;
          ObObjType start_source_type = cond_start.get_type();
          ObObjType end_source_type = cond_end.get_type();
          //modify xsl DECIMAL
          if ((target_type == ObVarcharType && start_source_type != ObVarcharType) || (target_type == ObDecimalType))
          //modify e
          {
            if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
            {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
            }
            else
            {
              start_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
              start_promoted_obj.set_varchar(start_string);
              //add xsl DECIMAL
              if(target_type == ObDecimalType)
              {
                  ret = obj_cast_for_rowkey(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj);
              }
              else
              {
                  ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj);
              }
              //add e
              //modify xsl DECIMAL
              if (OB_SUCCESS != ret/*(ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj))*/)
              //modify e
              {
                TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
                ob_free(varchar_buff);
                varchar_buff = NULL;
                break;
              }
              else
              {
                if (start_key_objs_[idx].is_min_value())
                {
                  start_key_objs_[idx] = *p_start_promoted_obj;
                  found_start = true;
                  start_key_mem_hold_[idx] = varchar_buff;
                }
                else if (*p_start_promoted_obj > start_key_objs_[idx])
                {
                  start_key_objs_[idx] = *p_start_promoted_obj;
                  if (start_key_mem_hold_[idx] != NULL)
                  {
                    ob_free(start_key_mem_hold_[idx]);
                    start_key_mem_hold_[idx] = varchar_buff;
                  }
                  else
                  {
                    start_key_mem_hold_[idx] = varchar_buff;
                  }
                }
                else
                {
                  ob_free(varchar_buff);
                  varchar_buff = NULL;
                }
              }
            }
          }
          else
          {
            if (OB_SUCCESS != (ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj)))
            {
              TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
            }
            else
            {
              if (start_key_objs_[idx].is_min_value())
              {
                start_key_objs_[idx] = *p_start_promoted_obj;
                found_start = true;
              }
              else
              {
                if (*p_start_promoted_obj > start_key_objs_[idx])
                {
                  start_key_objs_[idx] = *p_start_promoted_obj;
                  if (start_key_mem_hold_[idx] != NULL)
                  {
                    ob_free(start_key_mem_hold_[idx]);
                    start_key_mem_hold_[idx] = NULL;
                  }
                }
              }
            }
          }
          varchar_buff = NULL;
          if (OB_SUCCESS == ret)
          {
              //modify xsl DECIMAL
              if ((target_type == ObVarcharType && end_source_type != ObVarcharType) || (target_type == ObDecimalType))
              //modify e
            {
              if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
              {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
              }
              else
              {
                end_key_mem_hold_[idx] = varchar_buff;
                end_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                end_promoted_obj.set_varchar(end_string);
                //add xsl DECIMAL
                if(target_type == ObDecimalType)
                {
                    ret = obj_cast_for_rowkey(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj);
                }
                else
                {
                    ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj);
                }
                //add e
                if(OB_SUCCESS != ret)
                {
                  TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
                  ob_free(varchar_buff);
                  varchar_buff = NULL;
                  break;
                }
                else
                {
                  if (end_key_objs_[idx].is_max_value())
                  {
                    end_key_objs_[idx] = *p_end_promoted_obj;
                    found_end = true;
                    end_key_mem_hold_[idx] = varchar_buff;
                  }
                  else if (*p_end_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_end_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                    else
                    {
                      end_key_mem_hold_[idx] = varchar_buff;
                    }
                  }
                  else
                  {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                  }
                }
              }
            }
            else
            {
              if (OB_SUCCESS != (ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj)))
              {
                TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
              }
              else
              {
                if (end_key_objs_[idx].is_max_value())
                {
                  end_key_objs_[idx] = *p_end_promoted_obj;
                  found_end = true;
                }
                else
                {
                  if (*p_end_promoted_obj < end_key_objs_[idx])
                  {
                    end_key_objs_[idx] = *p_end_promoted_obj;
                    if (end_key_mem_hold_[idx] != NULL)
                    {
                      ob_free(end_key_mem_hold_[idx]);
                      end_key_mem_hold_[idx] = NULL;
                    }
                  }
                }
              }
            }
          }
				}
      }
    }

    if (ret != OB_SUCCESS)
    {
      break;
    }
    //if (found_start && found_end)
    //{
      /* we can break earlier here */
      //break;
    //}
  }
  return ret;
}

int ObSqlReadStrategy::add_filter(const ObSqlExpression &expr)
{
  int ret = OB_SUCCESS;
  uint64_t cid = OB_INVALID_ID;
  int64_t op = T_INVALID;
  ObObj val1;
  ObObj val2;

  if (expr.is_simple_condition(false, cid, op, val1))
  {
    // TBSYS_LOG(DEBUG, "simple condition [%s]", to_cstring(expr));
    if (OB_SUCCESS != (ret = simple_cond_filter_list_.push_back(expr)))
    {
      TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
    }
  }
  else if (expr.is_simple_between(false, cid, op, val1, val2))
  {
    // TBSYS_LOG(DEBUG, "simple between condition [%s]", to_cstring(expr));
    if (OB_SUCCESS != (ret = simple_cond_filter_list_.push_back(expr)))
    {
      TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
    }
  }
  else
  {
    ObArray<ObRowkey> rowkey_array;
    common::PageArena<ObObj,common::ModulePageAllocator> rowkey_objs_allocator(
        PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_READ_STATEGY));
    if (true == expr.is_simple_in_expr(false, *rowkey_info_, rowkey_array, rowkey_objs_allocator))
    {
      // TBSYS_LOG(DEBUG, "simple in expr [%s]", to_cstring(expr));
      if (OB_SUCCESS != (ret = simple_in_filter_list_.push_back(expr)))
      {
        TBSYS_LOG(WARN, "fail to add simple filter. ret=%d", ret);
      }
    }
  }

  return ret;
}

int ObSqlReadStrategy::find_rowkeys_from_equal_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &objs_allocator)
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  uint64_t column_id = OB_INVALID_ID;
  bool found = false;
  UNUSED(objs_allocator);
  OB_ASSERT(rowkey_info_->get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    start_key_objs_[idx].set_min_value();
    end_key_objs_[idx].set_max_value();
  }
  //遍历全主键
  for (idx = 0; idx < rowkey_info_->get_size(); idx++)
  {
    if (OB_SUCCESS != (ret = rowkey_info_->get_column_id(idx, column_id)))
    {
      TBSYS_LOG(WARN, "fail to get column id. idx=%ld, ret=%d", idx, ret);
      break;
    }
    else if (OB_SUCCESS != (ret = find_single_column_range(real_val, idx, column_id, found)))
    {
      TBSYS_LOG(WARN, "fail to find closed range for column %lu", column_id);
      break;
    }
    else if (!found)
    {
      break; // no more search
    }
  }/* end for */
  if (OB_SUCCESS == ret && found && idx == rowkey_info_->get_size())
  {
    ObRowkey rowkey;
    rowkey.assign(start_key_objs_, rowkey_info_->get_size());
    if (OB_SUCCESS != (ret = rowkey_array.push_back(rowkey)))
    {
      TBSYS_LOG(WARN, "fail to push rowkey to list. rowkey=%s, ret=%d", to_cstring(rowkey), ret);
    }
  }
  return ret;
}

int ObSqlReadStrategy::find_rowkeys_from_in_expr(bool real_val, ObIArray<ObRowkey> &rowkey_array, common::PageArena<ObObj,common::ModulePageAllocator> &objs_allocator)
{
  int ret = OB_SUCCESS;
  bool is_in_expr_with_rowkey = false;
  int i = 0;
  if (simple_in_filter_list_.count() > 1)
  {
    TBSYS_LOG(DEBUG, "simple in filter count[%ld]", simple_in_filter_list_.count());
    ret = OB_SUCCESS;
  }
  else
  {
    for (i = 0; i < simple_in_filter_list_.count(); i++)
    {
      // assume rowkey in sequence and all rowkey columns present
      if (false == (is_in_expr_with_rowkey = simple_in_filter_list_.at(i).is_simple_in_expr(real_val, *rowkey_info_, rowkey_array, objs_allocator)))
      {
        TBSYS_LOG(WARN, "fail to get rowkey(s) from in expression. ret=%d", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "simple in expr rowkey_array count = %ld", rowkey_array.count());
      }
    }
  }
  // cast rowkey if needed
  if (OB_SUCCESS == ret && true == is_in_expr_with_rowkey)
  {
    char *in_rowkey_buf = NULL;
    int64_t total_used_buf_len = 0;
    int64_t used_buf_len = 0;
    int64_t rowkey_idx = 0;
    for (rowkey_idx = 0; rowkey_idx < rowkey_array.count(); rowkey_idx++)
    {
      bool need_buf = false;
      if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(*rowkey_info_, rowkey_array.at(rowkey_idx), need_buf)))
      {
        TBSYS_LOG(WARN, "err=%d", ret);
      }
      else if (need_buf)
      {
        if (NULL == in_rowkey_buf)
        {
          in_rowkey_buf = (char*)objs_allocator.alloc(OB_MAX_ROW_LENGTH);
        }
        if (NULL == in_rowkey_buf)
        {
          TBSYS_LOG(ERROR, "no memory");
          ret = OB_ALLOCATE_MEMORY_FAILED;
        }
      }
      if (OB_LIKELY(OB_SUCCESS == ret))
      {
        if (OB_MAX_ROW_LENGTH <= total_used_buf_len)
        {
          TBSYS_LOG(WARN, "rowkey has too much varchar. len=%ld", total_used_buf_len);
        }
        else if (OB_SUCCESS != (ret = ob_cast_rowkey(*rowkey_info_, rowkey_array.at(rowkey_idx),
                in_rowkey_buf, OB_MAX_ROW_LENGTH - total_used_buf_len, used_buf_len)))
        {
          TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
        }
        else
        {
          total_used_buf_len = used_buf_len;
          in_rowkey_buf += used_buf_len;
        }
      }
    }
  }

  return ret;
}

int ObSqlReadStrategy::get_read_method(ObIArray<ObRowkey> &rowkey_array, PageArena<ObObj,common::ModulePageAllocator> &rowkey_objs_allocator, int32_t &read_method)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = find_rowkeys_from_in_expr(false, rowkey_array, rowkey_objs_allocator)))
    {
      TBSYS_LOG(WARN, "fail to find rowkeys in IN operator. ret=%d", ret);
    }
    else if (rowkey_array.count() > 0)
    {
      read_method = USE_GET;
    }
    else if (OB_SUCCESS != (ret = find_rowkeys_from_equal_expr(false, rowkey_array, rowkey_objs_allocator)))
    {
      TBSYS_LOG(WARN, "fail to find rowkeys in equal where operator. ret=%d", ret);
    }
    else if (rowkey_array.count() == 1)
    {
      read_method = USE_GET;
    }
    else
    {
      read_method = USE_SCAN;
    }
  }
  return ret;
}

void ObSqlReadStrategy::destroy()
{
  for (int i = 0 ;i < OB_MAX_ROWKEY_COLUMN_NUMBER; ++i)
  {
    if (start_key_mem_hold_[i] != NULL && end_key_mem_hold_[i] != NULL && start_key_mem_hold_[i] == end_key_mem_hold_[i])
    {
      // release only once on the same memory block
      ob_free(start_key_mem_hold_[i]);
      start_key_mem_hold_[i] = NULL;
      end_key_mem_hold_[i] = NULL;
    }
    else
    {
      if (start_key_mem_hold_[i] != NULL)
      {
        ob_free(start_key_mem_hold_[i]);
        start_key_mem_hold_[i] = NULL;
      }
      if (end_key_mem_hold_[i] != NULL)
      {
        ob_free(end_key_mem_hold_[i]);
        end_key_mem_hold_[i] = NULL;
      }
    }
  }
  release_rowkey_objs();//add wanglei [semi join in expr] 20170415
}

int ObSqlReadStrategy::assign(const ObSqlReadStrategy *other, ObPhyOperator *owner_op)
{
  int ret = OB_SUCCESS;
  CAST_TO_INHERITANCE(ObSqlReadStrategy);
  reset();
  for (int64_t i = 0; ret == OB_SUCCESS && i < o_ptr->simple_in_filter_list_.count(); i++)
  {
    if ((ret = simple_in_filter_list_.push_back(o_ptr->simple_in_filter_list_.at(i))) == OB_SUCCESS)
    {
      if (owner_op)
      {
        simple_in_filter_list_.at(i).set_owner_op(owner_op);
      }
    }
    else
    {
      break;
    }
  }
  for (int64_t i = 0; ret == OB_SUCCESS && i < o_ptr->simple_cond_filter_list_.count(); i++)
  {
    if ((ret = simple_cond_filter_list_.push_back(o_ptr->simple_cond_filter_list_.at(i))) == OB_SUCCESS)
    {
      if (owner_op)
      {
        simple_cond_filter_list_.at(i).set_owner_op(owner_op);
      }
    }
    else
    {
      break;
    }
  }
  return ret;
}

int64_t ObSqlReadStrategy::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ReadStrategy(in_filter=");
  pos += simple_in_filter_list_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ", cond_filter=");
  pos += simple_cond_filter_list_.to_string(buf+pos, buf_len-pos);
  databuff_printf(buf, buf_len, pos, ")\n");
  return pos;
}
//add wanglei [semi join in expr] 20170417:b
int  ObSqlReadStrategy::print_ranges() const
{
    int ret = OB_SUCCESS;
    if ( NULL != rowkey_info_)
    {
        int64_t rowkey_num = rowkey_info_->get_size();   //
        for ( int i = 0;i < range_count_;i++)
        {
            for ( int j = 0; j < rowkey_num; j++)
            {

                TBSYS_LOG(DEBUG, "wanglei::ranges:line %d  column %d start is=  %s", i, j, to_cstring(*(mutiple_start_key_objs_+ i*OB_MAX_ROWKEY_COLUMN_NUMBER+j)));
                TBSYS_LOG(DEBUG, "wanglei::ranges:line %d  column %d end is=  %s", i, j, to_cstring(*(mutiple_end_key_objs_+ i*OB_MAX_ROWKEY_COLUMN_NUMBER+j)));

            }
        }
    }
    else
    {
        ret = OB_ERR_UNEXPECTED ;
        TBSYS_LOG(WARN, "rowkey_info_ should not be null, ret=%d", ret);
    }
    return ret;
}
int ObSqlReadStrategy::sort_mutiple_range()
{
    int  ret = OB_SUCCESS;
    int64_t i ;
    if ( range_count_ > 1)
    {
        pRowKey_Objs start_rowkeys =  (pRowKey_Objs)mutiple_start_key_objs_ ;
        pRowKey_Objs end_rowkeys  =  (pRowKey_Objs)mutiple_end_key_objs_ ;
        // UNUSED(tmp_rowkeys);
        std::sort(start_rowkeys , start_rowkeys + range_count_ ,  ObSqlReadStrategy::Comparer(*rowkey_info_));
        // UNUSED(tmp_rowkeys);
        std::sort(end_rowkeys , end_rowkeys + range_count_ ,  ObSqlReadStrategy::Comparer(*rowkey_info_));
        //  	 qsort( (void*)tmp_rowkeys, range_count_ ,sizeof(RowKey_Objs), compare_range);
        for ( i = range_count_ - 1; i >= 1 ;i-- )
        {
            if ( 0 == compare_range( *(start_rowkeys+i), *(start_rowkeys+i-1) )  && 0 == compare_range( *(end_rowkeys+i), *(end_rowkeys+i-1) ) )
            {
                if ( (ret = eraseDuplicate( start_rowkeys,  end_rowkeys , false, i ) ) !=OB_SUCCESS )
                {
                    TBSYS_LOG(ERROR, "fail to  erase duplicate ranges  %d", ret);
                    break;
                }
                else
                {
                    TBSYS_LOG(DEBUG, "erase duplicate ranges success  %d", ret);
                }
            }
        }
    }
    else if (range_count_ <= 0)
    {
        ret = OB_ERR_UNEXPECTED ;
        TBSYS_LOG(WARN, "check your param ! range_count_=%ld, ret =  %d",range_count_, ret);
    }
    return ret ;
}
int ObSqlReadStrategy::malloc_rowkey_objs_space( int64_t num)
{
    int ret = OB_SUCCESS;
    if ( range_count_ > 0 )
    {
        ret = OB_ERR_UNEXPECTED ;
        TBSYS_LOG(ERROR, "malloc space twice,error, ret=%d", ret);
    }
    else if ( num <=0 )
    {
        ret = OB_ERR_UNEXPECTED ;
        TBSYS_LOG(ERROR, "Wrong parameter,should not be negative, ret=%d", ret);
    }
    else if ( (mutiple_start_key_objs_ = (common::ObObj *)ob_malloc(OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj), ObModIds::OB_SQL_READ_STRATEGY)) == NULL)
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d",OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj), ret);
    }
    else if ( (mutiple_end_key_objs_ = (common::ObObj *)ob_malloc(OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj), ObModIds::OB_SQL_READ_STRATEGY)) == NULL)
    {
        ob_free(mutiple_start_key_objs_);
        mutiple_start_key_objs_ = NULL;
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj), ret);
    }
    else if ( (mutiple_start_key_mem_hold_ = (char **)ob_malloc(sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER, ObModIds::OB_SQL_READ_STRATEGY)) == NULL)
    {
        ob_free(mutiple_start_key_objs_);
        mutiple_start_key_objs_ = NULL;
        ob_free(mutiple_end_key_objs_);
        mutiple_end_key_objs_ = NULL;
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER, ret);
    }
    else if ( (mutiple_end_key_mem_hold_ = (char **)ob_malloc(sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER, ObModIds::OB_SQL_READ_STRATEGY)) == NULL)
    {
        ob_free(mutiple_start_key_objs_);
        mutiple_start_key_objs_ = NULL;
        ob_free(mutiple_end_key_objs_);
        mutiple_end_key_objs_ = NULL;
        ob_free(mutiple_start_key_mem_hold_);
        mutiple_start_key_mem_hold_ = NULL;
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER, ret);
    }
    else
    {
        range_count_ = num;
        //add peiouya IN_EXPR  [PrefixKeyQuery_for_INstmt] 20140930:b
        range_count_cons_ = num;
        //add 20140930:e
        idx_key_ = 0 ;
        memset(mutiple_start_key_objs_, 0, OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj));
        memset(mutiple_end_key_objs_, 0, OB_MAX_ROWKEY_COLUMN_NUMBER*num*sizeof(common::ObObj));
        memset(mutiple_start_key_mem_hold_, 0, sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER);
        memset(mutiple_end_key_mem_hold_, 0, sizeof(char*)*num*OB_MAX_ROWKEY_COLUMN_NUMBER);
        // do nothing
    }
    return ret ;

}
int ObSqlReadStrategy::find_scan_range(bool &found, bool single_row_only)
{
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    uint64_t column_id = OB_INVALID_ID;
    bool found_start = false;
    bool found_end = false;
    //再次初始化，防止出错
    idx_key_ = 0;
    range_count_ = 0 ;
    if(NULL != rowkey_info_)
    {
        for (idx = 0; idx < rowkey_info_->get_size(); idx++)
        {
            if (OB_SUCCESS != (ret = rowkey_info_->get_column_id(idx, column_id)))
            {
                TBSYS_LOG(WARN, "fail to get column id ret=%d, idx=%ld, column_id=%ld", ret, idx, column_id);
                break;
            }
            else
            {
                if (OB_SUCCESS != (ret = find_closed_column_range_ex(true, idx, column_id, found_start, found_end, single_row_only)))
                {
                    TBSYS_LOG(WARN, "fail to find closed column range for column %lu", column_id);
                    break;
                }
            }
            if (!found_start || !found_end)
            {
                break; // no more search
            }
        }

        if (0 == idx && (!found_start) && (!found_end))
        {
            found = false;
        }
        if ( OB_SUCCESS == ret )
        {
            if ( (ret = print_ranges()) !=OB_SUCCESS)
            {
                TBSYS_LOG(ERROR, "print ranges error! ret = %d",ret);
            }
            else if ( ( ret = sort_mutiple_range() )!= OB_SUCCESS)
            {
                TBSYS_LOG(ERROR, "sorting ranges error! ret = %d",ret);
            }
            else if ( (ret = print_ranges()) !=OB_SUCCESS)
            {
                TBSYS_LOG(ERROR, "print ranges error! ret = %d",ret);
            }
        }
        else
        {
            if ( range_count_ > 0)
            {
                this ->range_count_ = 1;
                TBSYS_LOG(WARN, "construct ranges  failed,the error can be ignored !,ret=%d",ret);
                ret = OB_SUCCESS;
            }
            else
            {
                TBSYS_LOG(ERROR, "construct ranges  failed, ob_malloc may failed!,ret=%d",ret);
            }

        }
    }else
    {
        ret = OB_ERR_UNEXPECTED ;
        TBSYS_LOG(ERROR, "rowkey_info should not be null,ret=%d",ret);
    }
    return ret;
}
bool ObSqlReadStrategy::has_next_range()
{
    bool has_next = false;//ret = OB_SUCCESS;
    if ( idx_key_ >= 0 && idx_key_ < range_count_ )
    {
        has_next =true;
    }
    else //if (idx_key_ < range_count_ && idx_key_ >=0 )
    {
        has_next =false;
    }
    return has_next ;
}
int ObSqlReadStrategy::get_next_scan_range(ObNewRange &range,bool &has_next)
{
    int ret = OB_SUCCESS;
    if ( range_count_ >= 1 )
    {
        range.start_key_.assign(mutiple_start_key_objs_ + idx_key_*OB_MAX_ROWKEY_COLUMN_NUMBER, rowkey_info_->get_size());
        range.end_key_.assign(mutiple_end_key_objs_ + idx_key_*OB_MAX_ROWKEY_COLUMN_NUMBER, rowkey_info_->get_size());
        if ( idx_key_ >= 0  && idx_key_ < range_count_-1)
        {
            idx_key_++;
            has_next =true;
        }
        else
        {
            idx_key_ = -1;
            has_next =false;
        }
    }
    else
    {
        TBSYS_LOG(WARN, "range count should not be negative or zero ,range count =%ld!", range_count_);
        idx_key_ = -1;
        has_next = false;
        ret = OB_ERR_UNEXPECTED;
    }
    return ret ;
}
int ObSqlReadStrategy::find_closed_column_range_simple_con(bool real_val, const ObRowkeyColumn *column, int64_t column_idx, int64_t idx_of_ranges, int64_t cond_op, ObObj cond_val, bool &found_start, bool &found_end, bool single_row_only)
{
    int ret = OB_SUCCESS;
    UNUSED(real_val);
    ObObjType target_type ;
    ObObj expected_type;
    ObObj promoted_obj;
    const ObObj *p_promoted_obj = NULL;
    ObObjType source_type = cond_val.get_type();
    ObString string;
    int64_t idx = idx_of_ranges*OB_MAX_ROWKEY_COLUMN_NUMBER + column_idx ;// 当前的位置
    char *varchar_buff = NULL;

    ////////////////
    bool need_store_space  = false ;
    if ( NULL == column )
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "column should not be null,ret=%d",ret);
    }
    else if ( idx < 0 || idx >= range_count_ * OB_MAX_ROWKEY_COLUMN_NUMBER)
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, " obj index of ranges out of range ,column idx =%ld,range idx =%ld,ret=%d",column_idx, idx,ret);
    }
    else
    {
        target_type = column->type_;
        expected_type.set_type(target_type);
        ///
        //step 1
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        if ((target_type == ObVarcharType && source_type != ObVarcharType)||(target_type == ObDecimalType))
            //modify:e
        {
            if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
            }
            else
            {
                need_store_space = true ;
                string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                promoted_obj.set_varchar(string);
                //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                   if(target_type == ObDecimalType)
                                ret = obj_cast_for_rowkey(cond_val, expected_type, promoted_obj, p_promoted_obj);
                           else
                                ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj);
                            //add:e
                if (OB_SUCCESS != ret)
                    //modify:e
                {
                    TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                    ret = OB_ERR_UNEXPECTED;
                }
            }
        }
        else if (OB_SUCCESS != (ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj)))
        {
            TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
        }
        else
        {
            need_store_space = false ;
        }

        //step 2
        if ( OB_SUCCESS == ret )
        {
            switch (cond_op)
            {
            case T_OP_LT:// 小于
            case T_OP_LE: // 小于等于
                if (mutiple_end_key_objs_[idx].is_max_value())
                {
                    mutiple_end_key_objs_[idx] = *p_promoted_obj;
                    if ( need_store_space )
                        mutiple_end_key_mem_hold_[idx] = varchar_buff;
                    found_end = true;
                }
                else if (*p_promoted_obj < mutiple_end_key_objs_[idx])
                {
                    found_end = true;
                    mutiple_end_key_objs_[idx] = *p_promoted_obj;
                    if (mutiple_end_key_mem_hold_[idx] != NULL)
                    {
                        ob_free(mutiple_end_key_mem_hold_[idx]);
                        mutiple_end_key_mem_hold_[idx] = NULL;
                        if (need_store_space )
                            mutiple_end_key_mem_hold_[idx] = varchar_buff;
                    }
                    else if (need_store_space)
                    {
                        mutiple_end_key_mem_hold_[idx] = varchar_buff;
                    }
                }
                else if (need_store_space)
                {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                }
                break;
            case T_OP_GT:
            case T_OP_GE:
                if (mutiple_start_key_objs_[idx].is_min_value())
                {
                    mutiple_start_key_objs_[idx] = *p_promoted_obj;
                    found_start = true;
                    if ( need_store_space )
                        mutiple_start_key_mem_hold_[idx] = varchar_buff;
                }
                else if (*p_promoted_obj > mutiple_start_key_objs_[idx])
                {
                    found_start = true;
                    mutiple_start_key_objs_[idx] = *p_promoted_obj;
                    if (mutiple_start_key_mem_hold_[idx] != NULL)
                    {
                        ob_free(mutiple_start_key_mem_hold_[idx]);
                        mutiple_start_key_mem_hold_[idx] = NULL;
                        if ( need_store_space )
                            mutiple_start_key_mem_hold_[idx] = varchar_buff;
                    }
                    else if (need_store_space)
                    {
                        mutiple_start_key_mem_hold_[idx] = varchar_buff;
                    }
                }
                else if (need_store_space)
                {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                }
                break;
            case T_OP_EQ:
            case T_OP_IS:
                // not set value
                if (mutiple_start_key_objs_[idx].is_min_value() && mutiple_end_key_objs_[idx].is_max_value())
                {
                    mutiple_start_key_objs_[idx] = *p_promoted_obj;
                    mutiple_end_key_objs_[idx] = *p_promoted_obj;
                    // when free, we compare this two address, if equals, then release once
                    if (need_store_space)
                    {
                        mutiple_start_key_mem_hold_[idx] = varchar_buff;
                        mutiple_end_key_mem_hold_[idx] = varchar_buff;
                    }
                    // found_start = true;
                    // found_end = true;
                }
                else if (mutiple_start_key_objs_[idx] == mutiple_end_key_objs_[idx])
                {
                    //new value  != old value  renew the value
                    if (*p_promoted_obj != mutiple_start_key_objs_[idx])
                    {
                        TBSYS_LOG(WARN, "two different equal condition on the same column, column_id=%lu", column->column_id_);

                        // release old space
                        if (mutiple_start_key_mem_hold_[idx] == mutiple_end_key_mem_hold_[idx] && NULL != mutiple_start_key_mem_hold_[idx])
                        {
                            ob_free(mutiple_start_key_mem_hold_[idx]);
                            mutiple_start_key_mem_hold_[idx] = NULL;
                            mutiple_end_key_mem_hold_[idx] = NULL;
                        }
                        else
                        {
                            if (NULL != mutiple_start_key_mem_hold_[idx])
                            {
                                ob_free(mutiple_start_key_mem_hold_[idx]);
                                mutiple_start_key_mem_hold_[idx] = NULL;
                            }
                            if (NULL != mutiple_end_key_mem_hold_[idx])
                            {
                                ob_free(mutiple_end_key_mem_hold_[idx]);
                                mutiple_end_key_mem_hold_[idx] = NULL;
                            }
                        }
                        //specify new value
                        mutiple_start_key_objs_[idx] = *p_promoted_obj;
                        mutiple_end_key_objs_[idx] = *p_promoted_obj;
                        // storage new space
                        if (need_store_space)
                        {
                            mutiple_start_key_mem_hold_[idx] = varchar_buff;
                            mutiple_end_key_mem_hold_[idx] = varchar_buff;
                        }

                    }
                    // new value  == old value, ignore the new value,release  the new malloced space
                    else if (need_store_space)
                    {
                        ob_free(varchar_buff);
                        varchar_buff = NULL;
                    }
                }
                else
                {
                    mutiple_start_key_objs_[idx] = *p_promoted_obj;
                    mutiple_end_key_objs_[idx] = *p_promoted_obj;
                    if (mutiple_start_key_mem_hold_[idx] != NULL)
                    {
                        ob_free(mutiple_start_key_mem_hold_[idx]);
                        mutiple_start_key_mem_hold_[idx] = NULL;
                    }
                    if (mutiple_end_key_mem_hold_[idx] != NULL)
                    {
                        ob_free(mutiple_end_key_mem_hold_[idx]);
                        mutiple_end_key_mem_hold_[idx] = NULL;
                    }
                    if (need_store_space)
                    {
                        mutiple_start_key_mem_hold_[idx] = varchar_buff;
                        mutiple_end_key_mem_hold_[idx] = varchar_buff;
                    }
                }
                found_start = true;
                found_end = true;
                break;
            default:
                if (need_store_space )
                {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                }
                TBSYS_LOG(WARN, "unexpected cond op: %ld", cond_op);
                ret = OB_ERR_UNEXPECTED;
            }
        }
        else
        {
            TBSYS_LOG(WARN, "error, ret=%d", ret);
        }
        ////////////////
    }

    if (single_row_only && ( cond_op != T_OP_EQ && cond_op != T_OP_IS) )//
    {
        found_end = found_start = false;
    }
    varchar_buff = NULL;
    return  ret;
}
int ObSqlReadStrategy::find_closed_column_range_simple_btw(bool real_val, const ObRowkeyColumn *column, int64_t column_idx, int64_t idx_of_ranges,/* int64_t cond_op, ObObj cond_val,*/ ObObj cond_start, ObObj cond_end, bool &found_start, bool &found_end, bool single_row_only)
{
    int ret = OB_SUCCESS;
    ObObj expected_type;
    ObObj start_promoted_obj;
    ObString start_string;
    ObString end_string;
    char *varchar_buff = NULL;
    const ObObj *p_start_promoted_obj = NULL;
    ObObj end_promoted_obj;
    const ObObj *p_end_promoted_obj = NULL;
    ObObjType start_source_type = cond_start.get_type();
    ObObjType end_source_type = cond_end.get_type();
    UNUSED(real_val);
    UNUSED(single_row_only);
    int64_t idx = idx_of_ranges*OB_MAX_ROWKEY_COLUMN_NUMBER + column_idx ;// 当前的位置
    bool need_store_space  = false ;
    ObObjType target_type ;
    // TBSYS_LOG(DEBUG, "column idx =%ld,range idx =%ld,obj start:%s,end:%s",column_idx, idx_of_ranges,to_cstring(mutiple_start_key_objs_[idx]), to_cstring(mutiple_end_key_objs_[idx]));
    ///
    if ( NULL == column )
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "column should not be null,ret=%d",ret);
    }
    else if ( idx < 0 || idx >= range_count_ * OB_MAX_ROWKEY_COLUMN_NUMBER)
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, " obj index of ranges out of range ,column idx =%ld,range idx =%ld,ret=%d",column_idx, idx,ret);
    }
    else
    {
        target_type = column->type_;
        expected_type.set_type(target_type);
        //step 1
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        if ((target_type == ObVarcharType && start_source_type != ObVarcharType)||(target_type == ObDecimalType))
            //modify:e
        {
            if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
            }
            else
            {
                need_store_space = true ;
                start_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                start_promoted_obj.set_varchar(start_string);
                          //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                              if(target_type == ObDecimalType)
                                  ret = obj_cast_for_rowkey(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj);
                              else
                                  ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj);
                              //add:e
                           //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                if (OB_SUCCESS != ret )
                    //modify:e
                {
                    TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                    ret = OB_ERR_UNEXPECTED;
                }
            }
        }
        else if (OB_SUCCESS != (ret = obj_cast(cond_start, expected_type, start_promoted_obj, p_start_promoted_obj)))
        {
            TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, start_source_type, target_type);
        }
        else
        {
            need_store_space = false ;
            // success do nothing
            //nop
        }
        //step2
        if ( OB_SUCCESS == ret )
        {
            if (mutiple_start_key_objs_[idx].is_min_value())
            {
                mutiple_start_key_objs_[idx] = *p_start_promoted_obj;
                found_start = true;
                if ( need_store_space )
                    mutiple_start_key_mem_hold_[idx] = varchar_buff;
            }
            else if (*p_start_promoted_obj > mutiple_start_key_objs_[idx])
            {
                found_start = true;
                mutiple_start_key_objs_[idx] = *p_start_promoted_obj;
                if (mutiple_start_key_mem_hold_[idx] != NULL)
                {
                    ob_free(mutiple_start_key_mem_hold_[idx]);
                    mutiple_start_key_mem_hold_[idx] = NULL;
                    if ( need_store_space )
                        mutiple_start_key_mem_hold_[idx] = varchar_buff;
                }
                else if (need_store_space)
                {
                    mutiple_start_key_mem_hold_[idx] = varchar_buff;
                }
            }
            else if ( need_store_space )
            {
                ob_free(varchar_buff);
                varchar_buff = NULL;
            }
        }
        else
        {
            TBSYS_LOG(WARN, "error, ret=%d", ret);
        }

        // step 3
        varchar_buff = NULL;
        need_store_space = false ;
        if ( OB_SUCCESS != ret )
        {
            TBSYS_LOG(WARN, "error, ret=%d", ret);
        }
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        else if ((target_type == ObVarcharType && end_source_type != ObVarcharType)||(target_type == ObDecimalType))
            //modify:e
        {
            if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
            }
            else
            {
                need_store_space = true ;
                end_string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                end_promoted_obj.set_varchar(end_string);
                          //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                                if(target_type == ObDecimalType)
                                    ret = obj_cast_for_rowkey(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj);
                                else
                                    ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj);
                                //add:e
                          //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                if (OB_SUCCESS != ret)
                    //modify:e
                {
                    TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                    ret = OB_ERR_UNEXPECTED;
                }
            }
        }
        else if (OB_SUCCESS != (ret = obj_cast(cond_end, expected_type, end_promoted_obj, p_end_promoted_obj)))
        {
            TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, end_source_type, target_type);
        }
        else
        {
            need_store_space = false ;
            // success do nothing
            //nop
        }
        //step 4
        if ( OB_SUCCESS == ret )
        {
            if (mutiple_end_key_objs_[idx].is_max_value())
            {
                mutiple_end_key_objs_[idx] = *p_end_promoted_obj;
                found_end = true;
                if ( need_store_space )
                    mutiple_end_key_mem_hold_[idx] = varchar_buff;
            }
            else if (*p_end_promoted_obj < mutiple_end_key_objs_[idx])
            {
                found_end = true;
                mutiple_end_key_objs_[idx] = *p_end_promoted_obj;
                if (mutiple_end_key_mem_hold_[idx] != NULL)
                {
                    ob_free(mutiple_end_key_mem_hold_[idx]);
                    mutiple_end_key_mem_hold_[idx] = NULL;
                    if (need_store_space)
                        mutiple_end_key_mem_hold_[idx] = varchar_buff;
                }
                else if (need_store_space)
                {
                    mutiple_end_key_mem_hold_[idx] = varchar_buff;
                }
            }
            else if (need_store_space)
            {
                ob_free(varchar_buff);
                varchar_buff = NULL;
            }
        }
        else
        {
            TBSYS_LOG(WARN, "error, ret=%d", ret);
        }
    }
    return  ret;
}
int ObSqlReadStrategy::find_closed_column_range_ex(bool real_val, int64_t idx, uint64_t column_id, bool &found_start, bool &found_end, bool single_row_only)
{
    int ret = OB_SUCCESS;
    int i = 0;
    uint64_t cond_cid = OB_INVALID_ID;
    int64_t cond_op = T_MIN_OP;
    ObObj cond_val;
    ObObj cond_start;
    ObObj cond_end;
    const ObRowkeyColumn *column = NULL;
    found_end = false;
    found_start = false;

    if ( NULL != rowkey_info_)
    {
        // 如果有IN， 这里我们先解析IN 中前缀条件，然后再解析 其他条件
        //
        if (simple_in_filter_list_.count() && !single_row_only )
        {
            ObArray<ObRowkey> rowkey_array;
            common::PageArena<ObObj,common::ModulePageAllocator> rowkey_objs_allocator(PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_READ_STATEGY));
            if ( NULL == (column = rowkey_info_->get_column(idx)))
            {
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN, "fail to get rowkey  from rowkey info ,ret=%d", ret);
            } else if (  column->column_id_ != column_id )
            {
                ret = OB_ERR_UNEXPECTED;
                TBSYS_LOG(WARN, "column_id do not match idx  ,ret=%d", ret);
            }
            else
            {
                for ( i = 0; i < simple_in_filter_list_.count() && ret == OB_SUCCESS; i++ )
                {
                    if ( simple_in_filter_list_.at(i).is_in_expr_with_ex_rowkey(real_val, *rowkey_info_, rowkey_array, rowkey_objs_allocator))
                    {
                        if ( 0 >= rowkey_array.count() )
                        {
                            TBSYS_LOG(WARN, "fail to get rowkey prefix info   from in expr  ,ret=%d", ret);
                            continue;
                        }
                        else if ( range_count_ <= 0  )
                        {
                            if ( (ret = malloc_rowkey_objs_space(rowkey_array.count())) != OB_SUCCESS )
                            {
                                TBSYS_LOG(ERROR, "fail to malloc space for rowkey ranges ret=%d", ret);
                                break;
                            }
                            else
                            {
                                //这里按照需要对部分进行初始化，第一个 range的start_key,end_key 最开始的时候已经完成了初始化
                                for ( int k = 0; k < rowkey_array.count() ; k++ )
                                {
                                    for ( int j = 0; j < rowkey_info_->get_size(); j++ )
                                    {
                                        (mutiple_start_key_objs_ + k * OB_MAX_ROWKEY_COLUMN_NUMBER + j)->set_min_value();
                                        (mutiple_end_key_objs_ + k * OB_MAX_ROWKEY_COLUMN_NUMBER + j)->set_max_value();

                                    }
                                }
                            }
                        }
                        break;
                    }
                }//end?for ( i = 0; i < simple_in_filter_list_.count() && ret == OB_SUCCESS; i++ )

                ////说明该主键仍然属于指定的主键前缀的内容,在多IN 条件中
                if ( OB_SUCCESS == ret && rowkey_array.count() > 0 && rowkey_array.at(0).get_obj_cnt() > idx )
                {
                    for ( int rowkey_i = 0;rowkey_i < rowkey_array.count() && ret == OB_SUCCESS; rowkey_i++ )
                    {
                        cond_val = *(rowkey_array.at(rowkey_i).ptr() + idx);
                        TBSYS_LOG(DEBUG, "column idx=%ld, row idx = %d,write position = %ld,val=%s,prefx rk size = %ld,row num = %ld,current range idx = %d",
                                  idx,rowkey_i,rowkey_i*OB_MAX_ROWKEY_COLUMN_NUMBER+idx,to_cstring(cond_val),
                                  rowkey_array.at(0).get_obj_cnt() ,rowkey_array.count(), rowkey_i );
                        // 这里只针对具体的一个主键列的具体一个值，idx表示在 rowkey info中 具体一列的 index
                        ret = resolve_close_column_range_in(real_val, column, cond_val, idx, rowkey_i ,found_start, found_end, single_row_only);
                        //						TBSYS_LOG(INFO, "range idx = %d,ret=%d", rowkey_i, ret);
                    }
                    // currently, we just handle the first in_expr,and ignore the others,so we just break after we analysis the first in expr successfully !
                    if (ret != OB_SUCCESS)
                    {
                        TBSYS_LOG(WARN, "resolve  in expr for ranges error, ret=%d", ret);
                    } else
                    {
                        TBSYS_LOG(DEBUG,
                                  "total in expr = %ld ,break at %d in expr = %s  ret=%d",
                                  simple_in_filter_list_.count(), i,
                                  to_cstring(simple_in_filter_list_.at(i)), ret);
                    }

                }

            }//end?if (simple_in_filter_list_.count())

            rowkey_objs_allocator.free();
        }
        //////
        if (range_count_  <= 0 )
        {
            // 重试10次,必须保证至少有一个range
            int  repeat = 10;
            while ( OB_SUCCESS != ( ret =  malloc_rowkey_objs_space(1)) && repeat-- )
            {
                TBSYS_LOG(WARN, "malloc  range space failed!,repeat = %d,ret =%d",repeat, ret);
            }
            if ( OB_SUCCESS == ret )
            {
                for ( i = 0; i < rowkey_info_->get_size(); i++)
                {
                    (mutiple_start_key_objs_+i)->set_min_value();
                    (mutiple_end_key_objs_+i)->set_max_value();
                }
                TBSYS_LOG(DEBUG, "malloc  range space successed!");
            }
            else
            {
                // 如果开辟失败了，需要增加处理代码，后序处理都会失败
                // do something....
            }
        }
        ///////
        if ( NULL == (column = rowkey_info_->get_column(idx)))
        {
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(WARN, "rowkey column is null,the parameter should be checked! cid=%ld,ret=%d", column_id, ret );
        }
        else
        {
            for (i = 0; i < simple_cond_filter_list_.count() && ret == OB_SUCCESS; i++)
            {
                if (simple_cond_filter_list_.at(i).is_simple_condition(real_val, cond_cid, cond_op, cond_val) && cond_cid == column_id )
                {
                    for ( int64_t range_index = 0; range_index < range_count_ && ret == OB_SUCCESS ;range_index ++)
                    {
                        if( (ret = find_closed_column_range_simple_con(real_val, column, idx, range_index, cond_op, cond_val, found_start, found_end, single_row_only)) != OB_SUCCESS)
                        {
                            TBSYS_LOG(WARN, "construct range = %ld,column = %ld  failed!", range_index, idx);
                        }
                    }
                }
                else if ((!single_row_only) && simple_cond_filter_list_.at(i).is_simple_between(real_val, cond_cid, cond_op, cond_start, cond_end)  && cond_cid == column_id)
                {
                    OB_ASSERT(T_OP_BTW == cond_op);
                    for ( int64_t range_index = 0; range_index <  range_count_ && ret == OB_SUCCESS ;range_index ++)
                    {
                        if( (ret = find_closed_column_range_simple_btw(real_val, column, idx, range_index, cond_start, cond_end, found_start, found_end, single_row_only)) != OB_SUCCESS)
                        {
                            TBSYS_LOG(WARN, "construct range = %ld,column = %ld  failed!",range_index, idx);
                        }
                    }
                }
            }
        }

    }
    else
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(WARN, "rowkey_info_ should not be null, ret = %d!",ret);
    }
    return ret;
}
int  ObSqlReadStrategy::resolve_close_column_range_in(bool real_val, const ObRowkeyColumn *column, ObObj cond_val, int64_t column_idx, int64_t idx_of_ranges, bool &found_start, bool &found_end, bool single_row_only)
{
    //return find_closed_column_range_simple_btw(real_val, column, column_idx, idx_of_ranges, cond_val, cond_val, found_start, found_end, single_row_only);
    UNUSED(real_val);
    UNUSED(single_row_only);
    int ret = OB_SUCCESS;
    ObObjType target_type ;
    ObObj expected_type;
    ObObj promoted_obj;
    const ObObj *p_promoted_obj = NULL;
    ObObjType source_type = cond_val.get_type();
    ObString string;
    char *varchar_buff = NULL;
    int64_t current_index = idx_of_ranges*OB_MAX_ROWKEY_COLUMN_NUMBER + column_idx ;// 当前的位置

    ////////////////
    bool need_store_space  = false ;
    if ( NULL == column )
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "column should not be null,ret=%d",ret);
    }
    else if ( current_index < 0 || current_index >= range_count_ * OB_MAX_ROWKEY_COLUMN_NUMBER)
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, " obj index of ranges out of range ,column idx =%ld,range idx =%ld,ret=%d",column_idx, current_index,ret);
    }
    else
    {
        target_type = column->type_;
        expected_type.set_type(target_type);
        ///
        //step 1
        //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
        if ((target_type == ObVarcharType && source_type != ObVarcharType)||(target_type == ObDecimalType))
            //modify:e
        {
            if (NULL == (varchar_buff = (char*)ob_malloc(OB_MAX_VARCHAR_LENGTH, ObModIds::OB_SQL_READ_STRATEGY)))
            {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                TBSYS_LOG(WARN, "ob_malloc %ld bytes failed, ret=%d", OB_MAX_VARCHAR_LENGTH, ret);
            }
            else
            {
                need_store_space = true ;
                string.assign_ptr(varchar_buff, OB_MAX_VARCHAR_LENGTH);
                promoted_obj.set_varchar(string);
                          //add fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                            if(target_type == ObDecimalType)
                                ret = obj_cast_for_rowkey(cond_val, expected_type, promoted_obj, p_promoted_obj);
                            else
                                ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj);
                            //add:e
                            //modify fanqiushi DECIMAL OceanBase_BankCommV0.3 2014_7_19:b
                if (OB_SUCCESS != ret )
                    //modify:e
                {
                    TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                    ret = OB_ERR_UNEXPECTED;
                }
            }
        }
        else if (OB_SUCCESS != (ret = obj_cast(cond_val, expected_type, promoted_obj, p_promoted_obj)))
        {
            TBSYS_LOG(WARN, "failed to cast object, ret=%d, from_type=%d to_type=%d", ret, source_type, target_type);
        }
        else
        {
            need_store_space = false ;
        }
        //step 2
        if ( OB_SUCCESS == ret )
        {
            found_start = found_end = true;
            if (mutiple_start_key_objs_[ current_index ] == mutiple_end_key_objs_[ current_index ])
            {
                TBSYS_LOG(WARN, "duplicate value for the same column, ret=%d", ret);
                if ( need_store_space )
                {
                    ob_free(varchar_buff);
                    varchar_buff = NULL;
                }
            }
            else
            {
                if ((mutiple_end_key_objs_ + current_index)->is_max_value())
                {
                }
                else if (mutiple_end_key_mem_hold_ [current_index] != NULL )
                {
                    ob_free(mutiple_end_key_mem_hold_ [ current_index]);
                    mutiple_end_key_mem_hold_ = NULL;
                }
                *(mutiple_end_key_objs_ + current_index) = *p_promoted_obj;
                if ( need_store_space )
                    mutiple_end_key_mem_hold_[current_index] = varchar_buff;


                if ((mutiple_start_key_objs_ + current_index)->is_min_value())
                {
                }
                else if ( mutiple_start_key_mem_hold_[ current_index] != NULL)
                {
                    ob_free(mutiple_start_key_mem_hold_ [ current_index ] );
                    mutiple_start_key_mem_hold_ [ current_index ] = NULL;
                }
                *(mutiple_start_key_objs_ + current_index) = *p_promoted_obj;
                if ( need_store_space )
                    mutiple_start_key_mem_hold_[current_index] = varchar_buff;
            }
        }
        else
        {
            TBSYS_LOG(WARN, "error, ret=%d", ret);
        }

        /////////////
    }
    return  ret;
}
//add wanglei [semi join in expr] 20170417:e

//add wanglei [semi join] 20170417:b
void ObSqlReadStrategy::reset_for_semi_join()
{
    simple_in_filter_list_.clear();
    simple_cond_filter_list_.clear();
    rowkey_info_ = NULL;
    memset(start_key_mem_hold_, 0, sizeof(start_key_mem_hold_));
    memset(end_key_mem_hold_, 0, sizeof(end_key_mem_hold_));
    release_rowkey_objs() ;
    idx_key_ = 0;
    range_count_ = 0;
    range_count_cons_ = 0;
}
void ObSqlReadStrategy::remove_last_inexpr()
{
    if(simple_in_filter_list_.count ()>0)
        simple_in_filter_list_.remove (simple_in_filter_list_.count ()-1);
}
void ObSqlReadStrategy::remove_last_expr()
{
    if(simple_cond_filter_list_.count ()>0)
        simple_cond_filter_list_.pop_back ();
}
//add xsl [semi join]
//ObSEArray::iterator<ObSqlExpression> ObSqlReadStrategy::get_simple_cond_filter_list_begin()
//{
//    simple_cond_filter_list_.begin();
//}
//ObSEArray::iterator<ObSqlExpression> ObSqlReadStrategy::get_simple_cond_filter_list_end()
//{
//    simple_cond_filter_list_.end();
//}
//add e
//add wanglei [semi join] 20170417:e
