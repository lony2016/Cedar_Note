/**
<<<<<<< HEAD
 * Copyright (C) 2013-2016 ECNU_DaSE.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_physical_plan.h
 * @brief physical plan class definition
 *
 * modified by zhutao
 *
 * @version __DaSE_VERSION
 * @author zhutao <zhutao@stu.ecnu.edu.cn>
 * @author wangdonghui <zjnuwangdonghui@163.com>
 *
 * @date 2016_07_27
 */

/**
=======
>>>>>>> refs/remotes/origin/master
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_physical_plan.cpp
 *
 * Authors:
 *   Guibin Du <tianguan.dgb@taobao.com>
 *
 */
#include "ob_physical_plan.h"
#include "common/utility.h"
#include "ob_table_rpc_scan.h"
#include "ob_mem_sstable_scan.h"
#include "common/serialization.h"
#include "ob_phy_operator_factory.h"
<<<<<<< HEAD
#include "ob_postfix_expression.h"  //add zt 20151109
=======

>>>>>>> refs/remotes/origin/master
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::common::serialization;
namespace oceanbase
{
  namespace sql
  {
    //uint64_t phycount=0;
    REGISTER_CREATOR(oceanbase::sql::ObPhyPlanGFactory, ObPhysicalPlan, ObPhysicalPlan, 0);
  } // end namespace sql
} // end namespace oceanbase


ObPhysicalPlan::ObPhysicalPlan()
  :curr_frozen_version_(OB_INVALID_VERSION),
   ts_timeout_us_(0),
   main_query_(NULL),
   pre_query_id_(common::OB_INVALID_ID),
   operators_store_(common::OB_MALLOC_BLOCK_SIZE, ModulePageAllocator(ObModIds::OB_SQL_PHY_PLAN)),
   allocator_(NULL),
   op_factory_(NULL),
   my_result_set_(NULL),
   start_trans_(false),
   in_ups_executor_(false),
   cons_from_assign_(false),
<<<<<<< HEAD
   next_phy_operator_id_(0),
   //add zt 20151109 :b
   group_exec_mode_(false),
   //add zt 20151109 :e
   //add by qx 21070317 :b
   long_trans_exec_mode_(false),
   //add :e
   //add lbzhong [auto_increment] 20161218:b
   auto_increment_(false)
   //add:e
=======
   next_phy_operator_id_(0)
>>>>>>> refs/remotes/origin/master
{
}

ObPhysicalPlan::~ObPhysicalPlan()
{
  clear();
}

int ObPhysicalPlan::deserialize_header(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &start_trans_)))
  {
    TBSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = start_trans_req_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = trans_id_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "failed to decode trans_id_, err=%d", ret);
  }
<<<<<<< HEAD
  //add by qx 20170313 :b
  else if ((OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &group_exec_mode_))))
  {
    TBSYS_LOG(WARN, "failed to decode group_exec_mode_, err=%d",
              ret);
  }
  else if ((OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &long_trans_exec_mode_))))
  {
    TBSYS_LOG(WARN, "failed to decode long_trans_exec_mode_, err=%d",
              ret);
  }
  //add :e
  //add lbzhong [auto_increment] 20161218:b
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &auto_increment_)))
  {
    TBSYS_LOG(WARN, "failed to decode auto_increment, err=%d", ret);
  }
  //add:e
=======
>>>>>>> refs/remotes/origin/master
  return ret;
}

int ObPhysicalPlan::add_phy_query(ObPhyOperator *phy_query, int32_t* idx, bool main_query)
{
  int ret = OB_SUCCESS;
<<<<<<< HEAD
  if ( (ret = phy_querys_.push_back(phy_query)) == OB_SUCCESS)//slwang note:from子查询先执行到这里，所以，都是嵌套的最里层的from子查询先加入到数组中，而最外层父查询是在数组最后一个位置
  {
      if (idx != NULL)
      {
          *idx = static_cast<int32_t>(phy_querys_.count() - 1);//slwang note: phy_querys_是存储子查询的,但也把主查询放到了数组中最后一个位置,但主查询的index是NULL,就算有子查询传值，但仍未改变主查询的index,
                                                               //所以刚插入的一个select_stmt查询物理计划下标数组位置是从当然是count-1，所以，让idx指向数组的最后一个子查询插入的位置
          TBSYS_LOG(INFO, "slwang note, idx != NULL is first excute?");//这条日志如果先出现的话，就代表子查询先完成gen_physical_select执行，即先完成物理计划的生成
      }

      //验证idx是否指向最后一个子查询的位置，而不是数组的最后一个位置？
      if(idx != NULL)
          TBSYS_LOG(INFO, "slwang note, last_subquery=%d", *idx);

      if (main_query)
      {
          if(idx != NULL)//这儿不会执行，也说明了main_query后执行，idx为空
              TBSYS_LOG(INFO, "slwang note, last_subquery=%d", *idx);
          main_query_ = phy_query;//slwang note:最外层的主查询同时存在main_query_
          //把from子查询优化去掉，检查phy_querys_数组大小？
          TBSYS_LOG(INFO, "slwang note,phy_querys_.count()=%ld", phy_querys_.count());
          //根据数组大小来推测，是不是也把主查询放到phy_querys_中了？
      }
=======
  if ( (ret = phy_querys_.push_back(phy_query)) == OB_SUCCESS)
  {
    if (idx != NULL)
      *idx = static_cast<int32_t>(phy_querys_.count() - 1);
    if (main_query)
      main_query_ = phy_query;
>>>>>>> refs/remotes/origin/master
  }
  return ret;
}

<<<<<<< HEAD
//slwang note: 只有physical_plan->set_pre_phy_query(get_cur_time_op)用到,用来把ObGetCurTimePhyOperator操作符压入物理计划
=======
>>>>>>> refs/remotes/origin/master
int ObPhysicalPlan::set_pre_phy_query(ObPhyOperator *phy_query, int32_t* idx)
{
  int ret = OB_SUCCESS;
  if ( (ret = phy_querys_.push_back(phy_query)) == OB_SUCCESS)
  {
    if (phy_query)
      pre_query_id_ = phy_query->get_id();
    if (idx != NULL)
    {
      *idx = static_cast<int32_t>(phy_querys_.count() - 1);
    }
  }
  return ret;
}

int ObPhysicalPlan::store_phy_operator(ObPhyOperator *op)
{
  op->set_id(++next_phy_operator_id_);
  return operators_store_.push_back(op);
}

ObPhyOperator* ObPhysicalPlan::get_phy_query(int32_t index) const
{
  ObPhyOperator *op = NULL;
  if (index >= 0 && index < phy_querys_.count())
    op = phy_querys_.at(index);
  return op;
}

ObPhyOperator* ObPhysicalPlan::get_phy_query_by_id(uint64_t id) const
{
  ObPhyOperator *op = NULL;
  for(int64_t i = 0; i < phy_querys_.count(); i++)
  {
    if (phy_querys_.at(i)->get_id() == id)
    {
      op = phy_querys_.at(i);
      break;
    }
  }
  return op;
}

ObPhyOperator* ObPhysicalPlan::get_phy_operator(int64_t index) const
{
  ObPhyOperator *op = NULL;
  if (index >= 0 && index < operators_store_.count())
    op = operators_store_.at(index);
  return op;
}

ObPhyOperator* ObPhysicalPlan::get_main_query() const
{
  return main_query_;
}

void ObPhysicalPlan::set_main_query(ObPhyOperator *query)
{
  main_query_ = query;
}

ObPhyOperator* ObPhysicalPlan::get_pre_query() const
{
  return get_phy_query_by_id(pre_query_id_);
}

int ObPhysicalPlan::remove_phy_query(ObPhyOperator *phy_query)
{
  int ret = OB_SUCCESS;
  UNUSED(phy_query);
  // if (OB_SUCCESS != (ret = phy_querys_.remove_if(phy_query)))
  // {
  //   TBSYS_LOG(WARN, "phy query not exist, phy_query=%p", phy_query);
  // }
  return ret;
}

int ObPhysicalPlan::remove_phy_query(int32_t index)
{
  int ret = OB_SUCCESS;
  UNUSED(index);
  if (OB_SUCCESS != (ret = phy_querys_.remove(index)))
  {
    TBSYS_LOG(WARN, "phy query not exist, index=%d", index);
  }
  return ret;
}

bool ObPhysicalPlan::is_terminate(int &ret) const
{
  bool bret = false;
  if (NULL != my_result_set_)
  {
    ObSQLSessionInfo *session = my_result_set_->get_session();
    if (NULL != session)
    {
      if (QUERY_KILLED == session->get_session_state())
      {
        bret = true;
        TBSYS_LOG(WARN, "query(%.*s) interrupted session id=%lu", session->get_current_query_string().length(),
                  session->get_current_query_string().ptr(), session->get_session_id());
        ret = OB_ERR_QUERY_INTERRUPTED;
      }
      else if (SESSION_KILLED == session->get_session_state())
      {
        bret = true;
        ret = OB_ERR_SESSION_INTERRUPTED;
      }
    }
    else
    {
      TBSYS_LOG(WARN, "can not get session_info for current query result set is %p", my_result_set_);
    }
  }
  return ret;
}

// must be unique table_id
int ObPhysicalPlan::add_base_table_version(int64_t table_id, int64_t version)
{
  ObTableVersion table_version;
  table_version.table_id_ = table_id;
  table_version.version_ = version;
  return table_store_.push_back(table_version);
}

int ObPhysicalPlan::add_base_table_version(const ObTableVersion table_version)
{
  return table_store_.push_back(table_version);
}

int ObPhysicalPlan::get_base_table_version(int64_t table_id, int64_t& version)
{
  int ret = OB_ERR_TABLE_UNKNOWN;
  for (int32_t i = 0; i < table_store_.count(); ++i)
  {
    ObTableVersion& table_version = table_store_.at(i);
    if (table_version.table_id_ == table_id)
    {
      ret = OB_SUCCESS;
      version = table_version.version_;
    }
  }
  return ret;
}

const ObPhysicalPlan::ObTableVersion& ObPhysicalPlan::get_base_table_version(int64_t index) const
{
  OB_ASSERT(0 <= index && index < table_store_.count());
  return table_store_.at(index);
}

int64_t ObPhysicalPlan::get_base_table_version_count()
{
  return table_store_.count();
}

bool ObPhysicalPlan::is_user_table_operation() const
{
  bool ret = true;
  for (int32_t i = 0; i < table_store_.count(); ++i)
  {
    const ObTableVersion& table_version = table_store_.at(i);
    if (IS_SYS_TABLE_ID(table_version.table_id_))
    {
      ret = false;
      break;
    }
  }
  return ret;
}

int64_t ObPhysicalPlan::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "PhysicalPlan(operators_num=%ld query_num=%ld "
                  "trans_id=%s start_trans=%c trans_req=%s)\n",
                  operators_store_.count(), phy_querys_.count(),
                  to_cstring(trans_id_), start_trans_?'Y':'N', to_cstring(start_trans_req_));
  for (int32_t i = 0; i < phy_querys_.count(); ++i)
  {
    if (main_query_ == phy_querys_.at(i))
      databuff_printf(buf, buf_len, pos, "====MainQuery====\n");
    else
      databuff_printf(buf, buf_len, pos, "====SubQuery%d====\n", i);
    int64_t pos2 = phy_querys_.at(i)->to_string(buf + pos, buf_len-pos);
    pos += pos2;
  }
  return pos;
}

int ObPhysicalPlan::deserialize_tree(const char *buf, int64_t data_len, int64_t &pos, ModuleArena &allocator,
                                     OperatorStore &operators_store, ObPhyOperator *&root)
{
  int ret = OB_SUCCESS;
  int32_t phy_operator_type = 0;
  if (NULL == op_factory_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(ERROR, "op_factory == NULL");
  }
  else if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &phy_operator_type)))
  {
    TBSYS_LOG(WARN, "fail to decode phy operator type:ret[%d]", ret);
  }
<<<<<<< HEAD
=======

>>>>>>> refs/remotes/origin/master
  if (OB_SUCCESS == ret)
  {
    root = op_factory_->get_one(static_cast<ObPhyOperatorType>(phy_operator_type), allocator);
    if (NULL == root)
    {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TBSYS_LOG(WARN, "get operator fail:type[%d]", phy_operator_type);
    }
<<<<<<< HEAD
    //add zt 20151111:b
    else
    {
      root->set_phy_plan(this);
    }
    //add zt 20151111:e
=======
>>>>>>> refs/remotes/origin/master
  }
  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = root->deserialize(buf, data_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to deserialize operator:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = operators_store.push_back(root)))
    {
      TBSYS_LOG(WARN, "fail to push operator to operators_store:ret[%d]", ret);
    }
  }

  if (OB_SUCCESS == ret)
  {
    if (root->get_type() <= PHY_INVALID || root->get_type() >= PHY_END)
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "invalid operator type:[%d]", root->get_type());
    }
  }

  if (OB_SUCCESS == ret)
  {
    for (int32_t i=0; OB_SUCCESS == ret && i<root->get_child_num(); i++)
    {
      ObPhyOperator *child = NULL;
      if (OB_SUCCESS != (ret = deserialize_tree(buf, data_len, pos, allocator, operators_store, child)))
      {
        TBSYS_LOG(WARN, "fail to deserialize tree:ret[%d]", ret);
      }
      else if (OB_SUCCESS != (ret = root->set_child(i, *child)))
      {
        TBSYS_LOG(WARN, "fail to set child:ret[%d]", ret);
      }
    }
  }
  return ret;
}

int ObPhysicalPlan::serialize_tree(char *buf, int64_t buf_len, int64_t &pos, const ObPhyOperator &root) const
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS == ret)
  {
    if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, root.get_type())))
    {
      TBSYS_LOG(WARN, "fail to encode op type:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = root.serialize(buf, buf_len, pos)))
    {
      TBSYS_LOG(WARN, "fail to serialize root:ret[%d] type=%d op=%s", ret, root.get_type(), to_cstring(root));
    }
    else
    {
      TBSYS_LOG(DEBUG, "serialize operator succ, type=%d", root.get_type());
    }
  }

<<<<<<< HEAD
  for (int64_t i=0;OB_SUCCESS == ret && i<root.get_child_num();i++)  //value
=======
  for (int64_t i=0;OB_SUCCESS == ret && i<root.get_child_num();i++)
>>>>>>> refs/remotes/origin/master
  {
    if (NULL != root.get_child(static_cast<int32_t>(i)) )
    {
      if (OB_SUCCESS != (ret = serialize_tree(buf, buf_len, pos, *(root.get_child(static_cast<int32_t>(i))))))
      {
        TBSYS_LOG(WARN, "fail to serialize tree:ret[%d]", ret);
      }
    }
    else
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "this operator should has child:type[%d]", root.get_type());
    }
  }
  return ret;
}

int ObPhysicalPlan::assign(const ObPhysicalPlan& other)
{
  int ret = OB_SUCCESS;
  if (this == &other)
  {
    // skip
  }
  else if (phy_querys_.count() > 0 || operators_store_.count() > 0)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "ObPhysicalPlan is not emptyo to assign, ret=%d", ret);
  }
  else
  {
    trans_id_ = other.trans_id_;
    curr_frozen_version_ = other.curr_frozen_version_;
    ts_timeout_us_ = other.ts_timeout_us_;
    main_query_ = NULL;
    pre_query_id_ = other.pre_query_id_;
    // already set before
    // allocator_;
    // op_factory_;
    // my_result_set_; // The result set who owns this physical plan
    start_trans_ = other.start_trans_;
    start_trans_req_ = other.start_trans_req_;
<<<<<<< HEAD
    //add by qx 20170313 :b
    group_exec_mode_ = other.group_exec_mode_;
    long_trans_exec_mode_ = other.long_trans_exec_mode_;
    //add :e
    //add lbzhong [auto_increment] 20161218:b
    auto_increment_ = other.auto_increment_;
    //add:e
=======
>>>>>>> refs/remotes/origin/master
    for (int32_t i = 0; i < other.phy_querys_.count(); ++i)
    {
      const ObPhyOperator *subquery = other.phy_querys_.at(i);
      bool is_main_query = (subquery == other.main_query_);
      ObPhyOperator *out_op = NULL;
      ret = create_and_assign_tree(subquery, is_main_query, true, out_op);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(WARN, "failed to assign tree, err=%d", ret);
        break;
      }
      TBSYS_LOG(DEBUG, "copy subquery=%d", i);
    }
    for (int32_t i = 0; ret == OB_SUCCESS && i < other.table_store_.count(); ++i)
    {
      if ((ret = table_store_.push_back(other.table_store_.at(i))) != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "Assign table version failed, err=%d, idx=%d", ret, i);
        break;
      }
    }
    cons_from_assign_ = true;
  }
  return ret;
}

int ObPhysicalPlan::create_and_assign_tree(
    const ObPhyOperator *other,
    bool main_query,
    bool is_query,
    ObPhyOperator *&out_op)
{
  int ret = OB_SUCCESS;
  ObPhyOperator *op = NULL;
  if (!other)
  {
    ret = OB_ERR_UNEXPECTED;
    TBSYS_LOG(WARN, "Operator to be assigned cna not be NULL, ret=%d", ret);
  }
  else if ((op = ObPhyOperator::alloc(other->get_type())) == NULL)
  {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TBSYS_LOG(WARN, "Create operator fail:type[%d]", other->get_type());
  }
  else if ((ret = operators_store_.push_back(op)) != OB_SUCCESS)
  {
    // should free op here
    ObPhyOperator::free(op);
    TBSYS_LOG(WARN, "Fail to push operator to operators_store:ret[%d]", ret);
  }
  else if (is_query && (ret = this->add_phy_query(op, NULL, main_query)) != OB_SUCCESS)
  {
    TBSYS_LOG(WARN, "Add operator to physical plan failed, ret=%d", ret);
  }
  else
  {
    op->set_phy_plan(this);
    op->set_id(other->get_id());
    out_op = op;

    if ((ret = op->assign(other)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Assign operator of physical plan failed, ret=%d", ret);
    }
    TBSYS_LOG(DEBUG, "assign operator, type=%s main_query=%c", ob_phy_operator_type_str(other->get_type()),
              main_query?'Y':'N');
  }
  for (int32_t i = 0; ret == OB_SUCCESS && i < other->get_child_num(); i++)
  {
    ObPhyOperator *child = NULL;
    if (!other->get_child(i))
    {
      ret = OB_ERR_GEN_PLAN;
      TBSYS_LOG(WARN, "Wrong physical plan, ret=%d", ret);
    }
    else if ((ret = create_and_assign_tree(other->get_child(i), false, false, child)) != OB_SUCCESS)
    {
      TBSYS_LOG(WARN, "Fail to create_and_assign tree:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = op->set_child(i, *child)))
    {
      TBSYS_LOG(WARN, "Fail to set child:ret[%d]", ret);
    }
  }
  return ret;
}

DEFINE_SERIALIZE(ObPhysicalPlan)
{
  int ret = OB_SUCCESS;
  // @todo yzf, support multiple queries
  int32_t main_query_idx = 0;
  // get current trans id
  OB_ASSERT(my_result_set_);
  common::ObTransID trans_id = my_result_set_->get_session()->get_trans_id();
  FILL_TRACE_LOG("trans_id=%s", to_cstring(trans_id));
<<<<<<< HEAD
//  TBSYS_LOG(INFO, "trans_id=%s", to_cstring(trans_id)); //add by zt for test purpose
=======
>>>>>>> refs/remotes/origin/master
  if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, start_trans_)))
  {
    TBSYS_LOG(WARN, "failed to serialize trans_id_, err=%d buf_len=%ld pos=%ld",
              ret, buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = start_trans_req_.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(WARN, "serialize error, buf_len=%ld pos=%ld", buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = trans_id.serialize(buf, buf_len, pos)))
  {
    TBSYS_LOG(ERROR, "trans_id.serialize(buf=%p[%ld-%ld])=>%d", buf, pos, buf_len, ret);
  }
<<<<<<< HEAD
  //add by qx 20170313 :b
  else if ((OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, group_exec_mode_))))
  {
    TBSYS_LOG(WARN, "failed to serialize group_exec_mode_, err=%d buf_len=%ld pos=%ld",
              ret, buf_len, pos);
  }
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, long_trans_exec_mode_)))
  {
    TBSYS_LOG(WARN, "failed to serialize long_trans_exec_mode_, err=%d buf_len=%ld pos=%ld",
              ret, buf_len, pos);
  }
  //add :e
  //add lbzhong [auto_increment] 20161218:b
  else if (OB_SUCCESS != (ret = serialization::encode_bool(buf, buf_len, pos, auto_increment_)))
  {
    TBSYS_LOG(WARN, "failed to serialize auto_increment, err=%d buf_len=%ld pos=%ld", ret, buf_len, pos);
  }
  //add:e
=======
>>>>>>> refs/remotes/origin/master
  else if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, main_query_idx)))
  {
    TBSYS_LOG(WARN, "fail to encode main query idx:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = encode_vi32(buf, buf_len, pos, 1)))
  {
    TBSYS_LOG(WARN, "fail to encode phy queryes size :ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = serialize_tree(buf, buf_len, pos, *main_query_)))
  {
    TBSYS_LOG(WARN, "fail to serialize tree:ret[%d]", ret);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObPhysicalPlan)
{
  int ret = OB_SUCCESS;
  int32_t main_query_idx = -1;
  int32_t phy_querys_size = 0;
  ObPhyOperator *root = NULL;
  clear();
  if (NULL == allocator_)
  {
    ret = OB_NOT_INIT;
    TBSYS_LOG(WARN, "allocator_ is not setted");
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &start_trans_)))
  {
    TBSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = start_trans_req_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(WARN, "failed to decode start_trans_, err=%d", ret);
  }
  else if (OB_SUCCESS != (ret = trans_id_.deserialize(buf, data_len, pos)))
  {
    TBSYS_LOG(ERROR, "trans_id.deserialize(buf=%p[%ld-%ld])=>%d", buf, pos, data_len, ret);
  }
<<<<<<< HEAD
  //add by qx 20170313 :b
  else if ((OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &group_exec_mode_))))
  {
    TBSYS_LOG(WARN, "failed to decode group_exec_mode_, err=%d",ret);
  }
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &long_trans_exec_mode_)))
  {
    TBSYS_LOG(WARN, "failed to decode long_trans_exec_mode_, err=%d",ret);
  }
  //add :e
  //add lbzhong [auto_increment] 20161218:b
  else if (OB_SUCCESS != (ret = serialization::decode_bool(buf, data_len, pos, &auto_increment_)))
  {
    TBSYS_LOG(WARN, "failed to decode auto_increment_, err=%d", ret);
  }
  //add:e
=======
>>>>>>> refs/remotes/origin/master
  else if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &main_query_idx)))
  {
    TBSYS_LOG(WARN, "fail to decode main query idx:ret[%d]", ret);
  }
  else if (OB_SUCCESS != (ret = decode_vi32(buf, data_len, pos, &phy_querys_size)))
  {
    TBSYS_LOG(WARN, "fail to decode phy querys size:ret[%d]", ret);
  }
<<<<<<< HEAD
=======

>>>>>>> refs/remotes/origin/master
  for (int32_t i=0;OB_SUCCESS == ret && i<phy_querys_size;i++)
  {
    if (OB_SUCCESS != (ret = deserialize_tree(buf, data_len, pos, *allocator_, operators_store_, root)))
    {
      TBSYS_LOG(WARN, "fail to deserialize_tree:ret[%d]", ret);
    }
    else if (OB_SUCCESS != (ret = phy_querys_.push_back(root)))
    {
      TBSYS_LOG(WARN, "fail to push item to phy querys:ret[%d]", ret);
    }
  }

  if (OB_LIKELY(OB_SUCCESS == ret))
  {
    if (OB_SUCCESS != phy_querys_.at(main_query_idx, main_query_))
    {
      ret = OB_ERR_UNEXPECTED;
      TBSYS_LOG(WARN, "fail to get main query:main_query_idx[%d], size[%ld]", main_query_idx, phy_querys_.count());
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObPhysicalPlan)
{
  int64_t size = 0;
  return size;
}
<<<<<<< HEAD
//add zt 20151109:b
//namespace oceanbase
//{
//  namespace sql
//  {
//    //add zt 20151109 :b
//    int ObPhysicalPlan::get_variable(ObPostfixExpression::ObPostExprNodeType type, const ObObj &expr_node, const ObObj *&val) const
//    {
//      int ret = OB_SUCCESS;
//      if( NULL != my_result_set_ ) //read from result_set
//      {
//        if (type == ObPostfixExpression::PARAM_IDX)
//        {
//          int64_t param_idx = OB_INVALID_INDEX;
//          if ((ret = expr_node.get_int(param_idx)) != OB_SUCCESS)
//          {
//            TBSYS_LOG(ERROR, "Can not get param index, ret=%d", ret);
//          }
//          else if (param_idx < 0 || param_idx >= my_result_set_->get_params().count())
//          {
//            ret = OB_ERR_ILLEGAL_INDEX;
//            TBSYS_LOG(ERROR, "Wrong index of question mark position, pos = %ld\n", param_idx);
//          }
//          else
//          {
//            val = my_result_set_->get_params().at(param_idx);
//          }
//        }
//        else if (type == ObPostfixExpression::SYSTEM_VAR || type == ObPostfixExpression::TEMP_VAR)
//        {
//          ObString var_name;
//          ObSQLSessionInfo *session_info = my_result_set_->get_session();
//          if (!session_info)
//          {
//            ret = OB_ERR_UNEXPECTED;
//            TBSYS_LOG(WARN, "Can not get session info.err=%d", ret);
//          }
//          else if ((ret = expr_node.get_varchar(var_name)) != OB_SUCCESS)
//          {
//            TBSYS_LOG(ERROR, "Can not get variable name");
//          }
//          else if (type == ObPostfixExpression::SYSTEM_VAR
//                   && (val = session_info->get_sys_variable_value(var_name)) == NULL)
//          {
//            ret = OB_ERR_VARIABLE_UNKNOWN;
//            TBSYS_LOG(USER_ERROR, "System variable %.*s does not exists", var_name.length(), var_name.ptr());
//          }
//          else if (type == ObPostfixExpression::TEMP_VAR
//                   && (val = session_info->get_variable_value(var_name)) == NULL)
//          {
//            ret = OB_ERR_VARIABLE_UNKNOWN;
//            TBSYS_LOG(USER_ERROR, "Variable %.*s does not exists", var_name.length(), var_name.ptr());
//          }
//        }
//        else if (type == ObPostfixExpression::CUR_TIME_OP)
//        {
//          if ((val = my_result_set_->get_cur_time_place()) == NULL)
//          {
//            ret = OB_ERR_UNEXPECTED;
//            TBSYS_LOG(WARN, "Can not get current time. err=%d", ret);
//          }
//        }
//      }
//      else if( main_query_->get_type() == PHY_PROCEDURE)//read from procedure
//      {

//      }
//      return ret;
//    }
//    //add zt 20151109 :e

//  }
//}
//add zt 20151109:e
=======
>>>>>>> refs/remotes/origin/master