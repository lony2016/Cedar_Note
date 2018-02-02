/**
 * Copyright (C) 2013-2016 DaSE .
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_rpc_scan.cpp
 * @brief 用于MS进行全表扫描
 *
 * modified by longfei：some rule for Query Optimization
 *
 * modify by guojinwei, bingo: support REPEATABLE-READ isolation
 * complete fill_read_param()
 *
 * @version CEDAR 0.2
 * @author longfei <longfei@stu.ecnu.edu.cn>
 *         guojinwei <guojinwei@stu.ecnu.edu.cn>
 *         bingo <bingxiao@stu.ecnu.edu.cn>
 * @date 2016_01_22
 *       2016_06_16
 */

/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_rpc_scan.cpp
 *
 * ObRpcScan operator
 *
 * Authors:
 *   Yu Huang <xiaochu.yh@taobao.com>
 *
 */
#include "ob_rpc_scan.h"
#include "common/ob_tsi_factory.h"
#include "common/ob_obj_cast.h"
#include "mergeserver/ob_merge_server_service.h"
#include "mergeserver/ob_merge_server_main.h"
#include "mergeserver/ob_ms_sql_get_request.h"
#include "ob_sql_read_strategy.h"
#include "ob_duplicate_indicator.h"
#include "common/ob_profile_type.h"
#include "common/ob_profile_log.h"
#include "mergeserver/ob_insert_cache.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::mergeserver;

ObRpcScan::ObRpcScan() :
    timeout_us_(0),
    sql_scan_request_(NULL),
    sql_get_request_(NULL),

    //index_sql_get_request_(NULL),
    // is_use_index(false),
    //index_table_id(OB_INVALID_ID),
    is_use_index_(false),
    is_use_index_for_storing_(false),
    main_table_id_(OB_INVALID_ID),
    get_next_row_count_(0),
    main_project(),
    main_filter_(),
    second_row_desc_(),

    scan_param_(NULL),
    get_param_(NULL),
    read_param_(NULL),
    is_scan_(false),
    is_get_(false),
    cache_proxy_(NULL),
    async_rpc_(NULL),
    session_info_(NULL),
    merge_service_(NULL),
    cur_row_(),
    cur_row_desc_(),
    table_id_(OB_INVALID_ID),
    base_table_id_(OB_INVALID_ID),
    start_key_buf_(NULL),
    end_key_buf_(NULL),
    is_rpc_failed_(false),
    need_cache_frozen_data_(false),
    cache_bloom_filter_(false),
    rowkey_not_exists_(false),
    insert_cache_iter_counter_(0),
    insert_cache_need_revert_(false),
    //add by wanglei [semi join] 20170417:b
    is_use_semi_join_(false),
    semi_join_left_table_row_(NULL),
    left_row_desc_(NULL),
    left_table_id_(OB_INVALID_INDEX),
    left_column_id_(OB_INVALID_INDEX),
    right_column_id_(OB_INVALID_INDEX),
    is_right_table_ (false),
    is_left_row_cache_complete_(false),
    has_next_row_(false),
    is_first_set_read_stra_(true),
    is_in_expr_empty_(false),
    is_first_cons_scan_param_(true),
    filter_expr_(NULL),
    src_filter_expr_(NULL),
    use_in_expr_(false),
    use_btw_expr_(false),
    use_multi_thread_(false),
    row_iter_(0),
    row_count_(0),
    temp_row_count_(0)
  //add by wanglei [semi join] 20170417:e
{
    sql_read_strategy_.set_rowkey_info(rowkey_info_);
    //add wanglei [semi join multi thread] 20170417:b
    task_queue_thread_ = NULL;
    is_finish_ = false;
    is_first_get_ = true;
    //add wanglei [semi join multi thread] 20170417:e
}


ObRpcScan::~ObRpcScan()
{
    this->destroy();
}

void ObRpcScan::reset()
{
    this->destroy();
    timeout_us_ = 0;
    read_param_ = NULL;
    get_param_ = NULL;
    scan_param_ = NULL;
    is_scan_ = false;
    is_get_ = false;
    cache_proxy_ = NULL;
    async_rpc_ = NULL;
    session_info_ = NULL;
    merge_service_ = NULL;
    table_id_ = OB_INVALID_ID;
    base_table_id_ = OB_INVALID_ID;
    start_key_buf_ = NULL;
    end_key_buf_ = NULL;
    is_rpc_failed_ = false;
    need_cache_frozen_data_ = false;
    cache_bloom_filter_ = false;
    rowkey_not_exists_ = false;
    frozen_data_.clear();
    cached_frozen_data_.reset();
    insert_cache_iter_counter_ = 0;
    cleanup_request();
    //cur_row_.reset(false, ObRow::DEFAULT_NULL);
    cur_row_desc_.reset();
    sql_read_strategy_.destroy();
    insert_cache_need_revert_ = false;

    is_use_index_=false;
    is_use_index_for_storing_=false;
    get_next_row_count_=0;
    main_table_id_=OB_INVALID_ID;
    main_project.reset();
    main_filter_.reset();
    second_row_desc_.reset();
    //add wanglei [semi join] 20170417:b
    is_use_semi_join_ = false;
    //add wanglei [semi join] 20170417:e
    //add wanglei [semi join multi thread] 20170417:b
    release_memory();
    //add wanglei [semi join multi thread] 20170417:e

}

void ObRpcScan::reuse()
{
    reset();
}

int ObRpcScan::init(ObSqlContext *context, const common::ObRpcScanHint *hint)
{
    int ret = OB_SUCCESS;
    if (NULL == context)
    {
        ret = OB_INVALID_ARGUMENT;
    }
    else if (NULL == context->cache_proxy_
             || NULL == context->async_rpc_
             || NULL == context->schema_manager_
             || NULL == context->merge_service_)
    {
        ret = OB_INVALID_ARGUMENT;
    }
    else if (base_table_id_ == OB_INVALID_ID)
    {
        TBSYS_LOG(WARN, "must set table_id_ first. table_id_=%ld", table_id_);
        ret = OB_NOT_INIT;
    }
    else
    {
        // init rowkey_info
        const ObTableSchema * schema = NULL;
        if (NULL == (schema = context->schema_manager_->get_table_schema(base_table_id_)))
        {
            TBSYS_LOG(WARN, "fail to get table schema. table_id[%ld]", base_table_id_);
            ret = OB_ERROR;
        }
        else
        {
            cache_proxy_ = context->cache_proxy_;
            async_rpc_ = context->async_rpc_;
            session_info_ = context->session_info_;
            merge_service_ = context->merge_service_;
            // copy
            rowkey_info_ = schema->get_rowkey_info();\
            //add wanglei [semi join multi thread] 20170417:b
            task_queue_thread_ = context->semi_join_task_queue_;
            //add wanglei [semi join multi thread] 20170417:e
        }
    }
    obj_row_not_exists_.set_ext(ObActionFlag::OP_ROW_DOES_NOT_EXIST);
    if (hint)
    {
        this->set_hint(*hint);
        if (hint_.read_method_ == ObSqlReadStrategy::USE_SCAN)
        {
            OB_ASSERT(NULL == scan_param_);
            scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
            if (NULL == scan_param_)
            {
                TBSYS_LOG(WARN, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            else
            {
                scan_param_->set_phy_plan(get_phy_plan());
                read_param_ = scan_param_;
            }
        }
        else if (hint_.read_method_ == ObSqlReadStrategy::USE_GET)
        {
            OB_ASSERT(NULL == get_param_);
            get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
            if (NULL == get_param_)
            {
                TBSYS_LOG(WARN, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            else
            {
                get_param_->set_phy_plan(get_phy_plan());
                read_param_ = get_param_;
            }
            if (OB_SUCCESS == ret && hint_.is_get_skip_empty_row_)
            {
                ObSqlExpression special_column;
                special_column.set_tid_cid(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
                if (OB_SUCCESS != (ret = ObSqlExpressionUtil::make_column_expr(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, special_column)))
                {
                    TBSYS_LOG(WARN, "fail to create column expression. ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = get_param_->add_output_column(special_column)))
                {
                    TBSYS_LOG(WARN, "fail to add special is-row-empty-column to project. ret=%d", ret);
                }
                else
                {
                    TBSYS_LOG(DEBUG, "add special column to read param");
                }
            }
        }
        else
        {
            ret = OB_INVALID_ARGUMENT;
            TBSYS_LOG(WARN, "read method must be either scan or get. method=%d", hint_.read_method_);
        }
    }
    return ret;
}

int ObRpcScan::cast_range(ObNewRange &range)
{
    int ret = OB_SUCCESS;
    bool need_buf = false;
    int64_t used_buf_len = 0;
    if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(rowkey_info_, range.start_key_, need_buf)))
    {
        TBSYS_LOG(WARN, "err=%d", ret);
    }
    else if (need_buf)
    {
        if (NULL == start_key_buf_)
        {
            start_key_buf_ = (char*)ob_malloc(OB_MAX_ROW_LENGTH, ObModIds::OB_SQL_RPC_SCAN);
        }
        if (NULL == start_key_buf_)
        {
            TBSYS_LOG(ERROR, "no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        if (OB_SUCCESS != (ret = ob_cast_rowkey(rowkey_info_, range.start_key_,
                                                start_key_buf_, OB_MAX_ROW_LENGTH, used_buf_len)))
        {
            TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
        }
    }
    if (OB_LIKELY(OB_SUCCESS == ret))
    {
        if (OB_SUCCESS != (ret = ob_cast_rowkey_need_buf(rowkey_info_, range.end_key_, need_buf)))
        {
            TBSYS_LOG(WARN, "err=%d", ret);
        }
        else if (need_buf)
        {
            if (NULL == end_key_buf_)
            {
                end_key_buf_ = (char*)ob_malloc(OB_MAX_ROW_LENGTH, ObModIds::OB_SQL_RPC_SCAN);
            }
            if (NULL == end_key_buf_)
            {
                TBSYS_LOG(ERROR, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }
        }
        if (OB_LIKELY(OB_SUCCESS == ret))
        {
            if (OB_SUCCESS != (ret = ob_cast_rowkey(rowkey_info_, range.end_key_,
                                                    end_key_buf_, OB_MAX_ROW_LENGTH, used_buf_len)))
            {
                TBSYS_LOG(WARN, "failed to cast rowkey, err=%d", ret);
            }
        }
    }
    return ret;
}

int ObRpcScan::create_scan_param(ObSqlScanParam &scan_param)
{
    int ret = OB_SUCCESS;
    ObNewRange range;
    // until all columns and filters are set, we could know the exact range
    //modify by longfei
    if(is_use_index_)
    {
        ret=fill_read_param_for_first_scan(scan_param);
    }
    else
    {
        ret = fill_read_param(scan_param);
    }

    if(OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = cons_scan_range(range)))
        {
            TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
        }
        // TODO: lide.wd 将range 深拷贝到ObSqlScanParam内部的buffer_pool_中
        else if (OB_SUCCESS != (ret = scan_param.set_range(range)))
        {
            TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
        }
    }
    //modify:e
    TBSYS_LOG(DEBUG, "scan_param=%s", to_cstring(scan_param));
    TBSYS_LOG(TRACE, "dump scan range: %s", to_cstring(range));
    return ret;
}
int ObRpcScan::create_scan_param_test(ObSqlScanParam &scan_param)
{
    int ret = OB_SUCCESS;
    ObNewRange range;
    // until all columns and filters are set, we could know the exact range
    //modify by longfei
    if(is_use_index_)
    {
        ret=fill_read_param_for_first_scan(scan_param);
    }
    else
    {
        ret = fill_read_param(scan_param);
    }

    if(OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = cons_scan_range_test(range)))
        {
            TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
        }
        // TODO: lide.wd 将range 深拷贝到ObSqlScanParam内部的buffer_pool_中
        if (OB_SUCCESS != (ret = scan_param.set_range(range)))
        {
            TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
        }
    }
    //modify:e
    TBSYS_LOG(DEBUG, "scan_param=%s", to_cstring(scan_param));
    TBSYS_LOG(TRACE, "dump scan range: %s", to_cstring(range));
    return ret;
}

int ObRpcScan::create_get_param(ObSqlGetParam &get_param)
{
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = fill_read_param(get_param)))
    {
        TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
    }

    if (OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = cons_get_rows(get_param)))
        {
            TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
        }
    }
    TBSYS_LOG(DEBUG, "get_param=%s", to_cstring(get_param));
    return ret;
}

int ObRpcScan::fill_read_param(ObSqlReadParam &dest_param)
{
    int ret = OB_SUCCESS;
    ObObj val;
    OB_ASSERT(NULL != session_info_);
    // add by guojinwei [repeatable read] 20160311:b
    if ( NULL != session_info_)
    {
        dest_param.set_trans_id(session_info_->get_trans_id());
    }
    else
    {
        ObTransID trans_id;
        dest_param.set_trans_id(trans_id);
    }
    // add:e
    if (OB_SUCCESS == ret)
    {
        dest_param.set_is_result_cached(true);
        if (OB_SUCCESS != (ret = dest_param.set_table_id(table_id_, base_table_id_)))
        {
            TBSYS_LOG(WARN, "fail to set table id and scan range. ret=%d", ret);
        }
    }

    return ret;
}

namespace oceanbase{
namespace sql{
REGISTER_PHY_OPERATOR(ObRpcScan, PHY_RPC_SCAN);
}
}

int64_t ObRpcScan::to_string(char* buf, const int64_t buf_len) const
{
    int64_t pos = 0;
    databuff_printf(buf, buf_len, pos, "RpcScan(");
    pos += hint_.to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, ", ");
    pos += read_param_->to_string(buf+pos, buf_len-pos);
    databuff_printf(buf, buf_len, pos, ")");
    //databuff_printf(buf, buf_len, pos, "RpcScan(row_desc=");
    //pos += cur_row_desc_.to_string(buf+pos, buf_len-pos);
    //databuff_printf(buf, buf_len, pos, ", ");
    //pos += sql_read_strategy_.to_string(buf+pos, buf_len-pos);
    //databuff_printf(buf, buf_len, pos, ", read_param=");
    //pos += hint_.to_string(buf+pos, buf_len-pos);
    //databuff_printf(buf, buf_len, pos, ", ");
    //pos += read_param_->to_string(buf+pos, buf_len-pos);
    //databuff_printf(buf, buf_len, pos, ")");
    return pos;
}

void ObRpcScan::set_hint(const common::ObRpcScanHint &hint)
{
    hint_ = hint;
    // max_parallel_count
    if (hint_.max_parallel_count <= 0)
    {
        hint_.max_parallel_count = 20;
    }
    // max_memory_limit
    if (hint_.max_memory_limit < 1024 * 1024 * 2)
    {
        hint_.max_memory_limit = 1024 * 1024 * 2;
    }
}

int ObRpcScan::open()
{
    int ret = OB_SUCCESS;
    if (NULL == (sql_scan_request_ = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().alloc()))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "alloc scan request from scan request pool failed, ret=%d", ret);
    }
    else if (NULL == (sql_get_request_ = ObMergeServerMain::get_instance()->get_merge_server().get_get_req_pool().alloc()))
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(WARN, "alloc get request from get request pool failed, ret=%d", ret);
    }
    else
    {
        sql_scan_request_->set_tablet_location_cache_proxy(cache_proxy_);
        sql_get_request_->set_tablet_location_cache_proxy(cache_proxy_);
        sql_scan_request_->set_merger_async_rpc_stub(async_rpc_);
        sql_get_request_->set_merger_async_rpc_stub(async_rpc_);
        if (NULL != session_info_)
        {
            ///  query timeout for sql level
            ObObj val;
            int64_t query_timeout = 0;
            if (OB_SUCCESS == session_info_->get_sys_variable_value(ObString::make_string(OB_QUERY_TIMEOUT_PARAM), val))
            {
                if (OB_SUCCESS != val.get_int(query_timeout))
                {
                    TBSYS_LOG(WARN, "fail to get query timeout from session, ret=%d", ret);
                    query_timeout = 0; // use default
                }
            }

            if (OB_APP_MIN_TABLE_ID > base_table_id_)
            {
                // internal table
                // BUGFIX: this timeout value should not be larger than the plan timeout
                // (see ob_transformer.cpp/int ObResultSet::open())
                // bug ref: http://bugfree.corp.taobao.com/bug/252871
                hint_.timeout_us = std::max(query_timeout, OB_DEFAULT_STMT_TIMEOUT); // prevent bad config, min timeout=3sec
            }
            else
            {
                // app table
                if (query_timeout > 0)
                {
                    hint_.timeout_us = query_timeout;
                }
                else
                {
                    // use default hint_.timeout_us, usually 10 sec
                }
            }
            TBSYS_LOG(DEBUG, "query timeout is %ld", hint_.timeout_us);
        }

        timeout_us_ = hint_.timeout_us;
        OB_ASSERT(my_phy_plan_);
        FILL_TRACE_LOG(" hint.read_consistency=%s ", get_consistency_level_str(hint_.read_consistency_));
        if (hint_.read_consistency_ == common::FROZEN)
        {
            ObVersion frozen_version = my_phy_plan_->get_curr_frozen_version();
            read_param_->set_data_version(frozen_version);
            FILL_TRACE_LOG("static_data_version = %s", to_cstring(frozen_version));
        }
        read_param_->set_is_only_static_data(hint_.read_consistency_ == STATIC);
        read_param_->set_is_read_consistency(hint_.read_consistency_ == STRONG);
        FILL_TRACE_LOG("only_static = %c", read_param_->get_is_only_static_data() ? 'Y' : 'N');

        if (NULL == cache_proxy_ || NULL == async_rpc_)
        {
            ret = OB_NOT_INIT;
        }

        if (OB_SUCCESS == ret)
        {
            TBSYS_LOG(DEBUG, "read_method_ [%s]", hint_.read_method_ == ObSqlReadStrategy::USE_SCAN ? "SCAN" : "GET");
            // common initialization
            cur_row_.set_row_desc(cur_row_desc_);//slwang note:cur_row_desc_是在gen_phy_table中table_scan_op->add_output_column设置好的
            //add longfei [secondary index select] 20151116 :b
            if(is_use_index_for_storing_)
            {
                cur_row_for_storing_.set_row_desc(cur_row_desc_for_storing);
            }
            //add:e
            FILL_TRACE_LOG("open %s", to_cstring(cur_row_desc_));
        }
        // update语句的need_cache_frozen_data_ 才会被设置为true
        ObFrozenDataCache & frozen_data_cache = ObMergeServerMain::
                get_instance()->get_merge_server().get_frozen_data_cache();
        if (frozen_data_cache.get_in_use() && need_cache_frozen_data_ && (hint_.read_consistency_ == FROZEN))
        {
            int64_t size = 0;
            if (OB_SUCCESS != (ret = read_param_->set_table_id(table_id_, base_table_id_)))
            {
                TBSYS_LOG(ERROR, "set_table_id error, ret: %d", ret);
            }
            else
            {
                size = read_param_->get_serialize_size();
            }
            if (OB_SUCCESS != ret)
            {}
            else if (OB_SUCCESS != (ret = frozen_data_key_buf_.alloc_buf(size)))
            {
                TBSYS_LOG(ERROR, "ObFrozenDataKeyBuf alloc_buf error, ret: %d", ret);
            }
            else if (OB_SUCCESS != (ret = read_param_->serialize(frozen_data_key_buf_.buf,
                                                                 frozen_data_key_buf_.buf_len, frozen_data_key_buf_.pos)))
            {
                TBSYS_LOG(ERROR, "ObSqlReadParam serialize error, ret: %d", ret);
            }
            else
            {
                frozen_data_key_.frozen_version = my_phy_plan_->get_curr_frozen_version();
                frozen_data_key_.param_buf = frozen_data_key_buf_.buf;
                frozen_data_key_.len = frozen_data_key_buf_.pos;
                ret = frozen_data_cache.get(frozen_data_key_, cached_frozen_data_);
                if (OB_SUCCESS != ret)
                {
                    TBSYS_LOG(ERROR, "ObFrozenDataCache get_frozen_data error, ret: %d", ret);
                }
                else if (cached_frozen_data_.has_data())
                {
                    OB_STAT_INC(SQL, SQL_UPDATE_CACHE_HIT);
                    cached_frozen_data_.set_row_desc(cur_row_desc_);
                }
                else
                {
                    OB_STAT_INC(SQL, SQL_UPDATE_CACHE_MISS);
                }
            }
        }

        if (!cached_frozen_data_.has_data())
        {
            // Scan
            if (OB_SUCCESS == ret && hint_.read_method_ == ObSqlReadStrategy::USE_SCAN)
            {
                is_scan_ = true;
                if (OB_SUCCESS != (ret = sql_scan_request_->initialize()))
                {
                    TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
                }
                else
                {
                    sql_scan_request_->alloc_request_id();
                    //del wanglei [semi join in expr] 20161131:b
                    //                    if (OB_SUCCESS != (ret = sql_scan_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN)))
                    //                    {
                    //                        TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                    //                    }
                    //del wanglei [semi join in expr] 20161131:e
                    //add wanglei [semi join in expr] 20161131:b
                    if (OB_SUCCESS != (ret = sql_scan_request_->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN, this)))//slwang note:this是把当前的rpc_scan对象传过去
                    {                                                                             //slwang note:ObModIds::OB_SQL_RPC_SCAN用于申请内存时用的，可能是用于内存泄露时方便查找问题
                        TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                    }
                    //add wanglei [semi join in expr] 20161131:e
                    else if (OB_SUCCESS != (ret = create_scan_param(*scan_param_)))
                    {
                        TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                    }
                }

                if (OB_SUCCESS == ret)
                {
                    sql_scan_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                    if(OB_SUCCESS != (ret = sql_scan_request_->set_request_param(*scan_param_, hint_)))
                    {
                        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                                  hint_.max_parallel_count, ret);
                    }
                }
            }
            // Get
            if (OB_SUCCESS == ret && hint_.read_method_ == ObSqlReadStrategy::USE_GET)
            {
                // insert and select
                is_get_ = true;
                get_row_desc_.reset();
                sql_get_request_->alloc_request_id();
                if (OB_SUCCESS != (ret = sql_get_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_GET)))
                {
                    TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                }
                else if (OB_SUCCESS != (ret = create_get_param(*get_param_)))
                {
                    TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                }
                else
                {
                    // 暂时只处理单行
                    // 只有insert语句的cache_bloom_filter_才会设置为true
                    ObInsertCache & insert_cache = ObMergeServerMain::get_instance()->get_merge_server().get_insert_cache();
                    if (insert_cache.get_in_use() && cache_bloom_filter_ && get_param_->get_row_size() == 1)
                    {
                        int err = OB_SUCCESS;
                        ObTabletLocationList loc_list;
                        loc_list.set_buffer(tablet_location_list_buf_);
                        const ObRowkey *rowkey = (*get_param_)[0];
                        if (OB_SUCCESS == (err = cache_proxy_->get_tablet_location(get_param_->get_table_id(), *rowkey, loc_list)))
                        {
                            const ObNewRange &tablet_range = loc_list.get_tablet_range();
                            InsertCacheKey key;
                            key.range_ = tablet_range;
                            int64_t version = my_phy_plan_->get_curr_frozen_version();
                            key.tablet_version_ = (reinterpret_cast<ObVersion*>(&version))->major_;
                            if (OB_SUCCESS == (err = insert_cache.get(key, value_)))
                            {
                                if (!value_.bf_->may_contain(*rowkey))
                                {
                                    OB_STAT_INC(SQL,SQL_INSERT_CACHE_HIT);
                                    // rowkey not exists
                                    rowkey_not_exists_ = true;
                                }
                                else
                                {
                                    OB_STAT_INC(SQL,SQL_INSERT_CACHE_MISS);
                                }
                                insert_cache_need_revert_ = true;
                            }
                            else if (OB_ENTRY_NOT_EXIST == err)
                            {
                                OB_STAT_INC(SQL,SQL_INSERT_CACHE_MISS);
                                // go on
                                char *buff = reinterpret_cast<char*>(ob_tc_malloc(
                                                                         sizeof(ObBloomFilterTask) + rowkey->get_deep_copy_size(),
                                                                         ObModIds::OB_MS_UPDATE_BLOOM_FILTER));
                                if (buff == NULL)
                                {
                                    err = OB_ALLOCATE_MEMORY_FAILED;
                                    TBSYS_LOG(ERROR, "malloc failed, ret=%d", err);
                                }
                                else
                                {
                                    ObBloomFilterTask *task = new (buff) ObBloomFilterTask();
                                    task->table_id = get_param_->get_table_id();
                                    char *obj_buf = buff + sizeof(ObBloomFilterTask);
                                    // deep copy task->key
                                    common::AdapterAllocator alloc;
                                    alloc.init(obj_buf);
                                    if (OB_SUCCESS != (err = rowkey->deep_copy(task->rowkey, alloc)))
                                    {
                                        TBSYS_LOG(ERROR, "deep copy rowkey failed, err=%d", err);
                                    }
                                    else
                                    {
                                        err = ObMergeServerMain::get_instance()->get_merge_server().get_bloom_filter_task_queue_thread().push(task);
                                        if (OB_SUCCESS != err && OB_TOO_MANY_BLOOM_FILTER_TASK != err)
                                        {
                                            TBSYS_LOG(ERROR, "push task to bloom_filter_task_queue failed,ret=%d", err);
                                        }
                                        else if (OB_TOO_MANY_BLOOM_FILTER_TASK == err)
                                        {
                                            TBSYS_LOG(DEBUG, "push task to bloom_filter_task_queue failed, ret=%d", err);
                                        }
                                        else
                                        {
                                            TBSYS_LOG(DEBUG, "PUSH TASK SUCCESS");
                                        }
                                    }
                                    if (OB_SUCCESS != err)
                                    {
                                        task->~ObBloomFilterTask();
                                        ob_tc_free(reinterpret_cast<void*>(task));
                                    }
                                }
                            }// OB_ENTRY_NOT_EXIST == err
                            else
                            {
                                //go on
                                TBSYS_LOG(ERROR, "get from insert cache failed, err=%d", err);
                            }
                        }
                        else
                        {
                            //go on
                            TBSYS_LOG(ERROR, "get tablet location failed, table_id=%lu,rowkey=%s, err=%d", get_param_->get_table_id(), to_cstring(*rowkey), err);
                        }
                    }
                }
                if (!rowkey_not_exists_)
                {
                    if (OB_SUCCESS != (ret = cons_row_desc(*get_param_, get_row_desc_)))
                    {
                        TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
                    }
                    else if (OB_SUCCESS != (ret = sql_get_request_->set_row_desc(get_row_desc_)))
                    {
                        TBSYS_LOG(WARN, "fail to set row desc:ret[%d]", ret);
                    }
                    else if(OB_SUCCESS != (ret = sql_get_request_->set_request_param(*get_param_, timeout_us_)))
                    {
                        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                                  hint_.max_parallel_count, ret);
                    }
                    if (OB_SUCCESS == ret)
                    {
                        sql_get_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                        if (OB_SUCCESS != (ret = sql_get_request_->open()))
                        {
                            TBSYS_LOG(WARN, "fail to open get request. ret=%d", ret);
                        }
                    }
                }
            }//Get
        }
        if (OB_SUCCESS != ret)
        {
            is_rpc_failed_ = true;
        }
    }
    //add wanglei [semi join] 20170417:b
    if(ret == OB_SUCCESS &&is_use_semi_join_)
    {
        if(is_use_index_for_storing_)
        {
            if(right_table_id_ != (int64_t)table_id_) right_table_id_ = table_id_;
        }
        else if(is_use_index_)
        { /*do nothing*/ }
        hint_.max_parallel_count = 500;
        const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator,
                ObArrayExpressionCallBack<ObSqlExpression> > &columns = read_param_->get_project ().get_output_columns();
        for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
        {
            const ObSqlExpression &expr = columns.at(i);
            if (OB_SUCCESS != (ret = project_raw.add_output_column (expr)))
            {
                TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
        }
        //add wanglei [semi join multi thread] 20170417:b
//        int64_t usage = 0;
//        usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION);
//        TBSYS_LOG(INFO,"wanglei:: ObRowStore before malloc usage = %ld",usage);
//        usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
//        TBSYS_LOG(INFO,"wanglei:: ObRpcScanTask and Filter before malloc  usage= %ld",usage);
        if(use_multi_thread_ && task_queue_thread_!=NULL)
        {
            if(row_count_<10000)
            {
                use_in_expr_ = true;
            }
            else if( row_count_>=1000 && row_count_<=20000)
            {
                task_queue_thread_->set_cache_proxy(cache_proxy_);
                task_queue_thread_->set_hint(&hint_);
                task_queue_thread_->set_mergeserver_service(merge_service_);
                task_queue_thread_->set_phy_plan(my_phy_plan_);
                task_queue_thread_->set_project(&project_raw);
                task_queue_thread_->set_rowkey_info(&rowkey_info_);
                task_queue_thread_->set_row_desc(&cur_row_desc_);
                task_queue_thread_->set_rpc_scan(this);
                task_queue_thread_->set_rpc_stub(async_rpc_);
                task_queue_thread_->set_timeout(timeout_us_);
                int task_count = 0;
                while(task_count < task_queue_thread_->MAX_TASK)
                {
                    //ObSqlExpression *in_filter = (ObSqlExpression*)ob_malloc(sizeof(ObSqlExpression),ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                    ObSqlExpression *in_filter = ObSqlExpression::alloc ();
                    //in_filter = new(in_filter) ObSqlExpression();
                    if(task_count==task_queue_thread_->MAX_TASK-1)   //?
                    {
                        ret = cons_filter_for_right_table_multi_thread(in_filter,row_count_%(task_queue_thread_->MAX_TASK-1));

                    }
                    else
                    {
                        ret = cons_filter_for_right_table_multi_thread(in_filter,row_count_/(task_queue_thread_->MAX_TASK-1));

                    }
                    if(ret == OB_SUCCESS)
                    {
                        ObRpcScanTask *rpc_task = (ObRpcScanTask*)ob_malloc(sizeof(ObRpcScanTask),ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                        rpc_task->filter_expr_ = in_filter;
                        ObRowStore * result_set_temp = (ObRowStore*)ob_malloc(sizeof(ObRowStore),ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION);
                        result_set_temp = new(result_set_temp) ObRowStore();
                        result_set_.push_back(result_set_temp);
                        rpc_task->intermediate_result_ = result_set_temp;
                        filter_list_.push_back(in_filter);
                        task_list_.push_back(rpc_task);  //rpc_task
                        task_queue_thread_->push(rpc_task);
                    }
                    task_count++;
                }
                task_queue_thread_->wake_up_all();
//                usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION);
//                TBSYS_LOG(INFO,"wanglei:: ObRowStore after malloc usage = %ld",usage);
//                usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
//                TBSYS_LOG(INFO,"wanglei:: ObRpcScanTask and Filter after malloc  usage= %ld",usage);
            }
            else
            {
                ret = OB_ERR_CAN_NOT_USE_SEMI_JOIN;//fix
            }
        }
        //add wanglei [semi join multi thread] 20170417:e
    }
    //add wanglei [semi join] 20170417:e
    return ret;
}

void ObRpcScan::destroy()
{
    sql_read_strategy_.destroy();
    if (NULL != start_key_buf_)
    {
        ob_free(start_key_buf_, ObModIds::OB_SQL_RPC_SCAN);
        start_key_buf_ = NULL;
    }
    if (NULL != end_key_buf_)
    {
        ob_free(end_key_buf_, ObModIds::OB_SQL_RPC_SCAN);
        end_key_buf_ = NULL;
    }
    if (NULL != get_param_)
    {
        get_param_->~ObSqlGetParam();
        ob_free(get_param_);
        get_param_ = NULL;
    }
    if (NULL != scan_param_)
    {
        scan_param_->~ObSqlScanParam();
        ob_free(scan_param_);
        scan_param_ = NULL;
    }
    cleanup_request();
}

int ObRpcScan::close()
{
    int ret = OB_SUCCESS;
    ObFrozenDataCache & frozen_data_cache = ObMergeServerMain::
            get_instance()->get_merge_server().get_frozen_data_cache();
    if (cached_frozen_data_.has_data())
    {
        ret = frozen_data_cache.revert(cached_frozen_data_);
        if (OB_SUCCESS != ret)
        {
            TBSYS_LOG(ERROR, "ObFrozenDataCache revert error, ret: %d", ret);
        }
    }
    else if (frozen_data_cache.get_in_use() && need_cache_frozen_data_ && (hint_.read_consistency_ == FROZEN))
    {
        if (!is_rpc_failed_)
        {
            int err = frozen_data_cache.put(frozen_data_key_, frozen_data_);
            if (OB_SUCCESS != err)
            {
                TBSYS_LOG(ERROR, "ObFrozenDataCache put error, err: %d", err);
            }
        }
        frozen_data_.reuse();
    }
    if (is_scan_|| is_use_semi_join_) //add wanglei {is_use_semi_join_}[semi join] 20170417
    {
        sql_scan_request_->close();
        sql_scan_request_->reset();
        if (NULL != scan_param_)
        {
            scan_param_->reset_local();
        }
        is_scan_ = false;
    }
    if (is_get_ || is_use_index_)
    {
        //因为当is_use_index_为true的时候，即使is_get_为false，我也会使用到sql_get_request_。所以这里要把它释放掉
        sql_get_request_->close();
        sql_get_request_->reset();
        if (NULL != get_param_)
        {
            get_param_->reset_local();
        }
        is_get_ = false;
        is_use_index_=false;
        tablet_location_list_buf_.reuse();
        rowkey_not_exists_ = false;
        if (insert_cache_need_revert_)
        {
            if (OB_SUCCESS == (ret = ObMergeServerMain::get_instance()->get_merge_server().get_insert_cache().revert(value_)))
            {
                value_.reset();
            }
            else
            {
                TBSYS_LOG(WARN, "revert bloom filter failed, ret=%d", ret);
            }
            insert_cache_need_revert_ = false;
        }
        //rowkeys_exists_.clear();
    }
    insert_cache_iter_counter_ = 0;
    cleanup_request();
    is_confilct_.clear();//add wanglei [semi join] 20170417
    //add wanglei [semi join multi thread] 20170417:b
    if(use_multi_thread_)
        release_memory();
    //add wanglei [semi join multi thread] 20170417:e
    return ret;
}
void ObRpcScan::cleanup_request()
{
    int tmp_ret = OB_SUCCESS;
    if (sql_scan_request_ != NULL)
    {
        if (OB_SUCCESS != (tmp_ret = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().free(sql_scan_request_)))
        {
            TBSYS_LOG(WARN, "free scan request back to scan req pool failed, ret=%d", tmp_ret);
        }
        sql_scan_request_ = NULL;
    }
    if (sql_get_request_ != NULL)
    {
        if (OB_SUCCESS != (tmp_ret = ObMergeServerMain::get_instance()->get_merge_server().get_get_req_pool().free(sql_get_request_)))
        {
            TBSYS_LOG(WARN, "free get request back to get req pool failed, ret=%d", tmp_ret);
        }
        sql_get_request_ = NULL;
    }
}

int ObRpcScan::get_row_desc(const common::ObRowDesc *&row_desc) const
{
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(base_table_id_ <= 0 || 0 >= cur_row_desc_.get_column_num()))
    {
        TBSYS_LOG(ERROR, "not init, tid=%lu, column_num=%ld", base_table_id_, cur_row_desc_.get_column_num());
        ret = OB_NOT_INIT;
    }
    //modify longfei
    /*else
  {
    row_desc = &cur_row_desc_;
  }*/
    else if(is_use_index_)
    {
        row_desc = &second_row_desc_;
    }
    else if(is_use_index_for_storing_)
    {
        row_desc = &cur_row_desc_for_storing;
    }
    else
    {
        row_desc = &cur_row_desc_;
    }
    //modify:e
    return ret;
}

int ObRpcScan::get_next_row(const common::ObRow *&row)
{
    int ret = OB_SUCCESS;
    if (rowkey_not_exists_)
    {
        if (insert_cache_iter_counter_ == 0)
        {
            row_not_exists_.set_row_desc(cur_row_desc_);
            int err = OB_SUCCESS;
            if (OB_SUCCESS != (err = row_not_exists_.set_cell(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID, obj_row_not_exists_)))
            {
                TBSYS_LOG(ERROR, "set cell failed, err=%d", err);
            }
            else
            {
                row = &row_not_exists_;
                insert_cache_iter_counter_ ++;
                ret = OB_SUCCESS;
            }
        }
        else if (insert_cache_iter_counter_ == 1)
        {
            ret = OB_ITER_END;
        }
    }
    else
    {
        if (cached_frozen_data_.has_data())
        {
            ret = cached_frozen_data_.get_next_row(row);
            if (OB_SUCCESS != ret && OB_ITER_END != ret)
            {
                TBSYS_LOG(ERROR, "ObCachedFrozenData get_next_row error, ret: %d", ret);
            }
        }
        else
        {
            if (ObSqlReadStrategy::USE_SCAN == hint_.read_method_)
            {
                ret = get_next_compact_row(row); // 可能需要等待CS返回
            }
            else if (ObSqlReadStrategy::USE_GET == hint_.read_method_)
            {
                ret = sql_get_request_->get_next_row(cur_row_);
            }
            else
            {
                TBSYS_LOG(WARN, "not init. read_method_=%d", hint_.read_method_);
                ret = OB_NOT_INIT;
            }
            //modify longfei
            //row = &cur_row_;    //old code
            if(!is_use_index_for_storing_)
            {
                row = &cur_row_;
            }
            //modify:e
            if (ObMergeServerMain::get_instance()->get_merge_server().get_frozen_data_cache().get_in_use()
                    && need_cache_frozen_data_ && (hint_.read_consistency_ == FROZEN) && OB_SUCCESS == ret)
            {
                int64_t cur_size_counter;
                ret = frozen_data_.add_row(*row, cur_size_counter);
                if (OB_SUCCESS != ret)
                {
                    TBSYS_LOG(ERROR, "ObRowStore add_row error, ret: %d", ret);
                }
            }
        }
    }
    if (OB_SUCCESS != ret && OB_ITER_END != ret)
    {
        is_rpc_failed_ = true;
    }
    return ret;
}


/**
 * 函数功能： 从scan_event中获取一行数据
 * 说明：
 * wait的功能：从finish_queue中阻塞地pop出一个事件（如果没有事件则阻塞）， 然后调用process_result()处理事件
 */
int ObRpcScan::get_next_compact_row(const common::ObRow *&row)
{
    int ret = OB_SUCCESS;
    if(!is_use_index_)   //如果不使用回表的索引，则按照原来的实现走
    {
        //add wanglei [semi join] 20170417:b
        if(is_use_semi_join_ && is_right_table_)
        {
            if(use_multi_thread_)
            {
                if(row_count_>= 10000 && row_count_<= 20000)
                    ret = get_table_row_with_more_scan_multi_thread(row);
                else
                {
                    if(row_count_>20000)
                    {
                        ret = OB_ERR_CAN_NOT_USE_SEMI_JOIN; //fix?
                    }
                    else
                    {
                        use_in_expr_ = true;
                        ret = get_table_row_with_more_scan (row);
                    }
                }
            }
            else
            {
                ret = get_table_row_with_more_scan (row);
            }

        }
        else
        {
            //add wanglei [semi join] 20170417:e
            bool can_break = false;
            int64_t remain_us = 0;
            row = NULL;
            do
            {
                if (OB_UNLIKELY(my_phy_plan_->is_timeout(&remain_us)))
                {
                    can_break = true;
                    ret = OB_PROCESS_TIMEOUT;
                }
                else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
                {
                    can_break = true;
                    TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
                }
                else if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request_->get_next_row(cur_row_))))
                {
                    // got a row without block,
                    // no need to check timeout, leave this work to upper layer
                    can_break = true;
                }
                else if (OB_ITER_END == ret && sql_scan_request_->is_finish())    //finish
                {
                    // finish all data
                    // can break;
                    can_break = true;
                }
                else if (OB_ITER_END == ret)
                {
                    // need to wait for incomming data
                    can_break = false;
                    timeout_us_ = std::min(timeout_us_, remain_us);
                    if( OB_SUCCESS != (ret = sql_scan_request_->wait_single_event(timeout_us_)))
                    {
                        if (timeout_us_ <= 0)
                        {
                            TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", timeout_us_);
                        }
                        can_break = true;
                    }
                    else
                    {
                        TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", timeout_us_);
                    }
                }
                else
                {
                    // encounter an unexpected error or
                    TBSYS_LOG(WARN, "Unexprected error. ret=%d, cur_row_desc[%s], read_method_[%d]", ret, to_cstring(cur_row_desc_), hint_.read_method_);
                    can_break = true;
                }
            }while(false == can_break);
            //add wanglei [semi join] 20170417:b
        }
        //add wanglei [semi join] 20170417:e
    }
    else
    {
        if(get_next_row_count_==0)   //只有在第一次get_next_row的时候
        {
            if (NULL != get_param_)   //重置下get_param_
            {
                get_param_->~ObSqlGetParam();
                ob_free(get_param_);
                get_param_ = NULL;
            }
            OB_ASSERT(NULL == get_param_);
            get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
            if (NULL == get_param_)
            {
                TBSYS_LOG(WARN, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            get_row_desc_.reset();
            sql_get_request_->alloc_request_id();  //为远程调用申请一个连接
            if (OB_SUCCESS != (ret = sql_get_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_GET)))
            {
                TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = create_get_param_for_index(*get_param_)))
            {
                //mod longfei 2016-03-25 14:32:29
                if(OB_ITER_END != ret)
                {
                    TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                }
                //TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                //mod e
            }
            else if (OB_SUCCESS != (ret = cons_index_row_desc(*get_param_, get_row_desc_)))
            {
                TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
            }
            else if (OB_SUCCESS != (ret = sql_get_request_->set_row_desc(get_row_desc_)))
            {
                TBSYS_LOG(WARN, "fail to set row desc:ret[%d]", ret);
            }
            else if(OB_SUCCESS != (ret = sql_get_request_->set_request_param(*get_param_, timeout_us_, hint_.max_parallel_count)))
            {
                TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                          hint_.max_parallel_count, ret);
            }
            if (OB_SUCCESS == ret)
            {
                sql_get_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                if (OB_SUCCESS != (ret = sql_get_request_->open()))
                {
                    TBSYS_LOG(WARN, "fail to open get request. ret=%d", ret);
                }
                else
                {
                    ret = sql_get_request_->get_next_row(cur_row_);
                    if(OB_SUCCESS == ret)
                        get_next_row_count_++;
                }
            }


        }
        else if(get_next_row_count_% 5375 == 0)
        {
            //add wanglei [semi join] 20170417:b
            //重置scan param
            read_param_->reset();
            scan_param_->set_is_result_cached(false);
            scan_param_->set_table_id (base_table_id_,base_table_id_);
            read_param_ = scan_param_;
            read_param_->reset_project_and_filter ();
            read_param_->set_project(index_project_);
            read_param_->set_filter(index_filter_);
            //add wanglei [semi join] 20170417:e
            if (NULL != get_param_)   //重置下get_param_
            {
                get_param_->~ObSqlGetParam();
                ob_free(get_param_);
                get_param_ = NULL;
            }
            OB_ASSERT(NULL == get_param_);
            get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
            if (NULL == get_param_)
            {
                TBSYS_LOG(WARN, "no memory");
                ret = OB_ALLOCATE_MEMORY_FAILED;
            }
            get_row_desc_.reset();
            sql_get_request_->close();
            sql_get_request_->reset();
            sql_get_request_->set_tablet_location_cache_proxy(cache_proxy_);
            sql_get_request_->set_merger_async_rpc_stub(async_rpc_);
            sql_get_request_->alloc_request_id();  //为远程调用申请一个连接
            if (OB_SUCCESS != (ret = sql_get_request_->init(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_GET)))
            {
                TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = create_get_param_for_index(*get_param_)))
            {
                TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
            }
            else if (OB_SUCCESS != (ret = cons_index_row_desc(*get_param_, get_row_desc_)))
            {
                TBSYS_LOG(WARN, "fail to get row desc:ret[%d]", ret);
            }
            else if (OB_SUCCESS != (ret = sql_get_request_->set_row_desc(get_row_desc_)))
            {
                TBSYS_LOG(WARN, "fail to set row desc:ret[%d]", ret);
            }
            else if(OB_SUCCESS != (ret = sql_get_request_->set_request_param(*get_param_, timeout_us_, hint_.max_parallel_count)))
            {
                TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                          hint_.max_parallel_count, ret);
            }
            if (OB_SUCCESS == ret)
            {
                sql_get_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                if (OB_SUCCESS != (ret = sql_get_request_->open()))
                {
                    TBSYS_LOG(WARN, "fail to open get request. ret=%d", ret);
                }
                else
                {
                    ret = sql_get_request_->get_next_row(cur_row_);
                    if(OB_SUCCESS == ret)
                        get_next_row_count_++;

                }
            }
        }
        else
        {
            ret = sql_get_request_->get_next_row(cur_row_);
            if(OB_SUCCESS == ret)
                get_next_row_count_++;
        }
    }

    if (OB_SUCCESS == ret)
    {
        if(is_use_index_for_storing_)
        {
            uint64_t tid=OB_INVALID_ID;
            uint64_t cid=OB_INVALID_ID;
            const ObObj *obj_tmp=NULL;
            for(int64_t i=0;i<cur_row_.get_column_num();i++)   //根据索引表的一行构造原表的一行
            {
                cur_row_desc_.get_tid_cid(i,tid,cid);
                if(tid == base_table_id_)
                {
                    cur_row_.raw_get_cell(i,obj_tmp);
                    cur_row_for_storing_.set_cell(main_table_id_,cid,*obj_tmp);
                }
                else
                {
                    cur_row_.raw_get_cell(i,obj_tmp);
                    cur_row_for_storing_.set_cell(tid,cid,*obj_tmp);
                }
            }
            row = &cur_row_for_storing_;

            // const  ObRowDesc *desc=cur_row_for_storing_.get_row_desc();
        }
        else
            row = &cur_row_;
    }
    return ret;
}

int ObRpcScan::set_main_tid(uint64_t main_tid)
{
    is_use_index_=true;
    get_next_row_count_=0;
    main_table_id_ = main_tid;
    return OB_SUCCESS;
}

//add dhc [query_optimizer] 20170727 :b
bool ObRpcScan::get_is_use_index() const
{
  return is_use_index_;
}

//add dhc  :e

int ObRpcScan::set_is_use_index_for_storing(uint64_t main_tid, common::ObRowDesc &row_desc)
{
    int ret=OB_SUCCESS;
    is_use_index_for_storing_ = true;
    //get_next_row_count_=0;
    main_table_id_ = main_tid;

    uint64_t table_id=OB_INVALID_ID;
    uint64_t column_id=OB_INVALID_ID;
    cur_row_desc_for_storing.reset();
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_desc.get_column_num(); i ++)
    {
        if (OB_SUCCESS != (ret = row_desc.get_tid_cid(i, table_id, column_id)))
        {
            TBSYS_LOG(WARN, "fail to get tid cid:ret[%d]", ret);
        }
        if (OB_SUCCESS == ret && OB_ACTION_FLAG_COLUMN_ID != column_id)
        {
            if (OB_SUCCESS != (ret = cur_row_desc_for_storing.add_column_desc(table_id, column_id)))
            {
                TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
        }
    }
    return ret;
}

int ObRpcScan::set_main_rowkey_info(const common::ObRowkeyInfo& rowkey_info)
{
    main_rowkey_info_ = rowkey_info;
    return OB_SUCCESS;
}

int ObRpcScan::set_second_rowdesc(common::ObRowDesc *row_desc)
{
    int ret=OB_SUCCESS;
    uint64_t table_id=OB_INVALID_ID;
    uint64_t column_id=OB_INVALID_ID;
    second_row_desc_.reset();
    for (int64_t i = 0; OB_SUCCESS == ret && i < row_desc->get_column_num(); i ++)
    {
        if (OB_SUCCESS != (ret = row_desc->get_tid_cid(i, table_id, column_id)))
        {
            TBSYS_LOG(WARN, "fail to get tid cid:ret[%d]", ret);
        }
        if (OB_SUCCESS == ret && OB_ACTION_FLAG_COLUMN_ID != column_id)
        {
            if (OB_SUCCESS != (ret = second_row_desc_.add_column_desc(table_id, column_id)))
            {
                TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
        }
    }
    return ret;
}

int ObRpcScan::get_other_row_desc(const common::ObRowDesc *&row_desc)
{
    row_desc = &get_row_desc_;
    return OB_SUCCESS;
}


int ObRpcScan::cons_get_rows_for_index(ObSqlGetParam &get_param)  //根据第一次scan索引表返回的数据，构造第二次get原表的主键的范围
{
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    get_rowkey_array_.clear();
    // TODO lide.wd: rowkey obj storage needed. varchar use orginal buffer, will be copied later
    PageArena<ObObj,ModulePageAllocator> rowkey_objs_allocator(
                PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_RPC_SCAN2));
    const ObRow *index_row_;
    ObObj *rowkey_objs = NULL;
    int32_t key_count = 0;
    while(OB_SUCCESS==(ret=get_next_compact_row_for_index(index_row_)))  //不停的获得第一次scan索引表的数据
    {
        const ObRowDesc * index_row_desc=index_row_->get_row_desc();
        ObRowkey rowkey;
        if (NULL != (rowkey_objs = rowkey_objs_allocator.alloc(main_rowkey_info_.get_size() * sizeof(ObObj))))
        {
            int64_t array_index=0;
            for(int64_t i=0;i<main_rowkey_info_.get_size();i++)
            {
                uint64_t main_cid=OB_INVALID_ID;
                uint64_t index_cid=OB_INVALID_ID;
                uint64_t index_tid=OB_INVALID_ID;

                const ObObj *obj_tmp=NULL;
                main_rowkey_info_.get_column_id(i,main_cid);
                for(int64_t j=0;j<index_row_->get_column_num();j++)
                {
                    index_row_desc->get_tid_cid(j,index_tid,index_cid);
                    if(index_cid==main_cid)
                    {
                        index_row_->raw_get_cell(j,obj_tmp);
                        rowkey_objs[array_index]=*obj_tmp;
                        array_index++;
                    }
                }
            }
            rowkey.assign(rowkey_objs,main_rowkey_info_.get_size());
            get_rowkey_array_.push_back(rowkey);  //构造原表的主键，存到get_rowkey_array_数组里
        }
        key_count++;
        if(key_count == 5375)
        {
            ret = OB_ITER_END;
            break;
        }
        //index_row_desc->
    }
    if(ret!=OB_ITER_END)
    {
        TBSYS_LOG(WARN,"faild to get index row to construct roweky,ret=%d",ret);
    }
    else
    {
        if (get_rowkey_array_.count() > 0)
        {
            for (idx = 0; idx < get_rowkey_array_.count(); idx++)
            {
                //深拷贝，从rowkey_objs_allocator 拷贝到了allocator_中
                if (OB_SUCCESS != (ret = get_param.add_rowkey(get_rowkey_array_.at(idx), true)))   //把原表的主键数组存到get_param里面
                {
                    TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
                    break;
                }
            }
            //  get_rowkey_array_.clear();
        }
    }
    rowkey_objs_allocator.free();
    return ret;
}

int ObRpcScan::add_main_output_column(const ObSqlExpression& expr)
{
    int ret=OB_SUCCESS;
    ret = main_project.add_output_column(expr);
    return ret;
}

int ObRpcScan::add_main_filter(ObSqlExpression* expr)
{
    int ret=OB_SUCCESS;
    expr->set_owner_op(this);
    ret = main_filter_.add_filter(expr);
    return ret;
}

int ObRpcScan::reset_read_param_for_index()  //重新设置一下read_param
//因为回表情况下，第一次scan索引表的时候已经设置了一个read_param。在第二次get原表的时候，要把这个read_param清空，再重新赋值。
{
    int ret = OB_SUCCESS;
    read_param_->reset_project_and_filter();
    read_param_->set_project(main_project);
    read_param_->set_filter(main_filter_);
    return ret;
}

int ObRpcScan::fill_read_param_for_index(ObSqlReadParam &dest_param)
{
    int ret = OB_SUCCESS;
    ObObj val;
    OB_ASSERT(NULL != session_info_);
    if (OB_SUCCESS == ret)
    {
        dest_param.set_is_result_cached(false);
        if (OB_SUCCESS != (ret = dest_param.set_table_id(table_id_, main_table_id_)))
        {
            TBSYS_LOG(WARN, "fail to set table id and scan range. ret=%d", ret);
        }
    }

    return ret;
}

int ObRpcScan::cons_index_row_desc(const ObSqlGetParam &sql_get_param, ObRowDesc &row_desc)  //生成第二次get原表时用到的行描述
{
    int ret = OB_SUCCESS;

    if (OB_SUCCESS == ret)
    {
        const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &columns = main_project.get_output_columns();
        for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
        {
            const ObSqlExpression &expr = columns.at(i);
            if (OB_SUCCESS != (ret = row_desc.add_column_desc(expr.get_table_id(), expr.get_column_id())))
            {
                TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
        }
    }

    if (OB_SUCCESS == ret)
    {
        if ( sql_get_param.get_row_size() <= 0 )
        {
            ret = OB_ERR_LACK_OF_ROWKEY_COL;
            TBSYS_LOG(WARN, "should has a least one row");
        }
        else
        {
            row_desc.set_rowkey_cell_count(sql_get_param[0]->length());
        }
    }

    return ret;
}
/**
 * 函数功能： 从scan_event中获取一行数据
 * 说明：
 * wait的功能：从finish_queue中阻塞地pop出一个事件（如果没有事件则阻塞）， 然后调用process_result()处理事件
 */
int ObRpcScan::get_next_compact_row_for_index(const common::ObRow *&row)  //第一次scan索引表，返回索引表的数据
{
    //const common::ObRowDesc *tmp_desc =cur_row_.get_row_desc();
    // char buf[1000];
    //tmp_desc->to_string(buf,1000);
    cur_row_.set_row_desc(cur_row_desc_);
    int ret = OB_SUCCESS;
    bool can_break = false;
    //add wanglei [semi join] 20170417:b
    if(is_use_semi_join_ && is_right_table_)
    {
        if(use_multi_thread_)
        {
            if(row_count_>=10000 && row_count_<=20000)
                ret = get_table_row_with_more_scan_multi_thread(row);
            else
            {
                if(row_count_>20000)
                {
                    ret = OB_ERR_CAN_NOT_USE_SEMI_JOIN; //fix?
                }
                else
                {
                    use_in_expr_ = true;
                    ret = get_table_row_with_more_scan (row);
                }
            }
        }
        else
        {
            ret = get_table_row_with_more_scan (row);
        }
    }
    else
        //add wanglei [semi join] 20170417:e
    {
        int64_t remain_us = 0;
        row = NULL;
        do
        {
            if (OB_UNLIKELY(my_phy_plan_->is_timeout(&remain_us)))
            {
                can_break = true;
                ret = OB_PROCESS_TIMEOUT;
            }
            else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
            {
                can_break = true;
                TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
            }
            else if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request_->get_next_row(cur_row_))))
            {
                // got a row without block,
                // no need to check timeout, leave this work to upper layer
                can_break = true;
            }
            else if (OB_ITER_END == ret && sql_scan_request_->is_finish())
            {
                // finish all data
                // can break;
                can_break = true;
            }
            else if (OB_ITER_END == ret)
            {
                // need to wait for incomming data
                can_break = false;
                timeout_us_ = std::min(timeout_us_, remain_us);
                if( OB_SUCCESS != (ret = sql_scan_request_->wait_single_event(timeout_us_)))
                {
                    if (timeout_us_ <= 0)
                    {
                        TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", timeout_us_);
                    }
                    can_break = true;
                }
                else
                {
                    TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", timeout_us_);
                }
            }
            else
            {
                // encounter an unexpected error or
                TBSYS_LOG(WARN, "Unexprected error. ret=%d, cur_row_desc[%s], read_method_[%d]", ret, to_cstring(cur_row_desc_), hint_.read_method_);
                can_break = true;
            }
        }while(false == can_break);
        if (OB_SUCCESS == ret)
        {
            row = &cur_row_;
        }
        //add wanglei [semi join] 20170417:b
    }
    //add wanglei [semi join] 20170417:e
    return ret;
}

//add longfei
int ObRpcScan::create_get_param_for_index(ObSqlGetParam &get_param) //生成第二次get原表时的get_param
{
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != (ret = cons_get_rows_for_index(get_param)))  //构造主键的范围
    {
        //mod longfei 2016-03-25 13:27:27
        if(OB_ITER_END != ret)
        {
            TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
        }
        //TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
        //mod e
    }
    if (OB_SUCCESS == ret)
    {

        read_param_->reset();
        //add longfei 2016-03-25 13:44:31
        read_param_->set_is_read_consistency(hint_.read_consistency_ == STRONG);
        //add e
        get_param.set_phy_plan(get_phy_plan());
        //read_param_ = get_param_;

        if (OB_SUCCESS != (ret = fill_read_param_for_index(get_param)))
        {
            TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
        }
        else
        {
            read_param_=&get_param;
        }
    }
    //mod longfei
    //mod longfei 2016-04-05 22:13:53
    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = reset_read_param_for_index()))
        //  if (OB_SUCCESS != (ret = reset_read_param_for_index()))
        //mod e
    {
        ret = OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "error unexpected. ret = %d", ret);
    }
    //  reset_read_param_for_index();
    //mod e
    return ret;
}

int ObRpcScan::fill_read_param_for_first_scan(ObSqlReadParam &dest_param)
{
    int ret = OB_SUCCESS;
    //ObObj val;
    OB_ASSERT(NULL != session_info_);
    if (OB_SUCCESS == ret)
    {
        dest_param.set_is_result_cached(true);
        if (OB_SUCCESS != (ret = dest_param.set_table_id(base_table_id_, base_table_id_)))
        {
            TBSYS_LOG(WARN, "fail to set table id and scan range. ret=%d", ret);
        }
    }

    return ret;
}
//add:e

int ObRpcScan::add_output_column(const ObSqlExpression& expr)//slwang note:此处主要是设置行描述用的
{
    int ret = OB_SUCCESS;
    bool is_cid = false;
    //if (table_id_ <= 0 || table_id_ != expr.get_table_id())
    if (base_table_id_ <= 0)
    {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "must call set_table() first. base_table_id_=%lu",
                  base_table_id_);
    }
    else if ((OB_SUCCESS == (ret = expr.is_column_index_expr(is_cid)))  && (true == is_cid))
    {
        // 添加基本列
        if (OB_SUCCESS != (ret = read_param_->add_output_column(expr)))
        {
            TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
        }
    }
    else
    {
        // 添加复合列
        if (OB_SUCCESS != (ret = read_param_->add_output_column(expr)))
        {
            TBSYS_LOG(WARN, "fail to add output column ret=%d", ret);
        }
    }
    // cons row desc
    if ((OB_SUCCESS == ret) && (OB_SUCCESS != (ret = cur_row_desc_.add_column_desc(expr.get_table_id(), expr.get_column_id()))))//slwang note:cur_row_desc_：设置行描述
    {
        TBSYS_LOG(WARN, "fail to add column to scan param. ret=%d, tid_=%lu, cid=%lu", ret, expr.get_table_id(), expr.get_column_id());
    }
    return ret;
}

int ObRpcScan::set_table(const uint64_t table_id, const uint64_t base_table_id)
{
    int ret = OB_SUCCESS;
    if (0 >= base_table_id)
    {
        TBSYS_LOG(WARN, "invalid table id: %lu", base_table_id);
        ret = OB_INVALID_ARGUMENT;
    }
    else
    {
        table_id_ = table_id;
        base_table_id_ = base_table_id;
    }
    return ret;
}

int ObRpcScan::cons_get_rows(ObSqlGetParam &get_param)
{
    int ret = OB_SUCCESS;
    int64_t idx = 0;
    get_rowkey_array_.clear();
    // TODO lide.wd: rowkey obj storage needed. varchar use orginal buffer, will be copied later
    PageArena<ObObj,ModulePageAllocator> rowkey_objs_allocator(
                PageArena<ObObj, ModulePageAllocator>::DEFAULT_PAGE_SIZE,ModulePageAllocator(ObModIds::OB_SQL_RPC_SCAN2));
    // try  'where (k1,k2,kn) in ((a,b,c), (e,f,g))'
    if (OB_SUCCESS != (ret = sql_read_strategy_.find_rowkeys_from_in_expr(true, get_rowkey_array_, rowkey_objs_allocator)))
    {
        TBSYS_LOG(WARN, "fail to find rowkeys in IN operator. ret=%d", ret);
    }
    else if (get_rowkey_array_.count() > 0)
    {
        ObDuplicateIndicator indicator;
        bool is_dup = false;
        if (get_rowkey_array_.count() > 1)
        {
            if ((ret = indicator.init(get_rowkey_array_.count())) != OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "Init ObDuplicateIndicator failed:ret[%d]", ret);
            }
        }
        for (idx = 0; idx < get_rowkey_array_.count(); idx++)
        {
            if (OB_UNLIKELY(get_rowkey_array_.count() > 1))
            {
                if (OB_SUCCESS != (ret = indicator.have_seen(get_rowkey_array_.at(idx), is_dup)))
                {
                    TBSYS_LOG(WARN, "Check duplication failed, err=%d", ret);
                    break;
                }
                else if (is_dup)
                {
                    continue;
                }
            }
            //深拷贝，从rowkey_objs_allocator 拷贝到了allocator_中
            if (OB_SUCCESS != (ret = get_param.add_rowkey(get_rowkey_array_.at(idx), true)))
            {
                TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
                break;
            }
        }
    }
    // try  'where k1=a and k2=b and kn=n', only one rowkey
    else if (OB_SUCCESS != (ret = sql_read_strategy_.find_rowkeys_from_equal_expr(true, get_rowkey_array_, rowkey_objs_allocator)))
    {
        TBSYS_LOG(WARN, "fail to find rowkeys from where equal condition, ret=%d", ret);
    }
    else if (get_rowkey_array_.count() > 0)
    {
        for (idx = 0; idx < get_rowkey_array_.count(); idx++)
        {
            if (OB_SUCCESS != (ret = get_param.add_rowkey(get_rowkey_array_.at(idx), true)))
            {
                TBSYS_LOG(WARN, "fail to add rowkey to get param. ret=%d", ret);
                break;
            }
        }
        OB_ASSERT(idx == 1);
    }
    rowkey_objs_allocator.free();
    return ret;
}

int ObRpcScan::cons_scan_range(ObNewRange &range)
{
    int ret = OB_SUCCESS;
    bool found = false;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = base_table_id_;
    OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    // range 指向sql_read_strategy_的空间

    bool has_next = false; //add wanglei [semi join in expr] 20161130
    //del wanglei [semi join in expr] 20161131:b
    //      if (OB_SUCCESS != (ret = sql_read_strategy_.find_scan_range(range, found, false)))
    //      {
    //        TBSYS_LOG(WARN, "fail to find range %lu", base_table_id_);
    //      }
    //del wanglei [semi join in expr] 20161131:e
    //add wanglei [semi join in expr] 20161130:b
    if (OB_SUCCESS != (ret = sql_read_strategy_.find_scan_range(found,false)))
    {
        TBSYS_LOG(WARN, "construct  range %lu failed", base_table_id_);
    }
    else if (OB_SUCCESS != (ret = sql_read_strategy_.get_next_scan_range(range,has_next)))
    {
        TBSYS_LOG(WARN, "fail to find   range %lu ", base_table_id_);
    }
    //add wanglei [semi join in expr] 20161130:e
    return ret;
}
int ObRpcScan::cons_scan_range_test(ObNewRange &range)
{
    int ret = OB_SUCCESS;
    bool found = false;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = base_table_id_;
    OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    // range 指向sql_read_strategy_的空间

    bool has_next = false; //add wanglei [semi join in expr] 20161130
    //del wanglei [semi join in expr] 20161131:b
    //      if (OB_SUCCESS != (ret = sql_read_strategy_.find_scan_range(range, found, false)))
    //      {
    //        TBSYS_LOG(WARN, "fail to find range %lu", base_table_id_);
    //      }
    //del wanglei [semi join in expr] 20161131:e
    //add wanglei [semi join in expr] 20161130:b
    if (OB_SUCCESS != (ret = sql_read_strategy_.find_scan_range(found,false)))
    {
        TBSYS_LOG(WARN, "construct  range %lu failed", base_table_id_);
    }
    else if (OB_SUCCESS != (ret = sql_read_strategy_.get_next_scan_range(range,has_next)))
    {
        TBSYS_LOG(WARN, "fail to find   range %lu ", base_table_id_);
    }
    //add wanglei [semi join in expr] 20161130:e
    return ret;
}

int ObRpcScan::get_min_max_rowkey(const ObArray<ObRowkey> &rowkey_array, ObObj *start_key_objs_, ObObj *end_key_objs_, int64_t rowkey_size)
{
    int ret = OB_SUCCESS;
    int64_t i = 0;
    if (1 == rowkey_array.count())
    {
        const ObRowkey &rowkey = rowkey_array.at(0);
        for (i = 0; i < rowkey_size && i < rowkey.get_obj_cnt(); i++)
        {
            start_key_objs_[i] = rowkey.ptr()[i];
            end_key_objs_[i] = rowkey.ptr()[i];
        }
        for ( ; i < rowkey_size; i++)
        {
            start_key_objs_[i] = ObRowkey::MIN_OBJECT;
            end_key_objs_[i] = ObRowkey::MAX_OBJECT;
        }
    }
    else
    {
        TBSYS_LOG(WARN, "only support single insert row for scan optimization. rowkey_array.count=%ld", rowkey_array.count());
        ret = OB_NOT_SUPPORTED;
    }
    return ret;
}

int ObRpcScan::add_filter(ObSqlExpression* expr)
{
    int ret = OB_SUCCESS;
    expr->set_owner_op(this);
    if (OB_SUCCESS != (ret = sql_read_strategy_.add_filter(*expr)))
    {
        TBSYS_LOG(WARN, "fail to add filter to sql read strategy:ret[%d]", ret);
    }
    if (OB_SUCCESS == ret && OB_SUCCESS != (ret = read_param_->add_filter(expr)))
    {
        TBSYS_LOG(WARN, "fail to add composite column to scan param. ret=%d", ret);
    }
    return ret;
}

int ObRpcScan::add_group_column(const uint64_t tid, const uint64_t cid)
{
    return read_param_->add_group_column(tid, cid);
}

int ObRpcScan::add_aggr_column(const ObSqlExpression& expr)
{
    int ret = OB_SUCCESS;
    if (cur_row_desc_.get_column_num() <= 0)
    {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "Output column(s) of ObRpcScan must be set first, ret=%d", ret);
    }
    else if ((ret = cur_row_desc_.add_column_desc(
                  expr.get_table_id(),
                  expr.get_column_id())) != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "Failed to add column desc, err=%d", ret);
    }
    else if ((ret = read_param_->add_aggr_column(expr)) != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "Failed to add aggregate column desc, err=%d", ret);
    }
    return ret;
}

int ObRpcScan::set_limit(const ObSqlExpression& limit, const ObSqlExpression& offset)
{
    return read_param_->set_limit(limit, offset);
}

int ObRpcScan::cons_row_desc(const ObSqlGetParam &sql_get_param, ObRowDesc &row_desc)
{
    int ret = OB_SUCCESS;
    if ( !sql_get_param.has_project() )
    {
        ret = OB_INVALID_ARGUMENT;
        TBSYS_LOG(WARN, "should has project");
    }

    if (OB_SUCCESS == ret)
    {
        const common::ObSEArray<ObSqlExpression, OB_PREALLOCATED_NUM, common::ModulePageAllocator, ObArrayExpressionCallBack<ObSqlExpression> > &columns = sql_get_param.get_project().get_output_columns();
        for (int64_t i = 0; OB_SUCCESS == ret && i < columns.count(); i ++)
        {
            const ObSqlExpression &expr = columns.at(i);
            if (OB_SUCCESS != (ret = row_desc.add_column_desc(expr.get_table_id(), expr.get_column_id())))
            {
                TBSYS_LOG(WARN, "fail to add column desc:ret[%d]", ret);
            }
        }
    }

    if (OB_SUCCESS == ret)
    {
        if ( sql_get_param.get_row_size() <= 0 )
        {
            ret = OB_ERR_LACK_OF_ROWKEY_COL;
            TBSYS_LOG(WARN, "should has a least one row");
        }
        else
        {
            row_desc.set_rowkey_cell_count(sql_get_param[0]->length());
        }
    }

    return ret;
}
//add wanglei [semi join in expr] 20170417:b
int ObRpcScan::init_next_scan_param_with_one_request(ObNewRange &range,
                                                     ObSqlScanParam *scan_param,
                                                     mergeserver::ObMsSqlScanRequest *sql_scan_request
                                                     )
{
    int ret = OB_SUCCESS;
    //ObNewRange range;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = base_table_id_;
    OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    // until all columns and filters are set, we could know the exact range
    // modify by fanqiushi_index
    if(is_use_index_)
    {
        ret=fill_read_param_for_first_scan(*scan_param);
    }
    else
    {
        ret = fill_read_param(*scan_param);
    }
    if(OB_SUCCESS != ret)
    {
        TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
    }
    //modify:e
    else if (OB_SUCCESS != (ret = scan_param->set_range(range,false)))
    {
        TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = sql_scan_request->set_request_param_only(*scan_param,hint_)))
    {
        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",hint_.max_parallel_count, ret);
    }
    TBSYS_LOG(TRACE, "dump scan range: %s", to_cstring(range));
    return ret;
}
int ObRpcScan::init_next_scan_param(ObNewRange &range)
{
    int ret = OB_SUCCESS;
    //ObNewRange range;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = base_table_id_;
    OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    // until all columns and filters are set, we could know the exact range
    // modify by fanqiushi_index
    if(is_use_index_)
    {
        ret=fill_read_param_for_first_scan(*scan_param_);
    }
    else
    {
        ret = fill_read_param(*scan_param_);
    }
    if(OB_SUCCESS != ret)
    {
        TBSYS_LOG(WARN, "fail to fill read param to scan param. ret=%d", ret);
    }
    //modify:e
    else if (OB_SUCCESS != (ret = scan_param_->set_range(range,false)))
    {
        TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = sql_scan_request_->set_request_param_only(*scan_param_,hint_)))
    {
        TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",hint_.max_parallel_count, ret);
    }
    TBSYS_LOG(TRACE, "dump scan range: %s", to_cstring(range));
    return ret;
}
//add wanglei [semi join in expr] 20170417:e

//add wanglei [semi join] 20170417:b
int ObRpcScan::set_semi_join_left_table_row(common::ObRowStore *semi_join_left_table_row)
{
    int ret = OB_SUCCESS;
    semi_join_left_table_row_ = semi_join_left_table_row;
    return ret;
}
int ObRpcScan::set_is_use_semi_join(bool is_use_semi_join)
{
    int ret = OB_SUCCESS;
    is_use_semi_join_ =is_use_semi_join;
    return ret;
}
int ObRpcScan::set_left_table_id_column_id(int64_t table_id,int64_t column_id,int64_t right_column_id,int64_t right_table_id)
{
    int ret = OB_SUCCESS;
    left_table_id_ = table_id;
    left_column_id_ = column_id;
    right_column_id_ = right_column_id;
    right_table_id_ = right_table_id;
    return ret;
}
int ObRpcScan::set_is_right_table(bool flag)
{
    is_right_table_ = flag;
    return OB_SUCCESS;
}
void ObRpcScan::set_use_in(bool value)   //use in expr
{
    use_in_expr_ = value;
}
void ObRpcScan::set_use_btw(bool value)
{
    use_btw_expr_ = value;
}
void ObRpcScan::set_use_multi_thread(bool value)
{
    use_multi_thread_ = value;
}
int ObRpcScan::get_data_for_distinct(const common::ObObj *cell,
                                     ObObjType type,
                                     char *buff,
                                     int buff_size)
{
    int ret = OB_SUCCESS;
    if(cell == NULL)
    {
        ret = OB_ERR_POINTER_IS_NULL;
    }
    else
    {
        memset(buff,'\0',buff_size);
        int64_t int_val = 0;
        float float_val = 0.0;
        double double_val = 0.0;
        bool is_add = false;
        ObString str_val;
        switch (type) {
        case ObNullType:
            break;
        case ObIntType:
            cell->get_int(int_val,is_add);
            sprintf(buff,"%ld",int_val);
            break;
        case ObVarcharType:
            cell->get_varchar(str_val);
            snprintf(buff, buff_size, "%.*s", str_val.length(), str_val.ptr());
            break;
        case ObFloatType:
            cell->get_float(float_val,is_add);
            sprintf(buff,"%f",float_val);
            break;
        case ObDoubleType:
            cell->get_double(double_val,is_add);
            sprintf(buff,"%.12lf",double_val);
            break;
        case ObDateTimeType:
            cell->get_datetime(int_val,is_add);
            sprintf(buff,"%ld",int_val);
            break;
        case ObPreciseDateTimeType:
            cell->get_precise_datetime(int_val,is_add);
            sprintf(buff,"%ld",int_val);
            break;
        case ObSeqType:
            break;
        case ObCreateTimeType:
            cell->get_createtime(int_val);
            sprintf(buff,"%ld",int_val);
            break;
        case ObModifyTimeType:
            cell->get_modifytime(int_val);
            sprintf(buff,"%ld",int_val);
            break;
        case ObExtendType:
            cell->get_ext(int_val);
            if (common::ObObj::MIN_OBJECT_VALUE == int_val)
            {
            }
            else if (common::ObObj::MAX_OBJECT_VALUE == int_val)
            {
            }
            else
            {
            }
            break;
        case ObBoolType:
            break;
        case ObDecimalType:
        {
            ObNumber num;
            cell->get_decimal(num);
            num.to_string(buff,buff_size);
            break;
        }
        default:
            break;
        }
    }
    return ret;
}
int ObRpcScan::set_semi_join_left_row_desc(const ObRowDesc *row_desc)
{
    int ret = OB_SUCCESS;
    left_row_desc_ = row_desc;
    return ret;
}
int ObRpcScan::cons_filter_for_right_table(ObSqlExpression *&table_filter_expr_,ObSqlExpression *&src_table_filter_expr_)
{
    UNUSED(src_table_filter_expr_);
    int ret = OB_SUCCESS;
    if(use_btw_expr_)
    {
        ExprItem dem1,dem2;
        dem1.type_ = T_REF_COLUMN;
        //后缀表达式组建
        table_filter_expr_->set_tid_cid(right_table_id_,right_column_id_);
        dem1.value_.cell_.tid = right_table_id_;
        dem1.value_.cell_.cid = right_column_id_;
        table_filter_expr_->add_expr_item(dem1);
        ObRow last_row;
        last_row.set_row_desc (*left_row_desc_);
        //between表达式的最小值
        ObRow row_temp;
        row_temp.set_row_desc (*left_row_desc_);
        ret = semi_join_left_table_row_->get_next_row(row_temp);
        if(ret == OB_SUCCESS)
        {
            if(ret == OB_SUCCESS)
            {
                last_row = row_temp;
                TBSYS_LOG(DEBUG,"[semi join]::first row = %s",to_cstring(row_temp));
                const common::ObObj *cell_temp=NULL;
                if(OB_SUCCESS!=(ret=row_temp.get_cell(left_table_id_,left_column_id_,cell_temp)))
                {
                    TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                }
                else
                {
                    ObConstRawExpr     col_val;
                    if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                    {
                        TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                    }
                    else
                    {
                        if ((ret = col_val.fill_sql_expression(
                                 *table_filter_expr_)) != OB_SUCCESS)
                        {
                            TBSYS_LOG(ERROR,"Add cell value failed");
                        }
                    }
                }
            }
            //between表达式的最大值
            ObRow current_row;
            current_row.set_row_desc (*left_row_desc_);
            while(OB_SUCCESS == (ret = semi_join_left_table_row_->get_next_row(current_row)))
            {
                const common::ObObj *cell_temp=NULL;
                if(OB_SUCCESS!=(ret=current_row.get_cell(left_table_id_,left_column_id_,cell_temp)))
                {
                    TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                    break;
                }
                else
                {
                    char buff[65536];
                    if(OB_SUCCESS!=(ret=get_data_for_distinct(cell_temp,
                                                              cell_temp->get_type(),
                                                              buff,
                                                              sizeof(buff))))
                    {
                        TBSYS_LOG(WARN, "get data faliure , ret=[%d]", ret);
                        break;
                    }
                    else
                    {
                        if(strcmp (buff,"")==0)
                        {
                            ret = OB_ITER_END;
                            break;
                        }
                        else
                        {
                            last_row = current_row;
                        }
                        //string str_temp(buff);
                        TBSYS_LOG(DEBUG, "[semi join]::str_temp  =%s", buff);
                    }
                }

            }
            if(ret == OB_ITER_END)
            {
                TBSYS_LOG(DEBUG,"[semi join]::last_row = %s",to_cstring(last_row));
                ret = OB_SUCCESS;
                const common::ObObj *cell_temp=NULL;
                if(OB_SUCCESS!=(ret=last_row.get_cell(left_table_id_,left_column_id_,cell_temp)))
                {
                    TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                }
                else
                {
                    ObConstRawExpr     col_val;
                    if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                    {
                        TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                    }
                    else
                    {
                        if ((ret = col_val.fill_sql_expression(
                                 *table_filter_expr_)) != OB_SUCCESS)
                        {
                            TBSYS_LOG(ERROR,"Add cell value failed");
                        }
                    }
                }
            }
            if(ret == OB_SUCCESS)
            {
                dem2.type_ = T_OP_BTW;
                dem2.data_type_ = ObMinType;
                dem2.value_.int_ = 3;
                table_filter_expr_->add_expr_item(dem2);
                table_filter_expr_->add_expr_item_end();
                TBSYS_LOG(DEBUG,"[semi join]::table_filter_expr_ = %s",to_cstring(*table_filter_expr_));
                is_in_expr_empty_ = false;
                is_left_row_cache_complete_ = true;
            }
        }
        else
        {
            if(ret == OB_ITER_END)
            {
                is_in_expr_empty_ = true;
                is_left_row_cache_complete_ = true;
            }
        }
    }
    else if(use_in_expr_)
    {
        int limit_num = 100;
        int index_param_count = 0;
        if(OB_SUCCESS == ret)
        {
            ExprItem dem1,dem2,dem3,dem5,dem6,dem7;
            dem1.type_ = T_REF_COLUMN;
            //后缀表达式组建
            table_filter_expr_->set_tid_cid(right_table_id_,right_column_id_);
            dem1.value_.cell_.tid = right_table_id_;
            dem1.value_.cell_.cid = right_column_id_;
            table_filter_expr_->add_expr_item(dem1);
            dem2.type_ = T_OP_ROW;
            dem2.data_type_ = ObMinType;
            dem2.value_.int_ = 1;
            table_filter_expr_->add_expr_item(dem2);
            dem3.type_ = T_OP_LEFT_PARAM_END;
            dem3.data_type_ = ObMinType;
            dem3.value_.int_ = 2;
            table_filter_expr_->add_expr_item(dem3);
            ObRow row_temp;
            row_temp.set_row_desc (*left_row_desc_);
            while(OB_SUCCESS == (ret = semi_join_left_table_row_->get_next_row(row_temp)))
            {
                if(OB_SUCCESS!=ret)
                    TBSYS_LOG(WARN, "no more rows , sret=[%d]", ret);
                else
                {
                    TBSYS_LOG(DEBUG,"[semi join]::row_temp = %s",to_cstring(row_temp));
                    const common::ObObj *cell_temp=NULL;
                    if(OB_SUCCESS!=(ret=row_temp.get_cell(left_table_id_,left_column_id_,cell_temp)))
                    {
                        TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                        break;
                    }
                    else
                    {
                        char buff[300];
                        if(OB_SUCCESS!=(ret=get_data_for_distinct(cell_temp,
                                                                  cell_temp->get_type(),
                                                                  buff,
                                                                  sizeof(buff))))
                        {
                            TBSYS_LOG(WARN, "get data faliure , ret=[%d]", ret);
                            break;
                        }
                        else
                        {
                            string str_temp(buff);
                            bool tmp = true;
                            if(is_confilct_.find(str_temp) == is_confilct_.end())   //没找到
                            {
                                is_confilct_.insert(make_pair<string,bool>(str_temp,tmp));  //先插入

                                ObConstRawExpr     col_val;
                                if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                                {
                                    TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                                    break;
                                }
                                else
                                {
                                    if ((ret = col_val.fill_sql_expression(
                                             *table_filter_expr_)) != OB_SUCCESS)
                                    {
                                        TBSYS_LOG(ERROR,"Add cell value failed");
                                        break;
                                    }
                                }
                                dem5.type_ = T_OP_ROW;
                                dem5.data_type_ = ObMinType;
                                dem5.value_.int_ =1;
                                table_filter_expr_->add_expr_item(dem5);
                                index_param_count++;
                            }
                            else
                            {
                                continue;
                            }
                        }
                    }
                }
                if(index_param_count >= limit_num)
                {
                    break;
                }
            }
            if(OB_ITER_END == ret)
            {
                is_left_row_cache_complete_ =  true;
                ret =OB_SUCCESS;
            }
            if(OB_SUCCESS == ret)
            {
                dem6.type_ = T_OP_ROW;
                dem6.data_type_ = ObMinType;
                dem6.value_.int_ = index_param_count;
                table_filter_expr_->add_expr_item(dem6);
                dem7.type_ = T_OP_IN;
                dem7.data_type_ = ObMinType;
                dem7.value_.int_ = 2;
                table_filter_expr_->add_expr_item(dem7);
                table_filter_expr_->add_expr_item_end();
            }
        }
        if(index_param_count == 0)
        {
            is_in_expr_empty_ = true;
            is_left_row_cache_complete_ = true;
        }
    }

    return ret;
}
int ObRpcScan::get_table_row_with_more_scan(const common::ObRow *&row)
{
    //    UNUSED(row);
    //    int ret = OB_ITER_END;
    //    filter_expr_ = (ObSqlExpression*)ob_malloc(sizeof(ObSqlExpression),ObModIds::OB_SQL_TRANSFORMER);
    //    if (filter_expr_ == NULL)
    //    {
    //        ret = OB_ERR_PARSER_MALLOC_FAILED;
    //    }
    //    else
    //    {
    //        filter_expr_ = new(filter_expr_) ObSqlExpression();
    //    }
    //    cons_filter_for_right_table(filter_expr_,src_filter_expr_);
    //    ob_free(filter_expr_);
    //    ret = OB_ITER_END;
    //    return ret;
    UNUSED(row);
    int ret = OB_SUCCESS;
    //for memory leak check:b
    //int64_t usage = 0;
    //for memory leak check:e
    if(is_first_cons_scan_param_)
    {
        is_first_cons_scan_param_ = false;
        //filter_expr_ = ObSqlExpression::alloc ();
        //for memory leak check:b
        //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
        //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER before malloc usage = %ld",usage);
        //for memory leak check:e
        filter_expr_ = ObSqlExpression::alloc ();//(ObSqlExpression*)ob_malloc(sizeof(ObSqlExpression),ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);

        if(filter_expr_ == NULL)
        {
            TBSYS_LOG(WARN, "no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
            //filter_expr_ = new(filter_expr_) ObSqlExpression();
            //for memory leak check:b
            //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
            //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER after malloc usage = %ld",usage);
            //for memory leak check:e
            ret = cons_filter_for_right_table(filter_expr_,src_filter_expr_);

            if(!is_in_expr_empty_ && ret == OB_SUCCESS)
            {
                sql_read_strategy_.reset_for_semi_join ();
                sql_read_strategy_.set_rowkey_info (rowkey_info_);
                //modify xsl semi join
                if(OB_SUCCESS  != (ret = sql_read_strategy_.add_filter (*filter_expr_)))
                {
                    if(filter_expr_ != NULL)
                    {
                        ObSqlExpression::free (filter_expr_);
                        filter_expr_ = NULL;
                    }
                }
                //modify e

                /*
                //add xsl semi join
                if(OB_SUCCESS == ret)
                {

                    dlist_for_each_const(ObSqlExpression, p, f.get_filter())
                    {
                        ObSqlExpression *before_release = ObSqlExpression::alloc ();   //maybe memory leak
                        if(NULL == before_release)
                        {
                            //memory not enough
                            TBSYS_LOG(WARN, "no memory");
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                        }
                        else
                        {

                            *before_release = *p;
                            if(OB_SUCCESS == ret)
                            {
                                filter_raw.add_filter(before_release);
                            }
                        }
                    }
                }
                //add e
                */
                if(ret == OB_SUCCESS)
                {
                    //add xsl semi join
                    const ObFilter&  f = scan_param_->get_filter();
                    filter_raw.assign(&f);   //maybe memory leak
                    //add e
                    //modify xsl semi join
                    if(OB_SUCCESS != (ret = filter_raw.add_filter (filter_expr_)))   //filter
                    {
                        if(filter_expr_ != NULL)
                        {
                            ObSqlExpression::free (filter_expr_);
                            filter_expr_ = NULL;
                        }
                    }
                    //modify e
                }
                if(ret == OB_SUCCESS)
                {
                    TBSYS_LOG(DEBUG,"[semi join]::first>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                    TBSYS_LOG(DEBUG,"[semi join]::filter_expr_ = %s",to_cstring(*filter_expr_));
                    TBSYS_LOG(DEBUG,"[semi join]::filter_raw = %s",to_cstring(filter_raw));
                    TBSYS_LOG(DEBUG,"[semi join]::sql_read_strategy_ = %s",to_cstring(sql_read_strategy_));
                    TBSYS_LOG(DEBUG,"[semi join]::first<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                    if (NULL != scan_param_)   //重置下scan_param_
                    {
                        scan_param_->~ObSqlScanParam();
                        ob_free(scan_param_);
                        scan_param_ = NULL;
                    }
                    OB_ASSERT(NULL == scan_param_);
                    scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
                    if (NULL == scan_param_)
                    {
                        TBSYS_LOG(WARN, "no memory");
                        ret = OB_ALLOCATE_MEMORY_FAILED;
                    }
                    else
                    {
                        sql_scan_request_->close();
                        sql_scan_request_->reset();
                        sql_scan_request_->set_tablet_location_cache_proxy(cache_proxy_);
                        sql_scan_request_->set_merger_async_rpc_stub(async_rpc_);
                        if (OB_SUCCESS != (ret = sql_scan_request_->initialize()))
                        {
                            TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
                        }
                        else
                        {
                            sql_scan_request_->alloc_request_id();  //为远程调用申请一个连接
                            if (OB_SUCCESS != (ret = sql_scan_request_->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN, this)))
                            {
                                TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                            }
                            if (OB_SUCCESS != (ret = create_scan_param(*scan_param_)))
                            {
                                TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                            }
                            //重置read param
                            if(ret == OB_SUCCESS)
                            {
                                read_param_ = scan_param_;
                                read_param_->reset_project_and_filter ();
                                read_param_->set_project(project_raw);
                                read_param_->set_filter(filter_raw);
                            }
                            if (ret == OB_SUCCESS)
                            {
                                sql_scan_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                                if(OB_SUCCESS != (ret = sql_scan_request_->set_request_param(*scan_param_, hint_)))
                                {
                                    TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                                              hint_.max_parallel_count, ret);
                                }
                            }
                        }
                    }
                }
            }
            //add xsl semi join
            else
            {
                if(filter_expr_ != NULL)
                {
                    ObSqlExpression::free (filter_expr_);
                    filter_expr_ = NULL;
                }
            }
            //add e
            //delete xsl semi join
            /*
            if(is_in_expr_empty_)
            {
                ret = OB_ITER_END;
                if(filter_expr_ != NULL)
                {
                    //                    filter_expr_->~ObSqlExpression();
                    //                    ob_free (filter_expr_);
                    //                    filter_expr_ = NULL;
                    ObSqlExpression::free (filter_expr_);
                    filter_expr_ = NULL;
                    //for memory leak check:b
                    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                    //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                    //for memory leak check:e
                }
            }
            //防止内存泄露
            if(ret != OB_SUCCESS && filter_expr_ != NULL)
            {
                //                filter_expr_->~ObSqlExpression();
                //                ob_free (filter_expr_);
                //                filter_expr_ = NULL;
                ObSqlExpression::free (filter_expr_);
                filter_expr_ = NULL;
                //for memory leak check:b
                //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                //for memory leak check:e
            }
            */
            //delete e
        }
    }
    if(ret == OB_SUCCESS)
    {
        bool can_break = false;
        int64_t remain_us = 0;
        row = NULL;
        if (OB_UNLIKELY(my_phy_plan_->is_timeout(&remain_us)))
        {
            can_break = true;
            ret = OB_PROCESS_TIMEOUT;
        }
        else if (OB_UNLIKELY(NULL != my_phy_plan_ && my_phy_plan_->is_terminate(ret)))
        {
            can_break = true;
            TBSYS_LOG(WARN, "execution was terminated ret is %d", ret);
        }
        else
        {
            do
            {
                if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request_->get_next_row(cur_row_))))
                {
                    row = &cur_row_;
                    can_break = true;
                }
                else if (OB_ITER_END == ret && sql_scan_request_->is_finish())
                {
                    sql_read_strategy_.remove_last_inexpr ();
                    filter_raw.remove_last_filter ();
                    if(filter_expr_ != NULL)
                    {
                        //                        filter_expr_->~ObSqlExpression ();
                        //                        ob_free (filter_expr_);
                        //                        filter_expr_ = NULL;
                        ObSqlExpression::free (filter_expr_);
                        filter_expr_ = NULL;
                        //for memory leak check:b
                        //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                        //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                        //for memory leak check:e
                    }
                    if(!is_left_row_cache_complete_)   //false
                    {
                        //for memory leak check:b
                        //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                        //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER before malloc usage = %ld",usage);
                        //for memory leak check:e
                        filter_expr_ = ObSqlExpression::alloc ();//(ObSqlExpression*)ob_malloc(sizeof(ObSqlExpression),ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                        if(filter_expr_ == NULL)
                        {
                            TBSYS_LOG(WARN, "no memory");
                            ret = OB_ALLOCATE_MEMORY_FAILED;
                            can_break = true;
                        }
                        else
                        {
                            //filter_expr_ = new(filter_expr_) ObSqlExpression();
                            //for memory leak check:b
                            //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                            //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER after malloc usage = %ld",usage);
                            //for memory leak check:e
                            ret = cons_filter_for_right_table(filter_expr_,src_filter_expr_);
                            if(!is_in_expr_empty_ && ret == OB_SUCCESS)
                            {
                                //add xsl semi join
                                //const ObFilter&  f = scan_param_->get_filter();
                                //filter_raw.assign(&f);   //maybe memory leak
                                //add e
                                sql_read_strategy_.reset_for_semi_join ();
                                sql_read_strategy_.set_rowkey_info (rowkey_info_);
                                if(OB_SUCCESS != (ret = sql_read_strategy_.add_filter (*filter_expr_)))
                                {
                                    //add xsl semi join
                                    if(filter_expr_ != NULL)
                                    {
                                        ObSqlExpression::free (filter_expr_);
                                        filter_expr_ = NULL;
                                    }
                                    //add e
                                    can_break = true;
                                }
                                else if(OB_SUCCESS !=( ret  = filter_raw.add_filter (filter_expr_)))
                                {
                                    //add xsl semi join
                                    if(filter_expr_ != NULL)
                                    {
                                        ObSqlExpression::free (filter_expr_);
                                        filter_expr_ = NULL;
                                    }
                                    //add e
                                    can_break = true;
                                }
                                else
                                {
                                    TBSYS_LOG(DEBUG,"[semi join]::second>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                                    TBSYS_LOG(DEBUG,"[semi join]::filter_expr_ = %s",to_cstring(*filter_expr_));
                                    TBSYS_LOG(DEBUG,"[semi join]::filter_raw = %s",to_cstring(filter_raw));
                                    TBSYS_LOG(DEBUG,"[semi join]::sql_read_strategy_ = %s",to_cstring(sql_read_strategy_));
                                    TBSYS_LOG(DEBUG,"[semi join]::second<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                                    if (NULL != scan_param_)   //重置下scan_param_
                                    {
                                        scan_param_->~ObSqlScanParam();
                                        ob_free(scan_param_);
                                        scan_param_ = NULL;
                                    }
                                    OB_ASSERT(NULL == scan_param_);
                                    scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
                                    if (NULL == scan_param_)
                                    {
                                        TBSYS_LOG(WARN, "no memory");
                                        ret = OB_ALLOCATE_MEMORY_FAILED;
                                        can_break = true;
                                    }
                                    else
                                    {
                                        sql_scan_request_->close();
                                        sql_scan_request_->reset();
                                        sql_scan_request_->set_tablet_location_cache_proxy(cache_proxy_);
                                        sql_scan_request_->set_merger_async_rpc_stub(async_rpc_);
                                        if (OB_SUCCESS != (ret = sql_scan_request_->initialize()))
                                        {
                                            can_break = true;
                                            TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
                                        }
                                        else
                                        {
                                            sql_scan_request_->alloc_request_id();  //为远程调用申请一个连接
                                            if (OB_SUCCESS != (ret = sql_scan_request_->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN, this)))
                                            {
                                                TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                                                can_break = true;
                                            }
                                            if (OB_SUCCESS != (ret = create_scan_param(*scan_param_)))
                                            {
                                                TBSYS_LOG(WARN, "fail to create scan param. ret=%d", ret);
                                                can_break = true;
                                            }
                                            //重置read param
                                            if(ret == OB_SUCCESS)
                                            {
                                                read_param_ = scan_param_;
                                                read_param_->reset_project_and_filter ();
                                                read_param_->set_project(project_raw);
                                                read_param_->set_filter(filter_raw);
                                            }
                                            if (OB_SUCCESS == ret)
                                            {
                                                sql_scan_request_->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
                                                if(OB_SUCCESS != (ret = sql_scan_request_->set_request_param(*scan_param_, hint_)))
                                                {
                                                    TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                                                              hint_.max_parallel_count, ret);
                                                    can_break = true;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            //add xsl semi join
                            else
                            {
                                if(filter_expr_ != NULL)
                                {
                                    ObSqlExpression::free (filter_expr_);
                                    filter_expr_ = NULL;
                                }
                            }
                            //add e
                            //delete xsl semi join
                            /*
                            if(is_in_expr_empty_)
                            {
                                can_break = true;
                                ret = OB_ITER_END;
                                if(filter_expr_ != NULL)
                                {
                                    //                                    filter_expr_->~ObSqlExpression();
                                    //                                    ob_free (filter_expr_);
                                    //                                    filter_expr_ = NULL;
                                    ObSqlExpression::free (filter_expr_);
                                    filter_expr_ = NULL;
                                    //for memory leak check:b
                                    //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                                    //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                                    //for memory leak check:e
                                }
                            }
                            //防止内存泄露
                            if(ret != OB_SUCCESS && filter_expr_ != NULL)
                            {
                                //                                filter_expr_->~ObSqlExpression();
                                //                                ob_free (filter_expr_);
                                //                                filter_expr_ = NULL;
                                ObSqlExpression::free (filter_expr_);
                                filter_expr_ = NULL;
                                can_break = true;
                                //for memory leak check:b
                                //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                                //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                                //for memory leak check:e
                            }
                        */
                        //delete e
                        }
                    }
                    else
                    {
                        can_break = true;
                    }
                }
                else if (OB_ITER_END == ret)
                {
                    // need to wait for incomming data
                    can_break = false;
                    timeout_us_ = std::min(timeout_us_, remain_us);
                    if( OB_SUCCESS != (ret = sql_scan_request_->wait_single_event(timeout_us_)))
                    {
                        if (timeout_us_ <= 0)
                        {
                            TBSYS_LOG(WARN, "wait timeout. timeout_us_=%ld", timeout_us_);
                        }
                        can_break = true;
                    }
                    else
                    {
                        TBSYS_LOG(DEBUG, "got a scan event. timeout_us_=%ld", timeout_us_);
                    }
                }
                else
                {
                    // encounter an unexpected error or
                    TBSYS_LOG(WARN, "Unexprected error. ret=%d, cur_row_desc[%s], read_method_[%d]", ret, to_cstring(cur_row_desc_), hint_.read_method_);
                    can_break = true;
                }
            }while(false == can_break);
            //防止内存泄露
            //delete xsl semi join
            /*
            if(ret != OB_SUCCESS && filter_expr_ != NULL)
            {
                ObSqlExpression::free (filter_expr_);
                filter_expr_ = NULL;
                //for memory leak check:b
                //usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
                //TBSYS_LOG(INFO,"wanglei:: OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER free malloc usage = %ld",usage);
                //for memory leak check:e
            }
            */
            //delete e
        }
    }
    return ret;
}
int ObRpcScan::init_next_scan_param_for_one_scan_request(ObNewRange &range,
                                                         ObSqlScanParam * scan_param,
                                                         mergeserver::ObMsSqlScanRequest * scan_request)
{
    UNUSED(range);
    UNUSED(scan_param);
    UNUSED(scan_request);
    int ret = OB_SUCCESS;
    return ret;
}
int ObRpcScan::create_scan_param_for_one_scan_request(ObSqlScanParam &scan_param,
                                                      ObSqlReadStrategy &sql_read_strategy)
{
    int ret = OB_SUCCESS;
    ObNewRange range;
    if(is_use_index_)
    {
        ret=fill_read_param_for_first_scan(scan_param);
    }
    else
    {
        ret = fill_read_param(scan_param);
    }

    if(OB_SUCCESS == ret)
    {
        if (OB_SUCCESS != (ret = cons_scan_range_for_one_scan_request(range,sql_read_strategy)))
        {
            TBSYS_LOG(WARN, "fail to construct scan range. ret=%d", ret);
        }
        else if (OB_SUCCESS != (ret = scan_param.set_range(range)))
        {
            TBSYS_LOG(WARN, "fail to set range to scan param. ret=%d", ret);
        }
    }
    return ret;
}
int ObRpcScan::cons_scan_range_for_one_scan_request(ObNewRange &range,
                                                    ObSqlReadStrategy &sql_read_strategy)
{
    int ret = OB_SUCCESS;
    bool found = false;
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
    range.table_id_ = base_table_id_;
    OB_ASSERT(rowkey_info_.get_size() <= OB_MAX_ROWKEY_COLUMN_NUMBER);
    bool has_next = false;
    if (OB_SUCCESS != (ret = sql_read_strategy.find_scan_range(found,false)))
    {
        TBSYS_LOG(WARN, "construct  range %lu failed", base_table_id_);
    }
    else if (OB_SUCCESS != (ret = sql_read_strategy.get_next_scan_range(range,has_next)))
    {
        TBSYS_LOG(WARN, "fail to find   range %lu ", base_table_id_);
    }
    return ret;
}
//add wanglei fix now() bug
int64_t ObRpcScan::get_type_num(int64_t idx,int64_t type,ObSEArray<ObObj, 64> &expr_)
{
    int64_t num = 0;
    int ret = OB_SUCCESS;
    if(type == ObPostfixExpression::BEGIN_TYPE)
    {
        num = 1;
    }
    else if (type == ObPostfixExpression::OP)
    {
        num = 3;
        int64_t op_type = 0;
        if (OB_SUCCESS != (ret = expr_[idx+1].get_int(op_type)))
        {
            TBSYS_LOG(WARN, "Fail to get op type. unexpected! ret=%d", ret);
        }
        else if (T_FUN_SYS == op_type)
        {
            ++num;
        }
    }
    else if (type == ObPostfixExpression::COLUMN_IDX || type == T_OP_ROW)
    {
        num = 3;
    }
    else if (type == ObPostfixExpression::CONST_OBJ ||type == ObPostfixExpression::QUERY_ID||type == ObPostfixExpression::PARAM_IDX||type==ObPostfixExpression::TEMP_VAR)//add wanglei TEMP_VAR [second index fix] 20160513
    {
        num = 2;
    }
    else if (type == ObPostfixExpression::END || type == ObPostfixExpression::UPS_TIME_OP||ObPostfixExpression::CUR_TIME_OP
             ||ObPostfixExpression::UPS_TIME_OP)
    {
        num = 1;
    }
    else
    {
        TBSYS_LOG(WARN, "Unkown type %ld", type);
        return -1;
    }
    return num;
}
int ObRpcScan::add_index_filter_ll(ObSqlExpression* expr)   //索引过滤
{
    int ret=OB_SUCCESS;
    ObSqlExpression* tmp_expr = ObSqlExpression::alloc ();
    if(tmp_expr == NULL)
    {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        TBSYS_LOG(ERROR,"[semi join] allocate memory failed! no memory!");
    }
    else
    {
        tmp_expr->copy (expr);
        tmp_expr->set_owner_op(this);
        ret = index_filter_.add_filter(tmp_expr);
        if(ret != OB_SUCCESS)
            ObSqlExpression::free(tmp_expr);
    }
    return ret;
}
int ObRpcScan::add_index_output_column_ll(const ObSqlExpression& expr)
{
    int ret=OB_SUCCESS;
    ret = index_project_.add_output_column(expr);
    return ret;
}
//add wanglei [semi join] 20170417:e
//add wanglei [semi join multi thread] 20170417:b
int ObRpcScan::get_table_row_with_more_scan_multi_thread(const common::ObRow *&row)   //scan with more scan multi thread
{
    UNUSED(row);
    int ret = OB_SUCCESS;
    if(row_count_>=10000 && row_count_<=20000)
    {
        if(is_first_get_)
        {
            TBSYS_LOG(INFO,"wanglei::task number:%ld",task_list_.count());
            do
            {
                int f_count = 0;
                for(int i=0;i<task_list_.count();i++)
                {
                    if(task_list_.at(i)->is_finished_)
                    {
                        f_count++;
                    }
                }
                if(f_count == task_list_.count())
                    is_finish_ = true;
            }while(!is_finish_);
            is_first_get_ = false;
        }
        if(result_set_.count()==0)   //result_set
            ret = OB_ITER_END;
        else
        {
            int64_t result_set_count = result_set_.count();
            for(int64_t i=row_iter_;i<result_set_count;i++)
            {
                ret = result_set_.at(i)->get_next_row(cur_row_);
                if(ret == OB_SUCCESS)
                {
                    row = &cur_row_;
                    break;
                }
                else if(ret == OB_ITER_END)
                {
                    row_iter_++;
                    continue;
                }
                else
                {
                    break;
                }
            }
        }
    }
    else
        ret = OB_ERR_CAN_NOT_USE_SEMI_JOIN;
    return ret;
}
void ObRpcScan::release_memory()
{
    //task_queue_thread_->wait();
    //add wanglei [semi join multi thread] 20170417:b


    int64_t task_num = task_list_.count();
    for(int64_t i=0;i<task_num;i++)
    {
        if(task_list_.at(i)!=NULL)
        {
            ob_free(task_list_.at(i));
        }
    }
    task_list_.clear();
    int64_t result_num = result_set_.count();
    for(int64_t i=0;i<result_num;i++)
    {
        if(result_set_.at(i)!=NULL)
        {
            ob_free(result_set_.at(i));
        }
    }
    result_set_.clear();
    //    int64_t filter_num =filter_list_.count();
    //    for(int64_t i=0;i<filter_num;i++)
    //    {
    //        if(filter_list_.at(i)!=NULL)
    //        {
    //            ObSqlExpression::free(filter_list_.at(i));
    //        }
    //    }
    filter_list_.clear();
    //add wanglei [semi join multi thread] 20170417:e
//    int64_t usage = 0;
//    usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION);
//    TBSYS_LOG(INFO,"wanglei:: ObRowStore close malloc usage = %ld",usage);
//    usage = ob_get_mod_memory_usage(ObModIds::OB_SEMI_JOIN_SQL_EXPRESSION_CONS_FILTER);
//    TBSYS_LOG(INFO,"wanglei:: ObRpcScanTask and Filter close malloc usage = %ld",usage);
}
void ObRpcScan::set_row_count(int row_count)
{
    row_count_ = row_count;
}
int ObRpcScan::cons_filter_for_right_table_multi_thread(ObSqlExpression *&table_filter_expr_,int iter)
{
    int ret = OB_SUCCESS;

    int limit_num = iter;
    int index_param_count = 0;
    if(OB_SUCCESS == ret)
    {
        ExprItem dem1,dem2,dem3,dem5,dem6,dem7;
        dem1.type_ = T_REF_COLUMN;
        //后缀表达式组建
        table_filter_expr_->set_tid_cid(right_table_id_,right_column_id_);
        dem1.value_.cell_.tid = right_table_id_;
        dem1.value_.cell_.cid = right_column_id_;
        table_filter_expr_->add_expr_item(dem1);
        dem2.type_ = T_OP_ROW;
        dem2.data_type_ = ObMinType;
        dem2.value_.int_ = 1;
        table_filter_expr_->add_expr_item(dem2);
        dem3.type_ = T_OP_LEFT_PARAM_END;
        dem3.data_type_ = ObMinType;
        dem3.value_.int_ = 2;
        table_filter_expr_->add_expr_item(dem3);
        ObRow row_temp;
        row_temp.set_row_desc (*left_row_desc_);
        while(OB_SUCCESS == (ret = semi_join_left_table_row_->get_next_row(row_temp)))
        {
            if(OB_SUCCESS!=ret)
                TBSYS_LOG(WARN, "no more rows , sret=[%d]", ret);
            else
            {
                TBSYS_LOG(DEBUG,"[semi join]::row_temp = %s",to_cstring(row_temp));
                const common::ObObj *cell_temp=NULL;
                if(OB_SUCCESS!=(ret=row_temp.get_cell(left_table_id_,left_column_id_,cell_temp)))
                {
                    TBSYS_LOG(WARN, "get cell failure , ret=[%d]", ret);
                    break;
                }
                else
                {
                    char buff[300];
                    if(OB_SUCCESS!=(ret=get_data_for_distinct(cell_temp,
                                                              cell_temp->get_type(),
                                                              buff,
                                                              sizeof(buff))))
                    {
                        TBSYS_LOG(WARN, "get data faliure , ret=[%d]", ret);
                        break;
                    }
                    else
                    {
                        string str_temp(buff);
                        bool tmp = true;
                        if(is_confilct_.find(str_temp) == is_confilct_.end())
                        {
                            is_confilct_.insert(make_pair<string,bool>(str_temp,tmp));

                            ObConstRawExpr     col_val;
                            if (OB_SUCCESS != (ret = col_val.set_value_and_type(*cell_temp)))
                            {
                                TBSYS_LOG(ERROR, "failed to set column value, err=%d", ret);
                                break;
                            }
                            else
                            {
                                if ((ret = col_val.fill_sql_expression(
                                         *table_filter_expr_)) != OB_SUCCESS)
                                {
                                    TBSYS_LOG(ERROR,"Add cell value failed");
                                    break;
                                }
                            }
                            dem5.type_ = T_OP_ROW;
                            dem5.data_type_ = ObMinType;
                            dem5.value_.int_ =1;
                            table_filter_expr_->add_expr_item(dem5);
                            index_param_count++;
                        }
                        else
                        {
                            continue;
                        }
                    }
                }
            }
            if(index_param_count >= limit_num)
            {
                break;
            }
        }
        if(OB_ITER_END == ret)
        {
            is_left_row_cache_complete_ =  true;
            ret =OB_SUCCESS;
        }
        if(OB_SUCCESS == ret)
        {
            dem6.type_ = T_OP_ROW;
            dem6.data_type_ = ObMinType;
            dem6.value_.int_ = index_param_count;
            table_filter_expr_->add_expr_item(dem6);
            dem7.type_ = T_OP_IN;
            dem7.data_type_ = ObMinType;
            dem7.value_.int_ = 2;
            table_filter_expr_->add_expr_item(dem7);
            table_filter_expr_->add_expr_item_end();
        }
    }
    if(index_param_count == 0)
    {
        is_in_expr_empty_ = true;
        is_left_row_cache_complete_ = true;
    }
    return ret;
}
//add wanglei [semi join multi thread] 20170417:e
PHY_OPERATOR_ASSIGN(ObRpcScan)
{
    int ret = OB_SUCCESS;
    CAST_TO_INHERITANCE(ObRpcScan);
    reset();
    rowkey_info_ = o_ptr->rowkey_info_;
    table_id_ = o_ptr->table_id_;
    base_table_id_ = o_ptr->base_table_id_;
    hint_ = o_ptr->hint_;
    need_cache_frozen_data_ = o_ptr->need_cache_frozen_data_;
    cache_bloom_filter_ = o_ptr->cache_bloom_filter_;
    //add longfei [prepare bug fix] 2016-04-22 10:21:42
    is_use_index_ = o_ptr->is_use_index_;
    is_use_index_for_storing_ = o_ptr->is_use_index_for_storing_;
    main_table_id_ = o_ptr->main_table_id_;
    get_next_row_count_ = o_ptr->get_next_row_count_;
    main_filter_.set_phy_plan(my_phy_plan_);
    main_project.set_phy_plan(my_phy_plan_);
    if (OB_SUCCESS != (ret = main_filter_.assign(&o_ptr->main_filter_)))
    {
        TBSYS_LOG(WARN, "Assign main_filter_ for seIndex failed, ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = main_project.assign(&o_ptr->main_project)))
    {
        TBSYS_LOG(WARN, "Assign main_project_ for seIndex failed, ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = second_row_desc_.assign(o_ptr->second_row_desc_)))
    {
        TBSYS_LOG(WARN, "Assign second_row_desc_ for seIndex failed, ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = cur_row_desc_for_storing.assign(o_ptr->cur_row_desc_for_storing)))
    {
        TBSYS_LOG(WARN, "Assign cur_row_desc_for_storing_ for seIndex failed, ret=%d", ret);
    }
    else if(OB_SUCCESS != (ret = main_rowkey_info_.assign(o_ptr->main_rowkey_info_)))
    {
        TBSYS_LOG(WARN, "Assign main_rowkey_info_ for seIndex failed, ret=%d", ret);
    }
    //add e
    //mod longfei 2016-04-22 10:32:54
    //  if ((ret = cur_row_desc_.assign(o_ptr->cur_row_desc_)) != OB_SUCCESS)
    else if((ret = cur_row_desc_.assign(o_ptr->cur_row_desc_)) != OB_SUCCESS)
        //mod e
    {
        TBSYS_LOG(WARN, "Assign ObRowDesc failed, ret=%d", ret);
    }
    else if ((ret = sql_read_strategy_.assign(&o_ptr->sql_read_strategy_, this)) != OB_SUCCESS)
    {
        TBSYS_LOG(WARN, "Assign ObSqlReadStrategy failed, ret=%d", ret);
    }
    else if (o_ptr->scan_param_)
    {
        scan_param_ = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
        if (NULL == scan_param_)
        {
            TBSYS_LOG(WARN, "no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
            scan_param_->set_phy_plan(get_phy_plan());
            read_param_ = scan_param_;
            if ((ret = scan_param_->assign(o_ptr->scan_param_)) != OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "Assign Scan param failed, ret=%d", ret);
            }
        }
    }
    else if (o_ptr->get_param_)
    {
        get_param_ = OB_NEW(ObSqlGetParam, ObModIds::OB_SQL_GET_PARAM);
        if (NULL == get_param_)
        {
            TBSYS_LOG(WARN, "no memory");
            ret = OB_ALLOCATE_MEMORY_FAILED;
        }
        else
        {
            get_param_->set_phy_plan(get_phy_plan());
            read_param_ = get_param_;
            if ((ret = get_param_->assign(o_ptr->get_param_)) != OB_SUCCESS)
            {
                TBSYS_LOG(WARN, "Assign Get param failed, ret=%d", ret);
            }
        }
    }
    sql_read_strategy_.set_rowkey_info(rowkey_info_);
    return ret;
}
