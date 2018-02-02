#include "ob_semi_join_task_queue_thread.h"
#include "ob_merge_server_main.h"
#include "sql/ob_rpc_scan.h"
#include <iostream>
using namespace oceanbase;
using namespace oceanbase::mergeserver;
using namespace std;
using namespace oceanbase::common;
static long *get_no_ptr(void)
{
    static __thread long p = 0;
    return &p;
}
//设置线程号
static void set_thread_no(long n)
{
    long * p = get_no_ptr();
    if (NULL != p) *p = n;
}
//获取线程号
static long get_thread_no(void)
{
    long *p = get_no_ptr();
    long no = 0;
    if (NULL != p)
    {
        no = *p;
    }
    return no;
}
ObSemiJoinTaskQueueThread * ObSemiJoinTaskQueueThread::instance_ =new ObSemiJoinTaskQueueThread();
ObSemiJoinTaskQueueThread::ObSemiJoinTaskQueueThread()
    :stop_(false),task_limit_(TASK_LIMIT_DEFAULT), wait_time_(WAIT_TIME_DEFAULT)
{
    cur_row_desc_ = NULL;
    my_phy_plan_ = NULL;
    project_raw_ = NULL;
    rowkey_info_ = NULL;
    hint_ = NULL;
    rpc_scan_ = NULL;
    //instance_ = NULL;
    setThreadCount(MAX_TASK_THREAD);
    for(int i=0;i<MAX_TASK_THREAD;i++)
    {
        thread_status_[i].is_work = false;
        thread_status_[i].thread_id = i;
    }
}
void ObSemiJoinTaskQueueThread::init()
{}
void ObSemiJoinTaskQueueThread::init(common::ObRowDesc *cur_row_desc,
                                     ObPhysicalPlan * my_phy_plan,
                                     ObProject *project_raw,
                                     common::ObRowkeyInfo *rowkey_info,
                                     common::ObRpcScanHint *hint,
                                     ObRpcScan *rpc_scan,
                                     common::ObTabletLocationCacheProxy *cache_proxy,
                                     ObMergerAsyncRpcStub * async_rpc,
                                     const ObMergeServerService *merge_service
                                     )
{
    cur_row_desc_ = cur_row_desc;
    my_phy_plan_ = my_phy_plan;
    project_raw_ = project_raw;
    rowkey_info_ = rowkey_info;
    hint_ = hint;
    rpc_scan_ = rpc_scan;
    cache_proxy_ = cache_proxy;
    async_rpc_ = async_rpc;
    merge_service_ = merge_service;
}
void ObSemiJoinTaskQueueThread::run_bk(tbsys::CThread* thread, void* arg)
{
    //绑定CPU:begin
    //    int myid = (rand() % (20))+1;
    //    set_cpu(myid);
    //    TBSYS_LOG(INFO,"wanglei::thread start! cpu id=%d\n",myid);
    //绑定CPU:end

    UNUSED(thread);
    UNUSED(arg);
    int ret = OB_SUCCESS;
    ObRpcScanTask *rpc_task = NULL;
    long thread_no = reinterpret_cast<long>(arg);
    TBSYS_LOG(INFO,"wanglei::thread start! thread id=%ld\n",thread_no);
    cond_[thread_no].wait();
    while (!stop_) {
        rpc_task = NULL;
        pop_cond_.lock();
        rpc_task = task_queue_.at(thread_no);
        task_queue_.remove(thread_no);
        pop_cond_.unlock();

        if(rpc_task != NULL)
        {
            ret = handle_task(rpc_task,get_thread_no());
            if(rpc_task==NULL)
            {
                ret = OB_ERR_POINTER_IS_NULL;
            }
            else
            {
                rpc_task->is_finished_ = true;
                thread_status_[thread_no].is_work = false;
                //sleep(THREAD_WAITE_TIME);
            }
            TBSYS_LOG(INFO,"wanglei::Thread no[%ld] task done!",get_thread_no());
        }
        //cond_[thread_no].wait();
    }
    if(stop_)
        TBSYS_LOG(INFO,"[SEMI JOIN]:Semi join task queue thread stop success!");
}
void ObSemiJoinTaskQueueThread::run(tbsys::CThread* thread, void* arg)
{

    //绑定CPU:begin
    //    int myid = (rand() % (20))+1;
    //    set_cpu(myid);
    //    TBSYS_LOG(INFO,"wanglei::thread start! cpu id=%d\n",myid);
    //绑定CPU:end

    UNUSED(thread);
    UNUSED(arg);
    int ret = OB_SUCCESS;
    ObRpcScanTask *rpc_task = NULL;
    long thread_no = reinterpret_cast<long>(arg);
    set_thread_no(thread_no);
    int retry_count = 0;
    TBSYS_LOG(INFO,"wanglei::thread start! thread id=%ld\n",thread_no);
    while (!stop_) {
        rpc_task = NULL;
        pop_cond_.lock();
        if(task_queue_.count() != 0)
        {
            rpc_task = task_queue_.at(0);
            task_queue_.remove(0);
            ret = OB_SUCCESS;
        }
        else
        {
            ret = OB_INVALID_ARGUMENT;
        }
        pop_cond_.unlock();
        if (OB_SUCCESS != ret)
        {
            if(retry_count<MAX_RETRY_COUNT)
            {
                retry_count++;
                continue;
            }
            else
            {
                //TBSYS_LOG(INFO,"wanglei::Thread no[%ld] went to sleep!",get_thread_no());
                retry_count = 0;
                cond_[thread_no].wait();
            }
        }
        else
        {
            if(rpc_task != NULL)
            {
                ret = handle_task(rpc_task,get_thread_no());
                if(rpc_task==NULL)
                {
                    ret = OB_ERR_POINTER_IS_NULL;
                }
                else
                {
                    rpc_task->is_finished_ = true;
                    //sleep(THREAD_WAITE_TIME);
                }
                TBSYS_LOG(INFO,"wanglei::Thread no[%ld] task done!",get_thread_no());
                rpc_task = NULL;
            }
        }
    }
    if(stop_)
        TBSYS_LOG(INFO,"[SEMI JOIN]:Semi join task queue thread stop success!");
}
int ObSemiJoinTaskQueueThread::handle_task_bk(ObRpcScanTask *task,long thread_id)
{
    UNUSED(thread_id);
    UNUSED(task);

    return OB_SUCCESS;
}
int ObSemiJoinTaskQueueThread::handle_task(ObRpcScanTask *task,long thread_id)
{
    UNUSED(thread_id);
    UNUSED(task);
    //TBSYS_LOG(INFO,"wanglei::thread[%ld],msg:%s",thread_id,task->msg_);
    //    TBSYS_LOG(INFO,"wanglei::rowkey info:%s",to_cstring(*rowkey_info_));
    //    TBSYS_LOG(INFO,"wanglei::filter info:%s",to_cstring(*(task->filter_expr_)));
    //    TBSYS_LOG(INFO,"wanglei::rowdes info:%s",to_cstring(*cur_row_desc_));
    //    TBSYS_LOG(INFO,"wanglei::project info:%s",to_cstring(*project_raw_));

    int ret = OB_SUCCESS;
    if(task == NULL)
    {
        ret = OB_ERR_POINTER_IS_NULL;
    }
    else
    {
        //构造参数并初始化连接：b
        ObSqlScanParam *scan_param = NULL;
        ObSqlReadParam *read_param = NULL;
        ObSqlReadStrategy read_strategy;
        mergeserver::ObMsSqlScanRequest *sql_scan_request = NULL;
        common::ObRow cur_row;
        ObFilter filter;
        cur_row.set_row_desc(*cur_row_desc_);
        if (NULL == (sql_scan_request = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().alloc()))
        {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            TBSYS_LOG(WARN, "alloc scan request from scan request pool failed, ret=%d", ret);
        }
        else
        {
            sql_scan_request->set_tablet_location_cache_proxy(cache_proxy_);
            sql_scan_request->set_merger_async_rpc_stub(async_rpc_);
            if (OB_SUCCESS != (ret = sql_scan_request->initialize()))
            {
                TBSYS_LOG(WARN, "initialize sql_scan_request failed, ret=%d", ret);
            }
            else
            {
                sql_scan_request->alloc_request_id();  //为远程调用申请一个连接
                if (OB_SUCCESS != (ret = sql_scan_request->init_ex(REQUEST_EVENT_QUEUE_SIZE, ObModIds::OB_SQL_RPC_SCAN,rpc_scan_ )))
                {
                    TBSYS_LOG(WARN, "fail to init sql_scan_event. ret=%d", ret);
                }
            }
        }
        if(OB_SUCCESS == ret)
        {
            read_strategy.set_rowkey_info(*rowkey_info_);
            common_cond_.lock();
            if(task == NULL)
            {
                ret = OB_ERR_POINTER_IS_NULL;
            }
            else if(OB_SUCCESS != (ret = read_strategy.add_filter (*(task->filter_expr_))))
            {
                ret = OB_INVALID_ARGUMENT;
            }
            common_cond_.unlock();
        }

        if(OB_SUCCESS == ret)
        {
            if(task == NULL)
            {
                ret = OB_ERR_POINTER_IS_NULL;
            }
            else
            {
                common_cond_.lock();
                filter.add_filter(task->filter_expr_);
                common_cond_.unlock();
                scan_param = OB_NEW(ObSqlScanParam, ObModIds::OB_SQL_SCAN_PARAM);
                if (NULL == scan_param)
                {
                    TBSYS_LOG(WARN, "no memory");
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                }
                else
                {
                    ret = rpc_scan_->create_scan_param_for_one_scan_request(*scan_param,read_strategy);
                    if(ret == OB_SUCCESS)
                    {
                        read_param = scan_param;
                        read_param->set_project(*project_raw_);
                        read_param->set_filter(filter);
                    }
                }
            }
        }
        if (ret == OB_SUCCESS)
        {
            sql_scan_request->set_timeout_percent((int32_t)merge_service_->get_config().timeout_percent);
            if(OB_SUCCESS != (ret = sql_scan_request->set_request_param(*scan_param, *hint_)))
            {
                TBSYS_LOG(WARN, "fail to set request param. max_parallel=%ld, ret=%d",
                          hint_->max_parallel_count, ret);
            }
        }
        //构造参数并初始化连接：e

        //获取数据:b
        if(ret == OB_SUCCESS)
        {
            bool can_break = false;
            int64_t remain_us = 0;
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
                    if (OB_LIKELY(OB_SUCCESS == (ret = sql_scan_request->get_next_row_with_one_scan_request(cur_row,&read_strategy))))
                    {
                        //TBSYS_LOG(INFO,"wanglei::thread no [%ld],row =%s",thread_id,to_cstring(cur_row));
                        const ObRowStore::StoredRow *stored_row = NULL;
                        common_cond_.lock();
                        if(task==NULL)
                        {
                            ret = OB_ERR_POINTER_IS_NULL;
                            can_break = true;
                        }
                        else
                        {

                            task->intermediate_result_->add_row(cur_row,stored_row);

                        }
                        common_cond_.unlock();
                    }
                    else if (OB_ITER_END == ret && sql_scan_request->is_finish())
                    {
                        can_break = true;
                    }
                    else if (OB_ITER_END == ret)
                    {
                        can_break = false;
                        common_cond_.lock();
                        timeout_us_ = std::min(timeout_us_, remain_us);
                        common_cond_.unlock();
                        if( OB_SUCCESS != (ret = sql_scan_request->wait_single_event(timeout_us_)))
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
                        can_break = true;
                    }
                }while(false == can_break);
            }
        }
        //获取数据:e
        read_strategy.destroy();
        sql_scan_request->close();
        sql_scan_request->reset();
        if (NULL != scan_param)
        {
            scan_param->reset_local();
        }
        if (NULL != scan_param)
        {
            scan_param->~ObSqlScanParam();
            ob_free(scan_param);
            scan_param = NULL;
        }
        if (sql_scan_request != NULL)
        {
            if (OB_SUCCESS != (ret = ObMergeServerMain::get_instance()->get_merge_server().get_scan_req_pool().free(sql_scan_request)))
            {
            }
            sql_scan_request = NULL;
        }
        //ObSqlExpression::free(task->filter_expr_);
    }
    return ret;
}
ObSemiJoinTaskQueueThread::~ObSemiJoinTaskQueueThread()
{
    stop();
}
//void ObSemiJoinTaskQueueThread::wait()
//{
//    for(int i=0;i<MAX_TASK_THREAD;i++)
//    {
//        cond_[i].wait();
//    }
//}
void ObSemiJoinTaskQueueThread::wake_up_one()
{
    cond_[0].signal();
}
void ObSemiJoinTaskQueueThread::wake_up_all()
{
    for(int i=0;i<MAX_TASK_THREAD;i++)
    {
        thread_status_[i].is_work = true;
        cond_[i].signal();
    }
}
void ObSemiJoinTaskQueueThread::stop()
{
    common_cond_.lock();
    stop_ = true;
    common_cond_.broadcast();
    common_cond_.unlock();
}
void ObSemiJoinTaskQueueThread::clear()
{
    delete[] _thread;
    _thread = NULL;
}
int ObSemiJoinTaskQueueThread::push(ObRpcScanTask *task)
{
    int ret = OB_SUCCESS;
    if (_stop || _thread == NULL) {
        return OB_ERR_POINTER_IS_NULL;
    }
    if (NULL == task)
    {
        TBSYS_LOG(WARN, "task is null");
        ret  = OB_ERR_POINTER_IS_NULL;
    }
    else
    {
        push_cond_.lock();
        if (task_queue_.count() < task_limit_)
        {
            task_queue_.push_back(task);
        }
        else
        {
            ret = OB_ERR_TASK_FULL;
            TBSYS_LOG(DEBUG, "too many task, task_limit_=%d", task_limit_);
        }
        push_cond_.unlock();
    }
    return ret;
}
void ObSemiJoinTaskQueueThread::set_row_desc(common::ObRowDesc *cur_row_desc)
{
    cur_row_desc_ = cur_row_desc;
}
void ObSemiJoinTaskQueueThread::set_phy_plan(ObPhysicalPlan * my_phy_plan)
{
    my_phy_plan_ = my_phy_plan;
}
void ObSemiJoinTaskQueueThread::set_project(ObProject *project_raw)
{
    project_raw_ = project_raw;
}
void ObSemiJoinTaskQueueThread::set_rowkey_info(common::ObRowkeyInfo *rowkey_info)
{
    rowkey_info_ = rowkey_info;
}
void ObSemiJoinTaskQueueThread::set_hint(common::ObRpcScanHint *hint)
{
    hint_ = hint;
}
void ObSemiJoinTaskQueueThread::set_rpc_scan(ObRpcScan *rpc_scan)
{
    rpc_scan_ = rpc_scan;
}
void ObSemiJoinTaskQueueThread::set_cache_proxy(common::ObTabletLocationCacheProxy *cache_proxy)
{
    cache_proxy_ = cache_proxy;
}
void ObSemiJoinTaskQueueThread::set_rpc_stub(ObMergerAsyncRpcStub * async_rpc)
{
    async_rpc_ = async_rpc;
}
void ObSemiJoinTaskQueueThread::set_mergeserver_service(const ObMergeServerService *merge_service)
{
    merge_service_ = merge_service;
}
void ObSemiJoinTaskQueueThread::set_timeout(int64_t timeout_us)
{
    timeout_us_ = timeout_us;
}
void ObSemiJoinTaskQueueThread::set_row_store(ObRowStore *cache_row)
{
    cache_row_ = cache_row;
}
