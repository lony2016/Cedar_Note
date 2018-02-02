#ifndef OB_SEMI_JOIN_TASK_QUEUE_THREAD_H
#define OB_SEMI_JOIN_TASK_QUEUE_THREAD_H
#include "tbsys.h"
#include "common/ob_lighty_queue.h"
#include "sql/ob_sql_expression.h"
#include "sql/ob_project.h"
#include "common/ob_hint.h"
#include "common/location/ob_tablet_location_cache_proxy.h"
#include "ob_ms_async_rpc.h"
#include "ob_merge_server_service.h"
#include "ob_ms_sql_scan_request.h"
#include "common/ob_switch.h"
#include<stdlib.h>
#include<stdio.h>
#include<sys/types.h>
#include<sys/sysinfo.h>
#include <sys/syscall.h>
#include<unistd.h>
#include<sched.h>
#include<ctype.h>
#include<string.h>
#define gettidv1() syscall(__NR_gettid)
using namespace std;
using namespace oceanbase::sql;
namespace oceanbase
{
    namespace sql
    {
        class ObRpcScan;
    }
    namespace mergeserver
    {
        class ObMsSqlScanRequest;
        struct ObRpcScanTask
        {
            ObSqlExpression *filter_expr_;
            ObRowStore *intermediate_result_;
            bool is_finished_;
            bool is_timeout_;
            ObRpcScanTask()
            {
                filter_expr_ = NULL;
                intermediate_result_ = NULL;
                is_finished_ = false;
                is_timeout_ = false;
            }
            ~ObRpcScanTask()
            {
                filter_expr_ = NULL;
                intermediate_result_ =NULL;
            }

        };
        struct THREAD_STATUS
        {
            bool is_work;
            int thread_id;
        };
        class ObSemiJoinTaskQueueThread : public tbsys::CDefaultRunnable
        {
        private:
            ObSemiJoinTaskQueueThread();
        public:
            static ObSemiJoinTaskQueueThread* GET_INSTANCE()
            {
                 return instance_;
            }
            virtual ~ObSemiJoinTaskQueueThread();
            void run(tbsys::CThread* thread, void* arg);
            void run_bk(tbsys::CThread* thread, void* arg);
            //void init(ObMergeServerService *&merge_service, ObMergeServer*& merge_server, mergeserver::ObMergerAsyncRpcStub *& async_rpc, common::ObTabletLocationCacheProxy *&cache_proxy);
            void init(common::ObRowDesc *cur_row_desc,
                      ObPhysicalPlan * my_phy_plan,
                      ObProject *project_raw,
                      common::ObRowkeyInfo *rowkey_info,
                      common::ObRpcScanHint *hint,
                      ObRpcScan *rpc_scan,
                      common::ObTabletLocationCacheProxy *cache_proxy,
                      ObMergerAsyncRpcStub * async_rpc,
                      const ObMergeServerService *merge_service
                      );
            void init();
            int push(ObRpcScanTask *task);
            //void wait();
            void wake_up_all();
            void wake_up_one();
            void stop();
            void clear();
            int handle_task(ObRpcScanTask *task, long thread_id);
            int handle_task_bk(ObRpcScanTask *task,long thread_id);
            //set and get
            void set_row_desc(common::ObRowDesc *cur_row_desc);
            void set_phy_plan(ObPhysicalPlan * my_phy_plan);
            void set_project(ObProject *project_raw);
            void set_rowkey_info(common::ObRowkeyInfo *rowkey_info);
            void set_hint(common::ObRpcScanHint *hint);
            void set_rpc_scan(ObRpcScan *rpc_scan);
            void set_cache_proxy(common::ObTabletLocationCacheProxy *cache_proxy);
            void set_rpc_stub(ObMergerAsyncRpcStub * async_rpc);
            void set_mergeserver_service(const ObMergeServerService *merge_service);
            void set_timeout(int64_t timeout_us);
            void set_row_store(ObRowStore *cache_row);
            inline int set_cpu(int i)
            {
                long int cpu_nums = sysconf(_SC_NPROCESSORS_CONF);//获取cpu个数
                cpu_set_t mask;
                CPU_ZERO(&mask);
                if(i < cpu_nums)
                {
                    CPU_SET(i,&mask);
                    if(-1 == sched_setaffinity((int)gettidv1(),sizeof(&mask),&mask))
                    {
                        return -1;
                    }
                 }
                return 0;
            }
            bool ensure_thread_start()
            {
              return switch_.wait_on();
            }
            bool ensure_thread_stop()
            {
              return switch_.wait_off();
            }
            void wait_thread_exit()
            {
                if (_thread != NULL)
                {
                    for(int i=0;i<MAX_TASK_THREAD;++i)
                    {
                        _thread[i].join();
                    }
                }
            }
            void clean_task_queue()
            {

               // task_queue_.clear();
            }
        public:
            static const int TASK_LIMIT_DEFAULT = 5000;
            static const int MAX_TASK_THREAD = 50;
            static const int MAX_TASK = 45;
            static const int WAIT_TIME_DEFAULT = 1 * 1000;//1ms
            static const int MAX_WAIT_TIME = 5 * 1000 * 1000;//5 seconds
            static const int MAX_RETRY_COUNT = 5;
            static const int RPC_QUEUE_CAPACITY = 1 << 17;
            static const int64_t REQUEST_EVENT_QUEUE_SIZE = 8192;
            static const int THREAD_WAITE_TIME = 1;
        private:
            ObSwitch switch_;
            common::ObTabletLocationCacheProxy *cache_proxy_;
            ObMergerAsyncRpcStub * async_rpc_;
            const ObMergeServerService *merge_service_;
            bool stop_;
            common::ObArray<ObRpcScanTask*> task_queue_;
            common::ObArray<int> task_queue_int_;
            static ObSemiJoinTaskQueueThread * instance_;
            common::ObRowDesc *cur_row_desc_;
            ObPhysicalPlan * my_phy_plan_;
            ObProject *project_raw_;
            ObRpcScan *rpc_scan_;
            common::ObRowkeyInfo *rowkey_info_;
            oceanbase::common::ObRpcScanHint *hint_;
            ObRowStore *cache_row_;
            tbsys::CThreadCond cond_[MAX_TASK_THREAD];
            tbsys::CThreadCond push_cond_;
            tbsys::CThreadCond pop_cond_;
            tbsys::CThreadCond common_cond_;
            int task_limit_;
            int wait_time_;
            int64_t timeout_us_;
            struct THREAD_STATUS  thread_status_[MAX_TASK_THREAD];
        };
    }
}
#endif // OB_SEMI_JOIN_TASK_QUEUE_THREAD_H
