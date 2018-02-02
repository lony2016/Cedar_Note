<<<<<<< HEAD
/**
 * Copyright (C) 2013-2016 ECNU_DaSE.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_merge_server.h
 * @brief merge server
 *
 * modified by wangdonghui:add procedure manager function member and data member
 *
 * @version __DaSE_VERSION
 * @author zhutao <zhutao@stu.ecnu.edu.cn>
 * @author wangdonghui <zjnuwangdonghui@163.com>
 *
 * @date 2016_07_29
 */

=======
>>>>>>> refs/remotes/origin/master
#ifndef OCEANBASE_MERGESERVER_MERGESERVER_H_
#define OCEANBASE_MERGESERVER_MERGESERVER_H_

#include "common/ob_single_server.h"
#include "common/ob_packet_factory.h"
#include "common/ob_server.h"
#include "common/ob_timer.h"
#include "common/thread_buffer.h"
#include "common/ob_client_manager.h"
#include "common/ob_config_manager.h"
#include "common/ob_privilege_manager.h"
#include "common/ob_obj_pool.h"
#include "ob_merge_server_service.h"
#include "ob_frozen_data_cache.h"
#include "ob_insert_cache.h"
#include "ob_bloom_filter_task_queue_thread.h"
#include "ob_ms_sql_scan_request.h"
#include "ob_ms_sql_get_request.h"

<<<<<<< HEAD
//add by wangdonghui [procedure physical plan cache] 20160229 :b
#include "mergeserver/ob_physical_plan_cache_manager.h"
//add :e
//add wanglei [semi join multi thread] 20170417:b
#include "ob_semi_join_task_queue_thread.h"
//add wanglei [semi join multi thread] 20170417:e
//add huangcc [statistic information cache] 20170317:b
#include "ob_statistic_info_cache.h"
//add:e

=======
>>>>>>> refs/remotes/origin/master
namespace oceanbase
{
  namespace mergeserver
  {
    class ObMergeServer : public common::ObSingleServer
    {
      public:
        ObMergeServer(ObConfigManager &config_mgr,
                      ObMergeServerConfig &ms_config);
      public:
        void set_privilege_manager(ObPrivilegeManager *privilege_manager);

      public:
        int initialize();
        int start_service();
        void destroy();
        int reload_config();

        int do_request(common::ObPacket* base_packet);
        //
        bool handle_overflow_packet(common::ObPacket* base_packet);
        void handle_timeout_packet(common::ObPacket* base_packet);
      public:
        common::ThreadSpecificBuffer::Buffer* get_response_buffer() const;

        common::ThreadSpecificBuffer* get_rpc_buffer();

        const common::ObServer& get_self() const;

        const common::ObServer& get_root_server() const;

        common::ObTimer& get_timer();

        bool is_stoped() const;

        const common::ObClientManager& get_client_manager() const;

        ObMergeServerConfig& get_config();
        ObConfigManager& get_config_mgr();
        mergeserver::ObMergerRpcProxy  *get_rpc_proxy() const{return service_.get_rpc_proxy();}
        mergeserver::ObMergerRootRpcProxy * get_root_rpc() const{return service_.get_root_rpc();}
        mergeserver::ObMergerAsyncRpcStub   *get_async_rpc() const{return service_.get_async_rpc();}
        common::ObMergerSchemaManager *get_schema_mgr() const{return service_.get_schema_mgr();}
        common::ObTabletLocationCacheProxy *get_cache_proxy() const{return service_.get_cache_proxy();}
        common::ObStatManager* get_stat_manager() const { return service_.get_stat_manager(); }

        int set_sql_session_mgr(sql::ObSQLSessionMgr* mgr);
        void set_sql_id_mgr(sql::ObSQLIdMgr* mgr) {service_.set_sql_id_mgr(mgr);};
        inline const ObMergeServerService &get_service() const
        {
          return service_;
        }

        inline ObPrivilegeManager* get_privilege_manager()
        {
          return &privilege_mgr_;
        }
        inline ObFrozenDataCache & get_frozen_data_cache()
        {
          return frozen_data_cache_;
        }
        inline common::ObObjPool<mergeserver::ObMsSqlScanRequest> & get_scan_req_pool()
        {
          return scan_req_pool_;
        }
        inline common::ObObjPool<mergeserver::ObMsSqlGetRequest> & get_get_req_pool()
        {
          return get_req_pool_;
        }
        inline ObInsertCache & get_insert_cache()
        {
          return insert_cache_;
        }
<<<<<<< HEAD
        //add huangcc [statistic information cache] 20170317:b
        inline ObStatisticInfoCache* get_statistic_info_cache()
        {
          return &statistic_info_cache_;
        }
        //add:e
=======
>>>>>>> refs/remotes/origin/master
        inline ObBloomFilterTaskQueueThread & get_bloom_filter_task_queue_thread()
        {
          return bloom_filter_queue_thread_;
        }
<<<<<<< HEAD
        //add by wangdonghui [procedure physical plan cache] 20160229 :b
        /**
         * @brief get_procedure_manager
         * get procedure manager object
         * @return procedure manager object
         */
        inline mergeserver::ObProcedureManager & get_procedure_manager()
        {
          return procedure_manager_;
        }

        //add :e
=======
>>>>>>> refs/remotes/origin/master

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMergeServer);
        int init_root_server();
        int set_self(const char* dev_name, const int32_t port);
        // handle no response request add timeout as process time for monitor info
        void handle_no_response_request(common::ObPacket * base_packet);
        void on_ioth_start();

      private:
        static const int64_t DEFAULT_LOG_INTERVAL = 100;
        int64_t log_count_;
        int64_t log_interval_count_;
        /* ObMergeServerParams& ms_params_; */
        ObConfigManager& config_mgr_;
        ObMergeServerConfig& ms_config_;
        common::ObTimer task_timer_;
        common::ThreadSpecificBuffer response_buffer_;
        common::ThreadSpecificBuffer rpc_buffer_;
        common::ObClientManager client_manager_;
        common::ObServer self_;
        common::ObServer root_server_;
        ObMergeServerService service_;
        common::ObPrivilegeManager privilege_mgr_;
        ObFrozenDataCache frozen_data_cache_;
        ObInsertCache insert_cache_;
<<<<<<< HEAD
        //add huangcc [statistic information cache] 20170317:b
        ObStatisticInfoCache statistic_info_cache_;
        //add:e
=======
>>>>>>> refs/remotes/origin/master
        ObBloomFilterTaskQueueThread bloom_filter_queue_thread_;
        common::ObObjPool<mergeserver::ObMsSqlScanRequest> scan_req_pool_;
        common::ObObjPool<mergeserver::ObMsSqlGetRequest> get_req_pool_;

<<<<<<< HEAD

        //add by wangdonghui 20160229 [procedure physical plan cache] :b
        ObProcedureManager procedure_manager_;   ///<  procedure manager
        //add :e
=======
>>>>>>> refs/remotes/origin/master
    };
  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_MERGESERVER_H_ */
