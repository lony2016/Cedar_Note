/**
 * Copyright (C) 2013-2016 ECNU_DaSE.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_merge_service.h
 * @brief merge server support service,deal request from other server
 *
 * modified by wangdonghui:add 3 functions for procedure cache management in ms
 *
 * @version __DaSE_VERSION
 * @author zhutao <zhutao@stu.ecnu.edu.cn>
 * @author wangdonghui <zjnuwangdonghui@163.com>
 *
 * @date 2016_07_26
 */

#ifndef OCEANBASE_MERGESERVER_SERVICE_H_
#define OCEANBASE_MERGESERVER_SERVICE_H_

#include "common/ob_define.h"
#include "common/data_buffer.h"
#include "common/ob_obi_role.h"
#include "common/thread_buffer.h"
#include "common/ob_session_mgr.h"
#include "common/nb_accessor/ob_nb_accessor.h"
#include "common/ob_config_manager.h"
#include "common/ob_version.h"
#include "ob_ms_schema_task.h"
#include "ob_ms_monitor_task.h"
#include "ob_ms_service_monitor.h"
#include "ob_ms_lease_task.h"
#include "ob_ms_ups_task.h"
#include "ob_ms_sql_proxy.h"
#include "ob_query_cache.h"
#include "onev_struct.h"
#include "ob_merge_server_config.h"
#include "common/ob_privilege_manager.h"
#include "common/ob_statistics.h"
#include "ob_get_privilege_task.h"
#include "common/ob_name_code_map.h"

namespace oceanbase
{
  namespace common
  {
    class ObMergerSchemaManager;
    class ObTabletLocationCache;
    class ObTabletLocationCacheProxy;
    class ObGeneralRpcStub;
  }
  namespace mergeserver
  {
    class ObMergeServer;
    class ObMergerRpcProxy;
    class ObMergerRootRpcProxy;
    class ObMergerAsyncRpcStub;
    class ObMergerSchemaProxy;
    static const int32_t RESPONSE_PACKET_BUFFER_SIZE = 1024*1024*2; //2MB
    static const int64_t RETRY_INTERVAL_TIME = 1000 * 1000; // usleep 1 s
    class ObMergeServerService: public ObVersionProvider
    {
      public:
        ObMergeServerService();
        ~ObMergeServerService();

      public:
        int initialize(ObMergeServer* merge_server);
        int start();
        int destroy();
        bool check_instance_role(const bool read_master) const;

      public:
        /// extend lease valid time = sys.cur_timestamp + delay
        void extend_lease(const int64_t delay);

        /// check lease expired
        bool check_lease(void) const;

        /// register to root server
        int register_root_server(void);

        sql::ObSQLSessionMgr* get_sql_session_mgr() const;
        //add by wangdonghui :b
        /**
         * @brief get_sql_proxy_
         * get sql_proxy_
         * @return  ObMsSQLProxy sql_proxy_
         */
        ObMsSQLProxy get_sql_proxy_() const;
        //add :e
        void set_sql_session_mgr(sql::ObSQLSessionMgr* mgr);
        void set_sql_id_mgr(sql::ObSQLIdMgr *mgr) {sql_id_mgr_ = mgr;};
        /* reload config after update local configuration succ */
        int reload_config();

        int do_request(
          const int64_t receive_time,
          const int32_t packet_code,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        //int get_agent(ObMergeJoinAgent *&agent);
        void handle_failed_request(const int64_t timeout, const int32_t packet_code);

        mergeserver::ObMergerRpcProxy  *get_rpc_proxy() const {return rpc_proxy_;}
        mergeserver::ObMergerRootRpcProxy * get_root_rpc() const {return root_rpc_;}
        mergeserver::ObMergerAsyncRpcStub   *get_async_rpc() const {return async_rpc_;}
        common::ObMergerSchemaManager *get_schema_mgr() const {return schema_mgr_;}
        common::ObTabletLocationCacheProxy *get_cache_proxy() const {return cache_proxy_;}
        common::ObStatManager *get_stat_manager() const { return service_monitor_; }

        //add huangcc [statistic information cache] 20170317:b
        mergeserver::ObMsSQLProxy *get_sql_proxy() {return &sql_proxy_;}
        //add:e
        //add by wangdonghui 20160302 [ppc manager] :b
        /**
         * @brief get_merge_server
         * get merge_server_
         * @return ObMergeServer merge_server_
         */
        ObMergeServer *get_merge_server() const { return merge_server_; }
        /**
         * @brief fetch_source
         * fecth procedure name code map when ms start or restart
         * @param name_code_map
         * @return error code
         */
        int fetch_source(common::ObNameCodeMap * name_code_map);
        //add :e

        //add by qx 20160830 :b
        /**
         * @brief get_ups_state
         * get ups online or offline state
         * @return ups state
         */
        inline bool get_ups_state() const
        {
          return ups_state_;
        }
        /**
         * @brief set_ups_state
         * set ups online or offline state
         * @param ups_state
         */
        inline void set_ups_state(bool ups_state)
        {
          ups_state_=ups_state;
        }
        //add :e

        const common::ObVersion get_frozen_version() const
        {
          return frozen_version_;
        }

        ObMergeServerConfig& get_config();
        const ObMergeServerConfig& get_config() const;
      private:
        // lease init 10s
        static const int64_t DEFAULT_LEASE_TIME = 10 * 1000 * 1000L;

        // warning: fetch schema interval can not be too long
        // because of the heartbeat handle will block tbnet thread
        static const int64_t FETCH_SCHEMA_INTERVAL = 30 * 1000;

        // list sessions
        int ms_list_sessions(
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer);
        // list sessions
        int ms_kill_session(
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer);
        // kill sql session
        int ms_sql_kill_session(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us
          );
        //add weixing [statistics build v1]20170406:b
        int ms_fetch_collection_list(
            const int64_t receive_time,
            const int32_t version,
            const int32_t channel_id,
            onev_request_e* req,
            common::ObDataBuffer& in_buffer,
            common::ObDataBuffer& out_buffer,
            const int64_t timeout_us);
        //add e
        // heartbeat
        int ms_heartbeat(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // clear cache
        int ms_clear(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // monitor stat
        int ms_stat(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // reload conf
        int ms_reload_config(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // get query
        int ms_get(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        // scan query
        int ms_scan(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        int ms_sql_scan(
          const int64_t start_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        int ms_accept_schema(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);


        //add by wangdonghui 20160122 :b
        /**
         * @brief ms_accept_cache
         * mergeserver accept procedure name and source code
         * @param receive_time receive time
         * @param version rpc version
         * @param channel_id  tbnet need this packet channel_id
         * @param req packet request
         * @param in_buffer receive packet buffer
         * @param out_buffer databuffer
         * @param timeout_us timeout
         * @return error code
         */
        int ms_accept_cache(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);
        //add :e

        //add by wangdonghui 20160305 :b
        /**
         * @brief ms_delete_cache
         * destroy procedure
         * @param receive_time  receive time
         * @param version rpc version
         * @param channel_id  tbnet need this packet channel_id
         * @param req packet request
         * @param in_buffer receive packet buffer
         * @param out_buffer databuffer
         * @param timeout_us timeout
         * @return error code
         */
        int ms_delete_cache(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        //add :e

        //add by wdh 20160730 :b
        int ms_update_all_procedure(
                const int64_t receive_time,
                const int32_t version,
                const int32_t channel_id,
                onev_request_e* req,
                common::ObDataBuffer& in_buffer,
                common::ObDataBuffer& out_buffer,
                const int64_t timeout_us);

        //add :e
        int send_sql_response(
          onev_request_e* req,
          common::ObDataBuffer& out_buffer,
          ObSQLResultSet &result,
          int32_t channel_id,
          int64_t timeout_us);

        int ms_sql_execute(
          const int64_t start_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        int do_timeouted_req(
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer);

        // mutate update
        int ms_mutate(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        // change log level
        int ms_change_log_level(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        int ms_set_config(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

        int ms_get_config(
          const int64_t receive_time,
          const int32_t version,
          const int32_t channel_id,
          onev_request_e* req,
          common::ObDataBuffer& in_buffer,
          common::ObDataBuffer& out_buffer,
          const int64_t timeout_us);

      private:
        ObConfigManager& get_config_mgr();

        // init server properties
        int init_ms_properties_();

      private:
        DISALLOW_COPY_AND_ASSIGN(ObMergeServerService);
        ObMergeServer* merge_server_;
        bool inited_;
        // is registered or not
        volatile bool registered_;
        // lease timeout time
        int64_t lease_expired_time_;
        // instance role type
        common::ObiRole instance_role_;

        //add by qx 20160830 :b
        bool ups_state_;  ///<  ups online or offline state flag
        //add :e

      private:
        static const uint64_t MAX_INNER_TABLE_COUNT = 32;
        static const uint64_t MAX_ROOT_SERVER_ACCESS_COUNT = 32;
        static const int64_t REQUEST_EVENT_QUEUE_SIZE = 8192;
        //
        int64_t frozen_version_;
        ObMergerRpcProxy  *rpc_proxy_;
        oceanbase::common::ObGeneralRpcStub  *rpc_stub_;
        ObMsSQLProxy sql_proxy_;
        ObMergerAsyncRpcStub   *async_rpc_;
        common::ObMergerSchemaManager *schema_mgr_;
        ObMergerSchemaProxy *schema_proxy_;
        oceanbase::common::nb_accessor::ObNbAccessor *nb_accessor_;
        ObMergerRootRpcProxy * root_rpc_;
        ObMergerUpsTask fetch_ups_task_;
        ObMergerSchemaTask fetch_schema_task_;
        ObMergerProcedureTask fetch_procedure_task_;  //add wangdonghui [dev compile] 20160730
        ObMergerLeaseTask check_lease_task_;
        ObMergerMonitorTask monitor_task_;
        ObTabletLocationCache *location_cache_;
        common::ObTabletLocationCacheProxy *cache_proxy_;
        ObMergerServiceMonitor *service_monitor_;
        oceanbase::common::ObSessionManager session_mgr_;
        sql::ObSQLSessionMgr *sql_session_mgr_;
        ObQueryCache* query_cache_;
        common::ObPrivilegeManager *privilege_mgr_;
        ObGetPrivilegeTask update_privilege_task_;
        sql::ObSQLIdMgr *sql_id_mgr_;


    };

    inline void ObMergeServerService::extend_lease(const int64_t delay)
    {
      lease_expired_time_ = tbsys::CTimeUtil::getTime() + delay;
    }

    inline bool ObMergeServerService::check_lease(void) const
    {
      return tbsys::CTimeUtil::getTime() <= lease_expired_time_;
    }

    inline sql::ObSQLSessionMgr* ObMergeServerService::get_sql_session_mgr() const
    {
      return sql_session_mgr_;
    }
    //add by wangdonghui 20160320 :b
    /**
     * @brief ObMergeServerService::get_sql_proxy_
     * get sql_proxy_
     * @return ObMsSQLProxy sql_proxy_
     */
    inline ObMsSQLProxy ObMergeServerService::get_sql_proxy_() const
    {
        return sql_proxy_;
    }
    //add :e

    inline void ObMergeServerService::set_sql_session_mgr(sql::ObSQLSessionMgr* mgr)
    {
      sql_session_mgr_ = mgr;
    }

  } /* mergeserver */
} /* oceanbase */

#endif /* end of include guard: OCEANBASE_MERGESERVER_SERVICE_H_ */
