/*
 *add by huangcc[statistic information cache]2017/03/24
 *
 */
#include "ob_statistic_info_cache.h"
#include "ob_analysis_statistic_info.h"


namespace oceanbase
{
  namespace mergeserver
  {

    ObStatisticInfoCache::ObStatisticInfoCache() :
        inited_(false)
    {

    }

    ObStatisticInfoCache::~ObStatisticInfoCache()
    {

    }

    int ObStatisticInfoCache::init(ObMsSQLProxy* sql_proxy,const int64_t max_cache_size)
    {
        int ret = OB_SUCCESS;    
        kvcolumncache_.init(max_cache_size);
        kvtablecache_.init(max_cache_size);
        sql_proxy_=sql_proxy;
//        if (NULL == (analysis_manager_ = OB_NEW(ObAnalysisStatisticInfo, ObModIds::OB_TABLET_STATISTIC_HISTOGRAM_REPORT)))
//        {
//          TBSYS_LOG(ERROR, "new ObTabletInfoManager error");
//        }
//        else
//        {
//            inited_=true;
//        }
        inited_=true;
        return ret;
    }

    int ObStatisticInfoCache::clear()
    {
        int ret=OB_SUCCESS;
        //analysis_manager_->reset();
        return ret;

    }

    int ObStatisticInfoCache::destroy()
    {
        int ret=OB_SUCCESS;
        return ret;
    }


    int ObStatisticInfoCache::put_analysised_column_sinfo_into_cache(uint64_t tid,uint64_t cid)
    {
        TBSYS_LOG(DEBUG, "huangcc_test::analysis_column_statistic_info");
        int ret = OB_SUCCESS;

        ObAnalysisStatisticInfo analysis_stat_info;
        ObAnalysisStatisticInfo * analysis_manager_ = NULL;
        analysis_manager_ = &analysis_stat_info;
        if(OB_SUCCESS!=(ret=analysis_manager_->analysis_column_statistic_info(tid,cid,sql_proxy_)))
        {
            TBSYS_LOG(ERROR, "fail to analysis_column_statistic_info");
        }
        else if(analysis_manager_->get_row_count()==0)
        {
            TBSYS_LOG(WARN, "the column[%lu] of table[%lu] no statistic info ",cid,tid);
            ret=OB_ERROR;
        }
        else
        {
            StatisticColumnValue stv;
            ObTopValue* top_value=analysis_manager_->get_top_value();
            for(int i=0;i<analysis_manager_->get_top_value_num();i++)
            {
                stv.top_value_[i]=top_value[i];
            }
            stv.top_value_num_=analysis_manager_->get_top_value_num();
            common::ModuleArena allocator;
            ob_write_obj(allocator,analysis_manager_->get_min(),stv.min_);
            ob_write_obj(allocator,analysis_manager_->get_max(),stv.max_);
            stv.row_count_=analysis_manager_->get_row_count();
            stv.different_num_ = analysis_manager_->get_different_num();
            stv.size_=analysis_manager_->get_size();

            //StatisticColumnValue otv;
            if(!inited_)
            {
                TBSYS_LOG(WARN,"have not init!");
                ret=OB_ERROR;
            }
            else if(OB_SUCCESS!=(ret=add_column_statistic_info_into_cache(tid,cid,stv)))
            {
                TBSYS_LOG(WARN,"failed to add top_value into map!,ret=[%d]",ret);
            }
        }
        //analysis_manager_->reset();
        return ret;
    }
    int ObStatisticInfoCache::put_analysised_table_sinfo_into_cache(uint64_t tid)
    {
        //TBSYS_LOG(INFO, "huangcc_test::analysis_table_statistic_info");
        int ret = OB_SUCCESS;
        ObAnalysisStatisticInfo analysis_stat_info;
        ObAnalysisStatisticInfo * analysis_manager_ = NULL;
        analysis_manager_ = &analysis_stat_info;
        if(OB_SUCCESS!=(ret=analysis_manager_->analysis_table_statistic_info(tid,sql_proxy_)))
        {
            TBSYS_LOG(ERROR, "fail to analysis_table_statistic_info");
        }
        else if(analysis_manager_->get_row_count()==0)
        {
            TBSYS_LOG(WARN, "table[%lu] no statistic info or is a null table",tid);
            ret=OB_ERROR;
        }
        else
        {
            StatisticTableValue stv;

            stv.row_count_=analysis_manager_->get_row_count();
            stv.size_=analysis_manager_->get_size();
            stv.statistic_columns_num_=analysis_manager_->get_statistic_columns_num();
            uint64_t * statistic_columns=analysis_manager_->get_statistic_columns();

            if(stv.row_count_==0)
            {
                stv.mean_row_size_=0;
            }
            else
            {
                stv.mean_row_size_=(stv.size_)/(stv.row_count_);
            }

            for(int i=0;i<stv.statistic_columns_num_;i++)
            {
                stv.statistic_columns_[i]=statistic_columns[i];
            }

            if(!inited_)
            {
                TBSYS_LOG(WARN,"table_statistic_info_hashmap is not init!");
                ret=OB_ERROR;
            }
            else if(OB_SUCCESS!=(ret=add_table_statistic_info_into_cache(tid,stv)))
            {
                TBSYS_LOG(WARN,"failed to add table_statistic_info into map!,ret=[%d]",ret);
            }  
        }
        //analysis_manager_->reset();
        return ret;
    }
    int ObStatisticInfoCache::add_column_statistic_info_into_cache(uint64_t tid,uint64_t cid,StatisticColumnValue &scv)
     {
         int ret=OB_SUCCESS;
         StatisticColumnKey key;
         if(!inited_)
         {
           TBSYS_LOG(WARN,"have not init!");
           ret=OB_ERROR;
         }
         else if(OB_INVALID_ID ==tid ||OB_INVALID_ID ==cid)
         {
             TBSYS_LOG(WARN,"table_id or column_id invalid!");
             ret=OB_ERROR;
         }
         else
         {
             key.table_id_ = tid;
             key.column_id_ = cid;
             if(OB_SUCCESS!=(ret=kvcolumncache_.put(key,scv,true)))
             {
               TBSYS_LOG(WARN, "failed to put column statistic info to cache, err=%d", ret);
             }
         }
         return ret;
     }
    int ObStatisticInfoCache::get_column_statistic_info_from_cache(uint64_t tid,uint64_t cid,StatisticColumnValue &scv)
    {
        int ret = OB_SUCCESS;
        StatisticColumnKey key;
        Handle handle;
        if(!inited_)
        {
          TBSYS_LOG(WARN,"have not init!");
          ret=OB_ERROR;
        }
        else if(OB_INVALID_ID ==tid ||OB_INVALID_ID ==cid)
        {
            TBSYS_LOG(WARN,"table_id or column_id invalid!");
            ret=OB_ERROR;
        }
        else
        {
            key.table_id_=tid;
            key.column_id_=cid;
            TBSYS_LOG(DEBUG,"huangcc_test::tid=[%lu],cid=[%lu]",key.table_id_,key.column_id_);
            if(OB_SUCCESS== (ret = kvcolumncache_.get(key,scv,handle)))
            {
              TBSYS_LOG(INFO,"get column statistic info success");
            }
            else if(ret==OB_ENTRY_NOT_EXIST)
            {
              //try to get lastest statistic info
              if (OB_SUCCESS!=(ret = put_analysised_column_sinfo_into_cache(tid,cid)))
              {
                TBSYS_LOG(WARN,"failed to put column statistic info");
              }
              else if(OB_SUCCESS== (ret = kvcolumncache_.get(key,scv,handle)))
              {
                TBSYS_LOG(INFO,"get column statistic info success");
              }
              else if(ret==OB_ENTRY_NOT_EXIST)
              {
                TBSYS_LOG(WARN,"the column statistic info not exist");
              }
              else
              {
                TBSYS_LOG(ERROR,"get column statistic info err = %d",ret);
              }
            }
            else
            {
              TBSYS_LOG(ERROR,"get column statistic info err = %d",ret);
            }

            if (OB_SUCCESS == ret)
            {
                for(int i=0;i<scv.top_value_num_;i++)
                {
                    TBSYS_LOG(DEBUG,"huangcc_test::obj_=[%s],num_=[%ld]",to_cstring(scv.top_value_[i].obj_),scv.top_value_[i].num_);
                }
                TBSYS_LOG(DEBUG,"huangcc_test::min=[%s],max=[%s]",to_cstring(scv.min_),to_cstring(scv.max_));
                TBSYS_LOG(DEBUG,"huangcc_test::top_value_num=[%ld],row_count=[%ld],different_num=[%ld],size=[%ld]",scv.top_value_num_,scv.row_count_,scv.different_num_,scv.size_);
                kvcolumncache_.revert(handle);
            }

        }
        return ret;
    }
    int ObStatisticInfoCache::add_table_statistic_info_into_cache(uint64_t tid,StatisticTableValue &stv)
    {
        int ret=OB_SUCCESS;
        if(!inited_)
        {
            TBSYS_LOG(WARN,"have not init!");
            ret=OB_ERROR;
        }
        else if(OB_INVALID_ID ==tid)
        {
            TBSYS_LOG(WARN,"table_id invalid!");
            ret=OB_ERROR;
        }
        else
        {
            //TBSYS_LOG(INFO,"huangcc_test::statistic_columns_num=[%ld],row_count=[%ld],size=[%ld],mean_row_size=[%ld]",stv.statistic_columns_num_,stv.row_count_,stv.size_,stv.mean_row_size_);
            if(OB_SUCCESS!=(ret=kvtablecache_.put(tid,stv,true)))
            {
              TBSYS_LOG(WARN, "failed to put table statistic info to cache, err=%d", ret);
            }
        }
        return ret;
    }
    int ObStatisticInfoCache::get_table_statistic_info_from_cache(uint64_t tid,StatisticTableValue &stv)
    {
        int ret=OB_SUCCESS;
        Handle handle;
        if(!inited_)
        {
          TBSYS_LOG(WARN,"have not init!");
          ret=OB_ERROR;
        }
        else if(OB_INVALID_ID ==tid)
        {
          TBSYS_LOG(WARN,"table_id invalid!");
        }
        else if(OB_SUCCESS== (ret = kvtablecache_.get(tid,stv,handle)))
        {        
          TBSYS_LOG(INFO,"get table statistic info success");
        }
        else if(ret==OB_ENTRY_NOT_EXIST)
        {
          //try to get lastest table statistic info
          if (OB_SUCCESS!= (ret = put_analysised_table_sinfo_into_cache(tid)))
          {
            TBSYS_LOG(WARN,"failed to put table statistic info ");
          }
          else if (OB_SUCCESS == (ret = kvtablecache_.get(tid,stv,handle)))
          {
            TBSYS_LOG(INFO,"get table statistic info success");
          }
          else if(ret==OB_ENTRY_NOT_EXIST)
          {
            TBSYS_LOG(WARN,"the table statistic info not exist");
          }
          else
          {
            TBSYS_LOG(ERROR,"get table statistic info err=%d",ret);
          }
        }
        else
        {
          TBSYS_LOG(ERROR,"get table statistic info err=%d",ret);
        }
        //else
        if (OB_SUCCESS == ret)
        {
            for(int i=0;i<stv.statistic_columns_num_;i++)
            {
                TBSYS_LOG(DEBUG,"huangcc_test::statistic_columns_id=[%lu]",stv.statistic_columns_[i]);
            }
            TBSYS_LOG(DEBUG,"huangcc_test::statistic_columns_num=[%ld],row_count=[%ld],size=[%ld],mean_row_size=[%ld]",stv.statistic_columns_num_,stv.row_count_,stv.size_,stv.mean_row_size_);
            kvtablecache_.revert(handle);
        }
        return ret;
    }
  }
}

