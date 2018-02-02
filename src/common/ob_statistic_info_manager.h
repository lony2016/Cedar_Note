/**/
#ifndef OB_STATISTIC_INFO_MANAGER_H
#define OB_STATISTIC_INFO_MANAGER_H

#include <tblog.h>
#include "ob_define.h"
#include "ob_server.h"
#include "ob_array_helper.h"
#include "page_arena.h"
#include "ob_range2.h"
#include "ob_vector.h"
#include "ob_se_array.h"
#include "hash/ob_hashmap.h"
#include "ob_statistic_info.h"
#include "ob_statistic_report_info.h"
namespace oceanbase
{
  namespace common
  {
  struct StatisticTableColumnKey
  {
    uint64_t table_id_;
    uint64_t column_id_;

    int64_t hash() const
    {
      hash::hash_func<uint64_t> hash_uint64;
      return hash_uint64(table_id_) + hash_uint64(column_id_);
    }

    bool operator==(const StatisticTableColumnKey& key) const
    {
      bool ret = false;

      if (table_id_ == key.table_id_ && column_id_ == key.column_id_)
      {
        ret = true;
      }

      return ret;
    }
  };
  struct StatisticHistogramNode
  {
     int64_t version_;
     ObNewRange range_;
     ObTabletStatisticHistogram shist_;
     struct StatisticHistogramNode* next_;
     StatisticHistogramNode():next_(NULL)
     {
     }
     void dump() const
     {
       TBSYS_LOG(INFO,"StatisticHistogramNode's version_ = %ld",version_);
       TBSYS_LOG(INFO,"StatisticHistogramNode's range_ = %s",to_cstring(range_));
       shist_.dump();
     }
  };
  /*
   * 存储一个table所有CS上的tablet的统计信息直方图信息
   */
  class ObTabletStatisticHistogramManager
  {


    public:
      ObTabletStatisticHistogramManager();

      ObTabletStatisticHistogram* alloc_statistic_hist_object();
      int add_statistic_histogram(const ObTabletStatisticHistogram &histogram, int32_t &out_index, uint64_t &idx_tid);
      int get_statistic_histogram(const int32_t statistic_hist_index, ObTabletStatisticHistogram *&out_hist) const;
      void dump(const int32_t hist_index) const;

      int add_node_to_hist_list(const ObTabletStatisticHistogramRpInfo &report_info)
      {
          int ret = OB_SUCCESS;
          StatisticHistogramNode* tmp;
          StatisticHistogramNode* node = OB_NEW(StatisticHistogramNode,ObModIds::OB_TABLET_STATISTIC_HISTOGRAM_REPORT);
          node->shist_.deep_copy(module_arena_ ,report_info.statistic_histogram_);
          //node->range_=report_info.tablet_info_.range_;
          deep_copy_range(module_arena_,report_info.tablet_info_.range_,node->range_);
          node->version_=report_info.tablet_location_.tablet_version_;
          node->next_ = NULL;
          StatisticTableColumnKey key;
          if(!statistic_hash_init_)
          {
            TBSYS_LOG(WARN,"statistic_histogram_hashmap is not init!");
            ret=OB_ERROR;
          }
          else if(OB_INVALID_ID != report_info.statistic_histogram_.get_table_id()&&OB_INVALID_ID != report_info.statistic_histogram_.get_column_id())
          {
              key.column_id_ = report_info.statistic_histogram_.get_column_id();
              key.table_id_ = report_info.statistic_histogram_.get_table_id();
              if(hash::HASH_NOT_EXIST == statistic_histogram_hashmap_.get(key,tmp))
              {
                  StatisticTableColumnKey* key_v1 =OB_NEW(StatisticTableColumnKey,ObModIds::OB_TABLET_STATISTIC_HISTOGRAM_REPORT);
                  StatisticHistogramNode* head = OB_NEW(StatisticHistogramNode,ObModIds::OB_TABLET_STATISTIC_HISTOGRAM_REPORT);
		  head->version_ = -1;
                  head->next_ = node;
		  key_v1->column_id_ = key.column_id_;
                  key_v1->table_id_ = key.table_id_;
                  if(hash::HASH_INSERT_SUCC == statistic_histogram_hashmap_.set(*key_v1,head))
                  {
                      ret = OB_SUCCESS;
                      TBSYS_LOG(INFO,"insert node to statistic_histogram_hashmap_ SUCCESS!");
                  }
                  else
                  {
                      ret = OB_ERROR;
                      TBSYS_LOG(ERROR,"insert node to statistic_histogram_hashmap_ failed");
                  }
              }
              else if(hash::HASH_EXIST == statistic_histogram_hashmap_.get(key,tmp))
              {
                  if(report_info.is_tablet_unchanged)
                  {
                     inc_version(node,tmp);
                  }
                  else
                  {
                      //++node->version_;
                      node->next_ = tmp->next_;
                      tmp->next_ = node;
                      ret = OB_SUCCESS;
                      TBSYS_LOG(INFO,"add to the table hist list SUCCESS!");
                  }

              }
              else
              {
                  ret = OB_ERROR;
              }

          }
          return ret;
      }
      void inc_version(StatisticHistogramNode* node,StatisticHistogramNode* head)
      {
          if(NULL==head || NULL==node)
          {
             TBSYS_LOG(WARN,"head or node is null");
          }
          else
          {
              StatisticHistogramNode* tmp = head->next_;
              while( NULL!=tmp)
              {
                TBSYS_LOG(INFO,"test::weixing tmp range = %s",to_cstring(tmp->range_));
                if(node->range_.equal(tmp->range_))
                {
                  ++tmp->version_;
                  break;
                }
                else
                {
                  tmp = tmp->next_;
                }
              }
          }

      }
      StatisticHistogramNode* get_node_from_map(uint64_t table_id,uint64_t column_id)
      {
        TBSYS_LOG(ERROR,"get_statistic_histograms");

        StatisticTableColumnKey key;
        StatisticHistogramNode* out=NULL;
        if(!statistic_hash_init_)
        {
          TBSYS_LOG(WARN,"statistic_histogram_hashmap is not init!");
        }
        else if(OB_INVALID_ID ==table_id ||OB_INVALID_ID ==column_id)
        {
            TBSYS_LOG(WARN,"table_id or column_id invalid!");
        }
        else
        {
            key.table_id_=table_id;
            key.column_id_=column_id;

            if(-1== statistic_histogram_hashmap_.get(key,out))
            {
              TBSYS_LOG(ERROR,"get statistic histograms err");
            }

        }
        return out;

      }

      int add_tid(uint64_t tid,uint64_t cid)
      {
          int ret = OB_SUCCESS;
          bool exist=false;
          for(int64_t i=0;i<keys_helper_.get_array_index();i++)
          {
              if(keys[i].table_id_==tid && keys[i].column_id_==cid)
              {
                  exist=true;
                  break;
              }
          }
          if(!exist)
          {
              tbsys::CWLockGuard guard(lock_);
              StatisticTableColumnKey key;
              key.table_id_=tid;
              key.column_id_=cid;
              if (!keys_helper_.push_back(key))
              {
                ret = OB_ARRAY_OUT_OF_RANGE;
              }
          }

          return ret;
      }
      common::ObArrayHelper<StatisticTableColumnKey> get_keys_helper()
      {
          return keys_helper_;
      }
      StatisticTableColumnKey get_key(int64_t index)
      {
          if(OB_INVALID_INDEX!=index)
          {
            return keys[index];
          }
          else
          {
            return keys[0];
          }
      }

      int set_table_id(const uint64_t tid, const uint64_t index_tid)
      {
        int ret = OB_SUCCESS;
        if (OB_INVALID_ID == tid || OB_INVALID_ID == index_tid)
        {
          ret = OB_INVALID_ARGUMENT;
          TBSYS_LOG(WARN, "invalid tid, tid=%lu, index_tid=%lu", tid, index_tid);
        }
        else
        {
          table_id_ = tid;
          index_tid_ = index_tid;
        }
        return ret;
      }
      uint64_t get_table_id() const
      {
        return table_id_;
      }
      uint64_t get_index_tid() const
      {
        return index_tid_;
      }
      void reuse();

      //add weixing [statistic info build]20161122:b
      void del_sur_plus_node(const int64_t lastest_version)
      {
	TBSYS_LOG(INFO,"start to del node!");
        hash::ObHashMap<StatisticTableColumnKey,StatisticHistogramNode*,hash::NoPthreadDefendMode>::const_iterator iter = statistic_histogram_hashmap_.begin();
        for (;iter != statistic_histogram_hashmap_.end(); ++iter)
        {
	  TBSYS_LOG(INFO,"start to in for del node!");
          StatisticHistogramNode* top =iter->second;
          StatisticHistogramNode* index = top->next_;
          StatisticHistogramNode* pre =NULL;
          if(top->next_ != NULL)
          {
            if(top->next_->next_ == NULL)
            {
              if(top->next_->version_ != lastest_version)
              {
		TBSYS_LOG(INFO,"DEL node ");
                index->dump();
                OB_DELETE(StatisticHistogramNode,ObModIds::OB_TABLET_STATISTIC_HISTOGRAM_REPORT,index);
                top->next_ = NULL;
              }
            }
            else
            {
              pre = top;
              while(index != NULL)
              {
                if(index->version_ != lastest_version)
                {
		  TBSYS_LOG(INFO,"DEL node ");
                  index->dump();
                  pre->next_ =index->next_;
                  OB_DELETE(StatisticHistogramNode,ObModIds::OB_TABLET_STATISTIC_HISTOGRAM_REPORT,index);
                  index = pre->next_;
                }
                else
                {
                  pre = pre->next_;
                  index =index->next_;
                }
              }
            }
          }
          
        }
      }
      void dump_list() const
      {
	TBSYS_LOG(INFO,"start to dump node!");
        hash::ObHashMap<StatisticTableColumnKey,StatisticHistogramNode*,hash::NoPthreadDefendMode>::const_iterator iter = statistic_histogram_hashmap_.begin();
        for (;iter != statistic_histogram_hashmap_.end(); ++iter)
        {
          StatisticHistogramNode* top =iter->second;
          StatisticHistogramNode* index = top->next_;
          while(index != NULL)
          {
            index->dump();
            index = index->next_;
          }
        }
      }
      //add weixing 20161122:e
    public:
      static const int64_t MAX_TABLET_COUNT_PER_TABLE = 4 * 1024 * 1024;
      common::ModulePageAllocator mod_;
      common::ModuleArena module_arena_;

    private:
      friend class ObTabletStatisticHistogram;
      ObTabletStatisticHistogram* statistic_histograms_[MAX_TABLET_COUNT_PER_TABLE];
      common::ObArrayHelper<ObTabletStatisticHistogram*> statistic_histogram_helper_;
      uint64_t table_id_;
      uint64_t index_tid_;//有必要改动
      mutable tbsys::CThreadMutex alloc_mutex_;//线程锁

      hash::ObHashMap<StatisticTableColumnKey,StatisticHistogramNode*,hash::NoPthreadDefendMode> statistic_histogram_hashmap_;
      volatile bool statistic_hash_init_;

      StatisticTableColumnKey keys[OB_MAX_TABLE_NUMBER];
      common::ObArrayHelper<StatisticTableColumnKey> keys_helper_;
      mutable tbsys::CRWLock lock_;
  };

  inline ObTabletStatisticHistogramManager::ObTabletStatisticHistogramManager()
    :mod_(ObModIds::OB_TABLET_STATISTIC_HISTOGRAM_REPORT),
    module_arena_(ModuleArena::DEFAULT_PAGE_SIZE, mod_),
    table_id_(OB_INVALID_ID), index_tid_(OB_INVALID_ID),
    statistic_hash_init_(false)
  {
    statistic_histogram_helper_.init(MAX_TABLET_COUNT_PER_TABLE, statistic_histograms_);
    if(OB_SUCCESS!= statistic_histogram_hashmap_.create(hash::cal_next_prime(common::OB_MAX_TABLE_NUMBER)))
    {
       TBSYS_LOG(WARN, "statistic_histogram_hashmap_ creat failed");
    }
    else
    {
      statistic_hash_init_=true;
    }
    keys_helper_.init(OB_MAX_TABLE_NUMBER,keys);
  }

  }
}
#endif
