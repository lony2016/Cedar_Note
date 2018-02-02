/*
 *author:weixing [statistics.build]
 * date:20170103
 *
 *
*/
#ifndef OB_GATHER_TABLE_INFO_H
#define OB_GATHER_TABLE_INFO_H

#include <tblog.h>
#include "ob_define.h"
#include "ob_array_helper.h"
#include "page_arena.h"
#include "common/ob_object.h"


namespace oceanbase
{
  namespace common
  {
    struct ObGatherTableInfo
    {
      uint64_t table_id_;
      uint64_t columns_[OB_MAX_COLUMN_NUMBER];
      ObArrayHelper<uint64_t> columns_list_;
      ModuleArena allocator_;

      ObGatherTableInfo()
      {
        reset();
      }

      ObGatherTableInfo(const ObGatherTableInfo &other)
      {
        columns_list_.init(OB_MAX_COLUMN_NUMBER,columns_);
        columns_list_.clear();
        table_id_ = other.table_id_;
        if(0 <= other.columns_list_.get_array_index())
        {
          for(int64_t i = 0; i < other.columns_list_.get_array_index();i++)
          {
            columns_list_.push_back(other.columns_[i]);
          }
        }
      }

      ObGatherTableInfo& operator=(const ObGatherTableInfo &other)
      {
        if(OB_LIKELY(this != &other))
        {
          this->table_id_ = other.table_id_;
          columns_list_.clear();
          if(0 <= other.columns_list_.get_array_index())
          {
            for(int64_t i = 0; i < other.columns_list_.get_array_index();i++)
            {
              this->columns_list_.push_back(other.columns_[i]);
            }
          }
        }
        return *this;
      }


      void reset()
      {
        table_id_ = OB_INVALID_ID;
        columns_list_.init(OB_MAX_COLUMN_NUMBER,columns_);
        columns_list_.clear();
      }

      int set_table_id(const uint64_t table_id)
      {
        int ret = OB_SUCCESS;
        table_id_ = table_id;
        return ret;
      }

      uint64_t get_table_id() const
      {
        return table_id_;
      }

      int add_column_id(const uint64_t column_id)
      {
        int ret = OB_SUCCESS;
        if(!( columns_list_.push_back(column_id)))
        {
          ret = OB_ARRAY_OUT_OF_RANGE;
        }
        return ret;
      }

      uint64_t get_column_id(int64_t idx) const
      {
        if(0 <= idx || columns_list_.get_array_index() > idx)
        {
          return columns_[idx];
        }
        else
        {
          return OB_INVALID_ID;
        }
      }
      void dump() ;
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    struct ObTopValue
    {
        common::ObObj obj_;
        int64_t num_;
        common::ModuleArena allocator_;
        ObTopValue()
        {
          num_ = 0;
        }

        ObTopValue(const ObTopValue& other)
        {
          ob_write_obj(allocator_,other.obj_,obj_);
          num_ = other.num_;
        }

        ~ObTopValue()
        {
        }

        ObTopValue& operator=(const ObTopValue &other)
        {
           ob_write_obj(allocator_,other.obj_,obj_);
           num_ = other.num_;
           return *this;
        }
    };
  }
}

#endif // OB_GATHER_TABLE_INFO_H
