/*
 *  statistic info build
 *  author: weixing(simba_wei@outlook.com)
 *  20161025
*/
#ifndef OB_STATISTIC_INFO_H
#define OB_STATISTIC_INFO_H

#include <tblog.h>
#include "ob_define.h"
#include "ob_server.h"
#include "ob_array_helper.h"
#include "page_arena.h"
#include "ob_range2.h"
#include "ob_vector.h"
#include "ob_se_array.h"
#include "hash/ob_hashmap.h"

namespace oceanbase {
  namespace common {

    enum Column_Type{
      COLUMN_PK=1,
      COLUMN_INDEX=2,
      COLUMN_NORMAL=3,
    };

    //add dhc
    struct ObSortStatisticInfo{
      int64_t block_count_;
      ObNewRange block_range_;

      static bool cmp(const ObSortStatisticInfo *a, const ObSortStatisticInfo *b)
      {
        return (a->block_range_.end_key_ < b->block_range_.end_key_);
      };

      bool operator < (const ObSortStatisticInfo & other) const
      {
        if((block_range_.start_key_<other.block_range_.start_key_)||block_range_.start_key_.is_min_row())
            TBSYS_LOG(ERROR,"DHC true");
        else
            TBSYS_LOG(ERROR,"DHC false");
        return (block_range_.start_key_<other.block_range_.start_key_)||block_range_.start_key_.is_min_row();
      }

      ObSortStatisticInfo()
      {
        block_count_ = 0;
      }
    };
    //add e

    struct ObUserTabletInfo{
      uint64_t table_id_;
      int64_t tablet_version_;
      ObNewRange range_;
      ObUserTabletInfo():table_id_(OB_INVALID_ID)
      {
      }
      inline void set_table_id(const uint64_t table_id)
      {
        table_id_ = table_id;
      }
      inline uint64_t get_table_id(void)
      {
        return table_id_;
      }
      inline void set_tablet_version(const int64_t tablet_version)
      {
        tablet_version_ = tablet_version;
      }
      inline int64_t get_tablet_version(void)
      {
        return tablet_version_;
      }
      inline void set_range(const ObNewRange &range)
      {
        range_ = range;
      }
      inline const ObNewRange& get_range(void) const
      {
        return range_;
      }
      void dump() const;
      template <typename Allocator>
      int deep_copy(Allocator &allocator, const ObUserTabletInfo &other);
      NEED_SERIALIZE_AND_DESERIALIZE;
    };
    template <typename Allocator>
    inline int ObUserTabletInfo::deep_copy(Allocator &allocator, const ObUserTabletInfo &other)
    {
      int ret = OB_SUCCESS;
      this->table_id_ = other.table_id_;
      this->tablet_version_ = other.tablet_version_;
      ret = deep_copy_range(allocator,other.range_,this->range_);
      return ret;
    }



    //store statistic info from one table
    struct ObTabletStatisticInfo
    {
      uint64_t table_id_;//modify
      int64_t row_count_;
      int64_t row_size_;
      int64_t tablet_size_;
      ObNewRange range_;

      ObTabletStatisticInfo():table_id_(OB_INVALID_ID),row_count_(0),row_size_(0),tablet_size_(0)
      {
      }
      inline void set_range(const ObNewRange &range)
      {
        range_ = range;
      }
      inline const ObNewRange& get_range(void) const
      {
        return range_;
      }
      inline void set_table_id(const uint64_t table_id)
      {
        table_id_ = table_id;
      }
      inline uint64_t get_table_id(void) const
      {
        return table_id_;
      }
      inline void set_row_count(const int64_t row_count)
      {
        row_count_ = row_count;
      }
      inline int64_t get_row_count(void) const
      {
        return row_count_;
      }
      inline void set_row_size(const int64_t row_size)
      {
        row_size_ = row_size;
      }
      inline int64_t get_row_size(void) const
      {
        return row_size_;
      }
      inline void set_tablet_size(const int64_t tablet_size)
      {
        tablet_size_ = tablet_size;
      }
      inline int64_t get_tablet_size(void) const
      {
        return tablet_size_;
      }

      void dump() const;

      template <typename Allocator>
      int deep_copy(Allocator &allocator, const ObTabletStatisticInfo &other);
      NEED_SERIALIZE_AND_DESERIALIZE;
    };
    template <typename Allocator>
    inline int ObTabletStatisticInfo::deep_copy(Allocator &allocator, const ObTabletStatisticInfo &other)
    {
      int ret = OB_SUCCESS;
      this->table_id_ = other.table_id_;
      this->row_count_ = other.row_count_;
      this->row_size_ = other.row_size_;
      this->tablet_size_ =  other.tablet_size_;

      ret = deep_copy_range(allocator,other.range_,this->range_);
      return ret;
    }

    //store statistic info from one column
    struct ObColumnStatisticInfo
    {
      uint64_t table_id_;//modify
      uint64_t column_id_;//modify
      Column_Type column_type_;
      int64_t distinct_num_;
      ObNewRange range_;

      ObColumnStatisticInfo():table_id_(OB_INVALID_ID),column_id_(OB_INVALID_ID),column_type_(COLUMN_NORMAL),distinct_num_(0)
      {
      }
      inline void set_table_id(const uint64_t table_id)
      {
        table_id_ = table_id;
      }
      inline uint64_t get_table_id(void) const
      {
        return table_id_;
      }
      inline void set_column_id(const uint64_t column_id)
      {
        column_id_ = column_id;
      }
      inline uint64_t get_column_id(void) const
      {
        return column_id_;
      }
      inline void set_distinct_num(const int64_t distinct_num)
      {
        distinct_num_ = distinct_num;
      }
      inline void set_range(const ObNewRange &range)
      {
        range_ = range;
      }
      inline const ObNewRange& get_range(void) const
      {
        return range_;
      }

      void dump() const;

      template <typename Allocator>
      int deep_copy(Allocator &allocator, const ObColumnStatisticInfo &other);
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    template <typename Allocator>
    inline int ObColumnStatisticInfo::deep_copy(Allocator &allocator, const ObColumnStatisticInfo &other)
    {
      int ret = OB_SUCCESS;
      this->table_id_ = other.table_id_;
      this->column_id_ = other.column_id_;
      this->column_type_ = other.column_type_;
      this->distinct_num_ = other.distinct_num_;

      ret = deep_copy_range(allocator,other.range_,this->range_);
      return ret;
    }

    //store one sample for histogram
    struct ObTabletStatisticSample
    {
      ObNewRange range_;
      int64_t row_count_;

      ObTabletStatisticSample():row_count_(0)
      {
      }
      inline void set_range(const ObNewRange &range)
      {
        range_ = range;
      }
      inline const ObNewRange& get_range(void) const
      {
        return range_;
      }
      inline void set_row_count(const int64_t row_count)
      {
        row_count_ = row_count;
      }
      inline int64_t get_row_count(void) const
      {
        return row_count_;
      }

      void dump() const;

      template <typename Allocator>
      int deep_copy(Allocator &allocator, const ObTabletStatisticSample &other);
      NEED_SERIALIZE_AND_DESERIALIZE;
    };

    template <typename Allocator>
    inline int ObTabletStatisticSample::deep_copy(Allocator &allocator, const ObTabletStatisticSample &other)
    {
      int ret = OB_SUCCESS;
      this->row_count_ = other.row_count_;
      ret = deep_copy_range(allocator, other.range_, this->range_);
      return ret;
    }


    /*
     * 存储一个tablet的上某一列的统计等深直方图
    */
    class ObTabletStatisticHistogram
    {
      public:
        ObTabletStatisticHistogram()
          :table_id_(OB_INVALID_ID),column_id_(OB_INVALID_ID),allocator_(NULL),server_info_index_(OB_INVALID_INDEX)
        {
          init();
        }
        ObTabletStatisticHistogram(common::ModuleArena *allocator)
          :table_id_(OB_INVALID_ID),column_id_(OB_INVALID_ID),allocator_(allocator), server_info_index_(OB_INVALID_INDEX)
        {
          init();
        }
        ~ ObTabletStatisticHistogram()
        {
          allocator_ = NULL;
          server_info_index_ = OB_INVALID_INDEX;
        }

	bool compare(ObTabletStatisticHistogram other)
        {
          if(get_start_key() >= other.get_start_key() && get_start_key() <= other.get_end_key())
          {
            return true;
          }
          else if(get_end_key() >= other.get_start_key() && get_end_key() <= other.get_end_key())
          {
            return true;
          }
          else
          {
            return false;
          }
        }	

        void reset()
        {
          sample_helper_.clear();
          table_id_ = OB_INVALID_ID;
          column_id_ =OB_INVALID_ID;
        }

        void set_table_id(const uint64_t table_id)
        {
          table_id_ = table_id;
        }
        uint64_t get_table_id(void) const
        {
          return table_id_;
        }

        void set_column_id(const uint64_t column_id)
        {
          column_id_ = column_id;
        }
        uint64_t get_column_id(void) const
        {
          return column_id_;
        }

        void set_allocator(common::ModuleArena *allocator){
          allocator_ = allocator;
        }

        void init()
        {
          sample_helper_.init(MAX_SAMPLE_BUCKET, samples_);
        }

        int64_t get_sample_count() const
        {
          return sample_helper_.get_array_index();
        }

        void set_server_index(const int32_t server_index)
        {
          server_info_index_ = server_index;
        }
        int32_t get_server_index() const
        {
          return server_info_index_;
        }

	ObRowkey get_start_key()
        {
          return samples_[0].range_.start_key_;
        }
        ObRowkey get_end_key()
        {
          return samples_[sample_helper_.get_array_size()-1].range_.end_key_;
        }

        void dump() const;

        int get_sample(const int64_t sample_index, ObTabletStatisticSample *&return_sample);
        int get_sample(const int64_t sample_index, const ObTabletStatisticSample *&return_sample) const;
        int add_sample(const ObTabletStatisticSample &statistic_sample);

        int add_sample_with_deep_copy(const ObTabletStatisticSample &statistic_sample);
        template <typename Allocator>
        int add_sample_with_deep_copy(Allocator &allocator, const ObTabletStatisticSample &statistic_sample);

        int deep_copy(const ObTabletStatisticHistogram &other);
        template <typename Allocator>
        int deep_copy(Allocator &allocator, const ObTabletStatisticHistogram &other);

        NEED_SERIALIZE_AND_DESERIALIZE;//反序列化时必须给allocator_赋值!!!
      public:
        static const int64_t MAX_SAMPLE_BUCKET = 256;

      private:
        uint64_t table_id_;
        uint64_t column_id_;
        common::ModuleArena *allocator_;//add_sample_with_deep_copy或者反序列化时深拷贝用的内存分配器
        ObTabletStatisticSample samples_[MAX_SAMPLE_BUCKET];
        common::ObArrayHelper<ObTabletStatisticSample> sample_helper_;
        int32_t server_info_index_;//CS index
    };

    //add huangcc
    template <typename Allocator>
    int ObTabletStatisticHistogram::add_sample_with_deep_copy(Allocator &allocator, const ObTabletStatisticSample &index_sample)
    {
      int ret = OB_SUCCESS;
      ObTabletStatisticSample inner_sample;

      if (sample_helper_.get_array_index() >= MAX_SAMPLE_BUCKET){
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "too many sample in one histogram, max is %ld, now is %ld.", MAX_SAMPLE_BUCKET, sample_helper_.get_array_index());
      }
      else if(OB_SUCCESS != (ret = inner_sample.deep_copy(allocator, index_sample)))
      {
        TBSYS_LOG(WARN, "deep copy index sample failed!");
      }
      else if (!sample_helper_.push_back(inner_sample)) {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "can not push index_sample into sample_list_");
      }
      return ret;
    }
      /*
       * deep copy with specific allocator
       */
    template <typename Allocator>
    int ObTabletStatisticHistogram::deep_copy(Allocator &allocator, const ObTabletStatisticHistogram &other)
    {
      int ret = OB_SUCCESS;
      this->server_info_index_ = other.server_info_index_;
      int64_t size = other.get_sample_count();
      for (int64_t i = 0; i < size; i++)
      {
        const ObTabletStatisticSample *sample = NULL;
        if (OB_SUCCESS != (ret = other.get_sample(i, sample)))
        {
          TBSYS_LOG(WARN, "get sample failed");
          break;
        }
        else if (NULL == sample)
        {
          ret = OB_ERROR;
          TBSYS_LOG(WARN, "sample is NULL");
          break;
        }
        else if (OB_SUCCESS != (ret = add_sample_with_deep_copy(allocator, *sample))) {
          TBSYS_LOG(WARN, "add sample failed");
          break;
        }
      }
      return ret;
    }


  }
}



#endif // OB_STATISTIC_INFO_H
