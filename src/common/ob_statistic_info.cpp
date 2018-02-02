/*
 *  statistic info build
 *  author: weixing(simba_wei@outlook.com)
 *  20161025
*/

#include "ob_statistic_info.h"

namespace oceanbase {
  namespace common {

    void ObUserTabletInfo::dump() const
    {
      TBSYS_LOG(INFO,"table_id = %lu",table_id_);
      TBSYS_LOG(INFO,"tablet_version = %ld",tablet_version_);
      TBSYS_LOG(INFO,"range = %s",to_cstring(range_));
    }

    void ObTabletStatisticHistogram::dump() const
    {
      for (int32_t i = 0; i < sample_helper_.get_array_index(); i++)
      {
        TBSYS_LOG(DEBUG, "dump %d-th sample.", i);
        samples_[i].dump();
      }
    }

    int ObTabletStatisticHistogram::get_sample(const int64_t sample_index, ObTabletStatisticSample *&return_sample)
    {
      int ret = OB_SUCCESS;
      if (sample_index < 0 || sample_index >= sample_helper_.get_array_index())
      {
        TBSYS_LOG(WARN, "array index out of range.");
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else {
        return_sample = samples_ + sample_index;
      }
      return ret;
    }
    //add huangcc
    int ObTabletStatisticHistogram::get_sample(const int64_t sample_index, const ObTabletStatisticSample *&return_sample) const
    {
      int ret = OB_SUCCESS;
      if (sample_index < 0 || sample_index >= sample_helper_.get_array_index())
      {
        TBSYS_LOG(WARN, "array index out of range.");
        ret = OB_ARRAY_OUT_OF_RANGE;
      }
      else {
        return_sample = samples_ + sample_index;
      }
      return ret;
    }
    //add e

    /*
     * 浅拷贝
     */
    int ObTabletStatisticHistogram::add_sample(const ObTabletStatisticSample &statistic_sample)
    {
      int ret = OB_SUCCESS;
      if (sample_helper_.get_array_index() >= MAX_SAMPLE_BUCKET)
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "too many sample in one histogram, max is %ld, now is %ld.", MAX_SAMPLE_BUCKET, sample_helper_.get_array_index());
      }
      else if(!sample_helper_.push_back(statistic_sample)){
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "can not push statistic_sample into sample_list_");
      }
      return ret;
    }

    /*
     * deep copy add sample with internal allocator_
     */
    int ObTabletStatisticHistogram::add_sample_with_deep_copy(const ObTabletStatisticSample &statistic_sample)
    {
      int ret = OB_SUCCESS;
      ObTabletStatisticSample inner_sample;
      if(NULL == allocator_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN,"allocator_ is null");
      }
      else if (sample_helper_.get_array_index() >= MAX_SAMPLE_BUCKET)
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "too many sample in one histogram, max is %ld, now is %ld.", MAX_SAMPLE_BUCKET, sample_helper_.get_array_index());
      }
      else if(OB_SUCCESS != (ret = inner_sample.deep_copy(*allocator_, statistic_sample)))
      {
        TBSYS_LOG(WARN, "deep copy index sample failed!");
      }
      else if (!sample_helper_.push_back(inner_sample))
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "can not push statistic_sample into sample_list_");
      }
      return ret;
    }

    /*
     * deep copy with internal allocator_
     */
    int ObTabletStatisticHistogram::deep_copy(const ObTabletStatisticHistogram &other)
    {
      int ret = OB_SUCCESS;
      this->server_info_index_ = other.server_info_index_;
      this->set_column_id(other.get_column_id());
      this->set_table_id(other.get_table_id());
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
        else if (OB_SUCCESS != (ret = add_sample_with_deep_copy(*sample))) {
          TBSYS_LOG(WARN, "add sample failed");
          break;
        }
      }
      return ret;
    }




    void ObTabletStatisticInfo::dump() const
    {
      TBSYS_LOG(INFO,"table_id = %lu",table_id_);
      TBSYS_LOG(INFO,"row_count = %ld",row_count_);
      TBSYS_LOG(INFO,"row_size = %ld",row_size_);
      TBSYS_LOG(INFO,"table_size = %ld",tablet_size_);
      TBSYS_LOG(INFO,"range = %s",to_cstring(range_));
    }

    void ObColumnStatisticInfo::dump() const
    {
      TBSYS_LOG(INFO,"table_id = %lu",table_id_);
      TBSYS_LOG(INFO,"column_id = %lu",column_id_);
      TBSYS_LOG(INFO,"column_type = %d",column_type_);
      TBSYS_LOG(INFO,"distinct_num = %ld",distinct_num_);
      TBSYS_LOG(INFO,"range = %s",to_cstring(range_));
    }

    void ObTabletStatisticSample::dump() const
    {
      TBSYS_LOG(INFO, "range = %s", to_cstring(range_));
      TBSYS_LOG(INFO, "row_count = %ld", row_count_);
    }



    DEFINE_SERIALIZE(ObTabletStatisticHistogram)
    {
      int ret = OB_SUCCESS;
      int64_t size = sample_helper_.get_array_index();
      if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,table_id_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,column_id_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi32(buf,buf_len,pos,server_info_index_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,size)))
      {
      }
      if(OB_SUCCESS == ret)
      {
        for(int64_t i = 0;i < size;i++)
        {
          ret = samples_[i].serialize(buf,buf_len,pos);
          if(OB_SUCCESS != ret)
          {
            break;
          }
        }
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObTabletStatisticHistogram)
    {
      int ret = OB_SUCCESS;
      int64_t size = 0;
      ObObj *ptr = NULL;
      if(NULL == allocator_)
      {
        ret = OB_NOT_INIT;
        TBSYS_LOG(WARN, "allocator_ not set, deseriablize failed.");
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,reinterpret_cast<int64_t *>(&table_id_))))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,reinterpret_cast<int64_t *>(&column_id_))))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi32(buf,data_len,pos,&server_info_index_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,&size)))
      {
      }
      if(OB_SUCCESS == ret && size > 0)
      {
        for(int64_t i = 0;i < size; i++)
        {
          ObTabletStatisticSample sample;
          ptr = reinterpret_cast<ObObj*>(allocator_->alloc(sizeof(ObObj) * OB_MAX_ROWKEY_COLUMN_NUMBER * 2));
          sample.range_.start_key_.assign(ptr, OB_MAX_ROWKEY_COLUMN_NUMBER);
          sample.range_.end_key_.assign(ptr + OB_MAX_ROWKEY_COLUMN_NUMBER, OB_MAX_ROWKEY_COLUMN_NUMBER);
          ret = sample.deserialize(buf, data_len, pos);
          if (OB_SUCCESS != ret)
          {
            TBSYS_LOG(WARN, "fail to deserialize ObStaticIndexSample.");
            break;
          }
          else if (OB_SUCCESS != (ret = add_sample(sample)))//浅拷贝
          {
            TBSYS_LOG(WARN, "fail to add sample into histogram.");
            break;
          }
        }
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObTabletStatisticHistogram)
    {
      int64_t size = sample_helper_.get_array_index();
      int64_t total_size = 0;
      total_size += serialization::encoded_length_vi64(table_id_);
      total_size += serialization::encoded_length_vi64(column_id_);
      total_size += serialization::encoded_length_vi32(server_info_index_);
      total_size += serialization::encoded_length_vi64(sample_helper_.get_array_index());
      if(0 < sample_helper_.get_array_index())
      {
        for(int64_t i=0; i<size; ++i)
        {
          total_size += samples_[i].get_serialize_size();
        }
      }
      return total_size;
    }

    DEFINE_SERIALIZE(ObTabletStatisticInfo)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,table_id_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,row_count_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,row_size_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,tablet_size_)))
      {
      }
      else if(OB_SUCCESS != (ret = range_.serialize(buf, buf_len, pos)))
      {
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObTabletStatisticInfo)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,reinterpret_cast<int64_t *>(&table_id_))))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,&row_count_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,&row_size_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,&tablet_size_)))
      {
      }
      else if(OB_SUCCESS != (ret = range_.deserialize(buf,data_len,pos)))
      {
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObTabletStatisticInfo)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_vi64(table_id_);
      total_size += serialization::encoded_length_vi64(row_count_);
      total_size += serialization::encoded_length_vi64(row_size_);
      total_size += serialization::encoded_length_vi64(tablet_size_);
      total_size += range_.get_serialize_size();
      return total_size;
    }

    DEFINE_SERIALIZE(ObColumnStatisticInfo)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,table_id_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,column_id_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,column_type_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,distinct_num_)))
      {
      }
      else if(OB_SUCCESS != (ret = range_.serialize(buf, buf_len, pos)))
      {
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObColumnStatisticInfo)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,reinterpret_cast<int64_t *>(&table_id_))))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,reinterpret_cast<int64_t *>(&column_id_))))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,reinterpret_cast<int64_t *>(&column_type_))))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,&distinct_num_)))
      {
      }
      else if(OB_SUCCESS != (ret = range_.deserialize(buf,data_len,pos)))
      {
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObColumnStatisticInfo)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_vi64(table_id_);
      total_size += serialization::encoded_length_vi64(column_id_);
      total_size += serialization::encoded_length_vi64(column_type_);
      total_size += serialization::encoded_length_vi64(distinct_num_);
      total_size += range_.get_serialize_size();
      return total_size;
    }

    DEFINE_SERIALIZE(ObTabletStatisticSample)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,row_count_)))
      {
      }
      else if(OB_SUCCESS != (ret = range_.serialize(buf,buf_len,pos)))
      {
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObTabletStatisticSample)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,reinterpret_cast<int64_t *>(&row_count_))))
      {
      }
      else if(OB_SUCCESS != (ret = range_.deserialize(buf,data_len,pos)))
      {
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObTabletStatisticSample)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_vi64(row_count_);
      total_size += range_.get_serialize_size();
      return total_size;
    }
    DEFINE_SERIALIZE(ObUserTabletInfo)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,table_id_)))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::encode_vi64(buf,buf_len,pos,tablet_version_)))
      {
      }
      else if(OB_SUCCESS != (ret = range_.serialize(buf,buf_len,pos)))
      {
      }
      return ret;
    }
    DEFINE_DESERIALIZE(ObUserTabletInfo)
    {
      int ret = OB_SUCCESS;
      if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,reinterpret_cast<int64_t *>(&table_id_))))
      {
      }
      else if(OB_SUCCESS != (ret = serialization::decode_vi64(buf,data_len,pos,&tablet_version_)))
      {
      }
      else if(OB_SUCCESS != (ret = range_.deserialize(buf,data_len,pos)))
      {
      }
      return ret;
    }
    DEFINE_GET_SERIALIZE_SIZE(ObUserTabletInfo)
    {
      int64_t total_size = 0;
      total_size += serialization::encoded_length_vi64(table_id_);
      total_size += serialization::encoded_length_vi64(tablet_version_);
      total_size += range_.get_serialize_size();
      return total_size;
    }
  }
}
