#include "ob_statistic_info_manager.h"
namespace oceanbase {
  namespace common {
  //add huangcc
  void ObTabletStatisticHistogramManager::reuse()
  {
    tbsys::CThreadGuard mutex_gard(&alloc_mutex_);
    module_arena_.reuse();
    statistic_histogram_helper_.clear();
    /*
    sp_array_.clear();
    sp_sorted_list_.clear();
    cur_wrapper_ = sp_sorted_list_.begin();
    */
    table_id_ = OB_INVALID_ID;
    index_tid_ = OB_INVALID_ID;
  }



  int ObTabletStatisticHistogramManager::get_statistic_histogram(const int32_t statistic_hist_index, ObTabletStatisticHistogram *&out_hist) const
  {
    int ret = OB_SUCCESS;
    //add liumz, 20150429:b
    out_hist = NULL;
    //add:e
    if (statistic_hist_index < 0 || statistic_hist_index >= statistic_histogram_helper_.get_array_index())
    {
      ret = OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "index out of range, index=%d, histogram_helper_.size()=%ld", statistic_hist_index, statistic_histogram_helper_.get_array_index());
    }
    //mod liumz, 20150429:b
    //if (OB_SUCCESS == ret)
    else
    //mod:e
    {
      out_hist = statistic_histograms_[statistic_hist_index];
    }
    return ret;
  }

  int ObTabletStatisticHistogramManager::add_statistic_histogram(const ObTabletStatisticHistogram &histogram, int32_t &out_index, uint64_t &idx_tid)
  {
//      tbsys::CThreadGuard mutex_gard(&alloc_mutex_);
    int ret = OB_SUCCESS;
    out_index = OB_INVALID_INDEX;
    idx_tid = OB_INVALID_ID;
    const ObTabletStatisticSample *first_sample = NULL;
    //20150409, histogram must contains at least one sample
    if (histogram.get_sample_count() < 1)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "added histogram is null.");
    }
    //20150512, tid in histogram must be consistent with index_tid_.
    else if (OB_SUCCESS != (ret = histogram.get_sample(0, first_sample)))
    {
      TBSYS_LOG(WARN, "get first sample failed.");
    }
    else if (NULL == first_sample)
    {
      ret = OB_ERROR;
      TBSYS_LOG(WARN, "first sample is NULL.");
    }
    /*
    else if (index_tid_ != (idx_tid = first_sample->range_.table_id_))//
    {
      ret = OB_INVALID_DATA;
      TBSYS_LOG(WARN, "index tid is not consistent. index_tid_[%lu], sample's range tid[%lu]"
                , index_tid_, first_sample->range_.table_id_);
    }
    */
    else if (statistic_histogram_helper_.get_array_index() >= MAX_TABLET_COUNT_PER_TABLE)
    {
      ret = OB_ARRAY_OUT_OF_RANGE;
      TBSYS_LOG(WARN, "too many histogram in hist manager, max is %ld, now is %ld.", MAX_TABLET_COUNT_PER_TABLE, statistic_histogram_helper_.get_array_index());
    }
    else
    {
      ObTabletStatisticHistogram* inner_hist = alloc_statistic_hist_object();
//        ObTabletHistogram* inner_hist = alloc_hist_object();
//        inner_hist->set_allocator(module_arena_);
//        if (OB_SUCCESS != (ret = inner_hist->deep_copy(histogram)))
      if (NULL == inner_hist)
      {
        ret = OB_ERROR ;
        TBSYS_LOG(WARN, "alloc hist object failed.");
      }
      else if (OB_SUCCESS != (ret = inner_hist->deep_copy(module_arena_, histogram)))
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "deep copy histogram with specific allocator failed.");
      }
      else if (!statistic_histogram_helper_.push_back(inner_hist))
      {
        ret = OB_ARRAY_OUT_OF_RANGE;
        TBSYS_LOG(WARN, "failed to push hist into hist manager.");
      }
    }
    if (OB_SUCCESS == ret)
    {
      out_index = static_cast<int32_t>(statistic_histogram_helper_.get_array_index() - 1);
    }
    return ret;
  }

  ObTabletStatisticHistogram* ObTabletStatisticHistogramManager::alloc_statistic_hist_object()
  {
    tbsys::CThreadGuard mutex_gard(&alloc_mutex_);
    ObTabletStatisticHistogram* statistic_hist = NULL;
    char* ptr = module_arena_.alloc_aligned(sizeof(ObTabletStatisticHistogram));
    if (NULL != ptr)
    {
      statistic_hist = new (ptr) ObTabletStatisticHistogram();
    }
    return statistic_hist;
  }



  void ObTabletStatisticHistogramManager::dump(const int32_t statistic_hist_index) const
  {
    if (statistic_hist_index >=0 && statistic_hist_index < statistic_histogram_helper_.get_array_index())
    {
      statistic_histograms_[statistic_hist_index]->dump();
    }
  }

  //add e
  }
}
