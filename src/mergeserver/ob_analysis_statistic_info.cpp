/*
 *add by huangcc[statistic information cache]2017/03/24
 *
 */
#include "ob_analysis_statistic_info.h"

namespace oceanbase
{
  namespace mergeserver
  {
    using namespace common;
    ObAnalysisStatisticInfo::ObAnalysisStatisticInfo()
    {
        //sql_proxy_=NULL;
        top_value_num_=0;
        offset_=0;
        data_type_=0;//数据类型
        row_count_=0;
        different_num_=0;
        size_=0;
        top_value_helper_.init(10,top_value_);

        statistic_columns_helper_.init(OB_MAX_COLUMN_NUMBER,statistic_columns_);
        statistic_columns_num_=0;
    }
    ObAnalysisStatisticInfo::~ObAnalysisStatisticInfo()
    {

    }
    void ObAnalysisStatisticInfo::reset()
    {
        top_value_num_=0;
        offset_=0;
        data_type_=0;//数据类型
        row_count_=0;
        different_num_=0;
        size_=0;
        top_value_helper_.clear();
        statistic_top_value_.clear();
        top_value_vec_.clear();
        min_max_vec_.clear();

        statistic_columns_helper_.clear();
        statistic_columns_num_=0;
    }
    int ObAnalysisStatisticInfo::analysis_column_statistic_info(uint64_t tid, uint64_t cid, ObMsSQLProxy* sql_proxy)
    {
        TBSYS_LOG(DEBUG, "huangcc_test::analysis_column_statistic_info");
        int ret = OB_SUCCESS;
        const char * statistic_info_str = "SELECT * from __all_statistic_info where table_id = %lu and column_id=%lu";
        int64_t pos = 0;
        const int MAX_SQL_LENGTH = 256;
        char sqlbuf[MAX_SQL_LENGTH] = {0};
        databuff_printf(sqlbuf,MAX_SQL_LENGTH, pos, statistic_info_str, tid, cid);

        ObString sql_str;
        sql_str.assign_ptr(sqlbuf,static_cast<common::ObString::obstr_size_t>(strlen(sqlbuf)));

        ObSQLResultSet sql_rs;
        ObResultSet &rs = sql_rs.get_result_set();
        ObSQLSessionInfo session;
        ObSqlContext context;
        int64_t schema_version = 0;
        if (OB_SUCCESS != (ret = sql_proxy->init_sql_env(context, schema_version, sql_rs, session)))
        {
          TBSYS_LOG(WARN, "init sql env error, err=%d", ret);
        }
        else
        {
          if (OB_SUCCESS != (ret = sql_proxy->execute(sql_str, sql_rs, context, schema_version)))
          {
            TBSYS_LOG(WARN, "execute privilge sql failed,sql=%.*s, ret=%d", sql_str.length(), sql_str.ptr(), ret);
          }
          else if (OB_SUCCESS != (ret = rs.open()))
          {
            TBSYS_LOG(ERROR, "open rs failed, ret=%d", ret);
          }
          else
          {
            const ObRow *row = NULL;
            while (OB_SUCCESS == ret)
            {
              if (OB_SUCCESS != (ret = rs.get_next_row(row)))
              {
                if (OB_ITER_END == ret)
                {
                  ret = OB_SUCCESS;
                }
                else
                {
                  TBSYS_LOG(ERROR, "failed to get next row, ret=%d", ret);
                }
                break;
              }
              else if(row!=NULL)
              {
                //获取数据类型
                //TBSYS_LOG(INFO, "huangcc_test::row=[%s]",to_cstring(*row));
                uint64_t table_id=OB_INVALID_ID;
                uint64_t column_id=OB_INVALID_ID;
                const common::ObObj *cell1 = NULL;
                const common::ObObj *cell2 = NULL;
                const common::ObObj *cell3 = NULL;
                const common::ObObj *cell4 = NULL;
                const common::ObObj *cell5 = NULL;
                const common::ObObj *cell6 = NULL;

                //data_type
                if(OB_SUCCESS != (ret=row->get_row_desc()->get_tid_cid(4,table_id,column_id)))
                {
                    TBSYS_LOG(ERROR, "failed to get tid and cid ,ret=%d",ret);
                    break;
                }
                else if(OB_SUCCESS != (ret=row->get_cell(table_id,column_id,cell1)))
                {
                    TBSYS_LOG(ERROR, "failed to get cell,ret=%d",ret);
                    break;
                }
                else
                {
                   cell1->get_int(data_type_);
                }

                //row_count
                if(OB_SUCCESS != (ret=row->get_row_desc()->get_tid_cid(5,table_id,column_id)))
                {
                    TBSYS_LOG(ERROR, "failed to get tid and cid ,ret=%d",ret);
                    break;
                }
                if(OB_SUCCESS != (ret=row->get_cell(table_id,column_id,cell2)))
                {
                    TBSYS_LOG(ERROR, "failed to get cell,ret=%d",ret);
                    break;
                }
                else
                {
                    int64_t rc=0;
                    cell2->get_int(rc);
                    row_count_+=rc;
                }

                //different_num
                if(OB_SUCCESS != (ret=row->get_row_desc()->get_tid_cid(6,table_id,column_id)))
                {
                    TBSYS_LOG(ERROR, "failed to get tid and cid ,ret=%d",ret);
                    break;
                }
                if(OB_SUCCESS != (ret=row->get_cell(table_id,column_id,cell3)))
                {
                    TBSYS_LOG(ERROR, "failed to get cell,ret=%d",ret);
                    break;
                }
                else
                {
                    int64_t dn=0;
                    cell3->get_int(dn);
                    different_num_+=dn;
                }

                //size
                if(OB_SUCCESS != (ret=row->get_row_desc()->get_tid_cid(7,table_id,column_id)))
                {
                    TBSYS_LOG(ERROR, "failed to get tid and cid ,ret=%d",ret);
                    break;
                }
                if(OB_SUCCESS != (ret=row->get_cell(table_id,column_id,cell4)))
                {
                    TBSYS_LOG(ERROR, "failed to get cell,ret=%d",ret);
                    break;
                }
                else
                {
                    int64_t s=0;
                    cell4->get_int(s);
                    size_+=s;
                }

                //info
                if(OB_SUCCESS != (ret=row->get_row_desc()->get_tid_cid(8,table_id,column_id)))
                {
                    TBSYS_LOG(ERROR, "failed to get tid and cid ,ret=%d",ret);
                    break;
                }
                else if(OB_SUCCESS != (ret=row->get_cell(table_id,column_id,cell5)))
                {
                    TBSYS_LOG(ERROR, "failed to get cell,ret=%d",ret);
                    break;
                }
                else
                {
                    std::string cardinality;
                    cell5->to_string_v2(cardinality);
                    //开始分割统计信息
                    splite_string(cardinality,top_value_vec_);
                }

                //min_max
                if(OB_SUCCESS != (ret=row->get_row_desc()->get_tid_cid(9,table_id,column_id)))
                {
                    TBSYS_LOG(ERROR, "failed to get tid and cid ,ret=%d",ret);
                    break;
                }
                else if(OB_SUCCESS != (ret=row->get_cell(table_id,column_id,cell6)))
                {
                    TBSYS_LOG(ERROR, "failed to get cell,ret=%d",ret);
                    break;
                }
                else
                {
                    std::string mm;
                    cell6->to_string_v2(mm);
                    //开始分割min、max
                    splite_string(mm,min_max_vec_);
                }//end else
              }
            }

            if(OB_SUCCESS==ret)
            {
                //往statistic_top_value数组里添加值
                if(OB_SUCCESS!=(ret= merge_duplicates_and_sort()))
                {
                    TBSYS_LOG(WARN, "failed to merge duplicates");
                }
                else
                {
                    if(offset_<10)
                    {
                        top_value_num_=static_cast<int>(offset_);
                    }
                    else
                    {
                        top_value_num_=10;
                    }

                    if(OB_SUCCESS!=(ret= trans_string_to_obj()))
                    {
                        TBSYS_LOG(WARN, "failed to trans_string_to_obj");
                    }
                    else if(OB_SUCCESS!=(ret= compute_min_max()))
                    {
                        TBSYS_LOG(WARN, "failed to compute_min_max");
                    }

                }

            }

          }
          sql_proxy->cleanup_sql_env(context, sql_rs);
        }
        return ret;

    }
    int ObAnalysisStatisticInfo::analysis_table_statistic_info(uint64_t tid, ObMsSQLProxy* sql_proxy)
    {
        TBSYS_LOG(DEBUG, "huangcc_test::analysis_table_statistic_info");
        int ret = OB_SUCCESS;
        const char * statistic_info_str = "SELECT * from __all_statistic_info where table_id = %lu";
        int64_t pos = 0;
        const int MAX_SQL_LENGTH = 256;
        char sqlbuf[MAX_SQL_LENGTH] = {0};
        databuff_printf(sqlbuf,MAX_SQL_LENGTH, pos, statistic_info_str, tid);

        ObString sql_str;
        sql_str.assign_ptr(sqlbuf,static_cast<common::ObString::obstr_size_t>(strlen(sqlbuf)));

        ObSQLResultSet sql_rs;
        ObResultSet &rs = sql_rs.get_result_set();
        ObSQLSessionInfo session;
        ObSqlContext context;
        int64_t schema_version = 0;
        if (OB_SUCCESS != (ret = sql_proxy->init_sql_env(context, schema_version, sql_rs, session)))
        {
          TBSYS_LOG(WARN, "init sql env error, err=%d", ret);
        }
        else
        {
          if (OB_SUCCESS != (ret = sql_proxy->execute(sql_str, sql_rs, context, schema_version)))
          {
            TBSYS_LOG(WARN, "execute privilge sql failed,sql=%.*s, ret=%d", sql_str.length(), sql_str.ptr(), ret);
          }
          else if (OB_SUCCESS != (ret = rs.open()))
          {
            TBSYS_LOG(ERROR, "open rs failed, ret=%d", ret);
          }
          else
          {

            const ObRow *row = NULL;
            while (OB_SUCCESS == ret)
            {
              if (OB_SUCCESS != (ret = rs.get_next_row(row)))
              {
                if (OB_ITER_END == ret)
                {
                  ret = OB_SUCCESS;
                }
                else
                {
                  TBSYS_LOG(ERROR, "failed to get next row, ret=%d", ret);
                }
                break;
              }
              else if(row!=NULL)
              {
                //获取数据类型
                //TBSYS_LOG(INFO, "huangcc_test::row=[%s]",to_cstring(*row));
                uint64_t table_id=OB_INVALID_ID;
                uint64_t column_id=OB_INVALID_ID;
                const common::ObObj *cell1 = NULL;
                const common::ObObj *cell2 = NULL;

                const common::ObObj *cell4 = NULL;

                //column_id
                if(OB_SUCCESS != (ret=row->get_row_desc()->get_tid_cid(1,table_id,column_id)))
                {
                    TBSYS_LOG(ERROR, "failed to get tid and cid ,ret=%d",ret);
                    break;
                }
                if(OB_SUCCESS != (ret=row->get_cell(table_id,column_id,cell1)))
                {
                    TBSYS_LOG(ERROR, "failed to get cell,ret=%d",ret);
                    break;
                }
                else
                {
                    int64_t id=0;
                    cell1->get_int(id);
                    uint64_t col_id=static_cast<uint64_t>(id);

                    int64_t i=0;
                    for(;i<=statistic_columns_num_-1;i++)
                    {
                        if(col_id==statistic_columns_[i])
                        {
                            break;
                        }
                    }
                    if(i==statistic_columns_num_)
                    {
                        statistic_columns_[i]=col_id;
                        statistic_columns_num_=i+1;
                    }

                    if(col_id==statistic_columns_[0])
                    {
                        //row_count
                        if(OB_SUCCESS != (ret=row->get_row_desc()->get_tid_cid(5,table_id,column_id)))
                        {
                            TBSYS_LOG(ERROR, "failed to get tid and cid ,ret=%d",ret);
                            break;
                        }
                        if(OB_SUCCESS != (ret=row->get_cell(table_id,column_id,cell2)))
                        {
                            TBSYS_LOG(ERROR, "failed to get cell,ret=%d",ret);
                            break;
                        }
                        else
                        {
                            int64_t rc=0;
                            cell2->get_int(rc);
                            row_count_+=rc;
                        }


                        //size
                        if(OB_SUCCESS != (ret=row->get_row_desc()->get_tid_cid(7,table_id,column_id)))
                        {
                            TBSYS_LOG(ERROR, "failed to get tid and cid ,ret=%d",ret);
                            break;
                        }
                        if(OB_SUCCESS != (ret=row->get_cell(table_id,column_id,cell4)))
                        {
                            TBSYS_LOG(ERROR, "failed to get cell,ret=%d",ret);
                            break;
                        }
                        else
                        {
                            int64_t s=0;
                            cell4->get_int(s);
                            size_+=s;
                        }

                    }

                }
              }
            }
          }
          sql_proxy->cleanup_sql_env(context, sql_rs);
        }
        return ret;
    }
    void ObAnalysisStatisticInfo::splite_string(std::string &info, std::vector<std::string> &vec)
    {
        char splitchar=31;
        size_t length=info.length();
        size_t start=0;
        for(size_t i=0;i<length;i++)
        {
            if(info[i]==splitchar && i==0)
            {
                start+=1;
            }
            else if(info[i]==splitchar)
            {

                vec.push_back(info.substr(start,i-start));
                TBSYS_LOG(DEBUG, "huangcc_test::vec=[%s]",info.substr(start,i-start).c_str());
                start=i+1;


            }
            else if(i==length-1)
            {
                vec.push_back(info.substr(start,i+1-start));
                TBSYS_LOG(DEBUG, "huangcc_test::vec=[%s]",info.substr(start,i+1-start).c_str());
            }
        }
    }
    int ObAnalysisStatisticInfo::merge_duplicates_and_sort()
    {
        int ret=OB_SUCCESS;
        for(int j=0;j<static_cast<int>(top_value_vec_.size()/2);j++)
        {
            int64_t v1;
            ret=sscanf(top_value_vec_[2*j+1].c_str(),"%ld",&v1);
            if (ret!= 1)
            {
              TBSYS_LOG(WARN, "failed to trans string to int,err=%d",ret);
              ret = OB_ERROR;
            }
            else
            {
              ret = OB_SUCCESS;
            }

            int64_t i=0;
            for(;i<=offset_-1;i++)
            {
                if(top_value_vec_[2*j].compare(statistic_top_value_.at(i).top_value_)==0)
                {
                    statistic_top_value_.at(i).num_+=v1;
                    break;
                }
            }
            if(i==offset_)
            {
                StatisticTopValue stv_1;
                stv_1.top_value_=top_value_vec_[2*j];
                stv_1.num_=v1;
                statistic_top_value_.push_back(stv_1);
                //TBSYS_LOG(INFO, "obj=[%s],num[%ld]=[%ld]",statistic_top_value_[i].top_value_.c_str(),i,statistic_top_value_[i].num_);
                offset_=i+1;
            }

        }
        if (static_cast<int>(top_value_vec_.size()/2) > 0)
        {
          //排序
          std::sort(&(statistic_top_value_.at(0)),&(statistic_top_value_.at(0))+offset_,Compare());
        }
        return ret;

    }
    int ObAnalysisStatisticInfo::trans_string_to_obj()
    {
        int ret=OB_SUCCESS;
        ObString str_clone[top_value_num_];

        const char * s="NULL";
        std::stringstream temp_result;
        temp_result<<s;
        std::string snull;
        snull=temp_result.str();

        TBSYS_LOG(DEBUG, "huangcc_test::data_type=%lu",data_type_);
        for (int i=0;i<top_value_num_; i++)
        {
            if(statistic_top_value_.at(i).top_value_.compare(snull)==0)
            {
                top_value_[i].obj_.set_null();
            }
            else
            {
                //将std::string类型数据转换成ObString
                size_t len_1= statistic_top_value_.at(i).top_value_.length();
                str_clone[i].assign_ptr(const_cast<char *>(statistic_top_value_.at(i).top_value_.c_str()),static_cast<common::ObString::obstr_size_t>(len_1+1));

                switch(data_type_)
                {
                case ObFloatType:
                  float f1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%f",&f1);
                  TBSYS_LOG(DEBUG, "huangcc_test::f1=[%f]",f1);
                  top_value_[i].obj_.set_float(f1);
                  break;
                case ObDoubleType:
                  double d1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%lf",&d1);
                  TBSYS_LOG(DEBUG, "huangcc_test::d1=[%lf]",d1);
                  top_value_[i].obj_.set_double(d1);
                  break;
                case ObDateTimeType:
                  ObDateTime dt1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%ld",&dt1);
                  TBSYS_LOG(DEBUG, "huangcc_test::dt1=[%ld]",dt1);
                  top_value_[i].obj_.set_datetime(dt1);
                  break;
                case ObPreciseDateTimeType:
                  ObDateTime pdt1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%ld",&pdt1);
                  TBSYS_LOG(DEBUG, "huangcc_test::pdt1=[%ld]",pdt1);
                  top_value_[i].obj_.set_precise_datetime(pdt1);
                  break;
                case ObVarcharType:
                  top_value_[i].obj_.set_varchar(str_clone[i]);
                  TBSYS_LOG(DEBUG, "huangcc_test::vchar=[%s]",const_cast<char *>(statistic_top_value_.at(i).top_value_.c_str()));
                  break;
                case ObCreateTimeType:
                  ObCreateTime ct1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%ld",&ct1);
                  TBSYS_LOG(DEBUG, "huangcc_test::ct1[%ld]",ct1);
                  top_value_[i].obj_.set_createtime(ct1);
                  break;
                case ObModifyTimeType:
                  ObModifyTime mt1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%ld",&mt1);
                  TBSYS_LOG(DEBUG, "huangcc_test::mt1[%ld]",mt1);
                  top_value_[i].obj_.set_createtime(mt1);
                  break;
                  // sscanf bool?
                case ObBoolType:
                {
                  bool b1;
                  //ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%d",&b1);
                  char  str_true[] = "true";
                  if (!strcasecmp(str_true,statistic_top_value_.at(i).top_value_.c_str()))
                  {
                    b1 = true;
                  }
                  TBSYS_LOG(DEBUG, "huangcc_test::b1[%d]",b1);
                  top_value_[i].obj_.set_bool(b1);
                  break;
                }
                case ObIntType:
                {
                  int64_t key1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%lu",&key1);
                  TBSYS_LOG(DEBUG, "huangcc_test::key=[%lu]",key1);
                  top_value_[i].obj_.set_int(key1);
                  break;
                }
/*
                case ObDecimalType:
                  top_value_[i].obj_.set_decimal(str_clone[i]);
                  TBSYS_LOG(DEBUG, "huangcc_test::top_value[i].obj_=[%s],str_clone[i].length=[%d]",to_cstring(top_value_[i].obj_),static_cast<int>(str_clone[i].length()));
                  break;
                case ObDateType:
                  ObDate date1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%ld",&date1);
                  TBSYS_LOG(DEBUG, "huangcc_test::date1[%ld]",date1);
                  top_value_[i].obj_.set_date(date1);
                  break;
                case ObTimeType:
                  ObTime time1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%ld",&time1);
                  TBSYS_LOG(DEBUG, "huangcc_test::date1[%ld]",date1);
                  top_value_[i].obj_.set_date(date1);
                  break;
                case ObInt32Type:
                  int64_t key1;
                  ret=sscanf(statistic_top_value_.at(i).top_value_.c_str(),"%lu",&key1);
                  TBSYS_LOG(DEBUG, "huangcc_test::key=[%lu]",key1);
                  top_value_[i].obj_.set_int(key1);
                  break;
*/
                default:
                  break;
                }
            }

            if (ret != 1 && ret!=OB_SUCCESS )
            {
              TBSYS_LOG(WARN, "failed to trans string to obj,err=%d",ret);
              ret = OB_ERROR;
              break;
            }
            else
            {
              ret = OB_SUCCESS;
              top_value_[i].num_=statistic_top_value_.at(i).num_;
            }
        }

        return ret;
    }
    int ObAnalysisStatisticInfo::compute_min_max()
    {
        int ret=OB_SUCCESS;

        const char * s="NULL";
        std::stringstream temp_result;
        temp_result<<s;
        std::string snull;
        snull=temp_result.str();

        if(data_type_==ObNullType)
        {
            min_.set_null();
            max_.set_null();
        }
        else if (data_type_ == ObIntType)
        {
          bool flag=0;
          TBSYS_LOG(DEBUG, "huangcc_test::data_type=%lu",data_type_);
          int64_t key[static_cast<int>(min_max_vec_.size())];
          int64_t min_1=0;
          int64_t max_1=0;
          for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
          {
              if(min_max_vec_[j*2].compare(snull)==0)
              {
                  if(j==0)
                  {
                     flag=1;
                  }

              }
              else
              {
                  sscanf(min_max_vec_[j*2].c_str(),"%ld",&key[j*2]);
                  sscanf(min_max_vec_[j*2+1].c_str(),"%ld",&key[j*2+1]);
                  if(j==0)
                  {
                     min_1=key[j*2];
                     max_1=key[j*2+1];
                  }
                  else
                  {
                      if(flag==0)
                      {
                          if(key[j*2]<min_1)
                          {
                             min_1=key[j*2];
                          }
                          if(key[j*2+1]>max_1)
                          {
                             max_1=key[j*2+1];
                          }
                      }
                      else
                      {
                          min_1=key[j*2];
                          max_1=key[j*2+1];
                          flag=0;
                      }

                  }
              }

          }
          if(flag==0)
          {
              max_.set_int(max_1);
              min_.set_int(min_1);
          }
          else
          {
              max_.set_null();
              min_.set_null();
          }
        }
        else if (data_type_ == ObBoolType)
        {
          //min max is not used
          min_.set_null();
          max_.set_null();
        }
        else if(data_type_==ObFloatType)
        {
            TBSYS_LOG(DEBUG, "huangcc_test::data_type=%lu",data_type_);
            bool flag=0;
            float key[static_cast<int>(min_max_vec_.size())];
            float min_1=0;
            float max_1=0;

            for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
            {
                if(min_max_vec_[j*2].compare(snull)==0)
                {
                    if(j==0)
                    {
                       flag=1;
                    }

                }
                else
                {
                    sscanf(min_max_vec_[j*2].c_str(),"%f",&key[j*2]);
                    sscanf(min_max_vec_[j*2+1].c_str(),"%f",&key[j*2+1]);
                    if(j==0)
                    {
                       min_1=key[j*2];
                       max_1=key[j*2+1];
                    }
                    else
                    {
                        if(flag==0)
                        {
                            if(key[j*2]<min_1)
                            {
                               min_1=key[j*2];
                            }
                            if(key[j*2+1]>max_1)
                            {
                               max_1=key[j*2+1];
                            }
                        }
                        else
                        {
                            min_1=key[j*2];
                            max_1=key[j*2+1];
                            flag=0;
                        }

                    }
                }
            }

            if(flag==0)
            {
                max_.set_float(max_1);
                min_.set_float(min_1);
            }
            else
            {
                max_.set_null();
                min_.set_null();
            }

        }
        else if(data_type_==ObDoubleType)
        {
            TBSYS_LOG(DEBUG, "huangcc_test::data_type=%lu",data_type_);
            bool flag=0;
            double key[static_cast<int>(min_max_vec_.size())];
            double min_1=0;
            double max_1=0;

            for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
            {
                if(min_max_vec_[j*2].compare(snull)==0)
                {
                    if(j==0)
                    {
                       flag=1;
                    }

                }
                else
                {
                    sscanf(min_max_vec_[j*2].c_str(),"%lf",&key[j*2]);
                    sscanf(min_max_vec_[j*2+1].c_str(),"%lf",&key[j*2+1]);
                    if(j==0)
                    {
                       min_1=key[j*2];
                       max_1=key[j*2+1];
                    }
                    else
                    {
                        if(flag==0)
                        {
                            if(key[j*2]<min_1)
                            {
                               min_1=key[j*2];
                            }
                            if(key[j*2+1]>max_1)
                            {
                               max_1=key[j*2+1];
                            }
                        }
                        else
                        {
                            min_1=key[j*2];
                            max_1=key[j*2+1];
                            flag=0;
                        }

                    }
                }
            }
            if(flag==0)
            {
                max_.set_double(max_1);
                min_.set_double(min_1);
            }
            else
            {
                max_.set_null();
                min_.set_null();
            }
        }
        else if (data_type_ == ObDateTimeType)
        {
          bool flag=0;
          TBSYS_LOG(DEBUG, "huangcc_test::data_type=%lu",data_type_);
          ObDateTime key[static_cast<int>(min_max_vec_.size())];
          ObDateTime min_1=0;
          ObDateTime max_1=0;
          for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
          {
              if(min_max_vec_[j*2].compare(snull)==0)
              {
                  if(j==0)
                  {
                     flag=1;
                  }

              }
              else
              {
                  sscanf(min_max_vec_[j*2].c_str(),"%ld",&key[j*2]);
                  sscanf(min_max_vec_[j*2+1].c_str(),"%ld",&key[j*2+1]);
                  if(j==0)
                  {
                     min_1=key[j*2];
                     max_1=key[j*2+1];
                  }
                  else
                  {
                      if(flag==0)
                      {
                          if(key[j*2]<min_1)
                          {
                             min_1=key[j*2];
                          }
                          if(key[j*2+1]>max_1)
                          {
                             max_1=key[j*2+1];
                          }
                      }
                      else
                      {
                          min_1=key[j*2];
                          max_1=key[j*2+1];
                          flag=0;
                      }

                  }
              }

          }
          if(flag==0)
          {
              max_.set_datetime(max_1);
              min_.set_datetime(min_1);
          }
          else
          {
              max_.set_null();
              min_.set_null();
          }
        }
        else if(data_type_== ObPreciseDateTimeType)
        {
            bool flag=0;
            TBSYS_LOG(DEBUG, "huangcc_test::data_type=%lu",data_type_);
            ObPreciseDateTime key[static_cast<int>(min_max_vec_.size())];
            ObPreciseDateTime min_1=0;
            ObPreciseDateTime max_1=0;
            for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
            {
                if(min_max_vec_[j*2].compare(snull)==0)
                {
                    if(j==0)
                    {
                       flag=1;
                    }

                }
                else
                {
                    sscanf(min_max_vec_[j*2].c_str(),"%ld",&key[j*2]);
                    sscanf(min_max_vec_[j*2+1].c_str(),"%ld",&key[j*2+1]);
                    if(j==0)
                    {
                       min_1=key[j*2];
                       max_1=key[j*2+1];
                    }
                    else
                    {
                        if(flag==0)
                        {
                            if(key[j*2]<min_1)
                            {
                               min_1=key[j*2];
                            }
                            if(key[j*2+1]>max_1)
                            {
                               max_1=key[j*2+1];
                            }
                        }
                        else
                        {
                            min_1=key[j*2];
                            max_1=key[j*2+1];
                            flag=0;
                        }

                    }
                }

            }
            if(flag==0)
            {
                max_.set_precise_datetime(max_1);
                min_.set_precise_datetime(min_1);
            }
            else
            {
                max_.set_null();
                min_.set_null();
            }
        }
        else if(data_type_== ObVarcharType)
        {
            bool flag_max=0;
            bool flag_min = 0;
            bool inited = 0;

            std::string  min_v;
            std::string  max_v;

            for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
            {
                if(min_max_vec_[j*2+1].compare(snull)==0 )
                {
                  flag_max=1;
                  if (min_max_vec_[j*2].compare(snull)==0)
                  {
                    flag_min = 1;
                  }
                }

                if(flag_max==0)
                {
                  if (!inited)
                  {
                    min_v=min_max_vec_[2*j];
                    max_v=min_max_vec_[2*j+1];
                    inited = 1;
                  }
                  else
                  {
                    if(min_max_vec_[2*j+1].compare(max_v)>0)
                    {
                      max_v=min_max_vec_[2*j+1];
                    }
                    if(min_max_vec_[2*j].compare(min_v)<0)
                    {
                      min_v=min_max_vec_[2*j];
                    }
                  }
                }
                else if (flag_min == 0)
                {
                  if (!inited)
                  {
                    min_v=min_max_vec_[2*j];
                    max_v=min_max_vec_[2*j];
                    inited = 1;
                  }
                  else
                  {
                    if (min_max_vec_[2*j].compare(min_v)<0)
                    {
                      min_v=min_max_vec_[2*j];
                    }
                    if (min_max_vec_[2*j].compare(max_v)>0)
                    {
                      max_v=min_max_vec_[2*j];
                    }
                  }
                }
                //clear state
                flag_max=0;
                flag_min = 0;
            }

            if(inited)
            {
                ObString max_2;
                size_t len_3= max_v.length();
                max_2.assign_ptr(const_cast<char *>(max_v.c_str()),static_cast<common::ObString::obstr_size_t>(len_3+1));
                max_.set_varchar(max_2);

                ObString min_2;
                size_t len_2= min_v.length();
                min_2.assign_ptr(const_cast<char *>(min_v.c_str()),static_cast<common::ObString::obstr_size_t>(len_2+1));
                min_.set_varchar(min_2);
            }
            else
            {
                TBSYS_LOG(WARN,"min max is NULL");
                max_.set_null();
                min_.set_null();
            }
        }
        else if(data_type_ == ObCreateTimeType)
        {
          bool flag=0;
          TBSYS_LOG(DEBUG, "huangcc_test::data_type=%lu",data_type_);
          ObCreateTime key[static_cast<int>(min_max_vec_.size())];
          ObCreateTime min_1=0;
          ObCreateTime max_1=0;
          for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
          {
              if(min_max_vec_[j*2].compare(snull)==0)
              {
                  if(j==0)
                  {
                     flag=1;
                  }

              }
              else
              {
                  sscanf(min_max_vec_[j*2].c_str(),"%ld",&key[j*2]);
                  sscanf(min_max_vec_[j*2+1].c_str(),"%ld",&key[j*2+1]);
                  if(j==0)
                  {
                     min_1=key[j*2];
                     max_1=key[j*2+1];
                  }
                  else
                  {
                      if(flag==0)
                      {
                          if(key[j*2]<min_1)
                          {
                             min_1=key[j*2];
                          }
                          if(key[j*2+1]>max_1)
                          {
                             max_1=key[j*2+1];
                          }
                      }
                      else
                      {
                          min_1=key[j*2];
                          max_1=key[j*2+1];
                          flag=0;
                      }

                  }
              }

          }
          if(flag==0)
          {
              max_.set_createtime(max_1);
              min_.set_createtime(min_1);
          }
          else
          {
              max_.set_null();
              min_.set_null();
          }
        }
        else if (data_type_ == ObModifyTimeType)
        {
          bool flag=0;
          TBSYS_LOG(DEBUG, "huangcc_test::data_type=%lu",data_type_);
          ObModifyTime key[static_cast<int>(min_max_vec_.size())];
          ObModifyTime min_1=0;
          ObModifyTime max_1=0;
          for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
          {
              if(min_max_vec_[j*2].compare(snull)==0)
              {
                  if(j==0)
                  {
                     flag=1;
                  }

              }
              else
              {
                  sscanf(min_max_vec_[j*2].c_str(),"%ld",&key[j*2]);
                  sscanf(min_max_vec_[j*2+1].c_str(),"%ld",&key[j*2+1]);
                  if(j==0)
                  {
                     min_1=key[j*2];
                     max_1=key[j*2+1];
                  }
                  else
                  {
                      if(flag==0)
                      {
                          if(key[j*2]<min_1)
                          {
                             min_1=key[j*2];
                          }
                          if(key[j*2+1]>max_1)
                          {
                             max_1=key[j*2+1];
                          }
                      }
                      else
                      {
                          min_1=key[j*2];
                          max_1=key[j*2+1];
                          flag=0;
                      }

                  }
              }

          }
          if(flag==0)
          {
              max_.set_modifytime(max_1);
              min_.set_modifytime(min_1);
          }
          else
          {
              max_.set_null();
              min_.set_null();
          }
        }
/*        else if (data_type_ == ObDateType)
        {
          bool flag=0;
          TBSYS_LOG(DEBUG, "huangcc_test::data_type=%d",data_type_);
          ObDate key[static_cast<int>(min_max_vec_.size())];
          ObDate min_1=0;
          ObDate max_1=0;
          for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
          {
              if(min_max_vec_[j*2].compare(snull)==0)
              {
                  if(j==0)
                  {
                     flag=1;
                  }

              }
              else
              {
                  sscanf(min_max_vec_[j*2].c_str(),"%ld",&key[j*2]);
                  sscanf(min_max_vec_[j*2+1].c_str(),"%ld",&key[j*2+1]);
                  if(j==0)
                  {
                     min_1=key[j*2];
                     max_1=key[j*2+1];
                  }
                  else
                  {
                      if(flag==0)
                      {
                          if(key[j*2]<min_1)
                          {
                             min_1=key[j*2];
                          }
                          if(key[j*2+1]>max_1)
                          {
                             max_1=key[j*2+1];
                          }
                      }
                      else
                      {
                          min_1=key[j*2];
                          max_1=key[j*2+1];
                          flag=0;
                      }

                  }
              }

          }
          if(flag==0)
          {
              max_.set_date(max_1);
              min_.set_date(min_1);
          }
          else
          {
              max_.set_null();
              min_.set_null();
          }
        }
        else if (data_type_ == ObTimeType)
        {
          bool flag=0;
          TBSYS_LOG(DEBUG, "huangcc_test::data_type=%d",data_type_);
          ObTime key[static_cast<int>(min_max_vec_.size())];
          ObTime min_1=0;
          ObTime max_1=0;
          for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
          {
              if(min_max_vec_[j*2].compare(snull)==0)
              {
                  if(j==0)
                  {
                     flag=1;
                  }

              }
              else
              {
                  sscanf(min_max_vec_[j*2].c_str(),"%ld",&key[j*2]);
                  sscanf(min_max_vec_[j*2+1].c_str(),"%ld",&key[j*2+1]);
                  if(j==0)
                  {
                     min_1=key[j*2];
                     max_1=key[j*2+1];
                  }
                  else
                  {
                      if(flag==0)
                      {
                          if(key[j*2]<min_1)
                          {
                             min_1=key[j*2];
                          }
                          if(key[j*2+1]>max_1)
                          {
                             max_1=key[j*2+1];
                          }
                      }
                      else
                      {
                          min_1=key[j*2];
                          max_1=key[j*2+1];
                          flag=0;
                      }

                  }
              }

          }
          if(flag==0)
          {
              max_.set_time(max_1);
              min_.set_time(min_1);
          }
          else
          {
              max_.set_null();
              min_.set_null();
          }
        }
        else if(data_type_==ObDecimalType)
        {
            TBSYS_LOG(DEBUG, "huangcc_test::data_type=%d",data_type_);
            bool flag=0;
            ObDecimal key[static_cast<int>(min_max_vec_.size())];
            ObDecimal min_1;
            ObDecimal max_1;
            int min_index=0;
            int max_index=0;

            for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
            {
                if(min_max_vec_[j*2].compare(snull)==0)
                {
                    if(j==0)
                    {
                       flag=1;
                    }
                }
                else
                {
                    key[j*2].from(const_cast<char *>(min_max_vec_[j*2].c_str()),static_cast<common::ObString::obstr_size_t>(min_max_vec_[j*2].length()));
                    key[j*2+1].from(const_cast<char *>(min_max_vec_[j*2+1].c_str()),static_cast<common::ObString::obstr_size_t>(min_max_vec_[j*2+1].length()));
                    if(j==0)
                    {
                       min_1=key[j*2];
                       min_index=j*2;
                       max_1=key[j*2+1];
                       max_index=j*2+1;
                    }
                    else
                    {
                        if(flag==0)
                        {
                            if(key[j*2].compare(min_1)<0)
                            {
                                min_1=key[j*2];
                                min_index=j*2;
                            }
                            if(key[j*2+1].compare(max_1)>0)
                            {
                                max_1=key[j*2+1];
                                max_index=j*2+1;
                            }
                        }
                        else
                        {
                            min_1=key[j*2];
                            min_index=j*2;
                            max_1=key[j*2+1];
                            max_index=j*2+1;
                            flag=0;
                        }

                    }
                }
            }

            if(flag==0)
            {
                ObString max_2;
                max_2.assign_ptr(const_cast<char *>(min_max_vec_[max_index].c_str()),static_cast<common::ObString::obstr_size_t>(min_max_vec_[max_index].length()));
                max_.set_decimal(max_2);
                ObString min_2;
                min_2.assign_ptr(const_cast<char *>(min_max_vec_[min_index].c_str()),static_cast<common::ObString::obstr_size_t>(min_max_vec_[min_index].length()));
                min_.set_decimal(min_2);
            }
            else
            {
                max_.set_null();
                min_.set_null();
            }

        }
        else if(data_type_==ObInt32Type)
        {
            TBSYS_LOG(DEBUG, "huangcc_test::data_type=%d",data_type_);
            bool flag=0;
            int64_t min_1=0;
            int64_t max_1=0;
            int64_t key[static_cast<int>(min_max_vec_.size())];

            for(int j=0;j<static_cast<int>(min_max_vec_.size()/2);j++)
            {
                if(min_max_vec_[j*2].compare(snull)==0)
                {
                    if(j==0)
                    {
                       flag=1;
                    }

                }
                else
                {
                    sscanf(min_max_vec_[j*2].c_str(),"%ld",&key[j*2]);
                    sscanf(min_max_vec_[j*2+1].c_str(),"%ld",&key[j*2+1]);
                    if(j==0)
                    {
                       min_1=key[j*2];
                       max_1=key[j*2+1];
                    }
                    else
                    {
                        if(flag==0)
                        {
                            if(key[j*2]<min_1)
                            {
                               min_1=key[j*2];
                            }
                            if(key[j*2+1]>max_1)
                            {
                               max_1=key[j*2+1];
                            }
                        }
                        else
                        {
                            min_1=key[j*2];
                            max_1=key[j*2+1];
                            flag=0;
                        }

                    }
                }
            }
            if(flag==0)
            {
                max_.set_int(max_1);
                min_.set_int(min_1);
            }
            else
            {
                max_.set_null();
                min_.set_null();
            }
        }
*/
        return ret;
    }
  }
}
