#ifndef OCEANBASE_SQL_OB_OPTIMIZER_RELATION_H_
#define OCEANBASE_SQL_OB_OPTIMIZER_RELATION_H_

#include "common/ob_define.h"

#include "common/ob_list.h"
#include "ob_raw_expr.h"
#include "common/hash/ob_hashmap.h"
#include "common/utility.h"
#include "common/ob_vector.h"
#include "common/ob_array.h"
#include <stdint.h>
#include "mergeserver/ob_merge_server_service.h"
//add huangcc 20170412:b
#include "mergeserver/ob_statistic_info_cache.h"
//add:e
//using namespace oceanbase::mergeserver;

namespace oceanbase
{
  namespace sql
  {
    typedef struct ObSelInfo
    {
      uint64_t table_id_;  ///< table id included in expr
      uint64_t columun_id_;  ///< column id included in expr
      double selectivity_;  ///< expr selectivity
      bool enable;  ///< selectivity is effect
      //QX:Notce you should avoid expr subquery repeat handle
      bool enable_expr_subquery_optimization; ///< enable expr_subquery optimization

      //bool is_simple_expr_;  ///< expr whether is simple expr

      ObSelInfo()
      {
        table_id_ = OB_INVALID_ID;
        columun_id_ = OB_INVALID_ID;
        selectivity_ = 1.0;
        enable = true;
        enable_expr_subquery_optimization = false;
        //is_simple_expr_ = true;
      }

    }ObSelInfo;

    typedef struct ObIndexTableInfo
    {
      uint64_t index_table_id_;  ///< index table id
      uint64_t index_column_id_;  ///< index table column id
      bool is_back_;  ///< need back origin table or not

      double cost_;  ///<  cpu and disk group/order by cost

      bool group_by_applyed_;  ///< applyed group by
      bool order_by_applyed_;  ///< applyed order by

      //default constuctor
      ObIndexTableInfo():
        index_table_id_(OB_INVALID_ID),
        index_column_id_(OB_INVALID_ID),
        is_back_(true),
        cost_(0),
        group_by_applyed_(false),
        order_by_applyed_(false)
      {}

    }ObIndexTableInfo;

    typedef struct ObSeqScanInfo
    {
      bool group_by_applyed_;  /// group by applyed in base table
      bool order_by_applyed_;  /// group by applyed in base table
      double cost_;  ///<  cpu and disk  group/order by cost in seq sacn

      //default constuctor
      ObSeqScanInfo():
        group_by_applyed_(false),
        order_by_applyed_(false),
        cost_(0)
      {}
    }ObSeqScanInfo;
  }

  namespace common
  {
    template <>
    struct ob_vector_traits<oceanbase::sql::ObSelInfo>
    {
      typedef oceanbase::sql::ObSelInfo* pointee_type;
      typedef oceanbase::sql::ObSelInfo value_type;
      typedef const oceanbase::sql::ObSelInfo const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  template <>
    struct ob_vector_traits<oceanbase::sql::ObIndexTableInfo>
    {
      typedef oceanbase::sql::ObIndexTableInfo* pointee_type;
      typedef oceanbase::sql::ObIndexTableInfo value_type;
      typedef const oceanbase::sql::ObIndexTableInfo const_value_type;
      typedef value_type* iterator;
      typedef const value_type* const_iterator;
      typedef int32_t difference_type;
    };
  }


  namespace sql
  {
    /**
     * @brief 选择率
     */
    typedef double Selectivity;		/* fraction of tuples a qualifier will pass */
    /**
     * @brief 代价
     */
    typedef double Cost;			/* execution cost (in page-access units??) */

    /* default selectivity estimate for equalities such as "A = b" */
    #define DEFAULT_EQ_SEL	0.005
   /* default selectivity estimate for inequalities such as "A < b" */
    #define DEFAULT_INEQ_SEL  0.3333333333333333
    /* default selectivity estimate for range inequalities "A > b AND A < c" */
    #define DEFAULT_RANGE_INEQ_SEL	0.005
    /* default selectivity estimate for pattern-match operators such as LIKE */
    #define DEFAULT_MATCH_SEL	0.005
    /* default number of distinct values in a table */
    #define DEFAULT_NUM_DISTINCT  200
    /* default selectivity estimate for boolean and null test nodes */
    #define DEFAULT_UNK_SEL			0.005
    #define DEFAULT_NOT_UNK_SEL		(1.0 - DEFAULT_UNK_SEL)

    /* default seq access page cost */
    #define DEFAULT_SEQ_PAGE_COST   1.0
    /* default random access page cost */
    #define DEFAULT_RANDOM_PAGE_COST  4.0
    /* default cpu tuple cost seq table */
    #define DEFAULT_CPU_TUPLE_COST  0.01
    /* default cpu tuple cost index table */
    #define DEFAULT_CPU_INDEX_TUPLE_COST 0.005
    /* default cpu operate cost */
    #define DEFAULT_CPU_OPERATOR_COST 0.0025
    /* default group by operate cost */
    #define DEFAULT_GROUP_BY_OPERATOR_COST 0.0025
    /* default order by operate cost*/
    #define DEFAULT_ORDER_BY_OPERATOR_COST 0.0025

    /* default table tuples*/
    #define DEFAULT_TABLE_TUPLES 20000
    /* default start cost*/
    #define DEFAULT_START_COST 10.0;

    #define DEFAULT_CPU_DISK_PROPORTION 0.03

    #define LOG2(x)  (log(x) / 0.693147180559945)

    #define CLAMP_PROBABILITY(p) \
      do { \
        if (p < 0.0) \
          p = 0.0; \
        else if (p > 1.0) \
          p = 1.0; \
      } while (0)

    static const uint64_t OPT_TABLE_MAP_SIZE = 100;
    static const uint64_t STAT_INFO_TABLE_MAP_SIZE = 100;
    static const uint64_t STAT_INFO_COL_MAP_SIZE = 200;
    static const uint64_t STAT_INFO_FREQ_MAP_SIZE = 10;
    static const double WIDTH_ARRY[]= {1.0,4.0,4.0, //ObNullType,ObIntType,ObFloatType
                                8.0,4.0,8.0,  //ObDoubleType,ObDateTimeType,ObPreciseDateTimeType
                                4.0,4.0,4.0,  //ObVarcharType,ObSeqType,ObCreateTimeType
                                4.0,4.0,1.0,  //ObModifyTimeType,ObExtendType,ObBoolType
                                4.0,4.0,4.0,  //ObDecimalType,ObDateType,ObTimeType
                                4.0,4.0       //ObIntervalType,ObInt32Type,
                                };


    /**
     * @brief The ObColumnStatInfo class
     */
    typedef struct ObColumnStatInfo
    {
      uint64_t  column_id_;    ///<  column id


      double  avg_width_;   ///< average length of column
      bool  unique_rowkey_column_;    ///< the column is primary key and primary key without another column

      double  distinct_num_;    ///< distinct value number
      oceanbase::common::ObExprObj  min_value_;   ///< min value
      oceanbase::common::ObExprObj  max_value_;   ///< max value
      double  avg_frequency_;    ///< average frequency in the column
      //high frequency value and count
      ObObj obj[10];  /// < high frequency value obj
      common::hash::ObHashMap<oceanbase::common::ObObj, double, common::hash::NoPthreadDefendMode> value_frequency_map_;   ///< k-column value v-frequency

      //add dhc [query optimization] 20170811 :b
      ObColumnStatInfo(){
        if (!value_frequency_map_.created())
        {
          value_frequency_map_.create(STAT_INFO_FREQ_MAP_SIZE);
        }
      }
      ~ObColumnStatInfo()
      {
        if(value_frequency_map_.created())
        {
          value_frequency_map_.destroy();
        }
      }
      //add e
    }ObColumnStatInfo;

    /**
     * @brief The ObBaseRelStatInfo class
     *
     */
    typedef struct ObBaseRelStatInfo
    {
      uint64_t  table_id_;    ///<  table id

      bool enable_statinfo;  ///< statistics info maybe is null

      //row info
      double tuples_;   ///< relation tuples
      //uint64_t sstable_num_;   ///< sstable number
      double size_;   ///<  sum all sstable size

      double  column_num_;  ///< column number
      double  avg_width_;   ///< average length of row

      //double  columns_width_ [OB_MAX_COLUMN_NUMBER];  ///< from schemua info

      uint64_t  statistic_columns_[OB_MAX_COLUMN_NUMBER];  ///< exist statistic columns
      int64_t  statistic_columns_num_;  ///<  columns number

      bool empty_table_;  ///< empty table

      //column info
      common::hash::ObHashMap<uint64_t, ObColumnStatInfo*, common::hash::NoPthreadDefendMode> column_id_value_map_;  ///< k-column_id v-col_stat_info

      //add dhc [query optimization] 20170811 :b
      ObBaseRelStatInfo(){
        enable_statinfo=true;
        if (!column_id_value_map_.created())
        {
          column_id_value_map_.create(STAT_INFO_COL_MAP_SIZE);
        }
      }
      ~ObBaseRelStatInfo()
      {
        if(column_id_value_map_.created())
        {
          for (common::hash::ObHashMap<uint64_t, ObColumnStatInfo*, common::hash::NoPthreadDefendMode>::iterator iter = column_id_value_map_.begin(); iter != column_id_value_map_.end(); iter++)
          {
            if(iter->second)
            {
              ObColumnStatInfo* col_stat_info = (ObColumnStatInfo*)(iter->second);;
              col_stat_info->~ObColumnStatInfo();
              iter->second = NULL;
            }
          }
          column_id_value_map_.destroy();
        }
      }
      //add e
    }ObBaseRelStatInfo;

    class ObOptimizerRelation;
    //    class mergeserver::ObMergeServerService;
    //    class mergeserver::StatisticTableValue;
    /**
     * @brief The ObStatSelCalculator class
     * selectivity calculator by statistics information
     */
    class ObStatSelCalculator
    {
      public:
        //rowkey column is unique
        static bool is_unique_rowkey_column(ObOptimizerRelation *rel_opt ,
                                            const uint64_t table_id,
                                            const uint64_t column_id);
        //equal selectivity
        static double get_equal_selectivity(ObOptimizerRelation *rel_opt ,
                                            ObSelInfo &sel_info,
                                            const oceanbase::common::ObObj &value);
        //equal subquery selectivity
        static double get_equal_subquery_selectivity(ObOptimizerRelation *rel_opt ,
                                                     ObSelInfo &sel_info);
        //less than <
        static double get_lt_selectivity(ObOptimizerRelation *rel_opt ,
                                         ObSelInfo &sel_info,
                                         const oceanbase::common::ObObj &value);
        //less than and equal <=
        static double get_le_selectivity(ObOptimizerRelation *rel_opt ,
                                         ObSelInfo &sel_info,
                                         const oceanbase::common::ObObj &value);
        // between and
        static double get_btw_selectivity(ObOptimizerRelation *rel_opt ,
                                          ObSelInfo &sel_info,
                                          const oceanbase::common::ObObj &value1,
                                          const oceanbase::common::ObObj &value2);
        // like 'xxx'
        static double get_like_selectivity(ObOptimizerRelation *rel_opt ,
                                           ObSelInfo &sel_info,
                                           const oceanbase::common::ObObj &value);

        //in ('xxx','xxx')
        static double get_in_selectivity(ObOptimizerRelation *rel_opt,
                                         ObSelInfo &sel_info,
                                         const common::ObArray<ObObj> &value_array);
    };

    /**
     * @brief The ObJoinStatCalculator class
     * join selectivity calculator by base table relinfo
     */
    class ObJoinStatCalculator
    {
      public:
        //joinrel selectivity
        static double calc_joinrel_size_estimate(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            const uint64_t join_type,
            const double outer_rows,
            const double inner_rows,
            oceanbase::common::ObList<ObSqlRawExpr*>& restrictlist);
        static Selectivity clauselist_selectivity(
            ObLogicalPlan *logical_plan,
            ObSelectStmt *select_stmt,
            const uint64_t join_type,
            oceanbase::common::ObList<ObSqlRawExpr*>& restrictlist);
        //Force a row-count estimate to a sane value.
        static double clamp_row_est(double nrows);
        static double rint(double x);
    };

    /**
     * @brief The ObStatCostCalculator class
     * cost calculator by statistics information
     */
    class ObStatCostCalculator
    {
       private:
          /**
           * @brief get_sort_cost
           * we don't separate in-memory sort and in-file sort
           * due to only open in-memory sort now but close in-file sort at oceanbase.
           * @param tuples
           * @param cost
           * @return
           */
          static int get_sort_cost(double tuples, Cost & cost);
          /**
           * @brief get_group_by_cost
           * @param rel_opt
           * @param index_table_info
           * @param cost
           * @return
           */
          static int get_group_by_cost(ObSelectStmt *select_stmt,
                                       ObOptimizerRelation *rel_opt,
                                       ObIndexTableInfo *index_table_info,
                                       Cost &cost);
          /**
           * @brief get_order_by_cost
           * group_by give priority to where index table,
           * if without index table then consider group by columns of index table
           * now we don't consider and compare group by columns of index table
           * and where cnd columns of index table
           * @param rel_opt
           * @param cost
           * @return
           */
          static int get_order_by_cost(ObSelectStmt *select_stmt,
                                       ObOptimizerRelation *rel_opt,
                                       ObIndexTableInfo *index_table_info,
                                       Cost &cost);
       public:
         // seq scan cost
          /**
          * @brief get_cost_seq_scan
          * @param select_stmt
          * @param rel_opt
          * @param cost
          * @return
          */
         static int get_cost_seq_scan(ObSelectStmt *select_stmt,
                                      ObOptimizerRelation *rel_opt,
                                      Cost & cost);
         // index scan cost
         /**
          * @brief get_cost_index_scan
          * commpute cost of index scan
          * @param rel_opt  realation optimization
          * @param index_table_id   id of index table
          * @param index_column_id   id of column of origin table as index table frist primary key
          * @param sel  where condition selectivity
          * @param is_back_table  need back origin table
          * @param cost return result
          * @return error code
          */
          static int get_cost_index_scan(ObSelectStmt *select_stmt,
                                         ObOptimizerRelation *rel_opt,
                                         ObIndexTableInfo &index_table_info,
                                         const double sel);
    };
    /**
     * @brief The ObStatExtractor class
     * get statistics content
     */
    class ObStatExtractor
    {
    public:
      /**
       * @brief fill_col_statinfo_map
       * get column level statistics information
       * @param table_id_statInfo_map
       * @param table_id
       * @param column_id
       * @param unique_column_rowkey
       * @return
       */
      int fill_col_statinfo_map(common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> * table_id_statInfo_map,
                                ObOptimizerRelation *rel_opt,
                                uint64_t column_id,
                                bool unique_column_rowkey = false);
      /**
       * @brief fill_table_statinfo_map
       * get table level statistics information
       * @param table_id_statInfo_map
       * @param table_id
       * @return
       */
      int fill_table_statinfo_map(common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> * table_id_statInfo_map,
                                  ObOptimizerRelation *rel_opt);

      //add huangcc 20170412:b
      /**
       * @brief get_statistic_info_cache
       * @return
       */
      inline mergeserver::ObStatisticInfoCache * get_statistic_info_cache()
      {
        return statistic_info_cache_;
      }
      /**
       * @brief set_statistic_info_cache
       * @param mss
       */
      inline void set_statistic_info_cache(mergeserver::ObStatisticInfoCache * cache)
      {
        statistic_info_cache_ = cache;
      }
      //add:e
    private:
      mergeserver::ObStatisticInfoCache *statistic_info_cache_;  ///< statistic info cache

    };


    /**
     * @brief The ObOptimizerFromItemHelper class
     *
     */
    typedef struct ObOptimizerFromItemHelper{
      double nrows;
      ObOptimizerRelation *left_base_rel_opt;
      ObOptimizerRelation *right_base_rel_opt;
      ObOptimizerRelation *left_rel_opt;
      ObOptimizerRelation *right_rel_opt;
      ObSqlRawExpr *cnd_id_expr;
      double left_join_rows;
      double right_join_rows;
      double diff_num_left;
      double diff_num_right;
      int cnd_it_id;
      ObOptimizerFromItemHelper()
      {
        nrows = 0;
        left_join_rows = 0;
        right_join_rows = 0;
        diff_num_left = 0;
        diff_num_right = 0;
        cnd_it_id = -1;
        left_base_rel_opt = NULL;
        right_base_rel_opt = NULL;
        left_rel_opt = NULL;
        right_rel_opt = NULL;
      }
      bool operator < (const ObOptimizerFromItemHelper & r) const
      {
        return nrows < r.nrows;
      }
    }ObOptimizerFromItemHelper;

    /**
     * @brief The ObFromItemJoinMethodHelper class
     *
     */
    typedef struct ObFromItemJoinMethodHelper{
      bool exchange_order;
      ObSqlRawExpr* where_sql_expr;
      int join_method;
      ObFromItemJoinMethodHelper()
      {
        exchange_order = false;
        where_sql_expr = NULL;
        join_method = 0;
      }
    }ObFromItemJoinMethodHelper;

    /**
     * @brief The ObOPtimizaerLoger class
     * print query optimizer handle sql process log
     */
    class ObOPtimizerLoger
    {
      public:
          static const bool log_switch_ = false;
          static const FILE* getFile()
          {
            FILE * file = fopen("query_optimizer.log", "a");
            if (NULL == file)
            {
              TBSYS_LOG(ERROR, "dhc check fopen output file failed");
            }
            return file;
          }
          static const void closeFile(const FILE* file)
          {
            FILE* fp = const_cast<FILE*>(file);
            if (NULL == fp)
            {
              TBSYS_LOG(ERROR, "dhc fclose failed");
            }
            else
            {
              fclose(fp);
            }
          }
          static const void resetFile()
          {
            FILE * file = fopen("query_optimizer.log", "w");
            if (NULL == file)
            {
              TBSYS_LOG(ERROR, "dhc check fopen output file failed");
            }
            else
            {
              fclose(file);
            }
          }

          static void print(const char * log_string)
          {
            if (!log_switch_)
            {
            }
            else
            {
              FILE* fp = NULL;
              //open file
              fp = fopen("query_optimizer.log","a");
              if (fp == NULL)
              {
                TBSYS_LOG(WARN,"QX can't open 'query_optimizer.log' file !");
              }
              //print
              else if (log_string != NULL)
              {
                fprintf(fp,"%s\n",log_string);
              }

              //close file
              if (fp != NULL)
              {
                fclose(fp);
              }
            }
          }
    };

    /**
     * @brief The ObOptimizerRelation class
     *
     */
    class ObOptimizerRelation
    {
      public:
        /**
         * @brief 节点类型，目前是基本表上构建OptInfo，Kind仅保留基本表
         */
        enum RelOptKind
        {
          RELOPT_BASEREL,
          RELOPT_JOINREL,
          RELOPT_SUBQUERY,
          RELOPT_SELECT_FOR_UPDATE,
          RELOPT_MAIN_QUERY,
          RELOPT_INIT
        };

        ObOptimizerRelation()
          : rel_opt_kind_(RELOPT_BASEREL)//,allocator_(page_size,ModulePageAllocator(ObModIds::OB_QUERY_OPT))
        {
          schema_managerv2_ = NULL;
          stat_extractor_ = NULL;
          table_id_statInfo_map_ = NULL;
          allocator_ = NULL;
          rows_ = 1.0;
          tuples_ = 1.0;
          join_rows_ = 1.0;
          table_id_ = OB_INVALID_ID;
          table_ref_id_ = OB_INVALID_ID;
          group_by_num_ = 0;
          order_by_num_ = 0;
        }
        explicit ObOptimizerRelation(const RelOptKind rel_opt_kind)
          : rel_opt_kind_(rel_opt_kind)
        {
          schema_managerv2_ = NULL;
          stat_extractor_ = NULL;
          table_id_statInfo_map_ = NULL;
          allocator_ = NULL;
          group_by_num_ = 0;
          order_by_num_ = 0;
        }
        explicit ObOptimizerRelation(const RelOptKind rel_opt_kind, uint64_t table_id)
          : rel_opt_kind_(rel_opt_kind), table_id_(table_id)
        {
          schema_managerv2_ = NULL;
          stat_extractor_ = NULL;
          table_id_statInfo_map_ = NULL;
          allocator_ = NULL;
          group_by_num_ = 0;
          order_by_num_ = 0;
        }
        //Notice: if derect free memory of ObOptimizerRelation object will not call ~ObOptimizerRelation() cause memory leak
        virtual ~ObOptimizerRelation()
        {
          //free memory
          clear();
        }

        void clear()
        {
          //free memory
          index_table_array_.clear();
          sel_info_array_.clear();
          //index_table_array_.~ObArray();
          //sel_info_array_.~ObArray();

          base_cnd_list_.clear();
          needed_columns_.clear();
          join_cnd_list_.clear();

        }

        void set_rel_opt_kind(const RelOptKind rel_opt_kind);
        void set_table_id(const uint64_t table_id);
        inline void set_table_ref_id(const uint64_t table_ref_id) {table_ref_id_ = table_ref_id;}
        void set_expr_id(const uint64_t expr_id);
        RelOptKind get_rel_opt_kind() const;
        uint64_t get_table_id() const;
        inline uint64_t get_table_ref_id() const {return table_ref_id_;}
        uint64_t get_expr_id() const;

        inline double get_rows() const {return rows_;}
        inline void set_rows(double rows) {rows_ = rows;}
        inline double get_tuples() const {return tuples_;}
        inline void set_tuples(double tuples) {tuples_ = tuples;}

        inline double get_join_rows() const {return join_rows_;}
        inline void set_join_rows(double join_rows) {join_rows_ = join_rows;}

        inline double get_width() const {return width_;}
        inline void set_width(double width) {width_ = width;}

        //inline bool get_is_back_table() {return is_back_table_;}
        //inline void set_is_back_table( bool is_back) {is_back_table_ = is_back;}

        inline int32_t get_group_by_num(){ return group_by_num_;}
        inline void set_group_by_num(int32_t group_by_num){group_by_num_ = group_by_num;}
        inline int32_t get_order_by_num(){ return order_by_num_;}
        inline void set_order_by_num(int32_t order_by_num){order_by_num_ = order_by_num;}

        inline Cost get_seq_scan_cost() const {return seq_scan_info_.cost_;}
        inline void set_seq_scan_cost(Cost seq_scan_cost) {seq_scan_info_.cost_ = seq_scan_cost;}

        inline ObSeqScanInfo get_seq_scan_info() {return seq_scan_info_;}
        inline void set_seq_scan_info(ObSeqScanInfo &seq_scan_info) {seq_scan_info_ = seq_scan_info;}

        //inline Cost get_index_scan_cost() const {return index_scan_cost_;}
        //inline void set_index_scan_cost(Cost index_scan_cost) {index_scan_cost_ = index_scan_cost;}

//        inline ObStatSelCalculator* get_sel_calculator() {return sel_calculator_;}
//        inline void set_sel_calculator(ObStatSelCalculator* sel_calculator) { sel_calculator_ = sel_calculator;}
        inline common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> * get_table_id_statInfo_map()
        {
          return table_id_statInfo_map_;
        }
        inline void set_table_id_statInfo_map(common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> *map)
        {
          table_id_statInfo_map_ = map;
        }

        inline ObStatExtractor * get_stat_extractor()
        {
          return stat_extractor_;
        }
        inline void set_stat_extractor(ObStatExtractor * stat_extractor)
        {
          stat_extractor_ = stat_extractor;
        }

        inline void set_name_pool(common::ObStringBuf* allocator)
        {
          allocator_ = allocator;
        }
        inline common::ObStringBuf* get_name_pool()
        {
          return allocator_;
        }

        inline common::ObVector<ObSqlRawExpr*> & get_base_cnd_list();
        inline common::ObVector<ObSqlRawExpr*> & get_join_cnd_list();

        inline common::ObVector<uint64_t> & get_needed_columns();

        inline const common::ObSchemaManagerV2 * get_schema_managerv2()
        {
          return schema_managerv2_;
        }
        inline void set_schema_managerv2(const common::ObSchemaManagerV2 *sm)
        {
          schema_managerv2_ = sm;
        }

        int get_column_stat_info(uint64_t table_id, uint64_t column_id, ObColumnStatInfo* &col_stat_info);

        int get_base_rel_stat_info(uint64_t table_id, ObBaseRelStatInfo* & rel_stat_info);

        inline common::ObVector<ObIndexTableInfo> &get_index_table_array();//{ return index_table_array_;}

        inline common::ObVector<ObSelInfo> & get_sel_info_array();//{return sel_info_array_;}

        void reset_semi_join_right_index_table_cost(uint64_t column_id,double sel = 0.0);

        inline int copy_obj(ObObj &src,ObObj &dest);

        //virtual void print(FILE* fp, int32_t level, int32_t index) {}
        void print_rel_opt_info();
        void print_rel_opt_info(const FILE* file);
        void print_rel_opt_info_V2();
      protected:
        void print_indentation(FILE* fp, int32_t level) const;

      private:
        RelOptKind  rel_opt_kind_;    ///<  opt type
        uint64_t  table_id_;    ///<  table id
        uint64_t  table_ref_id_;   ///< real table id

        /* size estimates generated by planner */
        double	rows_;			/* estimated number of result tuples */
        double  tuples_;    ///< relation tuples
        double  join_rows_;  ///<  rows after join

        double  width_;   ///< relation row width for IO of net

        /* cost of various query relation ways */
        //Cost    seq_scan_cost_;    ///< sequential scan
        //Cost    index_scan_cost_;   ///< index scan
        //oceanbase::common::ObList<ObPath*>    path_lsit_;

        //bool    is_back_table_;   ///< need back table if use index
        //uint64_t index_table_id_;   ///< index table id

        int32_t  group_by_num_;   ///< group by exprs number
        int32_t  order_by_num_;   ///< order by exprs number

        ObSeqScanInfo seq_scan_info_;  ///<  sequential scan info

        common::ObVector<ObIndexTableInfo> index_table_array_;  ///< canndidate index table info

        common::ObVector<ObSelInfo> sel_info_array_; ///< where cnd expr selectivity store


        common::ObVector<ObSqlRawExpr*> base_cnd_list_;    ///< base condition expr in the relation
        common::ObVector<ObSqlRawExpr*> join_cnd_list_;    ///< join condition expr in the relation

        common::ObVector<uint64_t> needed_columns_;   ///< need to project columns

        const common::ObSchemaManagerV2 * schema_managerv2_;  ///< relation schema

        //ObStatSelCalculator * sel_calculator_;   ///< selectivity calculator
        common::hash::ObHashMap<uint64_t, ObBaseRelStatInfo*, common::hash::NoPthreadDefendMode> * table_id_statInfo_map_;  ///<  k-table_id v-table_stat_info
        ObStatExtractor * stat_extractor_;  ///< extract stat_info

        //common::ModuleArena allocator_;
        //static const int64_t page_size = 64 * 1024;

        common::ObStringBuf* allocator_;

    };

    inline void ObOptimizerRelation::set_rel_opt_kind(RelOptKind rel_opt_kind)
    {
      rel_opt_kind_ = rel_opt_kind;
    }

    inline ObOptimizerRelation::RelOptKind ObOptimizerRelation::get_rel_opt_kind() const
    {
      return rel_opt_kind_;
    }

    inline uint64_t ObOptimizerRelation::get_table_id() const
    {
      return table_id_;
    }

    inline void ObOptimizerRelation::set_table_id(const uint64_t table_id)
    {
      table_id_ = table_id;
    }

    inline void ObOptimizerRelation::print_indentation(FILE* fp, int32_t level) const
    {
      for(int i = 0; i < level; ++i)
        fprintf(fp, "    ");
    }

    inline common::ObVector<ObSqlRawExpr*> & ObOptimizerRelation::get_base_cnd_list()
    {
      return base_cnd_list_;
    }

    inline common::ObVector<ObSqlRawExpr*> & ObOptimizerRelation::get_join_cnd_list()
    {
      return join_cnd_list_;
    }

    inline common::ObVector<uint64_t> & ObOptimizerRelation::get_needed_columns()
    {
      return needed_columns_;
    }

    inline common::ObVector<ObIndexTableInfo> & ObOptimizerRelation::get_index_table_array()
    {
      return index_table_array_;
    }

    inline common::ObVector<ObSelInfo> &ObOptimizerRelation::get_sel_info_array()
    {
      return sel_info_array_;
    }

    inline int ObOptimizerRelation::copy_obj(ObObj &src,ObObj &dest)
    {
      int ret = OB_SUCCESS;
      if (allocator_ == NULL)
      {
        TBSYS_LOG(WARN,"QX allocator_ is null! using shallow copy.");
        src.obj_copy(dest);
      }
//      else if(OB_SUCCESS != (ret = ob_write_obj_v2(*allocator_,src,dest)))
//      {
//        TBSYS_LOG(WARN,"QX write object fail. ret = %d",ret);
//      }
      else if(OB_SUCCESS != (ret = ob_write_obj(*allocator_,src,dest)))
      {
        TBSYS_LOG(WARN,"QX write object fail. ret = %d",ret);
      }
      return ret;
    }

  }
}

#endif // OCEANBASE_SQL_OB_OPTIMIZER_RELATION_H_


