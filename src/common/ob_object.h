<<<<<<< HEAD
/**
 * Copyright (C) 2013-2016 ECNU_DaSE.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * @file ob_object.h
 * @brief ob basic object
 *
 * modified by wangdonghui:add a function member is_numerical
 *
 * @version __DaSE_VERSION
 * @author zhutao <zhutao@stu.ecnu.edu.cn>
 * @author wangdonghui <zjnuwangdonghui@163.com>
 * @date 2016_07_29
 */


=======
>>>>>>> refs/remotes/origin/master
#ifndef OCEANBASE_COMMON_OB_OBJECT_H_
#define OCEANBASE_COMMON_OB_OBJECT_H_

#include "ob_define.h"
#include "ob_string.h"
#include "tbsys.h"
#include "ob_action_flag.h"
#include "ob_obj_type.h"
#include "ob_number.h"
<<<<<<< HEAD
//add   fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
#include "Ob_Decimal.h"
//add e
=======

>>>>>>> refs/remotes/origin/master
namespace oceanbase
{
  namespace tests
  {
    namespace common
    {
      class ObjTest;
    }
  }
  namespace common
  {
    struct ObObjMeta
    {
      uint32_t type_:8;
      uint32_t dec_vscale_:6;
<<<<<<< HEAD
      uint32_t dec_precision_:7;
      uint32_t dec_scale_:6;
      uint32_t op_flag_:2;
      uint32_t dec_nwords_:3; // the actual nwords is dec_nwords_ + 1, so the range of nwords is [1, 8]
      //uint32_t dec_neg:1;
=======
      uint32_t op_flag_:2;
      uint32_t dec_precision_:7;
      uint32_t dec_scale_:6;
      uint32_t dec_nwords_:3; // the actual nwords is dec_nwords_ + 1, so the range of nwords is [1, 8]
>>>>>>> refs/remotes/origin/master
    };

    union _ObjValue
    {
      int64_t int_val;
      float float_val;
      double double_val;
      ObDateTime second_val;
      ObPreciseDateTime microsecond_val;
      ObCreateTime      create_time_val;
      ObModifyTime      modify_time_val;
      bool bool_val;
    };

    class ObBatchChecksum;
    class ObExprObj;
    class ObObj
    {
      public:
        // min, max extend value
        static const int64_t MIN_OBJECT_VALUE         = UINT64_MAX-2;
        static const int64_t MAX_OBJECT_VALUE         = UINT64_MAX-1;
      public:
        ObObj();

        ObObj(const ObObjType type, const int8_t flag, const int32_t val_len, const int64_t value);
        /// set add flag
        int set_add(const bool is_add = false);
        bool get_add()const;
        bool get_add_fast() const;
        /*
         *   设置类型
         */
        void set_type(const ObObjType& type);
        /*
         *   设置列值
         */
        void set_int(const int64_t value,const bool is_add = false);
        void set_float(const float value,const bool is_add = false);
        void set_double(const double value,const bool is_add = false);
        void set_ext(const int64_t value);
        /*
         *   设置日期类型，如果date_time为OB_SYS_DATE，表示设置为服务器当前时间
         */
        void set_datetime(const ObDateTime& value,const bool is_add = false);
        void set_precise_datetime(const ObPreciseDateTime& value,const bool is_add = false);
        void set_varchar(const ObString& value);
        void set_seq();
        void set_modifytime(const ObModifyTime& value);
        void set_createtime(const ObCreateTime& value);
        void set_bool(const bool value);

        /*
         *   设置列值为空
         */
        void set_null();
        bool is_null() const;
        void set_min_value();
        void set_max_value();

        /// @fn apply mutation to this obj
        int apply(const ObObj &mutation);


        void reset();
        void reset_op_flag();
        bool is_valid_type() const;
        bool is_min_value() const;
        bool is_max_value() const;

        void dump(const int32_t log_level = TBSYS_LOG_LEVEL_DEBUG) const;
        void print_value(FILE* fd);
        int64_t to_string(char* buffer, const int64_t length) const;
<<<<<<< HEAD
        int64_t to_string_v2(std::string &s) const;//add gaojt [ListAgg][JHOBv0.1]20150104
=======
>>>>>>> refs/remotes/origin/master
        //
        NEED_SERIALIZE_AND_DESERIALIZE;
        /*
         *   获取列值，用户根据已知的数据类型调用相应函数，如果类型不符则返回失败
         */
        int get_int(int64_t& value,bool& is_add) const;
        int get_int(int64_t& value) const;

        int get_float(float& value,bool& is_add) const;
        int get_float(float& value) const;

        int get_double(double& value,bool& is_add) const;
        int get_double(double& value) const;

        int get_ext(int64_t& value) const;
        int64_t get_ext() const;

        int get_datetime(ObDateTime& value,bool& is_add) const;
        int get_datetime(ObDateTime& value) const;

        int get_precise_datetime(ObPreciseDateTime& value,bool& is_add ) const;
        int get_precise_datetime(ObPreciseDateTime& value) const;

        int get_modifytime(ObModifyTime& value) const;
        int get_createtime(ObCreateTime& value) const;
        /*
         *   获取varchar类型数据，直接使用ObObj内部缓冲区
         */
        int get_varchar(ObString& value) const;
        int get_bool(bool &value) const;

<<<<<<< HEAD
        //add  fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
        int get_decimal(ObString& value) const;
        int set_decimal(const ObString& value);
        int set_decimal(const char* value);
        int set_decimal(const ObString& value,uint32_t p,uint32_t s,uint32_t v);
        //add e

        //add xsl ECNU_DECIMAL 2016_12
        int set_ttint(const uint64_t *ttint);        //not in use
        uint64_t* get_ttint() const;
        int set_decimal(const uint64_t* od, uint32_t p, uint32_t s, uint32_t v, uint32_t len);
        //int set_decimal(const TTInt* od,uint32_t p,uint32_t s,uint32_t v);
        int set_decimal(const ObDecimal* od);
        int set_decimal(const ObDecimal& od);
        int set_decimal_v2(const ObDecimal& od, uint32_t len);
        int get_decimal(ObDecimal& value) const;
        int get_decimal_v2(ObDecimal& value) const;
        //add e
        uint32_t get_precision() const;
        uint32_t get_scale() const;
        uint32_t get_vscale() const;
        uint32_t get_nwords() const;
        void set_nwords(uint32_t value);
        void set_precision(uint32_t value);
        void set_scale(uint32_t value);
        void set_vscale(uint32_t value);
        int get_varchar_d(ObString& value) const;
        const char* get_word();
        //add for obj_hash row_key
        int from_hash(TTCInt &tc,const char* buf, int64_t buf_len)const ;
        //add e
        //int set_decimal(const ObNumber &num, int8_t precision = 38, int8_t scale = 0, bool is_add = false);
=======
        int set_decimal(const ObNumber &num, int8_t precision = 38, int8_t scale = 0, bool is_add = false);
>>>>>>> refs/remotes/origin/master
        int get_decimal(ObNumber &num) const;
        int get_decimal(ObNumber &num, bool &is_add) const;
        /*
         *   获取数据类型
         */
        ObObjType get_type(void) const;
<<<<<<< HEAD
        //add dhc[in post-expression optimization/query optimizer]@20151121:b
        int obj_copy(ObObj&) const;
        //add:e
=======
>>>>>>> refs/remotes/origin/master

        int32_t get_val_len() const;
        void set_val_len(const int32_t val_len);
        bool need_deep_copy()const;
        bool is_true() const;
<<<<<<< HEAD
        //add by wdh 20160704 :b
        /**
         * @brief is_numerical
         * judge object whether is numerical type
         * @return bool value
         */
        bool is_numerical() const;
        //add :e
=======
>>>>>>> refs/remotes/origin/master
        static const char* get_sql_type(ObObjType type);
        /*
         *   计算obj内数据的校验和
         */
        int64_t checksum(const int64_t current) const;
        void checksum(ObBatchChecksum &bc) const;

        uint32_t murmurhash2(const uint32_t hash) const;
        uint64_t murmurhash64A(const uint64_t hash) const;

        int64_t hash() const;   // for ob_hashtable.h

        bool operator<(const ObObj &that_obj) const;
        bool operator>(const ObObj &that_obj) const;
        bool operator<=(const ObObj &that_obj) const;
        bool operator>=(const ObObj &that_obj) const;
        bool operator==(const ObObj &that_obj) const;
        bool operator!=(const ObObj &that_obj) const;
        int compare(const ObObj &other) const;
        int  compare_same_type(const ObObj &other) const;

        int get_timestamp(int64_t & timestamp) const;

        const void *get_data_ptr() const
        {
          const void *ret = NULL;
          if (ObVarcharType == get_type())
          {
            ret = const_cast<char*>(value_.varchar_val);
          }
          else
          {
            ret = &value_;
          }
          return ret;
        };

        int64_t get_data_length() const
        {
          int64_t ret = sizeof(value_);
          if (ObVarcharType == get_type())
          {
            ret = val_len_;
          }
          return ret;
        };
      private:
        friend class tests::common::ObjTest;
        friend class ObCompactCellWriter;
        friend class ObCompactCellIterator;
        friend class ObExprObj;
        bool is_datetime() const;
        bool can_compare(const ObObj & other) const;
        void set_flag(bool is_add);
      private:
        static const uint8_t INVALID_OP_FLAG = 0x0;
        static const uint8_t ADD = 0x1;
        static const uint8_t META_VSCALE_MASK = 0x3F;
        static const uint8_t META_OP_FLAG_MASK = 0x3;
        static const uint8_t META_PREC_MASK = 0x7F;
        static const uint8_t META_SCALE_MASK = 0x3F;
        static const uint8_t META_NWORDS_MASK = 0x7;
        ObObjMeta meta_;
      int32_t val_len_;
      union          // value实际内容
      {
        int64_t int_val;
        int64_t ext_val;
        float float_val;
        double double_val;
        ObDateTime time_val;
        ObPreciseDateTime precisetime_val;
        ObModifyTime modifytime_val;
        ObCreateTime createtime_val;
        const char *varchar_val;
        bool bool_val;
        uint32_t *dec_words_;
<<<<<<< HEAD
        //modify  fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
        const char* word;
        //modify e
        //add xsl ECNU_DECIMAL 2016_12
        //ObDecimal *dec;
        //TTInt *tt;
        uint64_t *ii;
        //add e
=======
>>>>>>> refs/remotes/origin/master
      } value_;
    };

    inline ObObj::ObObj()
    {
      reset();
    }

    inline ObObj::ObObj(const ObObjType type, const int8_t flag, const int32_t val_len, const int64_t value)
    {
      meta_.type_ = static_cast<int8_t>(type);
      meta_.op_flag_ = flag & 0x3;
      meta_.dec_vscale_ = 0;
      meta_.dec_precision_ = 0;
      meta_.dec_nwords_ = 0;
      meta_.dec_scale_ = 0;
      val_len_ = val_len;
      memcpy(&value_, &value, sizeof(value_));
    }

    inline void ObObj::reset()
    {
      memset(this, 0, sizeof(ObObj));
    }

    inline void ObObj::set_flag(bool is_add)
    {
      uint8_t flag = is_add ? ADD : INVALID_OP_FLAG;
      meta_.op_flag_ = flag & META_OP_FLAG_MASK;
    }

    inline void ObObj::reset_op_flag()
    {
      if (ObExtendType == get_type() && ObActionFlag::OP_NOP == value_.ext_val)
      {
        meta_.type_ = ObNullType;
      }
      meta_.op_flag_ = INVALID_OP_FLAG;
    }

    inline bool ObObj::get_add()const
    {
      bool ret = false;
      switch (get_type())
      {
        case ObIntType:
        case ObDateTimeType:
        case ObPreciseDateTimeType:
          ret = (meta_.op_flag_ == ADD);
          break;
        default:
          ;/// do nothing
      }
      return ret;
    }

    inline bool ObObj::get_add_fast() const
    {
      return (meta_.op_flag_ == ADD);
    }

    inline int ObObj::set_add(const bool is_add /*=false*/)
    {
      int ret = OB_SUCCESS;
      switch (get_type())
      {
        case ObIntType:
        case ObDateTimeType:
        case ObPreciseDateTimeType:
        case ObDecimalType:
        case ObFloatType:
        case ObDoubleType:
          set_flag(is_add);
          break;
        default:
          TBSYS_LOG(ERROR, "check obj type failed:type[%d]", get_type());
          ret = OB_ERROR;
      }
      return ret;
    }

    inline void ObObj::set_type(const ObObjType& type)
    {
      meta_.type_ = static_cast<unsigned char>(type);
    }

    inline void ObObj::set_int(const int64_t value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObIntType;
      value_.int_val = value;
    }

    inline void ObObj::set_float(const float value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObFloatType;
      value_.float_val = value;
    }

    inline void ObObj::set_double(const double value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObDoubleType;
      value_.double_val = value;
    }

    inline void ObObj::set_datetime(const ObDateTime& value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObDateTimeType;
      value_.time_val= value;
    }

    inline void ObObj::set_precise_datetime(const ObPreciseDateTime& value,const bool is_add /*=false*/)
    {
      set_flag(is_add);
      meta_.type_ = ObPreciseDateTimeType;
      value_.precisetime_val = value;
    }

    inline void ObObj::set_varchar(const ObString& value)
    {
      meta_.type_ = ObVarcharType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.varchar_val = value.ptr();
      val_len_ = value.length();
    }

    inline void ObObj::set_seq()
    {
      meta_.type_ = ObSeqType;
      meta_.op_flag_ = INVALID_OP_FLAG;
    }

    inline void ObObj::set_modifytime(const ObModifyTime& value)
    {
      meta_.type_ = ObModifyTimeType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.modifytime_val = value;
    }

    inline void ObObj::set_createtime(const ObCreateTime& value)
    {
      meta_.type_ = ObCreateTimeType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.createtime_val = value;
    }
<<<<<<< HEAD
    // add  fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
        inline uint32_t ObObj::get_precision() const
        {
            return meta_.dec_precision_;
        }

        inline uint32_t ObObj::get_scale() const {
            return meta_.dec_scale_;
        }

        inline uint32_t ObObj::get_vscale() const {
            return meta_.dec_vscale_;
        }

        inline uint32_t ObObj::get_nwords() const
        {
            return meta_.dec_nwords_;
        }
        inline void ObObj::set_nwords(uint32_t value)
        {
            meta_.dec_nwords_ = static_cast<uint8_t>(value) & META_NWORDS_MASK;
        }


        inline void ObObj::set_precision(uint32_t value) {
            meta_.dec_precision_ = static_cast<uint8_t>(value) & META_PREC_MASK;
        }
        inline void ObObj::set_scale(uint32_t value) {
            meta_.dec_scale_ = static_cast<uint8_t>(value) & META_SCALE_MASK;
        }
        inline void ObObj::set_vscale(uint32_t value) {
            meta_.dec_vscale_ = static_cast<uint8_t>(value) & META_SCALE_MASK;
        }

        inline int ObObj::get_varchar_d(ObString& value) const {
            int res = OB_OBJ_TYPE_ERROR;
            value.assign_ptr(const_cast<char *>(value_.varchar_val), val_len_);
            res = OB_SUCCESS;
            return res;
        }

    //add:e
=======

>>>>>>> refs/remotes/origin/master
    inline void ObObj::set_bool(const bool value)
    {
      meta_.type_ = ObBoolType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.bool_val = value;
    }

    inline void ObObj::set_null()
    {
      meta_.type_ = ObNullType;
      meta_.op_flag_ = INVALID_OP_FLAG;
    }

    inline bool ObObj::is_null() const
    {
      return meta_.type_ == ObNullType;
    }

    inline void ObObj::set_ext(const int64_t value)
    {
      meta_.type_ = ObExtendType;
      meta_.op_flag_ = INVALID_OP_FLAG;
      value_.ext_val = value;
    }

    inline void ObObj::set_min_value()
    {
      set_ext(MIN_OBJECT_VALUE);
    }

    inline void ObObj::set_max_value()
    {
      set_ext(MAX_OBJECT_VALUE);
    }

    inline bool ObObj::is_valid_type() const
    {
      ObObjType type = get_type();
      bool ret = true;
      if (type <= ObMinType || type >= ObMaxType)
      {
        ret = false;
      }
      return ret;
    }

    inline bool ObObj::is_min_value() const
    {
      return meta_.type_ == ObExtendType && value_.ext_val == MIN_OBJECT_VALUE;
    }

    inline bool ObObj::is_max_value() const
    {
      return meta_.type_ == ObExtendType && value_.ext_val == MAX_OBJECT_VALUE;
    }

    inline int ObObj::get_int(int64_t& value,bool& is_add) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObIntType)
      {
        is_add = (ADD == meta_.op_flag_);
        value = value_.int_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_int(int64_t& value) const
    {
      bool add = false;
      return get_int(value,add);
    }


    inline bool ObObj::need_deep_copy()const
    {
      return (meta_.type_ == ObVarcharType);
    }

    inline int ObObj::get_datetime(ObDateTime& value,bool& is_add) const
    {
      int ret = OB_OBJ_TYPE_ERROR;
      if (ObDateTimeType == meta_.type_)
      {
        value = value_.time_val;
        is_add = (meta_.op_flag_ == ADD);
        ret = OB_SUCCESS;
      }
      return ret;
    }

    inline int ObObj::get_datetime(ObDateTime& value) const
    {
      bool add = false;
      return get_datetime(value,add);
    }

    inline int ObObj::get_precise_datetime(ObPreciseDateTime& value,bool& is_add) const
    {
      int ret = OB_OBJ_TYPE_ERROR;
      if (ObPreciseDateTimeType == meta_.type_)
      {
        value = value_.precisetime_val;
        is_add = (meta_.op_flag_ == ADD);
        ret = OB_SUCCESS;
      }
      return ret;
    }

    inline int ObObj::get_precise_datetime(ObPreciseDateTime& value) const
    {
      bool add = false;
      return get_precise_datetime(value,add);
    }

    inline int ObObj::get_varchar(ObString& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObVarcharType)
      {
        value.assign_ptr(const_cast<char *>(value_.varchar_val), val_len_);
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_modifytime(ObModifyTime& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (ObModifyTimeType == meta_.type_)
      {
        value = value_.modifytime_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_createtime(ObCreateTime& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (ObCreateTimeType == meta_.type_)
      {
        value = value_.createtime_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_ext(int64_t& value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (ObExtendType == meta_.type_)
      {
        value = value_.ext_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int64_t ObObj::get_ext() const
    {
      int64_t res = 0;
      if (ObExtendType == meta_.type_)
      {
        res = value_.ext_val;
      }
      return res;
    }

    inline ObObjType ObObj::get_type(void) const
    {
      return static_cast<ObObjType>(meta_.type_);
    }

    inline int32_t ObObj::get_val_len() const
    {
      return val_len_;
    }

    inline void ObObj::set_val_len(const int32_t val_len)
    {
      val_len_ = val_len;
    }

    inline int ObObj::get_float(float& value,bool& is_add) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObFloatType)
      {
        value = value_.float_val;
        is_add = (meta_.op_flag_ == ADD);
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_float(float& value) const
    {
      bool add = false;
      return get_float(value,add);
    }

    inline int ObObj::get_double(double& value,bool& is_add) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (meta_.type_ == ObDoubleType)
      {
        value = value_.double_val;
        is_add = (meta_.op_flag_ == ADD);
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int ObObj::get_double(double& value) const
    {
      bool add = false;
      return get_double(value,add);
    }

    inline int ObObj::get_bool(bool &value) const
    {
      int res = OB_OBJ_TYPE_ERROR;
      if (get_type() == ObBoolType)
      {
        value = value_.bool_val;
        res = OB_SUCCESS;
      }
      return res;
    }

    inline int64_t ObObj::hash() const
    {
      return this->murmurhash2(0);
    }

    inline bool ObObj::is_datetime() const
    {
      return ((meta_.type_ == ObDateTimeType)
          || (meta_.type_ == ObPreciseDateTimeType)
          || (meta_.type_ == ObCreateTimeType)
          || (meta_.type_ == ObModifyTimeType));
    }
<<<<<<< HEAD
    // add by wdh 20160704 :b
    inline bool ObObj::is_numerical() const
    {
        return((meta_.type_ == ObIntType)
             ||(meta_.type_ == ObFloatType)
             ||(meta_.type_ == ObDoubleType));
    }
    //add :e
=======

>>>>>>> refs/remotes/origin/master
    inline bool ObObj::can_compare(const ObObj & other) const
    {
      bool ret = false;
      if ((get_type() == ObNullType) || (other.get_type() == ObNullType)
          || is_min_value() || is_max_value()
          || other.is_min_value() || other.is_max_value()
          || (get_type() == other.get_type()) || (is_datetime() && other.is_datetime()))
      {
        ret = true;
      }
      return ret;
    }

    inline int ObObj::get_timestamp(int64_t & timestamp) const
    {
      int ret = OB_SUCCESS;
      switch(meta_.type_)
      {
        case ObDateTimeType:
          timestamp = value_.time_val * 1000 * 1000L;
          break;
        case ObPreciseDateTimeType:
          timestamp = value_.precisetime_val;
          break;
        case ObModifyTimeType:
          timestamp = value_.modifytime_val;
          break;
        case ObCreateTimeType:
          timestamp = value_.createtime_val;
          break;
        default:
          TBSYS_LOG(ERROR, "unexpected branch");
          ret = OB_OBJ_TYPE_ERROR;
      }
      return ret;
    }

<<<<<<< HEAD
    //modify xsl ECNU_DECIMAL 2016_11
    //modify fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
   template <typename AllocatorT>
   int ob_write_obj(AllocatorT &allocator, const ObObj &src, ObObj &dst)
   {
     int ret = OB_SUCCESS;
     if (ObVarcharType != src.get_type()&&ObDecimalType != src.get_type())
     {
       dst = src;
     }
     else
     {
       if(ObDecimalType == src.get_type())
       {
           uint64_t *t1 = NULL;
           uint64_t *t2 = NULL;
           uint32_t len = src.get_nwords();
           t1 = src.get_ttint();
           if (OB_SUCCESS == (ret = ob_write_decimal(allocator, t1,len, t2)))
           {
               dst.set_decimal(t2,src.get_precision(),src.get_scale(),src.get_vscale(),len);
           }
       }
       else
       {
           ObString str;
           ObString str_clone;
           src.get_varchar(str);
           if (OB_SUCCESS == (ret = ob_write_string(allocator, str, str_clone)))
           {
               dst.set_varchar(str_clone);
           }
       }
     }
     return ret;
   }
   //modify:e

   //modify xsl ECNU_DECIMAL 2016_12
   //add fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
   template <typename AllocatorT>
   int ob_write_obj_v2(AllocatorT &allocator, const ObObj &src, ObObj &dst)   //引用
   {
       int ret = OB_SUCCESS;
       if (ObVarcharType != src.get_type()&&ObDecimalType != src.get_type())
       {
           dst = src;
       }
       else
       {
           if(ObDecimalType == src.get_type())
           {               
               TTInt tt;
               uint64_t *src_val = NULL;
               uint64_t *dst_val = NULL;
               uint32_t len = src.get_nwords();
               src_val = src.get_ttint();
               tt.FromUInt_v2(src_val,len);
               ObDecimal dec2;
               dec2.from(src.get_precision(),src.get_scale(),src.get_vscale(),tt); //p=schema_p,s=schema_s,vs=dec_vs
               if(OB_SUCCESS != ret)
               {
                   TBSYS_LOG(WARN, "faild to do get_decimal()");
               }
               else if(OB_SUCCESS != (ret = dec2.modify_value(src.get_precision(),src.get_scale())))
               {
                   //TBSYS_LOG(WARN, "faild to do modify_value(),od.p=%d,od.s=%d,od.v=%d,src.p=%d,src.s=%d", str2.get_precision(),str2.get_scale(),str2.get_vscale(),src.get_precision(),src.get_scale());
               }
               else
               {
                   if (OB_SUCCESS == (ret = ob_write_decimal(allocator, dec2.get_words()->ToUInt_v2(),len,dst_val)))   //深拷贝，在内存管理器中申请一块内存
                   {
                       dst.set_decimal(dst_val,dec2.get_precision(),dec2.get_scale(),dec2.get_vscale(),len);   //指针给了dst
                   }
               }
           }
           else  //ObString
           {
               ObString str;
               ObString str_clone;
               src.get_varchar(str);
               if (OB_SUCCESS == (ret = ob_write_string(allocator, str, str_clone)))
               {
                   dst.set_varchar(str_clone);
               }
           }
       }
       return ret;
   }
   //modify e
=======
    template <typename AllocatorT>
    int ob_write_obj(AllocatorT &allocator, const ObObj &src, ObObj &dst)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(ObVarcharType != src.get_type()))
      {
        dst = src;
      }
      else
      {
        ObString str;
        ObString str_clone;
        src.get_varchar(str);
        if (OB_SUCCESS == (ret = ob_write_string(allocator, str, str_clone)))
        {
          dst.set_varchar(str_clone);
        }
      }
      return ret;
    }
>>>>>>> refs/remotes/origin/master
  }
}

#endif //
