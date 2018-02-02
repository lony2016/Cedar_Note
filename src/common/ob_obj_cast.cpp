/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_obj_cast.cpp
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#include "ob_obj_cast.h"
#include "common/utility.h"
#include <time.h>

namespace oceanbase
{
  namespace common
  {
    static int identity(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      if (&in != &out)
      {
        out = in;
      }
      return OB_SUCCESS;
    }
    static int not_support(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      UNUSED(in);
      UNUSED(out);
      return OB_NOT_SUPPORTED;
    }
    ////////////////////////////////////////////////////////////////
    // Int -> XXX
    static int int_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_float(static_cast<float>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_double(static_cast<double>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_datetime(static_cast<ObDateTime>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_int()));
      return OB_SUCCESS;
    }
    static int varchar_printf(ObExprObj &out, const char* format, ...)  __attribute__ ((format (printf, 2, 3)));
    static int varchar_printf(ObExprObj &out, const char* format, ...)
    {
      int ret = OB_SUCCESS;
      ObString varchar = out.get_varchar();
      if (NULL == varchar.ptr() || varchar.length() <= 0)
      {
        TBSYS_LOG(WARN, "output buffer for varchar not enough, buf_len=%u", varchar.length());
        ret = OB_INVALID_ARGUMENT;
      }
      else
      {
        va_list args;
        va_start(args, format);
        int length = vsnprintf(varchar.ptr(), varchar.length(), format, args);
        va_end(args);
        ObString varchar2(varchar.length(), length, varchar.ptr());
        out.set_varchar(varchar2);
      }
      return ret;
    }
    static int int_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      ret = varchar_printf(out, "%ld", in.get_int());
      return ret;
    }
    //modify xsl ECNU_DECIMAL
    //add  fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
    static int int_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObIntType);
      int64_t tmp = in.get_int();    //-1
      TTInt t1(tmp);
      uint32_t num=0;
      if(tmp<0)
      {
          tmp = -tmp;
      }
      while(tmp)
      {
          num++;
          tmp/=10;
      }
      ObDecimal od;
      od.from(num,0,0,t1);
      if(params.is_modify)
      {
          if((params.precision-params.scale)< num)
          {
              ret=OB_DECIMAL_UNLEGAL_ERROR;
              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,params.precision=%d,params.scale=%d,num=%d", params.precision,params.scale,num);
          }
          else
          {
              od.set_precision(params.precision);
              od.set_scale(params.scale);
              od.set_vscale(0);
          }
      }
      if(ret==OB_SUCCESS)
      {
          out.set_decimal(od);
          //add xsl ECNU_DECIMAL 20170717
          uint32_t len = 1;
          if(od.get_words()->table[1] != 0)
              len = 2;
          out.set_len(len);
         //add e
      }
      return ret;
    }
    //add:e
    //modify e
    static int int_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_int()));
      return OB_SUCCESS;
    }
    static int int_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      out.set_bool(static_cast<bool>(in.get_int()));
      return OB_SUCCESS;
    }
    /* delete xsl ECNU_DECIMAL 2017_2
    static int int_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObIntType);
      ObNumber num;
      num.from(in.get_int());
      out.set_decimal(num);     // @todo optimize
      return OB_SUCCESS;
    }
    */
    ////////////////////////////////////////////////////////////////
    // Float -> XXX
    static int float_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_int(static_cast<int64_t>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_double(static_cast<double>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_datetime(static_cast<ObDateTime>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      return varchar_printf(out, "%f", in.get_float());
    }
    static int float_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_float()));
      return OB_SUCCESS;
    }
    static int float_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      out.set_bool(static_cast<bool>(in.get_float()));
      return OB_SUCCESS;
    }
    //modify xsl ECNU_DECIMAL 2017_2
    static int float_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObFloatType);
      static const int64_t MAX_FLOAT_PRINT_SIZE = 64;
      char buf[MAX_FLOAT_PRINT_SIZE];
      snprintf(buf, MAX_FLOAT_PRINT_SIZE, "%f", in.get_float());
      ObNumber num;
      if (OB_SUCCESS != (ret = num.from(buf)))
      {
        TBSYS_LOG(WARN, "failed to convert float to decimal, err=%d", ret);
      }
      else
      {
        out.set_decimal(num);
      }
      return ret;*/

      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObFloatType);
      ret=varchar_printf(out, "%f", in.get_float());
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in float_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  {
                              ret=OB_DECIMAL_UNLEGAL_ERROR;
                              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  //out.set_varchar(os);
                  out.set_decimal(od);
                  uint32_t len = 1;
                  if(od.get_words()[0].table[1] != 0)
                  {
                      len =2;
                  }
                  out.set_len(len);
              }
          }
      }
      return ret;
    }
    //modify e
    ////////////////////////////////////////////////////////////////
    // Double -> XXX
    static int double_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_int(static_cast<int64_t>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_float(static_cast<float>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_datetime(static_cast<ObDateTime>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      return varchar_printf(out, "%f", in.get_double());
    }
    static int double_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_double()));
      return OB_SUCCESS;
    }
    static int double_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDoubleType);
      out.set_bool(static_cast<bool>(in.get_double()));
      return OB_SUCCESS;
    }
    //modify xsl ECNU_DECIMAL 2017_2
    static int double_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)   //in->out  转换成decimal也是用obstring
    {
      UNUSED(params);
      int ret = OB_SUCCESS;
      //int p,s;
      OB_ASSERT(in.get_type() == ObDoubleType);
      ret=varchar_printf(out, "%f", in.get_double());
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          //modify xsl ECNU_DECIMAL 2017_2
          int len;
          if(os.length()>17)
          {
              len = 17;
              if(in.get_double() < 0)
              {
                  len++;
              }
              if(os.find('.') != NULL)
              {
                  len++;
              }
              os.set_length(len);
          }
          //modify e
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in double_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
              if(params.is_modify)
              {
                  if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  {
                              ret=OB_DECIMAL_UNLEGAL_ERROR;
                              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  //out.set_varchar(os);
                  out.set_decimal(od);
                  //add xsl ECNU_DECIMAL 2017_5
                  uint32_t len = 1;
                  if(od.get_words()[0].table[1] != 0)
                  {
                      len =2;
                  }
                  out.set_len(len);
                  //add e
              }
          }
      }
      return ret;
    }
    //modify e
    ////////////////////////////////////////////////////////////////
    // DateTime -> XXX
    static int datetime_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_int(static_cast<int>(in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_float(static_cast<float>(in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_double(static_cast<double>(in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(1000L*1000L*in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      time_t t = static_cast<time_t>(in.get_datetime());
      struct tm gtm;
      localtime_r(&t, &gtm);
      ret = varchar_printf(out, "%04d-%02d-%02d %02d:%02d:%02d",
                           gtm.tm_year+1900, gtm.tm_mon+1, gtm.tm_mday,
                           gtm.tm_hour, gtm.tm_min, gtm.tm_sec);
      return ret;
    }
    static int datetime_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_ctime(static_cast<ObCreateTime>(1000L*1000L*in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_mtime(static_cast<ObModifyTime>(1000L*1000L*in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      out.set_bool(static_cast<bool>(in.get_datetime()));
      return OB_SUCCESS;
    }
    static int datetime_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObDateTimeType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_datetime()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
      /*
       * delete xsl ECNU_DECIMAL
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObDateTimeType);
      int64_t date_time_int=static_cast<int64_t>(in.get_datetime());
      ret = varchar_printf(out, "%ld", date_time_int);
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in datetime_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  {
                              ret=OB_DECIMAL_UNLEGAL_ERROR;
                              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
                  out.set_len(1);   //add xsl ECNU_DECIMAL
              }
          }
      }
      return ret;
      */
        int ret = OB_SUCCESS;
        OB_ASSERT(in.get_type() == ObDateTimeType);
        int64_t tmp=static_cast<int64_t>(in.get_datetime());
        TTInt t1(tmp);
        uint32_t num=0;
        if(tmp<0)
        {
            tmp = -tmp;
        }
        while(tmp)
        {
            num++;
            tmp/=10;
        }
        ObDecimal od;
        od.from(num,0,0,t1);
        if(params.is_modify)
        {
            if((params.precision-params.scale)< num)
            {
                ret=OB_DECIMAL_UNLEGAL_ERROR;
                TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,params.precision=%d,params.scale=%d,num=%d", params.precision,params.scale,num);
            }
            else
            {
                od.set_precision(params.precision);
                od.set_scale(params.scale);
                od.set_vscale(0);
            }
        }
        if(ret==OB_SUCCESS)
        {
            out.set_decimal(od);
            //add xsl ECNU_DECIMAL 20170717
            uint32_t len = 1;
            if(od.get_words()->table[1] != 0)
                len = 2;
            out.set_len(len);
           //add e
        }
        return ret;
    }
    ////////////////////////////////////////////////////////////////
    // PreciseDateTime -> XXX
    static int pdatetime_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_int(static_cast<int64_t>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_float(static_cast<float>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_double(static_cast<double>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_datetime(static_cast<ObDateTime>(in.get_precise_datetime()/1000000L));
      return OB_SUCCESS;
    }
    static int timestamp_varchar(const int64_t timestamp, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      time_t t = static_cast<time_t>(timestamp/1000000L);
      int64_t usec = timestamp % 1000000L;
      struct tm gtm;
      localtime_r(&t, &gtm);
      ret = varchar_printf(out, "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
                           gtm.tm_year+1900, gtm.tm_mon+1, gtm.tm_mday,
                           gtm.tm_hour, gtm.tm_min, gtm.tm_sec, usec);
      return ret;
    }
    static int pdatetime_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      return timestamp_varchar(in.get_precise_datetime(), out);
    }
    static int pdatetime_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    static int pdatetime_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      out.set_bool(static_cast<bool>(in.get_precise_datetime()));
      return OB_SUCCESS;
    }
    //modify xsl ECNU_DECIMAL 201707
    static int pdatetime_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)   //暂时不修改
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_precise_datetime()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
      /*
       * delete xsl ECNU_DECIAML
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
      int64_t pdate_time_int=static_cast<int64_t>(in.get_precise_datetime());
      ret = varchar_printf(out, "%ld", pdate_time_int);
      if(ret==OB_SUCCESS)
      {
          ObString os;
          out.get_varchar(os);
          ObDecimal od;
          if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
          {
                TBSYS_LOG(WARN, "failed to do from in pdatetime_decimal,os=%.*s", os.length(),os.ptr());
          }
          else
          {
             // TBSYS_LOG(WARN, "test::f2 ,,(p==%d,,s==%d,,,,od.p=%d,,,s=%d", params.precision,params.scale,od.get_precision(),od.get_vscale());
              if(params.is_modify)
              {
                  if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                  {
                              ret=OB_DECIMAL_UNLEGAL_ERROR;
                              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                  }
                  else
                  {
                      od.set_precision(params.precision);
                      od.set_scale(params.scale);
                  }

              }
              if(ret==OB_SUCCESS)
              {
                  out.set_varchar(os);
                  out.set_decimal(od);
                  out.set_len(1);   //add xsl ECNU_DECIMAL

              }
          }
      }
      return ret;
      */
        int ret = OB_SUCCESS;
        OB_ASSERT(in.get_type() == ObPreciseDateTimeType);
        int64_t tmp=static_cast<int64_t>(in.get_precise_datetime());
        TTInt t1(tmp);
        uint32_t num=0;
        if(tmp<0)
        {
            tmp = -tmp;
        }
        while(tmp)
        {
            num++;
            tmp/=10;
        }
        ObDecimal od;
        od.from(num,0,0,t1);
        if(params.is_modify)
        {
            if((params.precision-params.scale)< num)
            {
                ret=OB_DECIMAL_UNLEGAL_ERROR;
                TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,params.precision=%d,params.scale=%d,num=%d", params.precision,params.scale,num);
            }
            else
            {
                od.set_precision(params.precision);
                od.set_scale(params.scale);
                od.set_vscale(0);
            }
        }
        if(ret==OB_SUCCESS)
        {
            out.set_decimal(od);
            //add xsl ECNU_DECIMAL 20170717
            uint32_t len = 1;
            if(od.get_words()->table[1] != 0)
                len = 2;
            out.set_len(len);
           //add e
        }
        return ret;
    }
    //modify e
    ////////////////////////////////////////////////////////////////
    // Varchar -> XXX
    static int varchar_scanf(const ObExprObj &in, const int items, const char* format, ...) __attribute__ ((format (scanf, 3, 4)));
    static int varchar_scanf(const ObExprObj &in, const int items, const char* format, ...)
    {
      int ret = OB_SUCCESS;
      const ObString& varchar = in.get_varchar();
      char* str = strndupa(varchar.ptr(), varchar.length()); // alloc from the stack, no need to free
      if (NULL == str)
      {
        TBSYS_LOG(ERROR, "no memory");
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
      else
      {
        va_list args;
        va_start(args, format);
        int matched = vsscanf(str, format, args);
        va_end(args);
        if (items != matched)
        {
          ret = OB_INVALID_ARGUMENT;
        }
      }
      return ret;
    }
    static int varchar_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int64_t value = 0;
      ret = varchar_scanf(in, 1, "%ld", &value);
      if (OB_SUCCESS == ret)
      {
        out.set_int(value);
      }
      else
      {
        out.set_int(0);
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      float value = 0.0f;
      ret = varchar_scanf(in, 1, "%f", &value);
      if (OB_SUCCESS == ret)
      {
        out.set_float(value);
      }
      else
      {
        out.set_float(0.0f);
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      double value = 0.0;
      ret = varchar_scanf(in, 1, "%lf", &value);
      if (OB_SUCCESS == ret)
      {
        out.set_double(value);
      }
      else
      {
        out.set_double(0.0);
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int year = 0;
      int month = 0;
      int day = 0;
      int hour = 0;
      int minute = 0;
      int second = 0;
      ret = varchar_scanf(in, 6, "%4d-%2d-%2d %2d:%2d:%2d",
                          &year, &month, &day,
                          &hour, &minute, &second);
      if (OB_SUCCESS != ret)
      {
        year = month = day = hour = minute = second = 0;
        ret = varchar_scanf(in, 3, "%4d-%2d-%2d",
                            &year, &month, &day);
      }
      if (OB_SUCCESS != ret)
      {
        year = month = day = hour = minute = second = 0;
        ret = varchar_scanf(in, 3, "%2d:%2d:%2d",
                            &hour, &minute, &second);
      }
      if (OB_SUCCESS == ret)
      {
        struct tm gtm;
        memset(&gtm, 0, sizeof(gtm));
        gtm.tm_year = year - 1900;
        gtm.tm_mon = month - 1;
        gtm.tm_mday = day;
        gtm.tm_hour = hour;
        gtm.tm_min = minute;
        gtm.tm_sec = second;
        gtm.tm_isdst = -1;
        time_t t = mktime(&gtm);
        out.set_datetime(static_cast<ObDateTime>(t));
      }
      else
      {
        const ObString& varchar = in.get_varchar();
        TBSYS_LOG(WARN, "failed to convert string `%.*s' to datetime type",
                  varchar.length(), varchar.ptr());
        out.set_datetime(static_cast<ObDateTime>(0));
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_timestamp(const ObExprObj &in, int64_t &timestamp)
    {
      int ret = OB_SUCCESS;
      int year = 0;
      int month = 0;
      int day = 0;
      int hour = 0;
      int minute = 0;
      int second = 0;
      int usec = 0;
      ret = varchar_scanf(in, 7, "%4d-%2d-%2d %2d:%2d:%2d.%6d",
                          &year, &month, &day,
                          &hour, &minute, &second, &usec);
      if (OB_SUCCESS != ret)
      {
        year = month = day = hour = minute = second = usec = 0;
        ret = varchar_scanf(in, 6, "%4d-%2d-%2d %2d:%2d:%2d",
                            &year, &month, &day,
                            &hour, &minute, &second);
      }
      if (OB_SUCCESS != ret)
      {
        year = month = day = hour = minute = second = usec = 0;
        ret = varchar_scanf(in, 3, "%4d-%2d-%2d",
                            &year, &month, &day);
      }
      if (OB_SUCCESS != ret)
      {
        year = month = day = hour = minute = second = usec = 0;
        ret = varchar_scanf(in, 3, "%2d:%2d:%2d",
                            &hour, &minute, &second);
      }
      if (OB_SUCCESS == ret)
      {
        struct tm gtm;
        memset(&gtm, 0, sizeof(gtm));
        gtm.tm_year = year - 1900;
        gtm.tm_mon = month - 1;
        gtm.tm_mday = day;
        gtm.tm_hour = hour;
        gtm.tm_min = minute;
        gtm.tm_sec = second;
        gtm.tm_isdst = -1;
        time_t t = mktime(&gtm);
        timestamp = static_cast<int64_t>(t) * 1000000L + usec;
      }
      return ret;
    }
    static int varchar_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int64_t timestamp = 0;
      ret = varchar_timestamp(in, timestamp);
      if (OB_SUCCESS == ret)
      {
        out.set_precise_datetime(static_cast<ObPreciseDateTime>(timestamp));
      }
      else
      {
        const ObString& varchar = in.get_varchar();
        TBSYS_LOG(WARN, "failed to convert string `%.*s' to precise_datetime type",
                  varchar.length(), varchar.ptr());
        out.set_precise_datetime(static_cast<ObPreciseDateTime>(0));
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int64_t timestamp = 0;
      ret = varchar_timestamp(in, timestamp);
      if (OB_SUCCESS == ret)
      {
        out.set_ctime(static_cast<ObCreateTime>(timestamp));
      }
      else
      {
        const ObString& varchar = in.get_varchar();
        TBSYS_LOG(WARN, "failed to convert string `%.*s' to createtime type",
                  varchar.length(), varchar.ptr());
        out.set_ctime(static_cast<ObCreateTime>(0));
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      int64_t timestamp = 0;
      ret = varchar_timestamp(in, timestamp);
      if (OB_SUCCESS == ret)
      {
        out.set_mtime(static_cast<ObModifyTime>(timestamp));
      }
      else
      {
        const ObString& varchar = in.get_varchar();
        TBSYS_LOG(WARN, "failed to convert string `%.*s' to modifytime type",
                  varchar.length(), varchar.ptr());
        out.set_mtime(static_cast<ObModifyTime>(0));
        ret = OB_SUCCESS;
      }
      return ret;
    }
    static int varchar_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      const ObString &varchar = in.get_varchar();
      bool value = false;
      if (varchar.ptr() != NULL && varchar.length() > 0)
      {
        static const int64_t len_true = strlen("true");
        static const int64_t len_t = strlen("T");
        static const int64_t len_yes = strlen("yes");
        static const int64_t len_y = strlen("y");
        if (varchar.length() == len_true
            && 0 == strncasecmp(varchar.ptr(), "true", len_true))
        {
          value = true;
        }
        else if (varchar.length() == len_t
            && 0 == strncasecmp(varchar.ptr(), "T", len_t))
        {
          value = true;
        }
        else if (varchar.length() == len_yes
            && 0 == strncasecmp(varchar.ptr(), "yes", len_yes))
        {
          value = true;
        }
        else if (varchar.length() == len_y
            && 0 == strncasecmp(varchar.ptr(), "y", len_y))
        {
          value = true;
        }
      }
      out.set_bool(value);
      return OB_SUCCESS;
    }
    /*static int varchar_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObVarcharType);
      const ObString &varchar = in.get_varchar();
      ObNumber num;
      if (OB_SUCCESS != (ret = num.from(varchar.ptr(), varchar.length())))
      {
        TBSYS_LOG(WARN, "failed to convert varchar to decimal, err=%d varchar=%.*s",
                  ret, varchar.length(), varchar.ptr());
      }
      else
      {
        out.set_decimal(num);   // @todo optimize
      }
      return OB_SUCCESS;
    }*/
    //modify xsl ECNU_DECIMAL 2016_12
    //add  fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
    static int varchar_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
        int ret = OB_SUCCESS;
        // UNUSED(params);
        uint32_t len =1;
        OB_ASSERT(in.get_type() == ObVarcharType);
        const ObString &varchar = in.get_varchar();
        if(varchar.ptr()==NULL||(int)(varchar.length())==0)
        {
            int64_t int_result;
            int_result=0;
            ret = varchar_printf(out, "%ld", int_result);
            if(ret==OB_SUCCESS)
            {
                ObString os;
                out.get_varchar(os);
                ObDecimal od;
                if(OB_SUCCESS!=(ret=od.from(os.ptr(),os.length())))
                {
                    TBSYS_LOG(WARN, "failed to do from in int_decimal,os=%.*s", os.length(),os.ptr());
                }
                else
                {
                    if(params.is_modify)
                    {
                        if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
                        {
                            ret=OB_DECIMAL_UNLEGAL_ERROR;
                            TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                        }
                        else
                        {
                            od.set_precision(params.precision);
                            od.set_scale(params.scale);
                        }
                    }
                    if(ret==OB_SUCCESS)
                    {
                        //add xsl ECNU_DECIMAL
                        out.set_len(1);
                        //add e
                        out.set_varchar(os);
                        out.set_decimal(od);
                    }
                }
            }
        }
        else
        {
            ObDecimal od;
            if (OB_SUCCESS != (ret = od.from(varchar.ptr(), varchar.length())))
            {
                TBSYS_LOG(WARN, "failed to convert varchar to decimal, err=%d varchar=%.*s",
                          ret, varchar.length(), varchar.ptr());
            }
            else
            {
                if(params.is_modify)
                {
                    if((od.get_precision()-od.get_vscale())>(params.precision-params.scale))
                    {
                        ret=OB_DECIMAL_UNLEGAL_ERROR;
                        TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
                    }
                    else
                    {
                        od.set_precision(params.precision);
                        od.set_scale(params.scale);
                    }
                }
                if(ret==OB_SUCCESS)
                {
                    len =1;
                    if(od.get_words()->table[1] !=0 )
                        len=2;
                    out.set_varchar(varchar);  //test xsl
                    out.set_decimal(od);
                    out.set_len(len);
                }
            }
        }
        return ret;
    }
    //add:e
    //modify e
    ////////////////////////////////////////////////////////////////
    // CreateTime -> XXX
    static int ctime_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_int(static_cast<int64_t>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_float(static_cast<float>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_double(static_cast<double>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_datetime(static_cast<ObDateTime>(in.get_ctime()/1000000L));
      return OB_SUCCESS;
    }
    static int ctime_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      return timestamp_varchar(in.get_ctime(), out);
    }
    static int ctime_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_ctime()));
      return OB_SUCCESS;
    }
    static int ctime_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      out.set_bool(static_cast<bool>(in.get_ctime()));
      return OB_SUCCESS;
    }
    //modify xsl ECNU_DECIMAL
    static int ctime_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_ctime()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
      /*
       * delete xsl ECNU_DECIMAL 201707
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObCreateTimeType);
      int64_t c_time_int=static_cast<int64_t>(in.get_ctime());
      ObDecimal od;
      uint64_t t1;
      t1 = (uint64_t)c_time_int;
      uint32_t len=1;
      od.set_word(&t1,len);
      if(params.is_modify)
      {
          if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
          {
              ret=OB_DECIMAL_UNLEGAL_ERROR;
              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
          }
          else
          {
              od.set_precision(params.precision);
              od.set_scale(params.scale);
          }

      }
      if(ret==OB_SUCCESS)
      {
          //out.set_varchar(os);
          out.set_decimal(od);
          out.set_len(1);   //add xsl ECNU_DECIMAL
      }
      return ret;
      */
        int ret = OB_SUCCESS;
        OB_ASSERT(in.get_type() == ObCreateTimeType);
        int64_t tmp=static_cast<int64_t>(in.get_ctime());
        TTInt t1(tmp);
        uint32_t num=0;
        if(tmp<0)
        {
            tmp = -tmp;
        }
        while(tmp)
        {
            num++;
            tmp/=10;
        }
        ObDecimal od;
        od.from(num,0,0,t1);
        if(params.is_modify)
        {
            if((params.precision-params.scale)< num)
            {
                ret=OB_DECIMAL_UNLEGAL_ERROR;
                TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,params.precision=%d,params.scale=%d,num=%d", params.precision,params.scale,num);
            }
            else
            {
                od.set_precision(params.precision);
                od.set_scale(params.scale);
                od.set_vscale(0);
            }
        }
        if(ret==OB_SUCCESS)
        {
            out.set_decimal(od);
            //add xsl ECNU_DECIMAL 20170717
            uint32_t len = 1;
            if(od.get_words()->table[1] != 0)
                len = 2;
            out.set_len(len);
           //add e
        }
        return ret;
    }
    //modify e
    ////////////////////////////////////////////////////////////////
    // ModifyTime -> XXX
    static int mtime_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_int(static_cast<int64_t>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_float(static_cast<float>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_double(static_cast<double>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_datetime(static_cast<ObDateTime>(in.get_mtime()/1000000L));
      return OB_SUCCESS;
    }
    static int mtime_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      return timestamp_varchar(in.get_mtime(), out);
    }
    static int mtime_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_ctime(static_cast<ObModifyTime>(in.get_mtime()));
      return OB_SUCCESS;
    }
    static int mtime_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      out.set_bool(static_cast<bool>(in.get_mtime()));
      return OB_SUCCESS;
    }
    //modify xsl ECNU_DECIMAL 2016_12
    static int mtime_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_mtime()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
      /*
       * delete xsl DECIMAL 201707
      int ret = OB_SUCCESS;
      OB_ASSERT(in.get_type() == ObModifyTimeType);
      int64_t m_time_int=static_cast<int64_t>(in.get_mtime());
      ObDecimal od;
      uint64_t t1;
      t1 = (uint64_t)m_time_int;     //18446744073709551615
      uint32_t len=1;        //default int len =1
      od.set_word(&t1,len);
      if(params.is_modify)
      {
          if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
          {
              ret=OB_DECIMAL_UNLEGAL_ERROR;
              TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
          }
          else
          {
              od.set_precision(params.precision);
              od.set_scale(params.scale);
          }

      }
      if(ret==OB_SUCCESS)
      {
          //out.set_varchar(os);
          out.set_decimal(od);
          out.set_len(1);   //add xsl ECNU_DECIMAL

      }
      return ret;
      */
        int ret = OB_SUCCESS;
        OB_ASSERT(in.get_type() == ObModifyTimeType);
        int64_t tmp=static_cast<int64_t>(in.get_mtime());
        TTInt t1(tmp);
        uint32_t num=0;
        if(tmp<0)
        {
            tmp = -tmp;
        }
        while(tmp)
        {
            num++;
            tmp/=10;
        }
        ObDecimal od;
        od.from(num,0,0,t1);
        if(params.is_modify)
        {
            if((params.precision-params.scale)< num)
            {
                ret=OB_DECIMAL_UNLEGAL_ERROR;
                TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,params.precision=%d,params.scale=%d,num=%d", params.precision,params.scale,num);
            }
            else
            {
                od.set_precision(params.precision);
                od.set_scale(params.scale);
                od.set_vscale(0);
            }
        }
        if(ret==OB_SUCCESS)
        {
            out.set_decimal(od);
            //add xsl ECNU_DECIMAL 20170717
            uint32_t len = 1;
            if(od.get_words()->table[1] != 0)
                len = 2;
            out.set_len(len);
           //add e
        }
        return ret;
    }
    //modify e
    ////////////////////////////////////////////////////////////////
    // Bool -> XXX
    static int bool_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_int(static_cast<int64_t>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_float(static_cast<float>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_double(static_cast<double>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_datetime(static_cast<ObDateTime>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_precise_datetime(static_cast<ObPreciseDateTime>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      ObString varchar;
      if (in.get_bool())
      {
        varchar.assign_ptr(const_cast<char*>("true"), sizeof("true")-1);
      }
      else
      {
        varchar.assign_ptr(const_cast<char*>("false"), sizeof("false")-1);
      }
      out.set_varchar(varchar);
      return OB_SUCCESS;
    }
    static int bool_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_ctime(static_cast<ObCreateTime>(in.get_bool()));
      return OB_SUCCESS;
    }
    static int bool_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      out.set_mtime(static_cast<ObModifyTime>(in.get_bool()));
      return OB_SUCCESS;
    }
    //modify xsl ECNU_DECIMAL 2016_12
    static int bool_decimal(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
        /*UNUSED(params);
      OB_ASSERT(in.get_type() == ObBoolType);
      ObNumber num;
      num.from(static_cast<int64_t>(in.get_bool()));
      out.set_decimal(num);
      return OB_SUCCESS;*/
        int ret = OB_SUCCESS;
        OB_ASSERT(in.get_type() == ObBoolType);
        int64_t bool_int=static_cast<int64_t>(in.get_bool());
        ObDecimal od;
        uint64_t t1;
        if(bool_int == 1)
            t1=1;
        else
            t1=0;
        uint32_t len =1;
        uint32_t pre =1;
        TTInt tt(t1);
        od.from(pre,0,0,tt);
        //od.set_word(&t1,len);
        //od.set_precision(pre);
        if(params.is_modify)
        {
            if((params.precision-params.scale)<(od.get_precision()-od.get_vscale()))
            {
                ret=OB_DECIMAL_UNLEGAL_ERROR;
                TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,od.get_precision()=%d,od.get_vscale()=%d", od.get_precision(),od.get_vscale());
            }
            else
            {
                od.set_precision(params.precision);
                od.set_scale(params.scale);
            }
        }
        if(ret==OB_SUCCESS)
        {
            out.set_decimal(od);
            out.set_len(len);   //add xsl ECNU_DECIMAL
        }
        return ret;
    }
    //modify e
    ////////////////////////////////////////////////////////////////
    // Decimal -> XXX
    static int decimal_int(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_int(i64);
      }
      return ret;
    }
    static int decimal_float(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      // decimal -> string -> float
      char buf[ObNumber::MAX_PRINTABLE_SIZE];
      memset(buf, 0, ObNumber::MAX_PRINTABLE_SIZE);
      in.get_decimal().to_string(buf, ObNumber::MAX_PRINTABLE_SIZE);
      float v = 0.0f;
      if (1 == sscanf(buf, "%f", &v))
      {
        out.set_float(v);
      }
      else
      {
        out.set_float(0.0f);
      }
      return OB_SUCCESS;
    }
    static int decimal_double(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
        int ret = OB_SUCCESS;
        UNUSED(params);
        OB_ASSERT(in.get_type() == ObDecimalType);
        char buf[MAX_PRINTABLE_SIZE];
        memset(buf, 0, MAX_PRINTABLE_SIZE);
        double v = 0.0;
        ObDecimal od_result=in.get_decimal();
        //test xsl 2017_3
        od_result.to_string(buf, MAX_PRINTABLE_SIZE);
        if (1 == sscanf(buf, "%lf", &v))
        {
            out.set_double(v);
        }
        else
        {
            out.set_double(0.0);
        }
        return ret;
    }
    static int decimal_datetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      /*
      int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_datetime(static_cast<ObDateTime>(i64));
      }
      return ret;
      */
        int ret = OB_SUCCESS;
        UNUSED(params);
        OB_ASSERT(in.get_type() == ObDecimalType);
        int64_t i64 = 0;
        //ObString os;
        //os=in.get_varchar();
        // ObDecimal od=in.get_decimal();
        ObDecimal od_result=in.get_decimal();
        //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
        // {
        // TBSYS_LOG(WARN, "failed to do from in decimal_datetime,os=%.*s", os.length(),os.ptr());
        //}
        // else
        // {
        //uint32_t p=od.get_precision();
        // uint32_t s=od.get_scale();
        // if (OB_SUCCESS != (ret=od_result.modify_value(p,s)))
        // {
        //  TBSYS_LOG(WARN, "failed to do modify_value in od_result.modify_value(p,s),od_result=%.*s,p=%d,s=%d", os.length(),os.ptr(),p,s);
        // }
        if (OB_SUCCESS != (ret = od_result.cast_to_int64(i64)))
        {
            TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int64(i64)");
        }
        else
        {
            out.set_datetime(static_cast<ObDateTime>(i64));
        }
        // }
        return ret;
    }
    static int decimal_pdatetime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
        /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_precise_datetime(static_cast<ObPreciseDateTime>(i64));
      }
      return ret;*/
        int ret = OB_SUCCESS;
        UNUSED(params);
        OB_ASSERT(in.get_type() == ObDecimalType);
        int64_t i64 = 0;
        // ObString os;
        // os=in.get_varchar();
        //ObDecimal od=in.get_decimal();
        ObDecimal od_result=in.get_decimal();
        //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
        // {
        //    TBSYS_LOG(WARN, "failed to do from in decimal_pdatetime,os=%.*s", os.length(),os.ptr());
        // }
        // else
        // {
        // uint32_t p=od.get_precision();
        // uint32_t s=od.get_scale();
        // if (OB_SUCCESS != (ret=od_result.modify_value(p,s)))
        // {
        //  TBSYS_LOG(WARN, "failed to do modify_value in od_result.modify_value(p,s),od_result=%.*s,p=%d,s=%d", os.length(),os.ptr(),p,s);
        // }
        if (OB_SUCCESS != (ret = od_result.cast_to_int64(i64)))
        {
            TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int64(i64)");
        }
        else
        {
            out.set_precise_datetime(static_cast<ObPreciseDateTime>(i64));
        }
        // }
        return ret;
    }
    static int decimal_varchar(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
        int ret = OB_SUCCESS;
        UNUSED(params);
        OB_ASSERT(in.get_type() == ObDecimalType);
        ObString varchar = out.get_varchar();  //如果原本没有varchar,是否就是初始值
        if (varchar.length() < (int)MAX_PRINTABLE_SIZE)
        {
            TBSYS_LOG(WARN, "output buffer for varchar not enough, buf_len=%d", varchar.length());
            ret = OB_INVALID_ARGUMENT;
        }
        else
        {
            // ObDecimal od=in.get_decimal();
            //ObString os;
            // os=in.get_varchar();
            ObDecimal od_result=in.get_decimal();
            //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
            //{
            //  TBSYS_LOG(WARN, "failed to do from in decimal_varchar,os=%.*s", os.length(),os.ptr());
            // }
            // else
            // {
            //od_result.set_precision(od.get_precision());
            // od_result.set_scale(od.get_scale());
            int64_t length = od_result.to_string(varchar.ptr(),MAX_PRINTABLE_SIZE);
            ObString varchar2(MAX_PRINTABLE_SIZE, static_cast<int32_t>(length), varchar.ptr());
            out.set_varchar(varchar2);
            //}

        }
        return ret;
    }
    static int decimal_ctime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
        /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_ctime(static_cast<ObCreateTime>(i64));
      }
      return ret;*/
        int ret = OB_SUCCESS;
        UNUSED(params);
        OB_ASSERT(in.get_type() == ObDecimalType);
        int64_t i64 = 0;
        // ObString os;
        // os=in.get_varchar();
        // ObDecimal od=in.get_decimal();
        ObDecimal od_result=in.get_decimal();
        //if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
        // {
        //  TBSYS_LOG(WARN, "failed to do from in decimal_ctime,os=%.*s", os.length(),os.ptr());
        //}
        // else
        // {
        // uint32_t p=od.get_precision();
        // uint32_t s=od.get_scale();
        //  if (OB_SUCCESS != (ret=od_result.modify_value(p,s)))
        // {
        //  TBSYS_LOG(WARN, "failed to do modify_value in od_result.modify_value(p,s),od_result=%.*s,p=%d,s=%d", os.length(),os.ptr(),p,s);
        //}
        if (OB_SUCCESS != (ret = od_result.cast_to_int64(i64)))
        {
            TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int64(i64)");
        }
        else
        {
            out.set_ctime(static_cast<ObCreateTime>(i64));
        }
        //}
        return ret;
    }
    static int decimal_mtime(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
        /*int ret = OB_SUCCESS;
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      int64_t i64 = 0;
      if (OB_SUCCESS != (ret = in.get_decimal().cast_to_int64(i64)))
      {
        TBSYS_LOG(WARN, "failed to cast to int64, err=%d", ret);
      }
      else
      {
        out.set_mtime(static_cast<ObModifyTime>(i64));
      }
      return ret;*/
        int ret = OB_SUCCESS;
        UNUSED(params);
        OB_ASSERT(in.get_type() == ObDecimalType);
        int64_t i64 = 0;
        //ObString os;
        //os=in.get_varchar();
        //ObDecimal od=in.get_decimal();
        ObDecimal od_result=in.get_decimal();
        // if(OB_SUCCESS != (ret = od_result.from(os.ptr(),os.length())))
        //{
        //TBSYS_LOG(WARN, "failed to do from in decimal_mtime,os=%.*s", os.length(),os.ptr());
        //}
        //else
        // {
        //uint32_t p=od.get_precision();
        // uint32_t s=od.get_scale();
        // if (OB_SUCCESS != (ret=od_result.modify_value(p,s)))
        // {
        // TBSYS_LOG(WARN, "failed to do modify_value in od_result.modify_value(p,s),od_result=%.*s,p=%d,s=%d", os.length(),os.ptr(),p,s);
        // }
        if (OB_SUCCESS != (ret = od_result.cast_to_int64(i64)))
        {
            TBSYS_LOG(WARN, "failed to do cast_to_int64 in od_result.cast_to_int64(i64)");
        }
        else
        {
            out.set_mtime(static_cast<ObModifyTime>(i64));
        }
        // }
        return ret;
    }
    static int decimal_bool(const ObObjCastParams &params, const ObExprObj &in, ObExprObj &out)
    {
      UNUSED(params);
      OB_ASSERT(in.get_type() == ObDecimalType);
      out.set_bool(!in.get_decimal().is_zero());
      return OB_SUCCESS;
    }
    ////////////////////////////////////////////////////////////////
    ObObjCastFunc OB_OBJ_CAST[ObMaxType][ObMaxType] =
    {
      {
        /*Null -> XXX*/
        identity, identity, identity,
        identity, identity, identity,
        identity, identity, identity,
        identity, identity, identity,
        identity
      }
      ,
      {
        /*Int -> XXX*/
        not_support/*Null*/, identity/*Int*/, int_float,
        int_double, int_datetime, int_pdatetime,
        int_varchar, not_support, int_ctime,
        int_mtime, not_support, int_bool,
        int_decimal
      }
      ,
      {
        /*Float -> XXX*/
        not_support/*Null*/, float_int, identity,
        float_double, float_datetime, float_pdatetime,
        float_varchar, not_support, float_ctime,
        float_mtime, not_support, float_bool,
        float_decimal
      }
      ,
      {
        /*Double -> XXX*/
        not_support/*Null*/, double_int, double_float,
        identity, double_datetime, double_pdatetime,
        double_varchar, not_support, double_ctime,
        double_mtime, not_support, double_bool,
        double_decimal
      }
      ,
      {
        /*DateTime -> XXX*/
        not_support/*Null*/, datetime_int, datetime_float,
        datetime_double, identity, datetime_pdatetime,
        datetime_varchar, not_support/*Seq*/, datetime_ctime,
        datetime_mtime, not_support/*Extend*/, datetime_bool,
        datetime_decimal
      }
      ,
      {
        /*PreciseDateTime -> XXX*/
        not_support/*Null*/, pdatetime_int, pdatetime_float,
        pdatetime_double, pdatetime_datetime, identity,
        pdatetime_varchar, not_support/*Seq*/,pdatetime_ctime,
        pdatetime_mtime, not_support/*Extend*/,pdatetime_bool,
        pdatetime_decimal
      }
      ,
      {
        /*Varchar -> XXX*/
        not_support/*Null*/, varchar_int, varchar_float,
        varchar_double, varchar_datetime, varchar_pdatetime,
        identity, not_support/*Seq*/, varchar_ctime,
        varchar_mtime, not_support/*Extend*/, varchar_bool,
        varchar_decimal
      }
      ,
      {
        /*Seq -> XXX*/
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support
      }
      ,
      {
        /*CreateTime -> XXX*/
        not_support/*Null*/, ctime_int, ctime_float,
        ctime_double, ctime_datetime, ctime_pdatetime,
        ctime_varchar, not_support/*Seq*/, identity,
        ctime_mtime, not_support/*Extend*/, ctime_bool,
        ctime_decimal
      }
      ,
      {
        /*ModifyTime -> XXX*/
        not_support/*Null*/, mtime_int, mtime_float,
        mtime_double, mtime_datetime, mtime_pdatetime,
        mtime_varchar, not_support/*Seq*/, mtime_ctime,
        identity, not_support/*Extend*/, mtime_bool,
        mtime_decimal
      }
      ,
      {
        /*Extend -> XXX*/
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support, not_support, not_support,
        not_support
      }
      ,
      {
        /*Bool -> XXX*/
        not_support/*Null*/, bool_int, bool_float,
        bool_double, bool_datetime, bool_pdatetime,
        bool_varchar, not_support/*Seq*/, bool_ctime,
        bool_mtime, not_support/*Extend*/, identity,
        bool_decimal
      }
      ,
      {
        /*Decimal -> XXX*/
        not_support/*Null*/, decimal_int, decimal_float,
        decimal_double, decimal_datetime, decimal_pdatetime,
        decimal_varchar, not_support/*Seq*/, decimal_ctime,
        decimal_mtime, not_support/*Extend*/, decimal_bool,
        identity
      }
    };
    ////////////////////////////////////////////////////////////////
    //modify xsl ECNU_DECIMAL 2016_12
    int obj_cast(const ObObj &orig_cell, const ObObj &expected_type,
                 ObObj &casted_cell, const ObObj *&res_cell)
    {
        int ret = OB_SUCCESS;
        ObObj obj_tmp;
        //add  fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
        if(orig_cell.get_type()==ObDecimalType && expected_type.get_type()==ObDecimalType)
        {
          TBSYS_LOG(ERROR,"DHC both decimal");
            uint64_t *tt = NULL;
            tt = orig_cell.get_ttint();
            if(OB_SUCCESS!=(ret=(casted_cell.set_ttint(tt))))    //指针指向这个os
            {
                TBSYS_LOG(ERROR,"DHC error decimal");
                TBSYS_LOG(WARN, "failed to do set_decimal in obj_cast()");
            }
            else
            {
                //uint32_t pre=orig_cell.get_precision();
                //uint32_t scale=casted_cell.get_scale();
                uint32_t vscale=orig_cell.get_vscale();
                uint32_t schema_p=expected_type.get_precision();  //模式定义的参数
                uint32_t schema_s=expected_type.get_scale();
                //add xsl DECIMAL
                ObDecimal tmp_dec;
                orig_cell.get_decimal(tmp_dec);
                TTInt *word = tmp_dec.get_words();
                TTInt whole;
                TTInt BASE(10),pp(vscale);
                BASE.Pow(pp);
                whole = word[0] / BASE;
                if(word->IsSign())
                {
                    whole.ChangeSign();
                }
                std::string str_for_int;
                whole.ToString(str_for_int);
                int len_int = (int)str_for_int.length();
                if( (uint32_t)len_int > (schema_p - schema_s))
                //add e
                //if((pre - vscale) > (schema_p - schema_s))
                {
                  TBSYS_LOG(ERROR,"DHC error decimal");
                    ret=OB_DECIMAL_UNLEGAL_ERROR;
                    TBSYS_LOG(WARN, "OB_DECIMAL_UNLEGAL_ERROR,schema_p=%d,schema_s=%d,"
                                    "casted_cell.get_precision()=%d,casted_cell.get_vscale()=%d"
                              ,schema_p,schema_s,orig_cell.get_precision(),orig_cell.get_vscale());
                }
                else
                {
                  TBSYS_LOG(ERROR,"DHC succ decimal");
                    //casted_cell.set_precision(pre - vscale + schema_s);   //add xsl ECNU_DECIMAL
                    casted_cell.set_precision(schema_p);  //delete xsl ECNU_DECIMAL p转化成模式的参数样式
                    casted_cell.set_scale(schema_s);
                    casted_cell.set_vscale(vscale);    //modify xsl ECNU_DECIMAL
                    casted_cell.set_nwords(orig_cell.get_nwords());    //add xsl ECNU_DECIMAL 2017_5
                    res_cell = &casted_cell;
                }
            }
        }
        else   //至少有一个不是decimal
        {
          TBSYS_LOG(ERROR,"DHC one is not decimal");
            if (orig_cell.get_type() != expected_type.get_type())
            {
                // type cast
                ObObjCastParams params;
                params.is_modify=true;
                params.precision=expected_type.get_precision();
                params.scale=expected_type.get_scale();
                ObExprObj from;
                ObExprObj to;
                if(orig_cell.get_type()==ObDecimalType)
                {
                    uint64_t *tt = NULL;
                    tt = orig_cell.get_ttint();
                    if (OB_SUCCESS != (ret=obj_tmp.set_decimal(tt,orig_cell.get_precision(),orig_cell.get_scale(),orig_cell.get_vscale(),orig_cell.get_nwords())))
                    {
                      TBSYS_LOG(ERROR,"DHC error decimal");
                        TBSYS_LOG(WARN, "failed to do set_decimal in obj_cast()");
                    }
                    else
                    {
                      TBSYS_LOG(ERROR,"DHC 1 decimal");
                        from.assign(obj_tmp);
                    }

                }
                else
                {
                  TBSYS_LOG(ERROR,"DHC 2 decimal");
                    from.assign(orig_cell);
                }
                if(OB_SUCCESS==ret)
                {
                    //add xsl DECIMAL
                    ObString sstr;
                    casted_cell.get_varchar_d(sstr);
                    //add e
                    to.assign(casted_cell);   //obj->expr_obj
                    if (OB_SUCCESS != (ret = OB_OBJ_CAST[orig_cell.get_type()][expected_type.get_type()](params, from, to)))
                    {
                      TBSYS_LOG(ERROR,"DHC error decimal");
                        TBSYS_LOG(WARN, "failed to type cast obj, err=%d", ret);
                    }
                    else if (OB_SUCCESS != (ret = to.to(casted_cell)))  //expr_obj->obj
                    {
                      TBSYS_LOG(ERROR,"DHC error decimal");
                        TBSYS_LOG(WARN, "failed to convert expr_obj to obj, err=%d", ret);
                    }
                    else
                    {
                        //add xsl ECNU_DECIMAL
                        if(ObDecimalType == expected_type.get_type() && casted_cell.get_type() == ObDecimalType)
                        {
                          TBSYS_LOG(ERROR,"DHC succ decimal");
                            char *p = sstr.ptr();
                            uint64_t *tt = reinterpret_cast<uint64_t *>(p);
                            int len = (int)sizeof(uint64_t)*casted_cell.get_nwords();
                            memcpy(tt,casted_cell.get_ttint(),len);
                            casted_cell.set_ttint(tt);
                        }
                        else {
                          TBSYS_LOG(ERROR,"DHC error decimal");
                        }
                        //add e
                        res_cell = &casted_cell;
                    }
                }
            }
            else
            {
              TBSYS_LOG(ERROR,"DHC 3 decimal");
                res_cell = &orig_cell;
            }
        }
        //TBSYS_LOG(WARN, "test::obj_cast::p=%d,s=%d, type=%d", res_cell->get_precision(),res_cell->get_scale(),res_cell->get_type());
        return ret;
    }
    //modify e

    //add  fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b
    int obj_cast_for_rowkey(const ObObj &orig_cell, const ObObj &expected_type,
                            ObObj &casted_cell, const ObObj *&res_cell)
    {
        int ret = OB_SUCCESS;
        ObObj obj_tmp;
        if (orig_cell.get_type() != expected_type.get_type())
        {
            // type cast
            ObObjCastParams params;
            params.is_modify=false;
            //params.precision=expected_type.get_precision();
            // params.scale=expected_type.get_scale();
            ObExprObj from;
            ObExprObj to;
            if(orig_cell.get_type()==ObDecimalType)
            {
                ObDecimal od;
                //ObString os;
                orig_cell.get_decimal(od);
                int len;
                len =orig_cell.get_nwords();
                if (OB_SUCCESS != (ret=obj_tmp.set_decimal_v2(od,len)))   //modify xsl
                {
                    TBSYS_LOG(WARN, "failed to do set_decimal in obj_cast(),ret=%d",ret);
                }
                else
                {
                    from.assign(obj_tmp);
                }

            }
            else
            {
                from.assign(orig_cell);
            }
            if(OB_SUCCESS==ret)
            {
                //add xsl DECIMAL
                ObString sstr;
                casted_cell.get_varchar_d(sstr);
                //add e
                to.assign(casted_cell);
                if (OB_SUCCESS != (ret = OB_OBJ_CAST[orig_cell.get_type()][expected_type.get_type()](params, from, to)))
                {
                    TBSYS_LOG(WARN, "failed to type cast obj, err=%d", ret);
                }
                else if (OB_SUCCESS != (ret = to.to(casted_cell)))
                {
                    TBSYS_LOG(WARN, "failed to convert expr_obj to obj, err=%d", ret);
                }
                else
                {
                    //add xsl ECNU_DECIMAL
                    if(ObDecimalType == expected_type.get_type() && casted_cell.get_type() == ObDecimalType)
                    {
                        char *p = sstr.ptr();
                        uint64_t *tt = reinterpret_cast<uint64_t *>(p);
                        int len = (int)sizeof(uint64_t)*casted_cell.get_nwords();
                        memcpy(tt,casted_cell.get_ttint(),len);
                        casted_cell.set_ttint(tt);
                    }
                    //add e
                    res_cell = &casted_cell;
                }
            }
        }
        else
        {
            res_cell = &orig_cell;
        }
        return ret;
    }
    //add e

    int obj_cast(ObObj &cell, const ObObjType expected_type, char* buf, int64_t buf_size, int64_t &used_buf_len)
    {
        int ret = OB_SUCCESS;
        used_buf_len = 0;
        if (cell.get_type() != expected_type)
        {
            ObObjCastParams params;
            ObExprObj from;
            ObExprObj to;

            from.assign(cell);
            if (ObVarcharType == expected_type)
            {
                ObString buffer;
                buffer.assign_ptr(buf, static_cast<ObString::obstr_size_t>(buf_size));
                ObObj varchar_cell;
                varchar_cell.set_varchar(buffer);
                to.assign(varchar_cell);
            }
            //add  fanqiushi ECNU_DECIMAL V0.1 2016_5_29:b   ??
            if(ObDecimalType == expected_type)
            {
                ObString buffer;
                buffer.assign_ptr(buf, static_cast<ObString::obstr_size_t>(buf_size));
                ObObj varchar_cell;
                varchar_cell.set_varchar(buffer);
                to.assign(varchar_cell);
            }
            //add:e
            if (OB_SUCCESS != (ret = OB_OBJ_CAST[cell.get_type()][expected_type](params, from, to)))  //cell=12,
            {
                TBSYS_LOG(WARN, "failed to type cast obj, err=%d", ret);
            }
            else if (OB_SUCCESS != (ret = to.to(cell)))
            {
                TBSYS_LOG(WARN, "failed to convert expr_obj to obj, err=%d", ret);
            }
            else
            {
                if (ObVarcharType == expected_type)
                {
                    ObString varchar;
                    cell.get_varchar(varchar);
                    used_buf_len = varchar.length(); // used buffer length for casting to varchar type
                }
                //add xsl ECNU_DECIMAL
                else if(ObDecimalType == expected_type)
                {
                    if(cell.get_type() != ObNullType)
                    {
                        uint64_t *tt = reinterpret_cast<uint64_t *>(buf);
                        int len = (int)sizeof(uint64_t)*cell.get_nwords();
                        memcpy(tt,cell.get_ttint(),len);
                        cell.set_ttint(tt);
                        used_buf_len = len;
                    }
                }
                //add e
            }
        }
        return ret;
    }

    int obj_cast(ObObj &cell, const ObObjType expected_type, ObString &cast_buffer)
    {
      int64_t used_buf_len = 0;
      return obj_cast(cell, expected_type, cast_buffer.ptr(), cast_buffer.length(), used_buf_len);
    }
  } // end namespace common
} // end namespace oceanbase
