/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * nb_scan_cond.cpp
 *
 * Authors:
 *   Junquan Chen <jianming.cjq@taobao.com>
 *
 */

#include "nb_scan_cond.h"

using namespace oceanbase;
using namespace common;
using namespace nb_accessor;

ScanConds::ScanConds(const char* column_name, const ObLogicOperator& cond_op, int64_t value)
{
  ObString name;
  name.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));

  ObObj obj;
  obj.set_int(value);

<<<<<<< HEAD
  OnevArray<ObSimpleCond>(ObSimpleCond(name, cond_op, obj));
=======
  EasyArray<ObSimpleCond>(ObSimpleCond(name, cond_op, obj));
>>>>>>> refs/remotes/origin/master
}

ScanConds::ScanConds()
{
}

ScanConds::ScanConds(const char* column_name, const ObLogicOperator& cond_op, ObString& value)
{
  ObString name;
  name.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));

  ObObj obj;
  obj.set_varchar(value);

<<<<<<< HEAD
  OnevArray<ObSimpleCond>(ObSimpleCond(name, cond_op, obj));
=======
  EasyArray<ObSimpleCond>(ObSimpleCond(name, cond_op, obj));
>>>>>>> refs/remotes/origin/master
}

ScanConds& ScanConds::operator()(const char* column_name, const ObLogicOperator& cond_op, int64_t value)
{
  ObString name;
  name.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));

  ObObj obj;
  obj.set_int(value);

<<<<<<< HEAD
  OnevArray<ObSimpleCond>::operator()(ObSimpleCond(name, cond_op, obj));
=======
  EasyArray<ObSimpleCond>::operator()(ObSimpleCond(name, cond_op, obj));
>>>>>>> refs/remotes/origin/master
  return *this;
}

ScanConds& ScanConds::operator()(const char* column_name, const ObLogicOperator& cond_op, ObString& value)
{
  ObString name;
  name.assign_ptr(const_cast<char*>(column_name), static_cast<int32_t>(strlen(column_name)));

  ObObj obj;
  obj.set_varchar(value);

<<<<<<< HEAD
  OnevArray<ObSimpleCond>::operator()(ObSimpleCond(name, cond_op, obj));
=======
  EasyArray<ObSimpleCond>::operator()(ObSimpleCond(name, cond_op, obj));
>>>>>>> refs/remotes/origin/master
  return *this;
}

