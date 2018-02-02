/**
* Copyright (C) 2013-2016 DaSE .
*
* This program is free software; you can redistribute it and/or
* modify it under the terms of the GNU General Public License
* version 2 as published by the Free Software Foundation.
*
* @file ob_cursor_fetch_last.h
* @brief this class  present a "cursor fetch last" physical plan in oceanbase
*
* Created by zhounan: support curosr
*
* @version CEDAR 0.2 
* @author zhounan <zn4work@gmail.com>
* @date 2014_11_23
*/
#ifndef OCEANBASE_SQL_OB_FETCH_LAST_H_
#define OCEANBASE_SQL_OB_FETCH_LAST_H_
#include "sql/ob_single_child_phy_operator.h"
#include "ob_sql_session_info.h"


namespace oceanbase
{
	namespace sql
	{
        /**
         * @brief The ObFetchLast class
         */
		class ObFetchLast: public ObSingleChildPhyOperator
		{
			public:
			ObFetchLast();
			virtual ~ObFetchLast();
			virtual void reset();
			virtual void reuse();
            /**
             * @brief set_cursor_name
             * @param cursor_name
             */
			void set_cursor_name(const common::ObString& cursor_name);
			//execute the declare statement
			virtual int open();
			virtual int close();
		    virtual ObPhyOperatorType get_type() const { return PHY_CURSOR_FETCH_LAST; }
			virtual int64_t to_string(char* buf, const int64_t buf_len) const;
			virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
			virtual int get_next_row(const common::ObRow *&row);
			private:
			//disallow copy
			ObFetchLast(const ObFetchLast &other);
			ObFetchLast& operator=(const ObFetchLast &other);
			private:
			//data members

            common::ObString cursor_name_;///> cursor name
		};

   		inline void ObFetchLast::set_cursor_name(const common::ObString& cursor_name)
	    {
	        cursor_name_ = cursor_name;
	    }
	}
}
#endif/* OCEANBASE_SQL_OB_FETCH_H_ */
