#include "ob_item_type.h"
const char* get_type_name(int type)
{
	switch(type){
<<<<<<< HEAD
	case T_CURSOR_DECLARE : return "T_CURSOR_DECLARE";
	case T_CURSOR_FETCH : return "T_CURSOR_FETCH";
	case T_CURSOR_FETCH_INTO : return "T_CURSOR_FETCH_INTO";
	case T_CURSOR_FETCH_PRIOR : return "T_CURSOR_FETCH_PRIOR";
	case T_CURSOR_FETCH_PRIOR_INTO : return "T_CURSOR_FETCH_PRIOR_INTO";
	case T_CURSOR_FETCH_FIRST : return "T_CURSOR_FETCH_FIRST";
	case T_CURSOR_FETCH_FIRST_INTO : return "T_CURSOR_FETCH_FIRST_INTO";
	case T_CURSOR_FETCH_LAST : return "T_CURSOR_FETCH_LAST";
	case T_CURSOR_FETCH_LAST_INTO : return "T_CURSOR_FETCH_LAST_INTO";
	case T_CURSOR_FETCH_RELATIVE : return "T_CURSOR_FETCH_RELATIVE";
	case T_CURSOR_FETCH_RELATIVE_INTO : return "T_CURSOR_FETCH_RELATIVE_INTO";
	case T_CURSOR_FETCH_ABSOLUTE : return "T_CURSOR_FETCH_ABSOLUTE";
	case T_CURSOR_FETCH_ABS_INTO : return "T_CURSOR_FETCH_ABS_INTO";
	case T_CURSOR_FETCH_FROMTO : return "T_CURSOR_FETCH_FROMTO";
	case T_CURSOR_OPEN : return "T_CURSOR_OPEN";
	case T_CURSOR_CLOSE : return "T_CURSOR_CLOSE";
	case T_CURSOR_FETCH_NEXT : return "T_CURSOR_FETCH_NEXT";
	case T_CURSOR_FETCH_NEXT_INTO : return "T_CURSOR_FETCH_NEXT_INTO";
=======
>>>>>>> refs/remotes/origin/master
	case T_INT : return "T_INT";
	case T_STRING : return "T_STRING";
	case T_BINARY : return "T_BINARY";
	case T_DATE : return "T_DATE";     // WE may need time and timestamp here
	case T_FLOAT : return "T_FLOAT";
	case T_DOUBLE : return "T_DOUBLE";
	case T_DECIMAL : return "T_DECIMAL";
	case T_BOOL : return "T_BOOL";
	case T_NULL : return "T_NULL";
	case T_QUESTIONMARK : return "T_QUESTIONMARK";
	case T_UNKNOWN : return "T_UNKNOWN";
	case T_REF_COLUMN : return "T_REF_COLUMN";
	case T_REF_EXPR : return "T_REF_EXPR";
	case T_REF_QUERY : return "T_REF_QUERY";
	case T_HINT : return "T_HINT";     // Hint message from rowkey
	case T_IDENT : return "T_IDENT";
	case T_STAR : return "T_STAR";
	case T_SYSTEM_VARIABLE : return "T_SYSTEM_VARIABLE";
	case T_TEMP_VARIABLE : return "T_TEMP_VARIABLE";
	case T_TYPE_INTEGER : return "T_TYPE_INTEGER";
	case T_TYPE_FLOAT : return "T_TYPE_FLOAT";
	case T_TYPE_DOUBLE : return "T_TYPE_DOUBLE";
	case T_TYPE_DECIMAL : return "T_TYPE_DECIMAL";
	case T_TYPE_BOOLEAN : return "T_TYPE_BOOLEAN";
	case T_TYPE_DATE : return "T_TYPE_DATE";
	case T_TYPE_TIME : return "T_TYPE_TIME";
	case T_TYPE_DATETIME : return "T_TYPE_DATETIME";
	case T_TYPE_TIMESTAMP : return "T_TYPE_TIMESTAMP";
	case T_TYPE_CHARACTER : return "T_TYPE_CHARACTER";
	case T_TYPE_VARCHAR : return "T_TYPE_VARCHAR";
	case T_TYPE_CREATETIME : return "T_TYPE_CREATETIME";
	case T_TYPE_MODIFYTIME : return "T_TYPE_MODIFYTIME";
	case T_OP_NEG : return "T_OP_NEG";   // negative
	case T_OP_POS : return "T_OP_POS";   // positive
	case T_OP_ADD : return "T_OP_ADD";
	case T_OP_MINUS : return "T_OP_MINUS";
	case T_OP_MUL : return "T_OP_MUL";
	case T_OP_DIV : return "T_OP_DIV";
	case T_OP_POW : return "T_OP_POW";
	case T_OP_REM : return "T_OP_REM";   // remainder
	case T_OP_MOD : return "T_OP_MOD";
	case T_OP_EQ : return "T_OP_EQ";      /* 2. Bool operators */
	case T_OP_LE : return "T_OP_LE";
	case T_OP_LT : return "T_OP_LT";
	case T_OP_GE : return "T_OP_GE";
	case T_OP_GT : return "T_OP_GT";
	case T_OP_NE : return "T_OP_NE";
	case T_OP_IS : return "T_OP_IS";
	case T_OP_IS_NOT : return "T_OP_IS_NOT";
	case T_OP_BTW : return "T_OP_BTW";
	case T_OP_NOT_BTW : return "T_OP_NOT_BTW";
	case T_OP_LIKE : return "T_OP_LIKE";
	case T_OP_NOT_LIKE : return "T_OP_NOT_LIKE";
	case T_OP_NOT : return "T_OP_NOT";
	case T_OP_AND : return "T_OP_AND";
	case T_OP_OR : return "T_OP_OR";
	case T_OP_IN : return "T_OP_IN";
	case T_OP_NOT_IN : return "T_OP_NOT_IN";
	case T_OP_ARG_CASE : return "T_OP_ARG_CASE";
	case T_OP_CASE : return "T_OP_CASE";
	case T_OP_ROW : return "T_OP_ROW";
	case T_OP_EXISTS : return "T_OP_EXISTS";
	case T_OP_CNN : return "T_OP_CNN";  /* 3. String operators */
	case T_FUN_SYS : return "T_FUN_SYS";                    // system functions, CHAR_LENGTH, ROUND, etc.
	case T_OP_LEFT_PARAM_END : return "T_OP_LEFT_PARAM_END";
	case T_MAX_OP : return "T_MAX_OP";
	case T_CUR_TIME : return "T_CUR_TIME";
	case T_CUR_TIME_UPS : return "T_CUR_TIME_UPS";
	case T_CUR_TIME_OP : return "T_CUR_TIME_OP";
	case T_ROW_COUNT : return "T_ROW_COUNT";
	case T_FUN_SYS_CAST : return "T_FUN_SYS_CAST";               // special system function : CAST(val AS type)
	case T_OP_NAME_FIELD : return "T_OP_NAME_FIELD";
	case T_FUN_MAX : return "T_FUN_MAX";
	case T_FUN_MIN : return "T_FUN_MIN";
	case T_FUN_SUM : return "T_FUN_SUM";
	case T_FUN_COUNT : return "T_FUN_COUNT";
	case T_FUN_AVG : return "T_FUN_AVG";
	case T_DELETE : return "T_DELETE";
	case T_SELECT : return "T_SELECT";
	case T_UPDATE : return "T_UPDATE";
	case T_INSERT : return "T_INSERT";
	case T_EXPLAIN : return "T_EXPLAIN";
	case T_LINK_NODE : return "T_LINK_NODE";
	case T_ASSIGN_LIST : return "T_ASSIGN_LIST";
	case T_ASSIGN_ITEM : return "T_ASSIGN_ITEM";
	case T_STMT_LIST : return "T_STMT_LIST";
	case T_EXPR_LIST : return "T_EXPR_LIST";
	case T_WHEN_LIST : return "T_WHEN_LIST";
	case T_PROJECT_LIST : return "T_PROJECT_LIST";
	case T_PROJECT_ITEM : return "T_PROJECT_ITEM";
	case T_FROM_LIST : return "T_FROM_LIST";
	case T_SET_UNION : return "T_SET_UNION";
	case T_SET_INTERSECT : return "T_SET_INTERSECT";
	case T_SET_EXCEPT : return "T_SET_EXCEPT";
	case T_WHERE_CLAUSE : return "T_WHERE_CLAUSE";
	case T_LIMIT_CLAUSE : return "T_LIMIT_CLAUSE";
	case T_SORT_LIST : return "T_SORT_LIST";
	case T_SORT_KEY : return "T_SORT_KEY";
	case T_SORT_ASC : return "T_SORT_ASC";
	case T_SORT_DESC : return "T_SORT_DESC";
	case T_ALL : return "T_ALL";
	case T_DISTINCT : return "T_DISTINCT";
	case T_ALIAS : return "T_ALIAS";
	case T_PROJECT_STRING : return "T_PROJECT_STRING";
	case T_COLUMN_LIST : return "T_COLUMN_LIST";
	case T_VALUE_LIST : return "T_VALUE_LIST";
	case T_VALUE_VECTOR : return "T_VALUE_VECTOR";
	case T_JOINED_TABLE : return "T_JOINED_TABLE";
	case T_JOIN_INNER : return "T_JOIN_INNER";
<<<<<<< HEAD
	case T_JOIN_SEMI : return "T_JOIN_SEMI";
	case T_JOIN_SEMI_LEFT : return "T_JOIN_SEMI_LEFT";
	case T_JOIN_SEMI_RIGHT : return "T_JOIN_SEMI_RIGHT";
=======
>>>>>>> refs/remotes/origin/master
	case T_JOIN_FULL : return "T_JOIN_FULL";
	case T_JOIN_LEFT : return "T_JOIN_LEFT";
	case T_JOIN_RIGHT : return "T_JOIN_RIGHT";
	case T_CASE : return "T_CASE";
	case T_WHEN : return "T_WHEN";
	case T_CREATE_TABLE : return "T_CREATE_TABLE";
<<<<<<< HEAD
	case T_CREATE_INDEX : return "T_CREATE_INDEX"; // add longfei [create index] [secondaryindex reconstruct] 20150916:e
	case T_TABLE_ELEMENT_LIST : return "T_TABLE_ELEMENT_LIST";
	case T_INDEX_ELEMENT_LIST : return "T_INDEX_ELEMENT_LIST"; // add longfei [create index] [secondaryindex reconstruct] 20150916:e
	case T_INDEX_STORING_LIST : return "T_INDEX_STORING_LIST"; // add longfei [create index] [secondaryindex reconstruct] 20150921:e
	case T_TABLE_OPTION_LIST : return "T_TABLE_OPTION_LIST";
	case T_INDEX_OPTION_LIST : return "T_INDEX_OPTION_LIST"; // add longfei [create index] [secondaryindex reconstruct] 20150916:e
=======
	case T_TABLE_ELEMENT_LIST : return "T_TABLE_ELEMENT_LIST";
	case T_TABLE_OPTION_LIST : return "T_TABLE_OPTION_LIST";
>>>>>>> refs/remotes/origin/master
	case T_PRIMARY_KEY : return "T_PRIMARY_KEY";
	case T_COLUMN_DEFINITION : return "T_COLUMN_DEFINITION";
	case T_COLUMN_ATTRIBUTES : return "T_COLUMN_ATTRIBUTES";
	case T_CONSTR_NOT_NULL : return "T_CONSTR_NOT_NULL";
	case T_CONSTR_NULL : return "T_CONSTR_NULL";
	case T_CONSTR_DEFAULT : return "T_CONSTR_DEFAULT";
	case T_CONSTR_AUTO_INCREMENT : return "T_CONSTR_AUTO_INCREMENT";
	case T_CONSTR_PRIMARY_KEY : return "T_CONSTR_PRIMARY_KEY";
	case T_IF_NOT_EXISTS : return "T_IF_NOT_EXISTS";
	case T_IF_EXISTS : return "T_IF_EXISTS";
	case T_JOIN_INFO : return "T_JOIN_INFO";
	case T_EXPIRE_INFO : return "T_EXPIRE_INFO";
	case T_TABLET_MAX_SIZE : return "T_TABLET_MAX_SIZE";
	case T_TABLET_BLOCK_SIZE : return "T_TABLET_BLOCK_SIZE";
	case T_TABLET_ID : return "T_TABLET_ID";
	case T_REPLICA_NUM : return "T_REPLICA_NUM";
	case T_COMPRESS_METHOD : return "T_COMPRESS_METHOD";
	case T_COMMENT : return "T_COMMENT";
	case T_USE_BLOOM_FILTER : return "T_USE_BLOOM_FILTER";
	case T_CONSISTENT_MODE : return "T_CONSISTENT_MODE";
	case T_DROP_TABLE : return "T_DROP_TABLE";
	case T_TABLE_LIST : return "T_TABLE_LIST";
<<<<<<< HEAD
	case T_DROP_INDEX : return "T_DROP_INDEX"; // add longfei [drop index] 20151024:e
	case T_INDEX_LIST : return "T_INDEX_LIST"; // add longfei [drop index] 20151024:e
=======
>>>>>>> refs/remotes/origin/master
	case T_ALTER_TABLE : return "T_ALTER_TABLE";
	case T_ALTER_ACTION_LIST : return "T_ALTER_ACTION_LIST";
	case T_TABLE_RENAME : return "T_TABLE_RENAME";
	case T_COLUMN_DROP : return "T_COLUMN_DROP";
	case T_COLUMN_ALTER : return "T_COLUMN_ALTER";
	case T_COLUMN_RENAME : return "T_COLUMN_RENAME";
	case T_ALTER_SYSTEM : return "T_ALTER_SYSTEM";
	case T_CHANGE_OBI : return "T_CHANGE_OBI";
	case T_FORCE : return "T_FORCE";
	case T_SET_MASTER : return "T_SET_MASTER";
	case T_SET_SLAVE : return "T_SET_SLAVE";
	case T_SET_MASTER_SLAVE : return "T_SET_MASTER_SLAVE";
	case T_SYTEM_ACTION_LIST : return "T_SYTEM_ACTION_LIST";
	case T_SYSTEM_ACTION : return "T_SYSTEM_ACTION";
	case T_CLUSTER : return "T_CLUSTER";
	case T_SERVER_ADDRESS : return "T_SERVER_ADDRESS";
	case T_SHOW_TABLES : return "T_SHOW_TABLES";
<<<<<<< HEAD
	case T_SHOW_INDEX : return "T_SHOW_INDEX";
=======
>>>>>>> refs/remotes/origin/master
	case T_SHOW_VARIABLES : return "T_SHOW_VARIABLES";
	case T_SHOW_COLUMNS : return "T_SHOW_COLUMNS";
	case T_SHOW_SCHEMA : return "T_SHOW_SCHEMA";
	case T_SHOW_CREATE_TABLE : return "T_SHOW_CREATE_TABLE";
	case T_SHOW_TABLE_STATUS : return "T_SHOW_TABLE_STATUS";
	case T_SHOW_PARAMETERS : return "T_SHOW_PARAMETERS";
	case T_SHOW_SERVER_STATUS : return "T_SHOW_SERVER_STATUS";
	case T_SHOW_WARNINGS : return "T_SHOW_WARNINGS";
	case T_SHOW_GRANTS : return "T_SHOW_GRANTS";
	case T_SHOW_PROCESSLIST : return "T_SHOW_PROCESSLIST";
	case T_SHOW_LIMIT : return "T_SHOW_LIMIT";
	case T_CREATE_USER : return "T_CREATE_USER";
	case T_CREATE_USER_SPEC : return "T_CREATE_USER_SPEC";
	case T_DROP_USER : return "T_DROP_USER";
	case T_SET_PASSWORD : return "T_SET_PASSWORD";
	case T_RENAME_USER : return "T_RENAME_USER";
	case T_RENAME_INFO : return "T_RENAME_INFO";
	case T_LOCK_USER : return "T_LOCK_USER";
	case T_GRANT : return "T_GRANT";
	case T_PRIVILEGES : return "T_PRIVILEGES";
	case T_PRIV_LEVEL : return "T_PRIV_LEVEL";
	case T_PRIV_TYPE : return "T_PRIV_TYPE";
	case T_USERS : return "T_USERS";
	case T_REVOKE : return "T_REVOKE";
	case T_BEGIN : return "T_BEGIN";
	case T_COMMIT : return "T_COMMIT";
	case T_PREPARE : return "T_PREPARE";
	case T_DEALLOCATE : return "T_DEALLOCATE";
	case T_EXECUTE : return "T_EXECUTE";
	case T_ARGUMENT_LIST : return "T_ARGUMENT_LIST";
	case T_VARIABLE_SET : return "T_VARIABLE_SET";
	case T_VAR_VAL : return "T_VAR_VAL";
	case T_ROLLBACK : return "T_ROLLBACK";
	case T_HINT_OPTION_LIST : return "T_HINT_OPTION_LIST";
	case T_READ_STATIC : return "T_READ_STATIC";
	case T_HOTSPOT : return "T_HOTSPOT";
	case T_READ_CONSISTENCY : return "T_READ_CONSISTENCY";
<<<<<<< HEAD
	case T_LONG_TRANS : return "T_LONG_TRANS"; //add by qx 21070317
	case T_NO_GROUP : return "T_NO_GROUP";//add by wdh 20160716
	case T_USE_INDEX : return "T_USE_INDEX";// add longfei
	case T_UNKOWN_HINT : return "T_UNKOWN_HINT";// add longfei
  //	case T_SEMI_JOIN : return "T_SEMI_JOIN";// add by yusj [SEMI_JOIN] 20150819
	case T_SEMI_JOIN : return "T_SEMI_JOIN";
	case T_SEMI_BTW_JOIN : return "T_SEMI_BTW_JOIN";
	case T_SEMI_MULTI_JOIN : return "T_SEMI_MULTI_JOIN";
	case T_KILL : return "T_KILL";
	case T_MAX : return "T_MAX";
	case T_LOCK_TABLE : return "T_LOCK_TABLE"; // add wangjiahao [table lock] 20160616
	case T_PROCEDURE_CREATE : return "T_PROCEDURE_CREATE";//add zz for test 2014-11-27
	case T_PROCEDURE_DECL : return "T_PROCEDURE_DECL";
	case T_PARAM_DEFINITION : return "T_PARAM_DEFINITION";
	case T_IN_PARAM_DEFINITION : return "T_IN_PARAM_DEFINITION";
	case T_OUT_PARAM_DEFINITION : return "T_OUT_PARAM_DEFINITION";
	case T_INOUT_PARAM_DEFINITION : return "T_INOUT_PARAM_DEFINITION";
	case T_PARAM_LIST : return "T_PARAM_LIST";
	case T_PROCEDURE_BLOCK : return "T_PROCEDURE_BLOCK";
	case T_PROCEDURE_STMTS : return "T_PROCEDURE_STMTS";
	case T_PROCEDURE_IF : return "T_PROCEDURE_IF";
	case T_PROCEDURE_ELSEIFS : return "T_PROCEDURE_ELSEIFS";
	case T_PROCEDURE_ELSEIF : return "T_PROCEDURE_ELSEIF";
	case T_PROCEDURE_ELSE : return "T_PROCEDURE_ELSE";
	case T_PROCEDURE_EXEC : return "T_PROCEDURE_EXEC";
	case T_PROCEDURE_DECLARE : return "T_PROCEDURE_DECLARE";
	case T_PROCEDURE_ASSGIN : return "T_PROCEDURE_ASSGIN";
	case T_VAR_TYPE_LIST : return "T_VAR_TYPE_LIST";
	case T_VAR_DEFINITION : return "T_VAR_DEFINITION";
	case T_VAR_VAL_LIST : return "T_VAR_VAL_LIST";
	case T_PROCEDURE_WHILE : return "T_PROCEDURE_WHILE";
	case T_PROCEDURE_LOOP : return "T_PROCEDURE_LOOP";
	case T_PROCEDURE_DROP : return "T_PROCEDURE_DROP";
	case T_PROCEDURE_SHOW : return "T_PROCEDURE_SHOW";
	case T_PROCEDURE_CASE : return "T_PROCEDURE_CASE";
	case T_PROCEDURE_CASE_WHEN_LIST : return "T_PROCEDURE_CASE_WHEN_LIST";
	case T_PROCEDURE_CASE_WHEN : return "T_PROCEDURE_CASE_WHEN";
	case T_PROCEDURE_EXIT : return "T_PROCEDURE_EXIT";
	case T_PROCEDURE_CONTINUE : return "T_PROCEDURE_CONTINUE";
	case T_SELECT_INTO : return "T_SELECT_INTO";/*select into sentence*/
	case T_JOIN_OP_TYPE_LIST : return "T_JOIN_OP_TYPE_LIST";
	case T_BLOOMFILTER_JOIN : return "T_BLOOMFILTER_JOIN";
	case T_MERGE_JOIN : return "T_MERGE_JOIN";
	case T_HASH_JOIN_SINGLE : return "T_HASH_JOIN_SINGLE";//add maoxx [hash join single] 20170110
	case T_GATHER_STATISTICS : return "T_GATHER_STATISTICS";
	case T_NO_QUERY_OPT : return "T_NO_QUERY_OPT"; // add by qx [query optimizer] 20170730
	case T_ARRAY : return "T_ARRAY";
	case T_VAR_ARRAY_VAL : return "T_VAR_ARRAY_VAL";
    case T_OP_LEFT_SEMI : return "T_OP_LEFT_SEMI";
    case T_OP_LEFT_ANTI_SEMI : return "T_OP_LEFT_ANTI_SEMI";
=======
	case T_KILL : return "T_KILL";
	case T_MAX : return "T_MAX";
>>>>>>> refs/remotes/origin/master
	default:return "Unknown";
	}
}
