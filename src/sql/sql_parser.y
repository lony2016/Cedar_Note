%define api.pure
%parse-param {ParseResult* result}
%locations
%no-lines
%verbose
%{
#include <stdint.h>
#include "parse_node.h"
#include "parse_malloc.h"
#include "ob_non_reserved_keywords.h"
#include "common/ob_privilege_type.h"
#define YYDEBUG 1
%}

%union{
  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  int    ival;
}

%{
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <ctype.h>

#include "sql_parser.lex.h"

#define YYLEX_PARAM result->yyscan_info_

extern void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s,...);

extern ParseNode* merge_tree(void *malloc_pool, ObItemType node_tag, ParseNode* source_tree);

extern ParseNode* new_terminal_node(void *malloc_pool, ObItemType type);

extern ParseNode* new_non_terminal_node(void *malloc_pool, ObItemType node_tag, int num, ...);

extern char* copy_expr_string(ParseResult* p, int expr_start, int expr_end);

#define ISSPACE(c) ((c) == ' ' || (c) == '\n' || (c) == '\r' || (c) == '\t' || (c) == '\f' || (c) == '\v')

#define malloc_terminal_node(node, malloc_pool, type) \
do \
{ \
  if ((node = new_terminal_node(malloc_pool, type)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for malloc"); \
    YYABORT; \
  } \
} while(0)

#define malloc_non_terminal_node(node, malloc_pool, node_tag, ...) \
do \
{ \
  if ((node = new_non_terminal_node(malloc_pool, node_tag, ##__VA_ARGS__)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for malloc"); \
    YYABORT; \
  } \
} while(0)

#define merge_nodes(node, malloc_pool, node_tag, source_tree) \
do \
{ \
  if (source_tree == NULL) \
  { \
    node = NULL; \
  } \
  else if ((node = merge_tree(malloc_pool, node_tag, source_tree)) == NULL) \
  { \
    yyerror(NULL, result, "No more space for merging nodes"); \
    YYABORT; \
  } \
} while (0)

#define dup_expr_string(str_ptr, result, expr_start, expr_end) \
  do \
  { \
    str_ptr = NULL; \
    int start = expr_start; \
    while (start <= expr_end && ISSPACE(result->input_sql_[start - 1])) \
      start++; \
    if (start >= expr_start \
      && (str_ptr = copy_expr_string(result, start, expr_end)) == NULL) \
    { \
      yyerror(NULL, result, "No more space for copying expression string"); \
      YYABORT; \
    } \
  } while (0)

%}

%destructor {destroy_tree($$);}<node>
%destructor {oceanbase::common::ob_free($$);}<str_value_>

%token <node> NAME
%token <node> STRING
%token <node> INTNUM
%token <node> DATE_VALUE
%token <node> HINT_VALUE
%token <node> BOOL
%token <node> APPROXNUM
%token <node> NULLX
%token <node> UNKNOWN
%token <node> QUESTIONMARK
%token <node> SYSTEM_VARIABLE
%token <node> TEMP_VARIABLE

%left	UNION EXCEPT
%left	INTERSECT
%left	OR
%left	AND
%right NOT
%left COMP_LE COMP_LT COMP_EQ COMP_GT COMP_GE COMP_NE
%left CNNOP
%left LIKE
%nonassoc BETWEEN
%nonassoc IN
%nonassoc IS NULLX BOOL UNKNOWN
%left '+' '-'
%left '*' '/' '%' MOD
%left '^'
%right UMINUS
%left '(' ')'
%left '.'

<<<<<<< HEAD
/*add by zhujun*/
%token PROCEDURE DECLARE ELSEIF OUT INOUT WHILE LOOP EXIT CONTINUE DO CALL ARRAY REVERSE
/*zhounan unmark*/
%token CURSOR OPEN FETCH CLOSE NEXT PRIOR FIRST LAST ABSOLUTE RELATIVE 

=======
>>>>>>> refs/remotes/origin/master
%token ADD AND ANY ALL ALTER AS ASC
%token BETWEEN BEGI BIGINT BINARY BOOLEAN BOTH BY
%token CASCADE CASE CHARACTER CLUSTER CNNOP COMMENT COMMIT
       CONSISTENT COLUMN COLUMNS CREATE CREATETIME
       CURRENT_USER CHANGE_OBI SWITCH_CLUSTER
%token DATE DATETIME DEALLOCATE DECIMAL DEFAULT DELETE DESC DESCRIBE
<<<<<<< HEAD
       DISTINCT DOUBLE DROP DUAL TRUNCATE /*add hxlong [Truncate Table]:20170403 add 'TRUNCATE'*/
=======
       DISTINCT DOUBLE DROP DUAL
>>>>>>> refs/remotes/origin/master
%token ELSE END END_P ERROR EXCEPT EXECUTE EXISTS EXPLAIN
%token FLOAT FOR FROM FULL FROZEN FORCE
%token GLOBAL GLOBAL_ALIAS GRANT GROUP
%token HAVING HINT_BEGIN HINT_END HOTSPOT
%token IDENTIFIED IF IN INNER INTEGER INTERSECT INSERT INTO IS
<<<<<<< HEAD
%token JOIN SEMI_JOIN
%token KEY
%token LEADING LEFT LIMIT LIKE LOCAL LOCKED LOCKWJH
=======
%token JOIN
%token KEY
%token LEADING LEFT LIMIT LIKE LOCAL LOCKED
>>>>>>> refs/remotes/origin/master
%token MEDIUMINT MEMORY MOD MODIFYTIME MASTER
%token NOT NUMERIC
%token OFFSET ON OR ORDER OPTION OUTER
%token PARAMETERS PASSWORD PRECISION PREPARE PRIMARY
%token READ_STATIC REAL RENAME REPLACE RESTRICT PRIVILEGES REVOKE RIGHT
<<<<<<< HEAD
       ROLLBACK KILL READ_CONSISTENCY NO_GROUP LONG_TRANS //add by wdh 20160716 add by qx 20170318
=======
       ROLLBACK KILL READ_CONSISTENCY
>>>>>>> refs/remotes/origin/master
%token SCHEMA SCOPE SELECT SESSION SESSION_ALIAS
       SET SHOW SMALLINT SNAPSHOT SPFILE START STATIC SYSTEM STRONG SET_MASTER_CLUSTER SET_SLAVE_CLUSTER SLAVE
%token TABLE TABLES THEN TIME TIMESTAMP TINYINT TRAILING TRANSACTION TO
%token UNION UPDATE USER USING
%token VALUES VARCHAR VARBINARY
%token WHERE WHEN WITH WORK PROCESSLIST QUERY CONNECTION WEAK
<<<<<<< HEAD
%token INDEX STORING /* add longfei [create index] [secondaryindex reconstruct] 20150917 e */
/* add maoxx [bloomfilter_join] 20160406 */
%token BLOOMFILTER_JOIN MERGE_JOIN
//add wanglei [semi join] 20170417:b
%token SI
%token SIB
%token SM
%token SEMI 
//add wanglei [semi join] 20170417:e
/* add e */
//add e/*add by qx [query optimization] 20170412 :b*/
%token NO_QUERY_OPT
//add qx 20170412 :e
%token GATHER STATISTICS //add weixing [statistics build] 20161212
//add maoxx [hash join single] 20170110
%token HASH_JOIN
=======

>>>>>>> refs/remotes/origin/master
%token <non_reserved_keyword>
       AUTO_INCREMENT CHUNKSERVER COMPRESS_METHOD CONSISTENT_MODE
       EXPIRE_INFO GRANTS JOIN_INFO
       MERGESERVER REPLICA_NUM ROOTSERVER ROW_COUNT SERVER SERVER_IP
       SERVER_PORT SERVER_TYPE STATUS TABLE_ID TABLET_BLOCK_SIZE TABLET_MAX_SIZE
       UNLOCKED UPDATESERVER USE_BLOOM_FILTER VARIABLES VERBOSE WARNINGS
<<<<<<< HEAD
/*add by zhujun*/

%type <node> create_procedure_stmt proc_decl proc_block 
%type <node> proc_sect proc_stmts proc_stmt control_sect control_stmts control_stmt
%type <node> proc_parameter_list proc_parameter
%type <node> stmt_if stmt_elsifs stmt_elsif stmt_else
%type <node> exec_procedure_stmt stmt_declare
%type <node> stmt_assign stmt_while stmt_null drop_procedure_stmt show_procedure_stmt 
%type <node> stmt_loop stmt_case case_when_list case_else case_when stmt_exit select_into_clause
/*zhounan unmark*/
%type <node> cursor_declare_stmt cursor_open_stmt cursor_fetch_stmt cursor_close_stmt cursor_name fetch_prior_stmt fetch_first_stmt fetch_last_stmt  fetch_relative_stmt fetch_absolute_stmt fetch_fromto_stmt next_or_prior
%type <node> cursor_fetch_into_stmt cursor_fetch_next_into_stmt cursor_fetch_first_into_stmt cursor_fetch_last_into_stmt cursor_fetch_prior_into_stmt cursor_fetch_absolute_into_stmt cursor_fetch_relative_into_stmt


//add zt 20151125:b
%type <node> array_expr
%type <node> array_vals_list array_val_list var_and_array_val
//add zt 20151125:e
=======

>>>>>>> refs/remotes/origin/master
%type <node> sql_stmt stmt_list stmt
%type <node> select_stmt insert_stmt update_stmt delete_stmt
%type <node> create_table_stmt opt_table_option_list table_option
%type <node> drop_table_stmt table_list
%type <node> explain_stmt explainable_stmt kill_stmt
%type <node> expr_list expr expr_const arith_expr simple_expr
%type <node> column_ref
%type <node> case_expr func_expr in_expr
%type <node> case_arg when_clause_list when_clause case_default
%type <node> update_asgn_list update_asgn_factor
%type <node> table_element_list table_element column_definition
%type <node> data_type opt_if_not_exists opt_if_exists
%type <node> replace_or_insert opt_insert_columns column_list
%type <node> insert_vals_list insert_vals
%type <node> select_with_parens select_no_parens select_clause
%type <node> simple_select no_table_select select_limit select_expr_list
%type <node> opt_where opt_groupby opt_order_by order_by opt_having
%type <node> opt_select_limit limit_expr opt_for_update
%type <node> sort_list sort_key opt_asc_desc
%type <node> opt_distinct distinct_or_all projection
%type <node> from_list table_factor relation_factor joined_table
%type <node> join_type join_outer
%type <node> opt_float opt_time_precision opt_char_length opt_decimal
%type <node> opt_equal_mark opt_precision opt_verbose
%type <node> opt_column_attribute_list column_attribute
%type <node> show_stmt opt_show_condition opt_like_condition
%type <node> prepare_stmt stmt_name preparable_stmt
%type <node> variable_set_stmt var_and_val_list var_and_val to_or_eq
%type <node> execute_stmt argument_list argument opt_using_args
%type <node> deallocate_prepare_stmt deallocate_or_drop
%type <ival> opt_scope opt_drop_behavior opt_full
%type <node> create_user_stmt user_specification user_specification_list user password
%type <node> drop_user_stmt user_list
%type <node> set_password_stmt opt_for_user
%type <node> rename_user_stmt rename_info rename_list
%type <node> lock_user_stmt lock_spec
%type <node> grant_stmt priv_type_list priv_type priv_level opt_privilege
%type <node> revoke_stmt opt_on_priv_level
%type <node> opt_limit opt_for_grant_user opt_flag opt_is_global
%type <node> parameterized_trim
%type <ival> opt_with_consistent_snapshot opt_config_scope
%type <node> opt_work begin_stmt commit_stmt rollback_stmt
%type <node> alter_table_stmt alter_column_actions alter_column_action
%type <node> opt_column alter_column_behavior
%type <node> alter_system_stmt alter_system_actions alter_system_action
%type <node> server_type opt_cluster_or_address opt_comment
%type <node> column_name relation_name function_name column_label
%type <node> opt_hint opt_hint_list hint_option opt_force
%type <node> when_func when_func_stmt opt_when when_func_name
%type <non_reserved_keyword> unreserved_keyword
%type <ival> consistency_level
%type <node> opt_comma_list hint_options
<<<<<<< HEAD
%type <node> lock_table_stmt
/* add [secondaryindex reconstruct] 20150925 longfei [create index] :b */
%type <node> create_index_stmt opt_index_columns opt_storing opt_index_option_list opt_storing_columns index_option
/* add e */

/* add longfei [drop index] 20151024 :b */
%type <node> drop_index_stmt index_list table_name
/* add e */

/* add maoxx [bloomfilter_join] 20160406 */
%type <node> join_op_type_list join_op_type
/* add e */

/*add weixing[statistics build]20161212*/
%type <node> gather_statistics_stmt opt_gather_columns
/*add 20161212:e*/

/*add hxlong[truncate table] 20170304:b*/
%type <node> truncate_table_stmt
/*add 20170304:e*/
=======

>>>>>>> refs/remotes/origin/master
%start sql_stmt
%%

sql_stmt:
    stmt_list END_P
    {
      merge_nodes($$, result->malloc_pool_, T_STMT_LIST, $1);
      result->result_tree_ = $$;
      YYACCEPT;
    }
  ;

stmt_list:
    stmt_list ';' stmt
    {
      if ($3 != NULL)
        malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
      else
        $$ = $1;
    }
  | stmt
    {
      $$ = ($1 != NULL) ? $1 : NULL;
    }
  ;

stmt:
<<<<<<< HEAD
	create_procedure_stmt { $$ = $1; }
  | exec_procedure_stmt   { $$ = $1; }
  | show_procedure_stmt   { $$ = $1; }
  | drop_procedure_stmt   { $$ = $1; }
  
  |cursor_declare_stmt       { $$ = $1; }
  |cursor_open_stmt          { $$ = $1; }
  |cursor_fetch_stmt         { $$ = $1; }
  |cursor_close_stmt         { $$ = $1; }
  |fetch_prior_stmt   { $$ = $1; }

  |fetch_first_stmt   { $$ = $1; }
  |fetch_last_stmt   { $$ = $1; } 
  |fetch_relative_stmt   { $$ = $1; }
  |fetch_absolute_stmt   { $$ = $1; } 
  |fetch_fromto_stmt   { $$ = $1; }
  
  | select_stmt       { $$ = $1; }
  | insert_stmt       { $$ = $1; }
  | create_table_stmt { $$ = $1; }
  | create_index_stmt { $$ = $1; }
  | update_stmt       { $$ = $1; }
  | delete_stmt       { $$ = $1; }
  | drop_table_stmt   { $$ = $1; }
  | drop_index_stmt   { $$ = $1; }
=======
    select_stmt       { $$ = $1; }
  | insert_stmt       { $$ = $1; }
  | create_table_stmt { $$ = $1; }
  | update_stmt       { $$ = $1; }
  | delete_stmt       { $$ = $1; }
  | drop_table_stmt   { $$ = $1; }
>>>>>>> refs/remotes/origin/master
  | explain_stmt      { $$ = $1; }
  | show_stmt         { $$ = $1; }
  | prepare_stmt      { $$ = $1; }
  | variable_set_stmt { $$ = $1; }
  | execute_stmt      { $$ = $1; }
  | alter_table_stmt  { $$ = $1; }
  | alter_system_stmt { $$ = $1; }
  | deallocate_prepare_stmt   { $$ = $1; }
  | create_user_stmt { $$ = $1; }
  | drop_user_stmt { $$ = $1;}
  | set_password_stmt { $$ = $1;}
  | rename_user_stmt { $$ = $1;}
  | lock_user_stmt { $$ = $1;}
  | grant_stmt { $$ = $1;}
  | revoke_stmt { $$ = $1;}
  | begin_stmt { $$ = $1;}
  | commit_stmt { $$ = $1;}
  | rollback_stmt {$$ = $1;}
  | kill_stmt {$$ = $1;}
<<<<<<< HEAD
  | lock_table_stmt {$$ = $1;}
  /*add weixing [statistics build]20161212*/
  |gather_statistics_stmt {$$ = $1;}
  /*add e*/
  /*add hxlong[truncate table] 20170403:b*/
  | truncate_table_stmt { $$ = $1;}
  /*add:e*/
=======
>>>>>>> refs/remotes/origin/master
  | /*EMPTY*/   { $$ = NULL; }
  ;

/*****************************************************************************
 *
 *	expression grammar
 *
 *****************************************************************************/

expr_list:
    expr
    {
      $$ = $1;
    }
  | expr_list ',' expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

column_ref:
    column_name
    { $$ = $1; }
  | relation_name '.' column_name
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NAME_FIELD, 2, $1, $3);
      dup_expr_string($$->str_value_, result, @3.first_column, @3.last_column);
    }
  |
    relation_name '.' '*'
    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NAME_FIELD, 2, $1, node);
    }
  ;

expr_const:
    STRING { $$ = $1; }
  | DATE_VALUE { $$ = $1; }
  | INTNUM { $$ = $1; }
  | APPROXNUM { $$ = $1; }
  | BOOL { $$ = $1; }
  | NULLX { $$ = $1; }
  | QUESTIONMARK { $$ = $1; }
  | TEMP_VARIABLE { $$ = $1; }
  | SYSTEM_VARIABLE { $$ = $1; }
  | SESSION_ALIAS '.' column_name { $3->type_ = T_SYSTEM_VARIABLE; $$ = $3; }
  ;

simple_expr:
    column_ref
    { $$ = $1; }
  | expr_const
    { $$ = $1; }
  | '(' expr ')'
    { $$ = $2; }
  | '(' expr_list ',' expr ')'
    {
      ParseNode *node = NULL;
      malloc_non_terminal_node(node, result->malloc_pool_, T_LINK_NODE, 2, $2, $4);
      merge_nodes($$, result->malloc_pool_, T_EXPR_LIST, node);
    }
  | case_expr
    {
      $$ = $1;
      /*
      yyerror(&@1, result, "CASE expression is not supported yet!");
      YYABORT;
      */
    }
  | func_expr
    {
      $$ = $1;
    }
  | when_func
    {
      $$ = $1;
    }
  | select_with_parens	    %prec UMINUS
    {
    	$$ = $1;
    }
  | EXISTS select_with_parens
    {
    	malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EXISTS, 1, $2);
    }
<<<<<<< HEAD
  | array_expr
    {
      $$ = $1;
    }
=======
>>>>>>> refs/remotes/origin/master
  ;

/* used by the expression that use range value, e.g. between and */
arith_expr:
    simple_expr   { $$ = $1; }
  | '+' arith_expr %prec UMINUS
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_POS, 1, $2);
    }
  | '-' arith_expr %prec UMINUS
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NEG, 1, $2);
    }
  | arith_expr '+' arith_expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_ADD, 2, $1, $3); }
  | arith_expr '-' arith_expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MINUS, 2, $1, $3); }
  | arith_expr '*' arith_expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MUL, 2, $1, $3); }
  | arith_expr '/' arith_expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_DIV, 2, $1, $3); }
  | arith_expr '%' arith_expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_REM, 2, $1, $3); }
  | arith_expr '^' arith_expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_POW, 2, $1, $3); }
  | arith_expr MOD arith_expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MOD, 2, $1, $3); }

expr:
    simple_expr   { $$ = $1; }
  | '+' expr %prec UMINUS
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_POS, 1, $2);
    }
  | '-' expr %prec UMINUS
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NEG, 1, $2);
    }
  | expr '+' expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_ADD, 2, $1, $3); }
  | expr '-' expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MINUS, 2, $1, $3); }
  | expr '*' expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MUL, 2, $1, $3); }
  | expr '/' expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_DIV, 2, $1, $3); }
  | expr '%' expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_REM, 2, $1, $3); }
  | expr '^' expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_POW, 2, $1, $3); }
  | expr MOD expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_MOD, 2, $1, $3); }
  | expr COMP_LE expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LE, 2, $1, $3); }
  | expr COMP_LT expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LT, 2, $1, $3); }
  | expr COMP_EQ expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_EQ, 2, $1, $3); }
  | expr COMP_GE expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GE, 2, $1, $3); }
  | expr COMP_GT expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_GT, 2, $1, $3); }
  | expr COMP_NE expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NE, 2, $1, $3); }
  | expr LIKE expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 2, $1, $3); }
  | expr NOT LIKE expr { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_LIKE, 2, $1, $4); }
  | expr AND expr %prec AND
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_AND, 2, $1, $3);
    }
  | expr OR expr %prec OR
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_OR, 2, $1, $3);
    }
  | NOT expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT, 1, $2);
    }
  | expr IS NULLX
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, $3);
    }
  | expr IS NOT NULLX
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4);
    }
  | expr IS BOOL
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, $3);
    }
  | expr IS NOT BOOL
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4);
    }
  | expr IS UNKNOWN
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS, 2, $1, $3);
    }
  | expr IS NOT UNKNOWN
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IS_NOT, 2, $1, $4);
    }
  | expr BETWEEN arith_expr AND arith_expr	    %prec BETWEEN
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_BTW, 3, $1, $3, $5);
    }
  | expr NOT BETWEEN arith_expr AND arith_expr	  %prec BETWEEN
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_BTW, 3, $1, $4, $6);
    }
  | expr IN in_expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_IN, 2, $1, $3);
    }
  | expr NOT IN in_expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_NOT_IN, 2, $1, $4);
    }
  | expr CNNOP expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_OP_CNN, 2, $1, $3);
    }
  ;

in_expr:
    select_with_parens
    {
    	$$ = $1;
    }
  | '(' expr_list ')'
    { merge_nodes($$, result->malloc_pool_, T_EXPR_LIST, $2); }
  ;

case_expr:
    CASE case_arg when_clause_list case_default END
    {
      merge_nodes($$, result->malloc_pool_, T_WHEN_LIST, $3);
      malloc_non_terminal_node($$, result->malloc_pool_, T_CASE, 3, $2, $$, $4);
    }
  ;

case_arg:
    expr                  { $$ = $1; }
  | /*EMPTY*/             { $$ = NULL; }
  ;

when_clause_list:
  	when_clause
    { $$ = $1; }
  | when_clause_list when_clause
    { malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2); }
  ;

when_clause:
  	WHEN expr THEN expr
    {
    	malloc_non_terminal_node($$, result->malloc_pool_, T_WHEN, 2, $2, $4);
    }
  ;

case_default:
  	ELSE expr                { $$ = $2; }
  | /*EMPTY*/                { malloc_terminal_node($$, result->malloc_pool_, T_NULL); }
  ;

<<<<<<< HEAD
//add zt 20151125:b
array_expr:
    TEMP_VARIABLE '(' INTNUM ')'
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_ARRAY, 2, $1, $3);
    }
  | TEMP_VARIABLE '(' TEMP_VARIABLE ')'
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_ARRAY, 2, $1, $3);
    }
  ;
//add zt 20151125:e

=======
>>>>>>> refs/remotes/origin/master
func_expr:
    function_name '(' '*' ')'
    {
      if (strcasecmp($1->str_value_, "count") != 0)
      {
        yyerror(&@1, result, "Only COUNT function can be with '*' parameter!");
        YYABORT;
      }
      else
      {
        ParseNode* node = NULL;
        malloc_terminal_node(node, result->malloc_pool_, T_STAR);
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 1, node);
      }
    }
  | function_name '(' distinct_or_all expr ')'
    {
      if (strcasecmp($1->str_value_, "count") == 0)
      {
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, $3, $4);
      }
      else if (strcasecmp($1->str_value_, "sum") == 0)
      {
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SUM, 2, $3, $4);
      }
      else if (strcasecmp($1->str_value_, "max") == 0)
      {
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MAX, 2, $3, $4);
      }
      else if (strcasecmp($1->str_value_, "min") == 0)
      {
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MIN, 2, $3, $4);
      }
      else if (strcasecmp($1->str_value_, "avg") == 0)
      {
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_AVG, 2, $3, $4);
      }
      else
      {
        yyerror(&@1, result, "Wrong system function with 'DISTINCT/ALL'!");
        YYABORT;
      }
    }
  | function_name '(' expr_list ')'
    {
      if (strcasecmp($1->str_value_, "count") == 0)
      {
        if ($3->type_ == T_LINK_NODE)
        {
          yyerror(&@1, result, "COUNT function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_COUNT, 2, NULL, $3);
      }
      else if (strcasecmp($1->str_value_, "sum") == 0)
      {
        if ($3->type_ == T_LINK_NODE)
        {
          yyerror(&@1, result, "SUM function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SUM, 2, NULL, $3);
      }
      else if (strcasecmp($1->str_value_, "max") == 0)
      {
        if ($3->type_ == T_LINK_NODE)
        {
          yyerror(&@1, result, "MAX function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MAX, 2, NULL, $3);
      }
      else if (strcasecmp($1->str_value_, "min") == 0)
      {
        if ($3->type_ == T_LINK_NODE)
        {
          yyerror(&@1, result, "MIN function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_MIN, 2, NULL, $3);
      }
      else if (strcasecmp($1->str_value_, "avg") == 0)
      {
        if ($3->type_ == T_LINK_NODE)
        {
          yyerror(&@1, result, "AVG function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_AVG, 2, NULL, $3);
      }
      else if (strcasecmp($1->str_value_, "trim") == 0)
      {
        if ($3->type_ == T_LINK_NODE)
        {
          yyerror(&@1, result, "TRIM function syntax error! TRIM don't take %d params", $3->num_child_);
          YYABORT;
        }
        else
        {
          ParseNode* default_type = NULL;
          malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
          default_type->value_ = 0;
          ParseNode* default_operand = NULL;
          malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
          default_operand->str_value_ = " "; /* blank for default */
          default_operand->value_ = strlen(default_operand->str_value_);
          ParseNode *params = NULL;
          malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, $3);
          malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $1, params);
        }
      }
      else  /* system function */
      {
        ParseNode *params = NULL;
        merge_nodes(params, result->malloc_pool_, T_EXPR_LIST, $3);
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $1, params);
      }
    }
  | function_name '(' expr AS data_type ')'
    {
      if (strcasecmp($1->str_value_, "cast") == 0)
      {
<<<<<<< HEAD
        /*modify fanqiushi DECIMAL OceanBase_BankCommV0.2 2014_6_16:b*/
        /*$5->value_ = $5->type_;
        $5->type_ = T_INT;*/
        if($5->type_!=T_TYPE_DECIMAL)
        {
            $5->value_ = $5->type_;
            $5->type_ = T_INT;
        }
        /*modify:e*/
=======
        $5->value_ = $5->type_;
        $5->type_ = T_INT;
>>>>>>> refs/remotes/origin/master
        ParseNode *params = NULL;
        malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, $3, $5);
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $1, params);
      }
      else
      {
        yyerror(&@1, result, "AS support cast function only!");
        YYABORT;
      }
    }
  | function_name '(' parameterized_trim ')'
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 2, $1, $3);
    }
  | function_name '(' ')'
    {
      if (strcasecmp($1->str_value_, "now") == 0 ||
          strcasecmp($1->str_value_, "current_time") == 0 ||
          strcasecmp($1->str_value_, "current_timestamp") == 0)
      {
        malloc_non_terminal_node($$, result->malloc_pool_, T_CUR_TIME, 1, $1);
      }
      else if (strcasecmp($1->str_value_, "strict_current_timestamp") == 0)
      {
        malloc_non_terminal_node($$, result->malloc_pool_, T_CUR_TIME_UPS, 1, $1);
      }
      else
      {
        malloc_non_terminal_node($$, result->malloc_pool_, T_FUN_SYS, 1, $1);
      }
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    }
  ;

when_func:
    when_func_name '(' when_func_stmt ')'
    {
      $$ = $1;
      $$->children_[0] = $3;
    }
  ;

when_func_name:
    ROW_COUNT
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_ROW_COUNT, 1, NULL);
    }
  ;

when_func_stmt:
    select_stmt
  | insert_stmt
  | update_stmt
  | delete_stmt
  ;

distinct_or_all:
    ALL
    {
      malloc_terminal_node($$, result->malloc_pool_, T_ALL);
    }
  | DISTINCT
    {
      malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
    }
  ;


/*****************************************************************************
 *
 *	delete grammar
 *
 *****************************************************************************/

delete_stmt:
    DELETE FROM relation_factor opt_where opt_when
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_DELETE, 3, $3, $4, $5);
    }
  ;


/*****************************************************************************
 *
 *	update grammar
 *
 *****************************************************************************/

update_stmt:
    UPDATE opt_hint relation_factor SET update_asgn_list opt_where opt_when
    {
      ParseNode* assign_list = NULL;
      merge_nodes(assign_list, result->malloc_pool_, T_ASSIGN_LIST, $5);
      malloc_non_terminal_node($$, result->malloc_pool_, T_UPDATE, 5, $3, assign_list, $6, $7, $2);
    }
  ;


update_asgn_list:
    update_asgn_factor
    {
      $$ = $1;
    }
  | update_asgn_list ',' update_asgn_factor
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

update_asgn_factor:
    column_name COMP_EQ expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_ASSIGN_ITEM, 2, $1, $3);
    }
  ;


<<<<<<< HEAD
// add longfei [create index] [secondaryindex reconstruct] 20150915:b 
/*****************************************************************************
 *
 *	create secondary index grammar 
 *
 *****************************************************************************/
create_index_stmt:
	CREATE INDEX opt_if_not_exists relation_factor ON relation_factor opt_index_columns
	opt_storing opt_index_option_list
	{
		ParseNode *index_options = NULL;
		merge_nodes(index_options, result->malloc_pool_, T_INDEX_OPTION_LIST, $9);
		malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_INDEX, 6,
								  $3, /* if not exists */
								  $6, /* table name */
								  $4, /* index name */ 
		                          $7, /* index columns */
		                          $8, /* storing list */
		                          index_options /* option list */
		                        );
	};
		  	
opt_index_columns:
    '(' column_list ')'
    {
      merge_nodes($$, result->malloc_pool_, T_COLUMN_LIST, $2);
    }
  ;
  
opt_storing:
  	STORING opt_storing_columns 
  	{
  		$$=$2;
  	}
  |
  	/*empty*/
  	{
  		$$=NULL;
  	}
  ;
  
opt_storing_columns:
   '(' column_list ')'
    {
      merge_nodes($$, result->malloc_pool_, T_COLUMN_LIST, $2);
    }
  ;
  
opt_index_option_list:
    index_option
    {
      $$ = $1;
    }
  | opt_index_option_list ',' index_option
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | /*EMPTY*/
    {
      $$ = NULL;
    }
  ;

index_option:
	table_option
	{
		$$ = $1;
	}
  ;
// add e




/*****************************************************************************
 * 
 *
 *           	declare cursor grammar
 *
 *****************************************************************************/
 cursor_declare_stmt: 
     DECLARE cursor_name CURSOR FOR select_stmt
     {					 
       malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_DECLARE, 2,
                                        $2,		/*cursor name*/
                                        $5		/*result from select_stmt*/
					             ); 
	 }
	;
	 
 
cursor_name:
    column_label
    { $$ = $1; } 
    ;

  

/*****************************************************************************
 *  
 *                     open grammar
 *
 *****************************************************************************/

cursor_open_stmt: 
   OPEN cursor_name
           {
            malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_OPEN, 1,
            $2     /* cursor_name*/
                                    );
            }
            ;

 

/*****************************************************************************
 *
 *	                    fetch grammar
 *
 *****************************************************************************/
 cursor_fetch_stmt:	
    FETCH cursor_name
           {
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH, 1,
                   $2     /* cursor_name*/
                                       );
            }
   | FETCH cursor_name NEXT
           {
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH, 1,
                   $2     /* cursor_name*/
                                       );
            }
            
            ;
/*****************************************************************************
 *
 *	                    fetch into grammar
 *
 *****************************************************************************/
 cursor_fetch_into_stmt:	
    FETCH cursor_name INTO argument_list
           {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH, 1, $2);
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $4);
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_INTO, 2, fetch, args);
            }
           ;
/*****************************************************************************
 *
 *	                    fetch next into grammar
 *
 *****************************************************************************/
cursor_fetch_next_into_stmt:	
    FETCH cursor_name NEXT INTO argument_list
           {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_NEXT, 1, $2);
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $5);
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_NEXT_INTO, 2, fetch, args);
            }
           ;          
/*****************************************************************************
 *
 *	                    fetch first into grammar
 *
 *****************************************************************************/
cursor_fetch_first_into_stmt:	
    FETCH cursor_name FIRST INTO argument_list
           {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_FIRST, 1, $2);
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $5);
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_FIRST_INTO, 2, fetch, args);
            }
           ;

/*****************************************************************************
 *
 *	                    fetch prior into grammar
 *
 *****************************************************************************/
 cursor_fetch_prior_into_stmt:	
    FETCH cursor_name PRIOR INTO argument_list
           {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_PRIOR, 1, $2);
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $5);
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_PRIOR_INTO, 2, fetch, args);
            }
           ;

/*****************************************************************************
 *
 *	                    fetch last into grammar
 *
 *****************************************************************************/
 cursor_fetch_last_into_stmt:	
    FETCH cursor_name LAST INTO argument_list
           {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_LAST, 1, $2);
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $5);
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_LAST_INTO, 2, fetch, args);
            }
           ;

/*****************************************************************************
 *
 *	                    fetch absolute into grammar
 *
 *****************************************************************************/
 cursor_fetch_absolute_into_stmt:	
    FETCH cursor_name ABSOLUTE INTNUM INTO argument_list
           {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_ABSOLUTE, 2, $2, $4);
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $6);
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_ABS_INTO, 2, fetch, args);
            }
           ;


/*****************************************************************************
 *
 *	                    fetch relative into grammar
 *
 *****************************************************************************/
 cursor_fetch_relative_into_stmt:	
    FETCH cursor_name next_or_prior RELATIVE INTNUM INTO argument_list
           {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_RELATIVE, 3, $2, $3, $5);
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $7);
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_RELATIVE_INTO, 2, fetch, args);
            }
           ;

/*****************************************************************************
 *
 *	                    fetch prior grammar
 *
 *****************************************************************************/
 fetch_prior_stmt:	
    FETCH cursor_name PRIOR
           {
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_PRIOR, 1,
                   $2     /* cursor_name*/
                    
                    
                    
                                       );
            }
            
            
            ;


                       
/*****************************************************************************
 *
 *	                    fetch first grammar
 *
 *****************************************************************************/
 fetch_first_stmt:	
    FETCH cursor_name FIRST
           {
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_FIRST, 1,
                   $2     /* cursor_name*/
                                       );
            }
            
            
            ;




/*****************************************************************************
 *
 *	                    fetch last grammar
 *
 *****************************************************************************/
 fetch_last_stmt:	
    FETCH cursor_name LAST
           {
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_LAST, 1,
                   $2     /* cursor_name*/
                                       );
            }
            
            
            ;
  

/*****************************************************************************
 *
 *	                    fetch relative grammar
 *
 *****************************************************************************/
 fetch_relative_stmt:	
    FETCH cursor_name next_or_prior RELATIVE INTNUM
           {
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_RELATIVE, 3,
                   $2,     /* cursor_name*/
                   $3,                  
                   $5                  
                                       );
            }
            
            
            ;
  
  next_or_prior:
    NEXT
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 1;
    }
  | PRIOR
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 0;
    }
  ;
  
  
 
/*****************************************************************************
 *
 *	                    fetch absolute grammar
 *
 *****************************************************************************/
 fetch_absolute_stmt:	
    FETCH cursor_name ABSOLUTE INTNUM
           {
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_ABSOLUTE, 2,
                   $2,     /* cursor_name*/
                   $4
                                       );
            }
            
            
            ; 
  
/*****************************************************************************
 *
 *	                    fetch fromto grammar
 *
 *****************************************************************************/
 fetch_fromto_stmt:	
    FETCH cursor_name FROM INTNUM TO INTNUM
           {
              malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_FETCH_FROMTO, 3,
                   $2,     /* cursor_name*/
                   $4,
                   $6
                                       );
            }
            
            
            ; 
  
  

/*****************************************************************************
 *
 *                    close grammar
 *
 *****************************************************************************/
cursor_close_stmt: 
    CLOSE cursor_name
           {
             malloc_non_terminal_node($$, result->malloc_pool_, T_CURSOR_CLOSE, 1,
                   $2     /* cursor_name*/
                                      );
             }
             ;






=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
 *
 *	create grammar
 *
 *****************************************************************************/

create_table_stmt:
    CREATE TABLE opt_if_not_exists relation_factor '(' table_element_list ')'
    opt_table_option_list
    {
      ParseNode *table_elements = NULL;
      ParseNode *table_options = NULL;
      merge_nodes(table_elements, result->malloc_pool_, T_TABLE_ELEMENT_LIST, $6);
      merge_nodes(table_options, result->malloc_pool_, T_TABLE_OPTION_LIST, $8);
      malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_TABLE, 4,
              $3,                   /* if not exists */
              $4,                   /* table name */
              table_elements,       /* columns or primary key */
              table_options         /* table option(s) */
              );
    }
  ;

opt_if_not_exists:
    IF NOT EXISTS
    { malloc_terminal_node($$, result->malloc_pool_, T_IF_NOT_EXISTS); }
  | /* EMPTY */
    { $$ = NULL; }
  ;

table_element_list:
    table_element
    {
      $$ = $1;
    }
  | table_element_list ',' table_element
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

table_element:
    column_definition
    {
      $$ = $1;
    }
  | PRIMARY KEY '(' column_list ')'
    {
      ParseNode* col_list= NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, $4);
      malloc_non_terminal_node($$, result->malloc_pool_, T_PRIMARY_KEY, 1, col_list);
    }
  ;

column_definition:
    column_name data_type opt_column_attribute_list
    {
      ParseNode *attributes = NULL;
      merge_nodes(attributes, result->malloc_pool_, T_COLUMN_ATTRIBUTES, $3);
      malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DEFINITION, 3, $1, $2, attributes);
    }
  ;

data_type:
    TINYINT
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_INTEGER ); }
  | SMALLINT
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_INTEGER); }
  | MEDIUMINT
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_INTEGER); }
  | INTEGER
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_INTEGER); }
  | BIGINT
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_INTEGER); }
  | DECIMAL opt_decimal
    {
      if ($2 == NULL)
        malloc_terminal_node($$, result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes($$, result->malloc_pool_, T_TYPE_DECIMAL, $2);
<<<<<<< HEAD
      /* modify xsl ECNU_DECIMAL 2017_2
      yyerror(&@1, result, "DECIMAL type is not supported");
      YYABORT;
      */
      //modify e
=======
      yyerror(&@1, result, "DECIMAL type is not supported");
      YYABORT;
>>>>>>> refs/remotes/origin/master
    }
  | NUMERIC opt_decimal
    {
      if ($2 == NULL)
        malloc_terminal_node($$, result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes($$, result->malloc_pool_, T_TYPE_DECIMAL, $2);
      yyerror(&@1, result, "NUMERIC type is not supported");
      YYABORT;
    }
  | BOOLEAN
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_BOOLEAN ); }
  | FLOAT opt_float
    { malloc_non_terminal_node($$, result->malloc_pool_, T_TYPE_FLOAT, 1, $2); }
  | REAL
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_DOUBLE); }
  | DOUBLE opt_precision
    {
      (void)($2) ; /* make bison mute */
      malloc_terminal_node($$, result->malloc_pool_, T_TYPE_DOUBLE);
    }
  | TIMESTAMP opt_time_precision
    {
      if ($2 == NULL)
        malloc_terminal_node($$, result->malloc_pool_, T_TYPE_TIMESTAMP);
      else
        malloc_non_terminal_node($$, result->malloc_pool_, T_TYPE_TIMESTAMP, 1, $2);
    }
  | DATETIME
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_TIMESTAMP); }
  | CHARACTER opt_char_length
    {
      if ($2 == NULL)
        malloc_terminal_node($$, result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node($$, result->malloc_pool_, T_TYPE_CHARACTER, 1, $2);
    }
  | BINARY opt_char_length
    {
      if ($2 == NULL)
        malloc_terminal_node($$, result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node($$, result->malloc_pool_, T_TYPE_CHARACTER, 1, $2);
    }
  | VARCHAR opt_char_length
    {
      if ($2 == NULL)
        malloc_terminal_node($$, result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node($$, result->malloc_pool_, T_TYPE_VARCHAR, 1, $2);
    }
  | VARBINARY opt_char_length
    {
      if ($2 == NULL)
        malloc_terminal_node($$, result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node($$, result->malloc_pool_, T_TYPE_VARCHAR, 1, $2);
    }
  | CREATETIME
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_CREATETIME); }
  | MODIFYTIME
    { malloc_terminal_node($$, result->malloc_pool_, T_TYPE_MODIFYTIME); }
  | DATE
    {
      malloc_terminal_node($$, result->malloc_pool_, T_TYPE_DATE);
      yyerror(&@1, result, "DATE type is not supported");
      YYABORT;
    }
  | TIME opt_time_precision
    {
      if ($2 == NULL)
        malloc_terminal_node($$, result->malloc_pool_, T_TYPE_TIME);
      else
        malloc_non_terminal_node($$, result->malloc_pool_, T_TYPE_TIME, 1, $2);
      yyerror(&@1, result, "TIME type is not supported");
      YYABORT;
    }
  ;

opt_decimal:
    '(' INTNUM ',' INTNUM ')'
    { malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $2, $4); }
  | '(' INTNUM ')'
    { $$ = $2; }
  | /*EMPTY*/
    { $$ = NULL; }
  ;

opt_float:
    '(' INTNUM ')'    { $$ = $2; }
  | /*EMPTY*/         { $$ = NULL; }
  ;

opt_precision:
    PRECISION    { $$ = NULL; }
  | /*EMPTY*/    { $$ = NULL; }
  ;

opt_time_precision:
    '(' INTNUM ')'    { $$ = $2; }
  | /*EMPTY*/         { $$ = NULL; }
  ;

opt_char_length:
    '(' INTNUM ')'    { $$ = $2; }
  | /*EMPTY*/         { $$ = NULL; }
  ;

opt_column_attribute_list:
    opt_column_attribute_list column_attribute
    { malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2); }
  | /*EMPTY*/
    { $$ = NULL; }
  ;

column_attribute:
    NOT NULLX
    {
      (void)($2) ; /* make bison mute */
      malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NOT_NULL);
    }
  | NULLX
    {
      (void)($1) ; /* make bison mute */
      malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
    }
  | DEFAULT expr_const
    { malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_DEFAULT, 1, $2); }
  | AUTO_INCREMENT
    { malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_AUTO_INCREMENT); }
  | PRIMARY KEY
    { malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_PRIMARY_KEY); }
  ;

opt_table_option_list:
    table_option
    {
      $$ = $1;
    }
  | opt_table_option_list ',' table_option
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | /*EMPTY*/
    {
      $$ = NULL;
    }
  ;

table_option:
    JOIN_INFO opt_equal_mark STRING
    {
      (void)($2) ; /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_JOIN_INFO, 1, $3);
    }
  | EXPIRE_INFO opt_equal_mark STRING
    {
      (void)($2) ; /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXPIRE_INFO, 1, $3);
    }
  | TABLET_MAX_SIZE opt_equal_mark INTNUM
    {
      (void)($2) ; /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_TABLET_MAX_SIZE, 1, $3);
    }
  | TABLET_BLOCK_SIZE opt_equal_mark INTNUM
    {
      (void)($2) ; /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_TABLET_BLOCK_SIZE, 1, $3);
    }
  | TABLE_ID opt_equal_mark INTNUM
    {
      (void)($2) ; /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_TABLET_ID, 1, $3);
    }
  | REPLICA_NUM opt_equal_mark INTNUM
    {
      (void)($2) ; /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_REPLICA_NUM, 1, $3);
    }
  | COMPRESS_METHOD opt_equal_mark STRING
    {
      (void)($2) ; /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_COMPRESS_METHOD, 1, $3);
    }
  | USE_BLOOM_FILTER opt_equal_mark BOOL
    {
      (void)($2) ; /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_USE_BLOOM_FILTER, 1, $3);
    }
  | CONSISTENT_MODE opt_equal_mark STATIC
    {
      (void)($2) ; /* make bison mute */
      malloc_terminal_node($$, result->malloc_pool_, T_CONSISTENT_MODE);
      $$->value_ = 1;
    }
  | COMMENT opt_equal_mark STRING
    {
      (void)($2); /*  make bison mute*/
      malloc_non_terminal_node($$, result->malloc_pool_, T_COMMENT, 1, $3);
    }
  ;

opt_equal_mark:
    COMP_EQ     { $$ = NULL; }
  | /*EMPTY*/   { $$ = NULL; }
  ;


<<<<<<< HEAD

/*****************************************************************************
 *
 *  gather statistic grammar //add by weixing :[statistics report]20161212
 *
 *****************************************************************************/
gather_statistics_stmt:
  GATHER STATISTICS relation_factor opt_gather_columns
  {
    malloc_non_terminal_node($$, result->malloc_pool_, T_GATHER_STATISTICS,2,
                              $3, /*table name*/
                              $4  /*column name*/
                            );
  };

  opt_gather_columns:
  '(' column_list ')'
  {
    merge_nodes($$, result->malloc_pool_, T_COLUMN_LIST, $2);
  };

=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
 *
 *	drop table grammar
 *
 *****************************************************************************/

drop_table_stmt:
    DROP TABLE opt_if_exists table_list
    {
      ParseNode *tables = NULL;
      merge_nodes(tables, result->malloc_pool_, T_TABLE_LIST, $4);
      malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_TABLE, 2, $3, tables);
    }
  ;

opt_if_exists:
    /* EMPTY */
    { $$ = NULL; }
  | IF EXISTS
    { malloc_terminal_node($$, result->malloc_pool_, T_IF_EXISTS); }
  ;

table_list:
    table_factor
    {
      $$ = $1;
    }
  | table_list ',' table_factor
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;
<<<<<<< HEAD
/*add hxlong[truncate table] 20170403:b*/
/*****************************************************************************
 *
 *	truncate table grammar
 *
 *****************************************************************************/
truncate_table_stmt:
        TRUNCATE TABLE opt_if_exists table_list opt_comment
        {
          ParseNode *tables = NULL;
          merge_nodes(tables, result->malloc_pool_, T_TABLE_LIST, $4);
          malloc_non_terminal_node($$, result->malloc_pool_, T_TRUNCATE_TABLE, 3, $3, tables, $5);
        }
      ;

 /*add:e*/
/*****************************************************************************
 *
 *	drop index grammar
 *	@author:longfei [drop index]
 *
 *****************************************************************************/

drop_index_stmt:
    DROP INDEX opt_if_exists index_list ON table_name
    {
      ParseNode *indexs = NULL;
      merge_nodes(indexs, result->malloc_pool_, T_INDEX_LIST, $4);
      malloc_non_terminal_node($$, result->malloc_pool_, T_DROP_INDEX, 3, $3, indexs, $6);
    }
  ;

index_list:
    /* EMPTY */
    { $$ = NULL; } 
  |
    relation_factor
    {
      $$ = $1;
    }
  | index_list ',' relation_factor
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;
  
table_name:
	relation_factor
	{
	  $$ = $1;
	}
=======

>>>>>>> refs/remotes/origin/master

/*****************************************************************************
 *
 *	insert grammar
 *
 *****************************************************************************/
insert_stmt:
    replace_or_insert INTO relation_factor opt_insert_columns VALUES insert_vals_list
    opt_when
    {
      ParseNode* val_list = NULL;
      merge_nodes(val_list, result->malloc_pool_, T_VALUE_LIST, $6);
      malloc_non_terminal_node($$, result->malloc_pool_, T_INSERT, 6,
                              $3,           /* target relation */
                              $4,           /* column list */
                              val_list,     /* value list */
                              NULL,         /* value from sub-query */
                              $1,           /* is replacement */
                              $7            /* when expression */
                              );
    }
  | replace_or_insert INTO relation_factor select_stmt
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_INSERT, 6,
                              $3,           /* target relation */
                              NULL,         /* column list */
                              NULL,         /* value list */
                              $4,           /* value from sub-query */
                              $1,           /* is replacement */
                              NULL          /* when expression */
                              );
    }
  | replace_or_insert INTO relation_factor '(' column_list ')' select_stmt
    {
      /* if opt_when is really needed, use select_with_parens instead */
      ParseNode* col_list = NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, $5);
      malloc_non_terminal_node($$, result->malloc_pool_, T_INSERT, 6,
                              $3,           /* target relation */
                              col_list,     /* column list */
                              NULL,         /* value list */
                              $7,           /* value from sub-query */
                              $1,           /* is replacement */
                              NULL          /* when expression */
                              );
    }
  ;

opt_when:
    /* EMPTY */
    { $$ = NULL; }
  | WHEN expr
    {
      $$ = $2;
    }
  ;

replace_or_insert:
    REPLACE
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 1;
    }
  | INSERT
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 0;
    }
  ;

opt_insert_columns:
    '(' column_list ')'
    {
      merge_nodes($$, result->malloc_pool_, T_COLUMN_LIST, $2);
    }
  | /* EMPTY */
    { $$ = NULL; }
  ;

column_list:
    column_name { $$ = $1; }
  | column_list ',' column_name
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

insert_vals_list:
    '(' insert_vals ')'
    {
      merge_nodes($$, result->malloc_pool_, T_VALUE_VECTOR, $2);
    }
  | insert_vals_list ',' '(' insert_vals ')' {
    merge_nodes($4, result->malloc_pool_, T_VALUE_VECTOR, $4);
    malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $4);
  }

insert_vals:
    expr { $$ = $1; }
  | insert_vals ',' expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;


<<<<<<< HEAD


=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
 *
 *	select grammar
 *
 *****************************************************************************/

select_stmt:
    select_no_parens opt_when    %prec UMINUS
    {
      $$ = $1;
      $$->children_[14] = $2;
      if ($$->children_[12] == NULL && $2 != NULL)
      {
        malloc_terminal_node($$->children_[12], result->malloc_pool_, T_BOOL);
        $$->children_[12]->value_ = 1;
      }
    }
  | select_with_parens    %prec UMINUS
    { $$ = $1; }
  ;

select_with_parens:
    '(' select_no_parens ')'      { $$ = $2; }
  | '(' select_with_parens ')'    { $$ = $2; }
  ;

select_no_parens:
    no_table_select
    {
      $$= $1;
    }
  | simple_select opt_for_update
    {
      $$ = $1;
      $$->children_[12] = $2;
    }
  | select_clause order_by opt_for_update
    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)$1;
      if (select->children_[10])
        destroy_tree(select->children_[10]);
      select->children_[10] = $2;
      select->children_[12] = $3;
      $$ = select;
    }
  | select_clause opt_order_by select_limit opt_for_update
    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)$1;
      if ($2)
      {
        if (select->children_[10])
          destroy_tree(select->children_[10]);
        select->children_[10] = $2;
      }

      /* set limit value */
      if (select->children_[11])
        destroy_tree(select->children_[11]);
      select->children_[11] = $3;
      select->children_[12] = $4;
      $$ = select;
    }
  ;

no_table_select:
    SELECT opt_hint opt_distinct select_expr_list opt_select_limit
    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, $4);
      malloc_non_terminal_node($$, result->malloc_pool_, T_SELECT, 15,
                              $3,             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              $5,             /* 12. limit */
                              NULL,           /* 13. for update */
                              $2,             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
  | SELECT opt_hint opt_distinct select_expr_list
    FROM DUAL opt_where opt_select_limit
    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, $4);
      malloc_non_terminal_node($$, result->malloc_pool_, T_SELECT, 15,
                              $3,             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              $7,             /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              $8,             /* 12. limit */
                              NULL,           /* 13. for update */
                              $2,             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
  ;

select_clause:
    simple_select	              { $$ = $1; }
  | select_with_parens	        { $$ = $1; }
  ;

<<<<<<< HEAD

/*Add by zz 2014-11-20 to apply select * into var from ....*/
/*===========================================================
 *	Select Into sentence
 *===========================================================*/
select_into_clause	:	SELECT opt_hint select_expr_list INTO argument_list FROM from_list opt_where opt_for_update
						{
						  ParseNode* project_list = NULL;
						  ParseNode* from_list = NULL;
						  ParseNode* select = NULL;
						  ParseNode* args = NULL;
              merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, $3);
              merge_nodes(from_list, result->malloc_pool_, T_FROM_LIST, $7);
						  malloc_non_terminal_node(select, result->malloc_pool_, T_SELECT, 16,
                              NULL,           /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              $8,             /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,             /* 12. limit */
                              $9,           /* 13. for update */
                              $2,             /* 14 hints */
                              NULL            /* 15 when clause */
                              ,NULL
                              );
              merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $5);
						  malloc_non_terminal_node($$,result->malloc_pool_, T_SELECT_INTO, 2, args, select);
						}
					;

=======
>>>>>>> refs/remotes/origin/master
simple_select:
    SELECT opt_hint opt_distinct select_expr_list
    FROM from_list
    opt_where opt_groupby opt_having
    {
      ParseNode* project_list = NULL;
      ParseNode* from_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, $4);
      merge_nodes(from_list, result->malloc_pool_, T_FROM_LIST, $6);
      malloc_non_terminal_node($$, result->malloc_pool_, T_SELECT, 15,
                              $3,             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              $7,             /* 4. where */
                              $8,             /* 5. group by */
                              $9,             /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              $2,             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
  | select_clause UNION opt_distinct select_clause
    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_UNION);
	    malloc_non_terminal_node($$, result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              $3,             /* 8. all specified? */
                              $1,             /* 9. former select stmt */
                              $4,             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
  | select_clause INTERSECT opt_distinct select_clause
    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_INTERSECT);
      malloc_non_terminal_node($$, result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              $3,             /* 8. all specified? */
                              $1,             /* 9. former select stmt */
                              $4,             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
  | select_clause EXCEPT opt_distinct select_clause
    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_EXCEPT);
	    malloc_non_terminal_node($$, result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              $3,             /* 8. all specified? */
                              $1,             /* 9. former select stmt */
                              $4,             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
  ;

opt_where:
    /* EMPTY */
    {$$ = NULL;}
  | WHERE expr
    {
      $$ = $2;
    }
  | WHERE HINT_VALUE expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_WHERE_CLAUSE, 2, $3, $2);
    }
  ;

select_limit:
  	LIMIT limit_expr OFFSET limit_expr
    {
      if ($2->type_ == T_QUESTIONMARK && $4->type_ == T_QUESTIONMARK)
      {
        $4->value_++;
      }
      malloc_non_terminal_node($$, result->malloc_pool_, T_LIMIT_CLAUSE, 2, $2, $4);
    }
  | OFFSET limit_expr LIMIT limit_expr
    {
      if ($2->type_ == T_QUESTIONMARK && $4->type_ == T_QUESTIONMARK)
      {
        $4->value_++;
      }
      malloc_non_terminal_node($$, result->malloc_pool_, T_LIMIT_CLAUSE, 2, $4, $2);
    }
  | LIMIT limit_expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LIMIT_CLAUSE, 2, $2, NULL);
    }
  | OFFSET limit_expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LIMIT_CLAUSE, 2, NULL, $2);
    }
  | LIMIT limit_expr ',' limit_expr
    {
      if ($2->type_ == T_QUESTIONMARK && $4->type_ == T_QUESTIONMARK)
      {
        $4->value_++;
      }
    	malloc_non_terminal_node($$, result->malloc_pool_, T_LIMIT_CLAUSE, 2, $4, $2);
    }
  ;

opt_hint:
    /* EMPTY */
    {
      $$ = NULL;
    }
  | HINT_BEGIN opt_hint_list HINT_END
    {
      if ($2)
      {
        merge_nodes($$, result->malloc_pool_, T_HINT_OPTION_LIST, $2);
      }
      else
      {
        $$ = NULL;
      }
    }
  ;

opt_hint_list:
    hint_options
    {
      $$ = $1;
    }
  | opt_hint_list ',' hint_options
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  | /*EMPTY*/
    {
      $$ = NULL;
    }
  ;

hint_options:
    hint_option
    {
      $$ = $1;
    }
  | hint_options hint_option
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
    }
  ;

hint_option:
    READ_STATIC
    {
      malloc_terminal_node($$, result->malloc_pool_, T_READ_STATIC);
    }
  | HOTSPOT
    {
      malloc_terminal_node($$, result->malloc_pool_, T_HOTSPOT);
    }
<<<<<<< HEAD
  | SEMI_JOIN '(' relation_factor ',' relation_factor ',' relation_factor '.' relation_factor ',' relation_factor '.' relation_factor ')'
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SEMI_JOIN, 6, $3, $5, $7, $9, $11, $13);
    }
=======
>>>>>>> refs/remotes/origin/master
  | READ_CONSISTENCY '(' consistency_level ')'
    {
      malloc_terminal_node($$, result->malloc_pool_, T_READ_CONSISTENCY);
      $$->value_ = $3;
<<<<<<< HEAD
    }  
    // add longfei 20151106
  | INDEX '(' relation_factor relation_factor ')'
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_USE_INDEX, 2, $3, $4);
    }
    // add e
    // add by zcd 20150601:b
  | NAME '(' NAME ')'
    {
	  (void)($1);
	  (void)($3);
      malloc_terminal_node($$, result->malloc_pool_, T_UNKOWN_HINT);
    }
    // add by zcd 20150601:e
=======
    }
>>>>>>> refs/remotes/origin/master
  | '(' opt_comma_list ')'
    {
      $$ = $2;
    }
<<<<<<< HEAD
    /* add maoxx [bloomfilter_join] 20160406 */
  | JOIN '(' join_op_type_list ')'
    {
      merge_nodes($$, result->malloc_pool_, T_JOIN_OP_TYPE_LIST, $3);
    }
    /* add e */
  | NO_GROUP //add by wdh 20160716
    {
      malloc_terminal_node($$, result->malloc_pool_, T_NO_GROUP);
    }
  | LONG_TRANS //add by qx 20170317
    {
      malloc_terminal_node($$, result->malloc_pool_, T_LONG_TRANS);
    }
  | NO_QUERY_OPT //add by qx [query optimizer] 20170730
    {
      malloc_terminal_node($$, result->malloc_pool_, T_NO_QUERY_OPT);
    }
=======
>>>>>>> refs/remotes/origin/master
  ;

opt_comma_list:
    opt_comma_list ','
    {
      $$ = $1;
    }
  | /*EMPTY*/
    {
      $$ = NULL;
    }
  ;
<<<<<<< HEAD

/* add maoxx [bloomfilter_join] 20160406 */
join_op_type_list:
    join_op_type
    {
      $$ = $1;
    }
  | join_op_type_list ',' join_op_type
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
   ;

join_op_type:
    BLOOMFILTER_JOIN
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BLOOMFILTER_JOIN);
    }
  | MERGE_JOIN
    {
      malloc_terminal_node($$, result->malloc_pool_, T_MERGE_JOIN);
    }
	//add wanglei [semi join] 20170417:b
    | SI
    {
      malloc_terminal_node($$, result->malloc_pool_, T_SEMI_JOIN);
    }
	| SIB
    {
      malloc_terminal_node($$, result->malloc_pool_, T_SEMI_BTW_JOIN);
    }
	| SM
    {
      malloc_terminal_node($$, result->malloc_pool_, T_SEMI_MULTI_JOIN);
    }
	//add wanglei [semi join] 20170417:e
    //add maoxx [hash join single] 20170110
  | HASH_JOIN
    {
      malloc_terminal_node($$, result->malloc_pool_, T_HASH_JOIN_SINGLE);
    }
   ;
/* add e */
=======
>>>>>>> refs/remotes/origin/master
 
consistency_level:
  WEAK
  {
    $$ = 3;
  }
| STRONG
  {
    $$ = 4;
  }
| STATIC
  {
    $$ = 1;
  }
| FROZEN
  {
    $$ = 2;
  }
  ;
limit_expr:
    INTNUM
    { $$ = $1; }
  | QUESTIONMARK
    { $$ = $1; }
  ;

opt_select_limit:
    /* EMPTY */
    { $$ = NULL; }
  | select_limit
    { $$ = $1; }
  ;

opt_for_update:
    /* EMPTY */
    { $$ = NULL; }
  | FOR UPDATE
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 1;
    }
  ;

parameterized_trim:
    expr FROM expr
    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $1, $3);
    }
  | BOTH expr FROM expr
    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $2, $4);
    }
  | LEADING expr FROM expr
    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $2, $4);
    }
  | TRAILING expr FROM expr
    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, $2, $4);
    }
  | BOTH FROM expr
    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, $3);
    }
  | LEADING FROM expr
    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, $3);
    }
  | TRAILING FROM expr
    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, $3);
    }
  ;

opt_groupby:
    /* EMPTY */
    { $$ = NULL; }
  | GROUP BY expr_list
    {
      merge_nodes($$, result->malloc_pool_, T_EXPR_LIST, $3);
    }
  ;

opt_order_by:
  	order_by	              { $$ = $1;}
  | /*EMPTY*/             { $$ = NULL; }
  ;

order_by:
  	ORDER BY sort_list
    {
      merge_nodes($$, result->malloc_pool_, T_SORT_LIST, $3);
    }
  ;

sort_list:
  	sort_key
    { $$ = $1; }
  | sort_list ',' sort_key
    { malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
  ;

sort_key:
    expr opt_asc_desc
    {
    	malloc_non_terminal_node($$, result->malloc_pool_, T_SORT_KEY, 2, $1, $2);
    }
  ;

opt_asc_desc:
    /* EMPTY */
    { malloc_terminal_node($$, result->malloc_pool_, T_SORT_ASC); }
  | ASC
    { malloc_terminal_node($$, result->malloc_pool_, T_SORT_ASC); }
  | DESC
    { malloc_terminal_node($$, result->malloc_pool_, T_SORT_DESC); }
  ;

opt_having:
    /* EMPTY */
    { $$ = 0; }
  | HAVING expr
    {
      $$ = $2;
    }
  ;

opt_distinct:
    /* EMPTY */
    {
      $$ = NULL;
    }
  | ALL
    {
      malloc_terminal_node($$, result->malloc_pool_, T_ALL);
    }
  | DISTINCT
    {
      malloc_terminal_node($$, result->malloc_pool_, T_DISTINCT);
    }
  ;

projection:
    expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, $1);
      dup_expr_string($$->str_value_, result, @1.first_column, @1.last_column);
    }
  | expr column_label
    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, $2);
      malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string($$->str_value_, result, @1.first_column, @1.last_column);
      dup_expr_string(alias_node->str_value_, result, @2.first_column, @2.last_column);
    }
  | expr AS column_label
    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, $1, $3);
      malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string($$->str_value_, result, @1.first_column, @1.last_column);
      dup_expr_string(alias_node->str_value_, result, @3.first_column, @3.last_column);
    }
  | '*'
    {
      ParseNode* star_node = NULL;
      malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node($$, result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
      dup_expr_string($$->str_value_, result, @1.first_column, @1.last_column);
    }
  ;

select_expr_list:
    projection
    {
      $$ = $1;
    }
  | select_expr_list ',' projection
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

from_list:
  	table_factor
    { $$ = $1; }
  | from_list ',' table_factor
    { malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3); }
  ;

table_factor:
    relation_factor
    {
      $$ = $1;
    }
  | relation_factor AS relation_name
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $3);
    }
  | relation_factor relation_name
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $2);
    }
  | select_with_parens AS relation_name
    {
    	malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $3);
    }
  | select_with_parens relation_name
    {
    	malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $1, $2);
    }
  | joined_table
    {
    	$$ = $1;
    }
  | '(' joined_table ')' AS relation_name
    {
    	malloc_non_terminal_node($$, result->malloc_pool_, T_ALIAS, 2, $2, $5);
    	yyerror(&@1, result, "qualied joined table can not be aliased!");
      YYABORT;
    }
  ;

relation_factor:
    relation_name
    { $$ = $1; }
  ;

joined_table:
  /* we do not support cross join and natural join
    * using clause is not supported either
    */
    '(' joined_table ')'
    {
    	$$ = $2;
    }
  | table_factor join_type JOIN table_factor ON expr
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 4, $2, $1, $4, $6);
    }
  | table_factor JOIN table_factor ON expr
    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_JOIN_INNER);
    	malloc_non_terminal_node($$, result->malloc_pool_, T_JOINED_TABLE, 4, node, $1, $3, $5);
    }
  ;

join_type:
    FULL join_outer
    {
      /* make bison mute */
      (void)($2);
      malloc_terminal_node($$, result->malloc_pool_, T_JOIN_FULL);
    }
  | LEFT join_outer
    {
      /* make bison mute */
      (void)($2);
      malloc_terminal_node($$, result->malloc_pool_, T_JOIN_LEFT);
    }
  | RIGHT join_outer
    {
      /* make bison mute */
      (void)($2);
      malloc_terminal_node($$, result->malloc_pool_, T_JOIN_RIGHT);
    }
  | INNER
    {
      malloc_terminal_node($$, result->malloc_pool_, T_JOIN_INNER);
    }
<<<<<<< HEAD
  //add by wanglei [semi join] 20170417:b
  | SEMI
    {
      malloc_terminal_node($$, result->malloc_pool_, T_JOIN_SEMI);
    }
  | LEFT SEMI
    {
      malloc_terminal_node($$, result->malloc_pool_, T_JOIN_SEMI_LEFT);
    }
  | RIGHT SEMI
    {
      malloc_terminal_node($$, result->malloc_pool_, T_JOIN_SEMI_RIGHT);
    }
	 //add by wanglei [semi join] 20170417:b
=======
>>>>>>> refs/remotes/origin/master
  ;

join_outer:
    OUTER                    { $$ = NULL; }
  | /* EMPTY */               { $$ = NULL; }
  ;


/*****************************************************************************
 *
 *	explain grammar
 *
 *****************************************************************************/
explain_stmt:
    EXPLAIN opt_verbose explainable_stmt
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXPLAIN, 1, $3);
      $$->value_ = ($2 ? 1 : 0); /* positive: verbose */
    }
  ;

explainable_stmt:
    select_stmt         { $$ = $1; }
  | delete_stmt         { $$ = $1; }
  | insert_stmt         { $$ = $1; }
  | update_stmt         { $$ = $1; }
  ;

opt_verbose:
    VERBOSE             { $$ = (ParseNode*)1; }
  | /*EMPTY*/           { $$ = NULL; }
  ;


/*****************************************************************************
 *
 *	show grammar
<<<<<<< HEAD
 *  add show index : longfei [show index]
=======
>>>>>>> refs/remotes/origin/master
 *
 *****************************************************************************/
show_stmt:
    SHOW TABLES opt_show_condition
    { malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TABLES, 1, $3); }
<<<<<<< HEAD
  | SHOW INDEX ON relation_factor opt_show_condition
    {  malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_INDEX, 2, $4, $5); }
=======
>>>>>>> refs/remotes/origin/master
  | SHOW COLUMNS FROM relation_factor opt_show_condition
    { malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_COLUMNS, 2, $4, $5); }
  | SHOW COLUMNS IN relation_factor opt_show_condition
    { malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_COLUMNS, 2, $4, $5); }
  | SHOW TABLE STATUS opt_show_condition
    { malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_TABLE_STATUS, 1, $4); }
  | SHOW SERVER STATUS opt_show_condition
    { malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_SERVER_STATUS, 1, $4); }
  | SHOW opt_scope VARIABLES opt_show_condition
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_VARIABLES, 1, $4);
      $$->value_ = $2;
    }
  | SHOW SCHEMA
    { malloc_terminal_node($$, result->malloc_pool_, T_SHOW_SCHEMA); }
  | SHOW CREATE TABLE relation_factor
<<<<<<< HEAD
    { malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, $4); }					
=======
    { malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, $4); }
>>>>>>> refs/remotes/origin/master
  | DESCRIBE relation_factor opt_like_condition
    { malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_COLUMNS, 2, $2, $3); }
  | DESC relation_factor opt_like_condition
    { malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_COLUMNS, 2, $2, $3); }
  | SHOW WARNINGS opt_limit
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_WARNINGS, 1, $3);
    }
  | SHOW func_expr WARNINGS
    {
      if ($2->type_ != T_FUN_COUNT)
      {
        yyerror(&@1, result, "Only COUNT(*) function is supported in SHOW WARNINGS statement!");
        YYABORT;
      }
      else
      {
        malloc_terminal_node($$, result->malloc_pool_, T_SHOW_WARNINGS);
        $$->value_ = 1;
      }
    }
  | SHOW GRANTS opt_for_grant_user
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_GRANTS, 1, $3);
    }
  | SHOW PARAMETERS opt_show_condition
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_PARAMETERS, 1, $3);
    }
  | SHOW opt_full PROCESSLIST
    {
      malloc_terminal_node($$, result->malloc_pool_, T_SHOW_PROCESSLIST);
      $$->value_ = $2;
    }
  ;
<<<<<<< HEAD
lock_table_stmt:
    LOCKWJH TABLE relation_factor
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LOCK_TABLE, 1, $3);
    }
  ;
=======

>>>>>>> refs/remotes/origin/master
opt_limit:
    LIMIT INTNUM ',' INTNUM
    {
      if ($2->value_ < 0 || $4->value_ < 0)
      {
        yyerror(&@1, result, "OFFSET/COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_LIMIT, 2, $2, $4);
    }
  | LIMIT INTNUM
    {
      if ($2->value_ < 0)
      {
        yyerror(&@1, result, "COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node($$, result->malloc_pool_, T_SHOW_LIMIT, 2, NULL, $2);
    }
  | /* EMPTY */
    { $$ = NULL; }
  ;

opt_for_grant_user:
    opt_for_user
    { $$ = $1; }
  | FOR CURRENT_USER
    { $$ = NULL; }
  | FOR CURRENT_USER '(' ')'
    { $$ = NULL; }
  ;

opt_scope:
    GLOBAL      { $$ = 1; }
  | SESSION     { $$ = 0; }
  | /* EMPTY */ { $$ = 0; }
  ;

opt_show_condition:
    /* EMPTY */
    { $$ = NULL; }
  | LIKE STRING
    { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 1, $2); }
  | WHERE expr
    { malloc_non_terminal_node($$, result->malloc_pool_, T_WHERE_CLAUSE, 1, $2); }
  ;

opt_like_condition:
    /* EMPTY */
    { $$ = NULL; }
  | STRING
    { malloc_non_terminal_node($$, result->malloc_pool_, T_OP_LIKE, 1, $1); }
  ;
opt_full:
    /* EMPTY */
    { $$ = 0; }
  | FULL
    { $$ = 1; }
  ;
<<<<<<< HEAD

=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
 *
 *	create user grammar
 *
 *****************************************************************************/
create_user_stmt:
    CREATE USER user_specification_list
    {
        merge_nodes($$, result->malloc_pool_, T_CREATE_USER, $3);
    }
;
user_specification_list:
    user_specification
    {
        $$ = $1;
    }
    | user_specification_list ',' user_specification
    {
        malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
;
user_specification:
    user IDENTIFIED BY password
    {
        malloc_non_terminal_node($$, result->malloc_pool_, T_CREATE_USER_SPEC, 2, $1, $4);
    }
;
user:
    STRING
    {
        $$ = $1;
    }
;
password:
    STRING
    {
        $$ = $1;
    }
;

/*****************************************************************************
 *
 *	drop user grammar
 *
 *****************************************************************************/
drop_user_stmt:
    DROP USER user_list
    {
        merge_nodes($$, result->malloc_pool_, T_DROP_USER, $3);
    }
;
user_list:
    user
    {
      $$ = $1;
    }
    | user_list ',' user
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
;

/*****************************************************************************
 *
 *	set password grammar
 *
 *****************************************************************************/
set_password_stmt:
    SET PASSWORD opt_for_user COMP_EQ password
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 2, $3, $5);
    }
    | ALTER USER user IDENTIFIED BY password
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SET_PASSWORD, 2, $3, $6);
    }
;
opt_for_user:
    FOR user
    {
      $$ = $2;
    }
    | /**/
    {
      $$ = NULL;
    }
;
<<<<<<< HEAD

=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
 *
 *	rename user grammar
 *
 *****************************************************************************/
rename_user_stmt:
    RENAME USER rename_list
    {
      merge_nodes($$, result->malloc_pool_, T_RENAME_USER, $3);
    }
;
rename_info:
    user TO user
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_RENAME_INFO, 2, $1, $3);
    }
;
rename_list:
    rename_info
    {
      $$ = $1;
    }
    | rename_list ',' rename_info
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
;
<<<<<<< HEAD

=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
 *
 *	lock user grammar
 *
 *****************************************************************************/
lock_user_stmt:
    ALTER USER user lock_spec
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LOCK_USER, 2, $3, $4);
    }
;
lock_spec:
    LOCKED
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 1;
    }
    | UNLOCKED
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 0;
    }
;
<<<<<<< HEAD

=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
*
*  begin/start transaction grammer
*
******************************************************************************/

opt_work:
    WORK
    {
      (void)$$;
    }
    | /*empty*/
    {
      (void)$$;
    }
opt_with_consistent_snapshot:
    WITH CONSISTENT SNAPSHOT
    {
      $$ = 1;
    }
    |/*empty*/
    {
      $$ = 0;
    }
begin_stmt:
    BEGI opt_work
    {
      (void)$2;
      malloc_terminal_node($$, result->malloc_pool_, T_BEGIN);
      $$->value_ = 0;
    }
    | START TRANSACTION opt_with_consistent_snapshot
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BEGIN);
      $$->value_ = $3;
    }
<<<<<<< HEAD

=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
*
*  commit grammer
*
******************************************************************************/
commit_stmt:
    COMMIT opt_work
    {
      (void)$2;
      malloc_terminal_node($$, result->malloc_pool_, T_COMMIT);
    }

/*****************************************************************************
*
*  rollback grammer
*
******************************************************************************/
rollback_stmt:
    ROLLBACK opt_work
    {
      (void)$2;
      malloc_terminal_node($$, result->malloc_pool_, T_ROLLBACK);
    }

/*****************************************************************************
*
*  kill grammer
*
******************************************************************************/
kill_stmt:
    KILL opt_is_global opt_flag INTNUM
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_KILL, 3, $2, $3, $4);
    }
   ;
opt_is_global:
    /*EMPTY*/
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 0;
    }
    | GLOBAL
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 1;
    }
  ;
opt_flag:
    /*EMPTY*/
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 0;
    }
    | QUERY
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 1;
    }
    | CONNECTION
    {
      malloc_terminal_node($$, result->malloc_pool_, T_BOOL);
      $$->value_ = 0;
    }
  ;


/*****************************************************************************
 *
 *	grant grammar
 *
 *****************************************************************************/
grant_stmt:
    GRANT priv_type_list ON priv_level TO user_list
    {
      ParseNode *privileges_node = NULL;
      ParseNode *users_node = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, $2);
      merge_nodes(users_node, result->malloc_pool_, T_USERS, $6);
      malloc_non_terminal_node($$, result->malloc_pool_, T_GRANT,
                                 3, privileges_node, $4, users_node);
    }
;
priv_type_list:
    priv_type
    {
      $$ = $1;
    }
    | priv_type_list ',' priv_type
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
;
priv_type:
    ALL opt_privilege
    {
      (void)$2;                 /* useless */
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_ALL;
    }
    | ALTER
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_ALTER;
    }
    | CREATE
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_CREATE;
    }
    | CREATE USER
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_CREATE_USER;
    }
    | DELETE
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_DELETE;
    }
    | DROP
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_DROP;
    }
<<<<<<< HEAD
    /*add hxlong [Truncate Table]:20170403 add 'TRUNCATE'*/
    | TRUNCATE
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_DROP;
    }
=======
>>>>>>> refs/remotes/origin/master
    | GRANT OPTION
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_GRANT_OPTION;
    }
    | INSERT
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_INSERT;
    }
    | UPDATE
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_UPDATE;
    }
    | SELECT
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_SELECT;
    }
    | REPLACE
    {
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_TYPE);
      $$->value_ = OB_PRIV_REPLACE;
    }
;
opt_privilege:
    PRIVILEGES
    {
      (void)$$;
    }
    | /*empty*/
    {
      (void)$$;
    }
;
priv_level:
    '*'
    {
      /* means global priv_level */
      malloc_terminal_node($$, result->malloc_pool_, T_PRIV_LEVEL);
    }
    | relation_name
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_PRIV_LEVEL, 1, $1);
    }
;
<<<<<<< HEAD

=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
 *
 *	revoke grammar
 *
 *****************************************************************************/
revoke_stmt:
    REVOKE priv_type_list opt_on_priv_level FROM user_list
    {
      ParseNode *privileges_node = NULL;
      ParseNode *priv_level = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, $2);
      if ($3 == NULL)
      {
        /* check privileges: should have and only have ALL PRIVILEGES, GRANT OPTION */
        int check_ok = 0;
        if (2 == privileges_node->num_child_)
        {
          assert(privileges_node->children_[0]->num_child_ == 0);
          assert(privileges_node->children_[0]->type_ == T_PRIV_TYPE);
          assert(privileges_node->children_[1]->num_child_ == 0);
          assert(privileges_node->children_[1]->type_ == T_PRIV_TYPE);
          if ((privileges_node->children_[0]->value_ == OB_PRIV_ALL
               && privileges_node->children_[1]->value_ == OB_PRIV_GRANT_OPTION)
              || (privileges_node->children_[1]->value_ == OB_PRIV_ALL
                  && privileges_node->children_[0]->value_ == OB_PRIV_GRANT_OPTION))
          {
            check_ok = 1;
          }
        }
        if (!check_ok)
        {
          yyerror(&@1, result, "support only ALL PRIVILEGES, GRANT OPTION");
          YYABORT;
        }
      }
      else
      {
        priv_level = $3;
      }
      ParseNode *users_node = NULL;
      merge_nodes(users_node, result->malloc_pool_, T_USERS, $5);
      malloc_non_terminal_node($$, result->malloc_pool_, T_REVOKE,
                                 3, privileges_node, priv_level, users_node);
    }
;
opt_on_priv_level:
    ON priv_level
    {
      $$ = $2;
    }
    | /*empty*/
    {
      $$ = NULL;
    }

<<<<<<< HEAD

=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
 *
 *	prepare grammar
 *
 *****************************************************************************/
prepare_stmt:
    /* PREPARE stmt_name FROM '"' preparable_stmt '"' */
    PREPARE stmt_name FROM preparable_stmt
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_PREPARE, 2, $2, $4);
    }
  ;

stmt_name:
    column_label
    { $$ = $1; }
  ;

preparable_stmt:
    select_stmt
    { $$ = $1; }
  | insert_stmt
    { $$ = $1; }
  | update_stmt
    { $$ = $1; }
  | delete_stmt
    { $$ = $1; }
  ;


<<<<<<< HEAD


=======
>>>>>>> refs/remotes/origin/master
/*****************************************************************************
 *
 *	set grammar
 *
 *****************************************************************************/
variable_set_stmt:
    SET var_and_val_list
    {
      merge_nodes($$, result->malloc_pool_, T_VARIABLE_SET, $2);;
      $$->value_ = 2;
    }
<<<<<<< HEAD
    //add zt 20151202:b
    | SET SET var_and_array_val
    {
      $$ = $3;
    }
    //add zt 20151202:e
=======
>>>>>>> refs/remotes/origin/master
  ;

var_and_val_list:
    var_and_val
    {
      $$ = $1;
    }
  | var_and_val_list ',' var_and_val
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

var_and_val:
    TEMP_VARIABLE to_or_eq expr
    {
      (void)($2);
      malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
      $$->value_ = 2;
    }
  | column_name to_or_eq expr
    {
      (void)($2);
      $1->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
      $$->value_ = 2;
    }
  | GLOBAL column_name to_or_eq expr
    {
      (void)($3);
      $2->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $2, $4);
      $$->value_ = 1;
    }
  | SESSION column_name to_or_eq expr
    {
      (void)($3);
      $2->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $2, $4);
      $$->value_ = 2;
    }
  | GLOBAL_ALIAS '.' column_name to_or_eq expr
    {
      (void)($4);
      $3->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $3, $5);
      $$->value_ = 1;
    }
  | SESSION_ALIAS '.' column_name to_or_eq expr
    {
      (void)($4);
      $3->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $3, $5);
      $$->value_ = 2;
    }
  | SYSTEM_VARIABLE to_or_eq expr
    {
      (void)($2);
      malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
      $$->value_ = 2;
    }
<<<<<<< HEAD
    //add zt 20151126:b
  | array_expr to_or_eq expr
    {
      (void)($2);
      malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_VAL, 2, $1, $3);
      $$->value_ = 2;
    }
    //add zt 20151126:e
  ;

//add zt 20151202:b
var_and_array_val:
    TEMP_VARIABLE to_or_eq  array_vals_list
    {
      (void)($2);
      malloc_non_terminal_node($$, result->malloc_pool_, T_VAR_ARRAY_VAL, 2, $1, $3);
      $$->value_ = 2;
    }
    ;
//add zt 20151202:e

=======
  ;

>>>>>>> refs/remotes/origin/master
to_or_eq:
    TO      { $$ = NULL; }
  | COMP_EQ { $$ = NULL; }
  ;

argument:
    TEMP_VARIABLE
    { $$ = $1; }
<<<<<<< HEAD
  |
    array_expr
    { $$ = $1; }
  ;

//add zt 20151202:b
array_vals_list:
    '(' array_val_list ')'
    {
      merge_nodes($$, result->malloc_pool_, T_ARRAY_VAL, $2);
    }
    ;

array_val_list:
    expr_const { $$ = $1; }
  | array_val_list ',' expr_const
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;
//add zt 20151202:e

=======
  ;
>>>>>>> refs/remotes/origin/master


/*****************************************************************************
 *
 *	execute grammar
 *
 *****************************************************************************/
execute_stmt:
    EXECUTE stmt_name opt_using_args
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_EXECUTE, 2, $2, $3);
    }
  ;

opt_using_args:
    USING argument_list
    {
      merge_nodes($$, result->malloc_pool_, T_ARGUMENT_LIST, $2);
    }
    | /*empty*/
    {
      $$ = NULL;
    }

argument_list:
    argument
    {
      $$ = $1;
    }
  | argument_list ',' argument
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;


/*****************************************************************************
 *
 *	DEALLOCATE grammar
 *
 *****************************************************************************/
deallocate_prepare_stmt:
    deallocate_or_drop PREPARE stmt_name
    {
      (void)($1);
      malloc_non_terminal_node($$, result->malloc_pool_, T_DEALLOCATE, 1, $3);
    }
  ;

deallocate_or_drop:
    DEALLOCATE
    { $$ = NULL; }
  | DROP
    { $$ = NULL; }
  ;


/*****************************************************************************
 *
 *	ALTER TABLE grammar
 *
 *****************************************************************************/
alter_table_stmt:
     ALTER TABLE relation_factor alter_column_actions
    {
      ParseNode *alter_actions = NULL;
      merge_nodes(alter_actions, result->malloc_pool_, T_ALTER_ACTION_LIST, $4);
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLE, 2, $3, alter_actions);
    }
  | ALTER TABLE relation_factor RENAME TO relation_factor
    {
      yyerror(&@1, result, "Table rename is not supported now");
      YYABORT;
      malloc_non_terminal_node($$, result->malloc_pool_, T_TABLE_RENAME, 1, $6);
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_ACTION_LIST, 1, $$);
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_TABLE, 2, $3, $$);
    }
  ;

alter_column_actions:
    alter_column_action
    {
      $$ = $1;
    }
  | alter_column_actions ',' alter_column_action
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

alter_column_action:
    ADD opt_column column_definition
    {
      (void)($2); /* make bison mute */
      $$ = $3; /* T_COLUMN_DEFINITION */
    }
  | DROP opt_column column_name opt_drop_behavior
    {
      (void)($2); /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_DROP, 1, $3);
      $$->value_ = $4;
    }
  | ALTER opt_column column_name alter_column_behavior
    {
      (void)($2); /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_ALTER, 2, $3, $4);
    }
  | RENAME opt_column column_name TO column_label
    {
      (void)($2); /* make bison mute */
      malloc_non_terminal_node($$, result->malloc_pool_, T_COLUMN_RENAME, 2, $3, $5);
    }
  /* we don't have table constraint, so ignore it */
  ;

opt_column:
    COLUMN          { $$ = NULL; }
  | /*EMPTY*/       { $$ = NULL; }
  ;

opt_drop_behavior:
    CASCADE         { $$ = 2; }
  | RESTRICT        { $$ = 1; }
  | /*EMPTY*/       { $$ = 0; }
  ;

alter_column_behavior:
    SET NOT NULLX
    {
      (void)($3); /* make bison mute */
      malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NOT_NULL);
    }
  | DROP NOT NULLX
    {
      (void)($3); /* make bison mute */
      malloc_terminal_node($$, result->malloc_pool_, T_CONSTR_NULL);
    }
  | SET DEFAULT expr_const
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_DEFAULT, 1, $3);
    }
  | DROP DEFAULT
    {
      malloc_terminal_node($$, result->malloc_pool_, T_NULL);
      malloc_non_terminal_node($$, result->malloc_pool_, T_CONSTR_DEFAULT, 1, $$);
    }
  ;


/*****************************************************************************
 *
 *	ALTER SYSTEM grammar
 *
 *****************************************************************************/
alter_system_stmt:
    ALTER SYSTEM SET alter_system_actions
    {
      merge_nodes($$, result->malloc_pool_, T_SYTEM_ACTION_LIST, $4);
      malloc_non_terminal_node($$, result->malloc_pool_, T_ALTER_SYSTEM, 1, $$);
    }
    |
    ALTER SYSTEM opt_force CHANGE_OBI MASTER COMP_EQ STRING
    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node($$, result->malloc_pool_, T_CHANGE_OBI, 3, node, $7, $3);
    }
    |
    ALTER SYSTEM opt_force SWITCH_CLUSTER MASTER COMP_EQ STRING
    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node($$, result->malloc_pool_, T_CHANGE_OBI, 3, node, $7, $3);
    }
    |
    ALTER SYSTEM SET_MASTER_CLUSTER MASTER COMP_EQ STRING
    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER);
      malloc_non_terminal_node($$, result->malloc_pool_, T_CHANGE_OBI, 2, node, $6);
    }
    |
    ALTER SYSTEM SET_SLAVE_CLUSTER SLAVE COMP_EQ STRING
    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_SLAVE);
      malloc_non_terminal_node($$, result->malloc_pool_, T_CHANGE_OBI, 2, node, $6);
    }
  ;

opt_force:
    /*EMPTY*/
    {
      $$ = NULL;
    }
  | FORCE
    {
      malloc_terminal_node($$, result->malloc_pool_, T_FORCE);
    }
    ;


alter_system_actions:
    alter_system_action
    {
      $$ = $1;
    }
  | alter_system_actions ',' alter_system_action
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
    }
  ;

alter_system_action:
    column_name COMP_EQ expr_const opt_comment opt_config_scope
    SERVER_TYPE COMP_EQ server_type opt_cluster_or_address
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SYSTEM_ACTION, 5,
                               $1,    /* param_name */
                               $3,    /* param_value */
                               $4,    /* comment */
                               $8,    /* server type */
                               $9     /* cluster or IP/port */
                               );
      $$->value_ = $5;                /* scope */
    }
  ;

opt_comment:
    COMMENT STRING
    { $$ = $2; }
  | /* EMPTY */
    { $$ = NULL; }
  ;

opt_config_scope:
    SCOPE COMP_EQ MEMORY
    { $$ = 0; }   /* same as ObConfigType */
  | SCOPE COMP_EQ SPFILE
    { $$ = 1; }   /* same as ObConfigType */
  | SCOPE COMP_EQ BOTH
    { $$ = 2; }   /* same as ObConfigType */
  | /* EMPTY */
    { $$ = 2; }   /* same as ObConfigType */
  ;

server_type:
    ROOTSERVER
    {
      malloc_terminal_node($$, result->malloc_pool_, T_INT);
      $$->value_ = 1;
    }
  | UPDATESERVER
    {
      malloc_terminal_node($$, result->malloc_pool_, T_INT);
      $$->value_ = 4;
    }
  | CHUNKSERVER
    {
      malloc_terminal_node($$, result->malloc_pool_, T_INT);
      $$->value_ = 2;
    }
  | MERGESERVER
    {
      malloc_terminal_node($$, result->malloc_pool_, T_INT);
      $$->value_ = 3;
    }
  ;

opt_cluster_or_address:
    CLUSTER COMP_EQ INTNUM
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_CLUSTER, 1, $3);
    }
  | SERVER_IP COMP_EQ STRING SERVER_PORT COMP_EQ INTNUM
    {
      malloc_non_terminal_node($$, result->malloc_pool_, T_SERVER_ADDRESS, 2, $3, $6);
    }
  | /* EMPTY */
    { $$ = NULL; }
  ;


/*===========================================================
 *
 *	Name classification
 *
 *===========================================================*/

column_name:
    NAME
    { $$ = $1; }
  | unreserved_keyword
    {
      malloc_terminal_node($$, result->malloc_pool_, T_IDENT);
      $$->str_value_ = parse_strdup($1->keyword_name, result->malloc_pool_);
      if ($$->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        $$->value_ = strlen($$->str_value_);
      }
    }
  ;

relation_name:
    NAME
    { $$ = $1; }
  | unreserved_keyword
    {
      malloc_terminal_node($$, result->malloc_pool_, T_IDENT);
      $$->str_value_ = parse_strdup($1->keyword_name, result->malloc_pool_);
      if ($$->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        $$->value_ = strlen($$->str_value_);
      }
    }
  ;

function_name:
    NAME
  ;

column_label:
    NAME
    { $$ = $1; }
  | unreserved_keyword
    {
      malloc_terminal_node($$, result->malloc_pool_, T_IDENT);
      $$->str_value_ = parse_strdup($1->keyword_name, result->malloc_pool_);
      if ($$->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
    }
  ;

unreserved_keyword:
    AUTO_INCREMENT
  | CHUNKSERVER
  | COMPRESS_METHOD
  | CONSISTENT_MODE
  | EXPIRE_INFO
  | GRANTS
  | JOIN_INFO
  | MERGESERVER
  | REPLICA_NUM
  | ROOTSERVER
  | ROW_COUNT
  | SERVER
  | SERVER_IP
  | SERVER_PORT
  | SERVER_TYPE
  | STATUS
  | TABLET_BLOCK_SIZE
  | TABLE_ID
  | TABLET_MAX_SIZE
  | UNLOCKED
  | UPDATESERVER
  | USE_BLOOM_FILTER
  | VARIABLES
  | VERBOSE
  | WARNINGS
  ;

<<<<<<< HEAD
/*Add by zz 2014-11-20,this part was used by analyse prodedure*/

/*****************************************************************************
 *
 *	create store procedure  grammar
 *
 *****************************************************************************/
create_procedure_stmt	:	proc_decl proc_block
							{
								malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_CREATE, 2, $1, $2);
							}
						;
proc_decl	:	CREATE PROCEDURE  NAME '(' proc_parameter_list ')'
				{
					ParseNode *params = NULL;
        			merge_nodes(params, result->malloc_pool_, T_PARAM_LIST, $5);
					malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_DECL, 2, $3, params);
				}
			|
				CREATE PROCEDURE  NAME '(' ')'
				{
					ParseNode *params = NULL;
					malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_DECL, 2, $3, params);
				}
			;
proc_parameter_list	:	proc_parameter_list ',' proc_parameter
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $3);
					}
				|	proc_parameter
					{
						$$ = $1;
					}
				;
proc_parameter	:	TEMP_VARIABLE data_type
					{
                malloc_non_terminal_node($$, result->malloc_pool_, T_PARAM_DEFINITION, 3, $1, $2, NULL);
					}
				|	IN TEMP_VARIABLE data_type
					{
            malloc_non_terminal_node($$, result->malloc_pool_, T_IN_PARAM_DEFINITION, 3, $2, $3, NULL);
					}
				|	OUT TEMP_VARIABLE data_type
					{
            malloc_non_terminal_node($$, result->malloc_pool_, T_OUT_PARAM_DEFINITION, 3, $2, $3, NULL);
					}
				|   INOUT TEMP_VARIABLE data_type
					{
            malloc_non_terminal_node($$, result->malloc_pool_, T_INOUT_PARAM_DEFINITION, 3, $2, $3, NULL);
          }
        | TEMP_VARIABLE data_type ARRAY
        {
          ParseNode *array_flag = NULL;
          malloc_terminal_node(array_flag, result->malloc_pool_, T_BOOL);
          array_flag->value_ = 1;
          malloc_non_terminal_node($$, result->malloc_pool_, T_PARAM_DEFINITION, 3, $1, $2, array_flag);
        }
        | IN TEMP_VARIABLE data_type ARRAY
        {
          ParseNode *array_flag = NULL;
          malloc_terminal_node(array_flag, result->malloc_pool_, T_BOOL);
          array_flag->value_ = 1;
          malloc_non_terminal_node($$, result->malloc_pool_, T_IN_PARAM_DEFINITION, 3, $2, $3, array_flag);
        }
        | OUT TEMP_VARIABLE data_type ARRAY
        {
          ParseNode *array_flag = NULL;
          malloc_terminal_node(array_flag, result->malloc_pool_, T_BOOL);
          array_flag->value_ = 1;
          malloc_non_terminal_node($$, result->malloc_pool_, T_OUT_PARAM_DEFINITION, 3, $2, $3, array_flag);
        }
        | INOUT TEMP_VARIABLE data_type ARRAY
        {
          ParseNode *array_flag = NULL;
          malloc_terminal_node(array_flag, result->malloc_pool_, T_BOOL);
          array_flag->value_ = 1;
          malloc_non_terminal_node($$, result->malloc_pool_, T_INOUT_PARAM_DEFINITION, 3, $2, $3, array_flag);
        }
				;
proc_block	: 	BEGI proc_sect END
				{ 
					//malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_BLOCK, 1, $2);
					$$=$2;
				}
			;
proc_sect	:		
				{ 
					$$=NULL;
				}
			| 	proc_stmts	
				{ 
        			merge_nodes($$, result->malloc_pool_, T_PROCEDURE_STMTS, $1);
				}
			;
proc_stmts	: 	proc_stmts proc_stmt
				{
						malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
				}
			| 	proc_stmt
				{
					$$=$1;
				}
			;
proc_stmt		: stmt_if
						{ $$ = $1; }
				| stmt_case
						{ $$ = $1; }
				| stmt_while
						{ $$ = $1; }
				| stmt_loop
						{ $$ = $1; }
				| stmt_declare
						{ $$ = $1; }
				| stmt_assign
						{ $$ = $1; }
				| select_into_clause ';'
						{ $$ = $1; }
				| insert_stmt ';'
						{ $$ = $1; }
				| update_stmt ';'
						{ $$ = $1; }
				| delete_stmt ';'
						{ $$ = $1; }
				| select_stmt ';'
						{ $$ = $1; }
				| cursor_declare_stmt ';'
						{ $$ = $1; }
  				| cursor_open_stmt ';'
						{ $$ = $1; }
				| cursor_close_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_next_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_first_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_last_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_prior_into_stmt ';'
						{ $$ = $1; }						
				| cursor_fetch_absolute_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_relative_into_stmt ';'
						{ $$ = $1; }

				| stmt_exit
						{ $$ =$1; }
				| stmt_null
						{ $$ = $1; }
				;
/*===========================================================
 *	Procedure's control body sentence didn't contain declare
 *===========================================================*/
control_sect	:		
				{ 
					$$=NULL;
				}
			| 	control_stmts	
				{ 
        			merge_nodes($$, result->malloc_pool_, T_PROCEDURE_STMTS, $1);
				}
			;
control_stmts	: 	control_stmts control_stmt
				{
						malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
				}
			| 	control_stmt
				{
					$$=$1;
				}
			;
control_stmt	: stmt_if
						{ $$ = $1; }
				| stmt_case
						{ $$ = $1; }
				| stmt_while
						{ $$ = $1; }
				| stmt_loop
						{ $$ = $1; }
				| stmt_assign
						{ $$ = $1; }
				| select_into_clause ';'
						{ $$ = $1; }
				| insert_stmt ';'
						{ $$ = $1; }
				| update_stmt ';'
						{ $$ = $1; }
				| delete_stmt ';'
						{ $$ = $1; }
				| select_stmt ';'
						{ $$ = $1; }
				| cursor_declare_stmt ';'
						{ $$ = $1; }
  				| cursor_open_stmt ';'
						{ $$ = $1; }
				| cursor_close_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_next_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_first_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_last_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_prior_into_stmt ';'
						{ $$ = $1; }						
				| cursor_fetch_absolute_into_stmt ';'
						{ $$ = $1; }
				| cursor_fetch_relative_into_stmt ';'
						{ $$ = $1; }
					
				| stmt_exit
						{ $$ =$1; }
				| stmt_null
						{ $$ = $1; }
				;

/*===========================================================
 *	Procedure's declare variable sentence
 *===========================================================*/
 
stmt_declare	:	DECLARE argument_list data_type ';'
					{
						ParseNode *args = NULL;
						merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $2);
            malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_DECLARE, 4, args, $3, NULL, NULL);
					}
				|	DECLARE argument_list data_type DEFAULT expr_const ';'
					{
						ParseNode *args = NULL;
						merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $2);
            malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_DECLARE, 4, args, $3, $5, NULL);
          }
        | DECLARE argument_list data_type ARRAY ';'
          {
            ParseNode *args = NULL;
            ParseNode *arr = NULL;
            malloc_terminal_node(arr, result->malloc_pool_, T_BOOL);
            arr->value_ = 1;
            merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, $2);
            malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_DECLARE, 4,
                                     args,  //arguments
                                     $3,    //data type
                                     NULL, 	//default value
                                     arr 		//is array
                                     );
          }
				;

				
/*===========================================================
 *	Procedure's assign variable a value sentence
 *===========================================================*/
stmt_assign		:	SET var_and_val_list ';'
					{
						ParseNode *var_list = NULL;
						merge_nodes(var_list, result->malloc_pool_, T_VAR_VAL_LIST, $2);
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_ASSGIN, 1,var_list);
					}
				;


/*===========================================================
 *	Procedure's if sentence
 *===========================================================*/
stmt_if			:	IF expr THEN control_sect stmt_elsifs stmt_else END IF ';'//add ; by wdh
					{
						ParseNode *elsifs = NULL;
						merge_nodes(elsifs, result->malloc_pool_, T_PROCEDURE_ELSEIFS, $5);
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_IF, 4, $2, $4, elsifs ,$6);
					}
                |	IF expr THEN control_sect stmt_elsifs END IF ';'
					{
						ParseNode *elsifs = NULL;
						merge_nodes(elsifs, result->malloc_pool_, T_PROCEDURE_ELSEIFS, $5);
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_IF, 4, $2, $4, elsifs ,NULL);
					}
                |	IF expr THEN control_sect stmt_else END IF ';'
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_IF, 4, $2, $4, NULL ,$5);
					}
                |	IF expr THEN control_sect END IF ';'
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_IF, 4, $2, $4, NULL ,NULL);
					}
				;
stmt_elsifs		:	stmt_elsif
					{
						$$=$1;
					}
				| 	stmt_elsifs stmt_elsif
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
					}
				;
stmt_elsif		:	ELSEIF expr THEN control_sect
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_ELSEIF, 2, $2, $4);
					}
				;
				
stmt_else		:	ELSE control_sect
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_ELSE, 1, $2);
						//$$ = $2;
					}
				;


/*===========================================================
 *	Procedure's case-when sentence
 *===========================================================*/
stmt_case		: 	CASE expr case_when_list case_else END CASE ';'
					{
						ParseNode *casewhenlist = NULL;
						merge_nodes(casewhenlist, result->malloc_pool_, T_PROCEDURE_CASE_WHEN_LIST, $3);
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_CASE, 3, $2, casewhenlist, $4);
					}
				;
case_when_list	: 	case_when_list case_when
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_LINK_NODE, 2, $1, $2);
					}
				| 	case_when
					{
						$$=$1;
					}
			;
case_when		: 	WHEN expr THEN control_sect
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_CASE_WHEN, 2, $2, $4);
					}
				;
case_else		:
					{
						$$ = NULL;
					}
				| 	ELSE control_sect
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_ELSE, 1, $2);
						//$$ = $2;
					}
				;


/*===========================================================
 *	Procedure's loop sentence
 *===========================================================*/
stmt_loop		:	LOOP control_sect END LOOP ';'
					{
            malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_LOOP, 5,
                                     NULL,
                                     NULL,
                                     NULL,
                                     NULL,
                                     $2);
          }
        | FOR TEMP_VARIABLE IN arith_expr TO arith_expr LOOP control_sect END LOOP';'
            {
              malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_LOOP, 5,
                                       $2,
                                       NULL,
                                       $4,
                                       $6,
                                       $8);
            }
        | FOR TEMP_VARIABLE IN REVERSE arith_expr TO arith_expr LOOP control_sect END LOOP';'
            {
              ParseNode *rev_flag = NULL;
              malloc_terminal_node(rev_flag, result->malloc_pool_, T_BOOL);
              rev_flag->value_ = 1;
              malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_LOOP, 5,
                                       $2, 				//loop counter
                                       rev_flag,  //reverse loop
                                       $5,        //lowest_number
                                       $7,        //highest number
                                       $9);       //loop body
            }
				;


/*===========================================================
 *	Procedure's while sentence
 *===========================================================*/
stmt_while		: 	WHILE expr DO control_sect END WHILE ';'
					{
						malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_WHILE, 2, $2, $4);
					}
				;

stmt_null		: ';'
					{
						/* We do not bother building a node for NULL */
						$$ = NULL;
					}
				;

/*===========================================================
 *	Procedure's exit sentence can exit and continue
 *===========================================================*/
stmt_exit		:	EXIT ';'
					{
                        malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_EXIT, 1, NULL);
						$$->value_=1;
					}
//				|	CONTINUE ';'
//					{
//						malloc_terminal_node($$, result->malloc_pool_, T_PROCEDURE_CONTINUE);
//						$$->value_=0;
//					}
                |   EXIT WHEN expr ';'
                    {
                        malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_EXIT, 1, $3);//when_expr
                    }//add by wangdonghui 20160623
				;

/*****************************************************************************
 *
 *	execute  procedure  grammar
 *
 *****************************************************************************/
exec_procedure_stmt	:	CALL NAME '(' expr_list ')' opt_hint //add by wdh20160716
						{
        					ParseNode *param_list = NULL;
            				merge_nodes(param_list, result->malloc_pool_, T_EXPR_LIST, $4);
        					
                            malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_EXEC, 3, $2, param_list, $6);
						}
					|
                        CALL NAME '(' ')' opt_hint
						{
							ParseNode *params = NULL;
                            malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_EXEC, 3, $2, params, $5);
                        }
					;


/*****************************************************************************
 *
 *	drop store procedure  grammar
 *
 *****************************************************************************/
drop_procedure_stmt	:	DROP PROCEDURE IF EXISTS NAME
						{
							malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_DROP, 2, $5,$5);
						}
					|	DROP PROCEDURE NAME
						{
							malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_DROP, 1, $3);
						}
					;


/*****************************************************************************
 *
 *	show  procedure status  grammar
 *
 *****************************************************************************/


show_procedure_stmt	:	SHOW PROCEDURE NAME
						{
							malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_SHOW, 1, $3);
						}
					;
					
									
=======
>>>>>>> refs/remotes/origin/master

%%

void yyerror(YYLTYPE* yylloc, ParseResult* p, char* s, ...)
{
  if (p != NULL)
  {
    p->result_tree_ = 0;
    va_list ap;
    va_start(ap, s);
    vsnprintf(p->error_msg_, MAX_ERROR_MSG, s, ap);
    if (yylloc != NULL)
    {
      if (p->input_sql_[yylloc->first_column - 1] != '\'')
        p->start_col_ = yylloc->first_column;
      p->end_col_ = yylloc->last_column;
      p->line_ = yylloc->first_line;
    }
  }
}

int parse_init(ParseResult* p)
{
  int ret = 0;  // can not include C++ file "ob_define.h"
  if (!p || !p->malloc_pool_)
  {
    ret = -1;
    if (p)
    {
      snprintf(p->error_msg_, MAX_ERROR_MSG, "malloc_pool_ must be set");
    }
  }
  if (ret == 0)
  {
    ret = yylex_init_extra(p, &(p->yyscan_info_));
  }
  return ret;
}

int parse_terminate(ParseResult* p)
{
  return yylex_destroy(p->yyscan_info_);
}

int parse_sql(ParseResult* p, const char* buf, size_t len)
{
  int ret = -1;
  p->result_tree_ = 0;
  p->error_msg_[0] = 0;
  p->input_sql_ = buf;
  p->input_sql_len_ = len;
  p->start_col_ = 1;
  p->end_col_ = 1;
  p->line_ = 1;
  p->yycolumn_ = 1;
  p->yylineno_ = 1;
  p->tmp_literal_ = NULL;

  if (buf == NULL || len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be empty");
    return ret;
  }

  while(len > 0 && isspace(buf[len - 1]))
    --len;

  if (len <= 0)
  {
    snprintf(p->error_msg_, MAX_ERROR_MSG, "Input SQL can not be while space only");
    return ret;
  }

  YY_BUFFER_STATE bp;

  //bp = yy_scan_string(buf, p->yyscan_info_);
  bp = yy_scan_bytes(buf, len, p->yyscan_info_);
  yy_switch_to_buffer(bp, p->yyscan_info_);
  ret = yyparse(p);
  yy_delete_buffer(bp, p->yyscan_info_);
  return ret;
}
