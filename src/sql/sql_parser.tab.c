<<<<<<< HEAD

/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton implementation for Bison's Yacc-like parsers in C
   
      Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.
=======
/* A Bison parser, made by GNU Bison 2.5.  */

/* Bison implementation for Yacc-like parsers in C
   
      Copyright (C) 1984, 1989-1990, 2000-2011 Free Software Foundation, Inc.
>>>>>>> refs/remotes/origin/master
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output.  */
#define YYBISON 1

/* Bison version.  */
<<<<<<< HEAD
#define YYBISON_VERSION "2.4.1"
=======
#define YYBISON_VERSION "2.5"
>>>>>>> refs/remotes/origin/master

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1

/* Using locations.  */
#define YYLSP_NEEDED 1



/* Copy the first part of user declarations.  */


#include <stdint.h>
#include "parse_node.h"
#include "parse_malloc.h"
#include "ob_non_reserved_keywords.h"
#include "common/ob_privilege_type.h"
#define YYDEBUG 1



/* Enabling traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif

/* Enabling verbose error messages.  */
#ifdef YYERROR_VERBOSE
# undef YYERROR_VERBOSE
# define YYERROR_VERBOSE 1
#else
# define YYERROR_VERBOSE 0
#endif

/* Enabling the token table.  */
#ifndef YYTOKEN_TABLE
# define YYTOKEN_TABLE 0
#endif


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING = 259,
     INTNUM = 260,
     DATE_VALUE = 261,
     HINT_VALUE = 262,
     BOOL = 263,
     APPROXNUM = 264,
     NULLX = 265,
     UNKNOWN = 266,
     QUESTIONMARK = 267,
     SYSTEM_VARIABLE = 268,
     TEMP_VARIABLE = 269,
     EXCEPT = 270,
     UNION = 271,
     INTERSECT = 272,
     OR = 273,
     AND = 274,
     NOT = 275,
     COMP_NE = 276,
     COMP_GE = 277,
     COMP_GT = 278,
     COMP_EQ = 279,
     COMP_LT = 280,
     COMP_LE = 281,
     CNNOP = 282,
     LIKE = 283,
     BETWEEN = 284,
     IN = 285,
     IS = 286,
     MOD = 287,
     UMINUS = 288,
<<<<<<< HEAD
     PROCEDURE = 289,
     DECLARE = 290,
     ELSEIF = 291,
     OUT = 292,
     INOUT = 293,
     WHILE = 294,
     LOOP = 295,
     EXIT = 296,
     CONTINUE = 297,
     DO = 298,
     CALL = 299,
     ARRAY = 300,
     REVERSE = 301,
     CURSOR = 302,
     OPEN = 303,
     FETCH = 304,
     CLOSE = 305,
     NEXT = 306,
     PRIOR = 307,
     FIRST = 308,
     LAST = 309,
     ABSOLUTE = 310,
     RELATIVE = 311,
     ADD = 312,
     ANY = 313,
     ALL = 314,
     ALTER = 315,
     AS = 316,
     ASC = 317,
     BEGI = 318,
     BIGINT = 319,
     BINARY = 320,
     BOOLEAN = 321,
     BOTH = 322,
     BY = 323,
     CASCADE = 324,
     CASE = 325,
     CHARACTER = 326,
     CLUSTER = 327,
     COMMENT = 328,
     COMMIT = 329,
     CONSISTENT = 330,
     COLUMN = 331,
     COLUMNS = 332,
     CREATE = 333,
     CREATETIME = 334,
     CURRENT_USER = 335,
     CHANGE_OBI = 336,
     SWITCH_CLUSTER = 337,
     DATE = 338,
     DATETIME = 339,
     DEALLOCATE = 340,
     DECIMAL = 341,
     DEFAULT = 342,
     DELETE = 343,
     DESC = 344,
     DESCRIBE = 345,
     DISTINCT = 346,
     DOUBLE = 347,
     DROP = 348,
     DUAL = 349,
     TRUNCATE = 350,
     ELSE = 351,
     END = 352,
     END_P = 353,
     ERROR = 354,
     EXECUTE = 355,
     EXISTS = 356,
     EXPLAIN = 357,
     FLOAT = 358,
     FOR = 359,
     FROM = 360,
     FULL = 361,
     FROZEN = 362,
     FORCE = 363,
     GLOBAL = 364,
     GLOBAL_ALIAS = 365,
     GRANT = 366,
     GROUP = 367,
     HAVING = 368,
     HINT_BEGIN = 369,
     HINT_END = 370,
     HOTSPOT = 371,
     IDENTIFIED = 372,
     IF = 373,
     INNER = 374,
     INTEGER = 375,
     INSERT = 376,
     INTO = 377,
     JOIN = 378,
     SEMI_JOIN = 379,
     KEY = 380,
     LEADING = 381,
     LEFT = 382,
     LIMIT = 383,
     LOCAL = 384,
     LOCKED = 385,
     LOCKWJH = 386,
     MEDIUMINT = 387,
     MEMORY = 388,
     MODIFYTIME = 389,
     MASTER = 390,
     NUMERIC = 391,
     OFFSET = 392,
     ON = 393,
     ORDER = 394,
     OPTION = 395,
     OUTER = 396,
     PARAMETERS = 397,
     PASSWORD = 398,
     PRECISION = 399,
     PREPARE = 400,
     PRIMARY = 401,
     READ_STATIC = 402,
     REAL = 403,
     RENAME = 404,
     REPLACE = 405,
     RESTRICT = 406,
     PRIVILEGES = 407,
     REVOKE = 408,
     RIGHT = 409,
     ROLLBACK = 410,
     KILL = 411,
     READ_CONSISTENCY = 412,
     NO_GROUP = 413,
     LONG_TRANS = 414,
     SCHEMA = 415,
     SCOPE = 416,
     SELECT = 417,
     SESSION = 418,
     SESSION_ALIAS = 419,
     SET = 420,
     SHOW = 421,
     SMALLINT = 422,
     SNAPSHOT = 423,
     SPFILE = 424,
     START = 425,
     STATIC = 426,
     SYSTEM = 427,
     STRONG = 428,
     SET_MASTER_CLUSTER = 429,
     SET_SLAVE_CLUSTER = 430,
     SLAVE = 431,
     TABLE = 432,
     TABLES = 433,
     THEN = 434,
     TIME = 435,
     TIMESTAMP = 436,
     TINYINT = 437,
     TRAILING = 438,
     TRANSACTION = 439,
     TO = 440,
     UPDATE = 441,
     USER = 442,
     USING = 443,
     VALUES = 444,
     VARCHAR = 445,
     VARBINARY = 446,
     WHERE = 447,
     WHEN = 448,
     WITH = 449,
     WORK = 450,
     PROCESSLIST = 451,
     QUERY = 452,
     CONNECTION = 453,
     WEAK = 454,
     INDEX = 455,
     STORING = 456,
     BLOOMFILTER_JOIN = 457,
     MERGE_JOIN = 458,
     SI = 459,
     SIB = 460,
     SM = 461,
     SEMI = 462,
     NO_QUERY_OPT = 463,
     GATHER = 464,
     STATISTICS = 465,
     HASH_JOIN = 466,
     AUTO_INCREMENT = 467,
     CHUNKSERVER = 468,
     COMPRESS_METHOD = 469,
     CONSISTENT_MODE = 470,
     EXPIRE_INFO = 471,
     GRANTS = 472,
     JOIN_INFO = 473,
     MERGESERVER = 474,
     REPLICA_NUM = 475,
     ROOTSERVER = 476,
     ROW_COUNT = 477,
     SERVER = 478,
     SERVER_IP = 479,
     SERVER_PORT = 480,
     SERVER_TYPE = 481,
     STATUS = 482,
     TABLE_ID = 483,
     TABLET_BLOCK_SIZE = 484,
     TABLET_MAX_SIZE = 485,
     UNLOCKED = 486,
     UPDATESERVER = 487,
     USE_BLOOM_FILTER = 488,
     VARIABLES = 489,
     VERBOSE = 490,
     WARNINGS = 491
=======
     ADD = 289,
     ANY = 290,
     ALL = 291,
     ALTER = 292,
     AS = 293,
     ASC = 294,
     BEGI = 295,
     BIGINT = 296,
     BINARY = 297,
     BOOLEAN = 298,
     BOTH = 299,
     BY = 300,
     CASCADE = 301,
     CASE = 302,
     CHARACTER = 303,
     CLUSTER = 304,
     COMMENT = 305,
     COMMIT = 306,
     CONSISTENT = 307,
     COLUMN = 308,
     COLUMNS = 309,
     CREATE = 310,
     CREATETIME = 311,
     CURRENT_USER = 312,
     CHANGE_OBI = 313,
     SWITCH_CLUSTER = 314,
     DATE = 315,
     DATETIME = 316,
     DEALLOCATE = 317,
     DECIMAL = 318,
     DEFAULT = 319,
     DELETE = 320,
     DESC = 321,
     DESCRIBE = 322,
     DISTINCT = 323,
     DOUBLE = 324,
     DROP = 325,
     DUAL = 326,
     ELSE = 327,
     END = 328,
     END_P = 329,
     ERROR = 330,
     EXECUTE = 331,
     EXISTS = 332,
     EXPLAIN = 333,
     FLOAT = 334,
     FOR = 335,
     FROM = 336,
     FULL = 337,
     FROZEN = 338,
     FORCE = 339,
     GLOBAL = 340,
     GLOBAL_ALIAS = 341,
     GRANT = 342,
     GROUP = 343,
     HAVING = 344,
     HINT_BEGIN = 345,
     HINT_END = 346,
     HOTSPOT = 347,
     IDENTIFIED = 348,
     IF = 349,
     INNER = 350,
     INTEGER = 351,
     INSERT = 352,
     INTO = 353,
     JOIN = 354,
     KEY = 355,
     LEADING = 356,
     LEFT = 357,
     LIMIT = 358,
     LOCAL = 359,
     LOCKED = 360,
     MEDIUMINT = 361,
     MEMORY = 362,
     MODIFYTIME = 363,
     MASTER = 364,
     NUMERIC = 365,
     OFFSET = 366,
     ON = 367,
     ORDER = 368,
     OPTION = 369,
     OUTER = 370,
     PARAMETERS = 371,
     PASSWORD = 372,
     PRECISION = 373,
     PREPARE = 374,
     PRIMARY = 375,
     READ_STATIC = 376,
     REAL = 377,
     RENAME = 378,
     REPLACE = 379,
     RESTRICT = 380,
     PRIVILEGES = 381,
     REVOKE = 382,
     RIGHT = 383,
     ROLLBACK = 384,
     KILL = 385,
     READ_CONSISTENCY = 386,
     SCHEMA = 387,
     SCOPE = 388,
     SELECT = 389,
     SESSION = 390,
     SESSION_ALIAS = 391,
     SET = 392,
     SHOW = 393,
     SMALLINT = 394,
     SNAPSHOT = 395,
     SPFILE = 396,
     START = 397,
     STATIC = 398,
     SYSTEM = 399,
     STRONG = 400,
     SET_MASTER_CLUSTER = 401,
     SET_SLAVE_CLUSTER = 402,
     SLAVE = 403,
     TABLE = 404,
     TABLES = 405,
     THEN = 406,
     TIME = 407,
     TIMESTAMP = 408,
     TINYINT = 409,
     TRAILING = 410,
     TRANSACTION = 411,
     TO = 412,
     UPDATE = 413,
     USER = 414,
     USING = 415,
     VALUES = 416,
     VARCHAR = 417,
     VARBINARY = 418,
     WHERE = 419,
     WHEN = 420,
     WITH = 421,
     WORK = 422,
     PROCESSLIST = 423,
     QUERY = 424,
     CONNECTION = 425,
     WEAK = 426,
     AUTO_INCREMENT = 427,
     CHUNKSERVER = 428,
     COMPRESS_METHOD = 429,
     CONSISTENT_MODE = 430,
     EXPIRE_INFO = 431,
     GRANTS = 432,
     JOIN_INFO = 433,
     MERGESERVER = 434,
     REPLICA_NUM = 435,
     ROOTSERVER = 436,
     ROW_COUNT = 437,
     SERVER = 438,
     SERVER_IP = 439,
     SERVER_PORT = 440,
     SERVER_TYPE = 441,
     STATUS = 442,
     TABLE_ID = 443,
     TABLET_BLOCK_SIZE = 444,
     TABLET_MAX_SIZE = 445,
     UNLOCKED = 446,
     UPDATESERVER = 447,
     USE_BLOOM_FILTER = 448,
     VARIABLES = 449,
     VERBOSE = 450,
     WARNINGS = 451
>>>>>>> refs/remotes/origin/master
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


  struct _ParseNode *node;
  const struct _NonReservedKeyword *non_reserved_keyword;
  int    ival;



} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

#if ! defined YYLTYPE && ! defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
# define yyltype YYLTYPE /* obsolescent; will be withdrawn */
# define YYLTYPE_IS_DECLARED 1
# define YYLTYPE_IS_TRIVIAL 1
#endif


/* Copy the second part of user declarations.  */


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




#ifdef short
# undef short
#endif

#ifdef YYTYPE_UINT8
typedef YYTYPE_UINT8 yytype_uint8;
#else
typedef unsigned char yytype_uint8;
#endif

#ifdef YYTYPE_INT8
typedef YYTYPE_INT8 yytype_int8;
#elif (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
typedef signed char yytype_int8;
#else
typedef short int yytype_int8;
#endif

#ifdef YYTYPE_UINT16
typedef YYTYPE_UINT16 yytype_uint16;
#else
typedef unsigned short int yytype_uint16;
#endif

#ifdef YYTYPE_INT16
typedef YYTYPE_INT16 yytype_int16;
#else
typedef short int yytype_int16;
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif ! defined YYSIZE_T && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned int
# endif
#endif

#define YYSIZE_MAXIMUM ((YYSIZE_T) -1)

#ifndef YY_
<<<<<<< HEAD
# if YYENABLE_NLS
=======
# if defined YYENABLE_NLS && YYENABLE_NLS
>>>>>>> refs/remotes/origin/master
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(msgid) dgettext ("bison-runtime", msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(msgid) msgid
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YYUSE(e) ((void) (e))
#else
# define YYUSE(e) /* empty */
#endif

/* Identity function, used to suppress warnings about constant conditions.  */
#ifndef lint
# define YYID(n) (n)
#else
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static int
YYID (int yyi)
#else
static int
YYID (yyi)
    int yyi;
#endif
{
  return yyi;
}
#endif

#if ! defined yyoverflow || YYERROR_VERBOSE

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
<<<<<<< HEAD
#    if ! defined _ALLOCA_H && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef _STDLIB_H
#      define _STDLIB_H 1
=======
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
>>>>>>> refs/remotes/origin/master
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's `empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (YYID (0))
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
<<<<<<< HEAD
#  if (defined __cplusplus && ! defined _STDLIB_H \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef _STDLIB_H
#    define _STDLIB_H 1
=======
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
	     && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
>>>>>>> refs/remotes/origin/master
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
<<<<<<< HEAD
#   if ! defined malloc && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
=======
#   if ! defined malloc && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
>>>>>>> refs/remotes/origin/master
     || defined __cplusplus || defined _MSC_VER)
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
<<<<<<< HEAD
#   if ! defined free && ! defined _STDLIB_H && (defined __STDC__ || defined __C99__FUNC__ \
=======
#   if ! defined free && ! defined EXIT_SUCCESS && (defined __STDC__ || defined __C99__FUNC__ \
>>>>>>> refs/remotes/origin/master
     || defined __cplusplus || defined _MSC_VER)
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* ! defined yyoverflow || YYERROR_VERBOSE */


#if (! defined yyoverflow \
     && (! defined __cplusplus \
	 || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
	     && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yytype_int16 yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (sizeof (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (sizeof (yytype_int16) + sizeof (YYSTYPE) + sizeof (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

<<<<<<< HEAD
/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif
=======
# define YYCOPY_NEEDED 1
>>>>>>> refs/remotes/origin/master

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)				\
    do									\
      {									\
	YYSIZE_T yynewbytes;						\
	YYCOPY (&yyptr->Stack_alloc, Stack, yysize);			\
	Stack = &yyptr->Stack_alloc;					\
	yynewbytes = yystacksize * sizeof (*Stack) + YYSTACK_GAP_MAXIMUM; \
	yyptr += yynewbytes / sizeof (*yyptr);				\
      }									\
    while (YYID (0))

#endif

<<<<<<< HEAD
/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  207
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   3987

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  248
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  223
/* YYNRULES -- Number of rules.  */
#define YYNRULES  680
/* YYNRULES -- Number of states.  */
#define YYNSTATES  1279

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   491
=======
#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from FROM to TO.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(To, From, Count) \
      __builtin_memcpy (To, From, (Count) * sizeof (*(From)))
#  else
#   define YYCOPY(To, From, Count)		\
      do					\
	{					\
	  YYSIZE_T yyi;				\
	  for (yyi = 0; yyi < (Count); yyi++)	\
	    (To)[yyi] = (From)[yyi];		\
	}					\
      while (YYID (0))
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  161
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   2722

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  208
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  156
/* YYNRULES -- Number of rules.  */
#define YYNRULES  494
/* YYNRULES -- Number of states.  */
#define YYNSTATES  871

/* YYTRANSLATE(YYLEX) -- Bison symbol number corresponding to YYLEX.  */
#define YYUNDEFTOK  2
#define YYMAXUTOK   451
>>>>>>> refs/remotes/origin/master

#define YYTRANSLATE(YYX)						\
  ((unsigned int) (YYX) <= YYMAXUTOK ? yytranslate[YYX] : YYUNDEFTOK)

/* YYTRANSLATE[YYLEX] -- Bison symbol number corresponding to YYLEX.  */
static const yytype_uint8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,    36,     2,     2,
<<<<<<< HEAD
      40,    41,    34,    32,   247,    33,    42,    35,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   246,
=======
      40,    41,    34,    32,   207,    33,    42,    35,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,   206,
>>>>>>> refs/remotes/origin/master
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,    38,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    37,    39,    43,
      44,    45,    46,    47,    48,    49,    50,    51,    52,    53,
      54,    55,    56,    57,    58,    59,    60,    61,    62,    63,
      64,    65,    66,    67,    68,    69,    70,    71,    72,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     114,   115,   116,   117,   118,   119,   120,   121,   122,   123,
     124,   125,   126,   127,   128,   129,   130,   131,   132,   133,
     134,   135,   136,   137,   138,   139,   140,   141,   142,   143,
     144,   145,   146,   147,   148,   149,   150,   151,   152,   153,
     154,   155,   156,   157,   158,   159,   160,   161,   162,   163,
     164,   165,   166,   167,   168,   169,   170,   171,   172,   173,
     174,   175,   176,   177,   178,   179,   180,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     194,   195,   196,   197,   198,   199,   200,   201,   202,   203,
<<<<<<< HEAD
     204,   205,   206,   207,   208,   209,   210,   211,   212,   213,
     214,   215,   216,   217,   218,   219,   220,   221,   222,   223,
     224,   225,   226,   227,   228,   229,   230,   231,   232,   233,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     244,   245
=======
     204,   205
>>>>>>> refs/remotes/origin/master
};

#if YYDEBUG
/* YYPRHS[YYN] -- Index of the first RHS symbol of rule number YYN in
   YYRHS.  */
static const yytype_uint16 yyprhs[] =
{
       0,     0,     3,     6,    10,    12,    14,    16,    18,    20,
      22,    24,    26,    28,    30,    32,    34,    36,    38,    40,
      42,    44,    46,    48,    50,    52,    54,    56,    58,    60,
<<<<<<< HEAD
      62,    64,    66,    68,    70,    72,    74,    76,    78,    80,
      82,    84,    86,    88,    90,    92,    94,    96,    98,   100,
     101,   103,   107,   109,   113,   117,   119,   121,   123,   125,
     127,   129,   131,   133,   135,   139,   141,   143,   147,   153,
     155,   157,   159,   161,   164,   166,   168,   171,   174,   178,
     182,   186,   190,   194,   198,   202,   204,   207,   210,   214,
     218,   222,   226,   230,   234,   238,   242,   246,   250,   254,
     258,   262,   266,   271,   275,   279,   282,   286,   291,   295,
     300,   304,   309,   315,   322,   326,   331,   335,   337,   341,
     347,   349,   350,   352,   355,   360,   363,   364,   369,   374,
     379,   385,   390,   397,   402,   406,   411,   413,   415,   417,
     419,   421,   423,   425,   431,   439,   441,   445,   449,   459,
     463,   466,   467,   471,   473,   477,   478,   480,   486,   488,
     491,   494,   498,   503,   509,   515,   521,   527,   534,   542,
     546,   550,   554,   560,   562,   564,   569,   576,   579,   588,
     592,   593,   595,   599,   601,   607,   611,   613,   615,   617,
     619,   621,   624,   627,   629,   632,   634,   637,   640,   642,
     645,   648,   651,   654,   656,   658,   660,   663,   669,   673,
     674,   678,   679,   681,   682,   686,   687,   691,   692,   695,
     696,   699,   701,   704,   706,   709,   711,   715,   716,   720,
     724,   728,   732,   736,   740,   744,   748,   752,   756,   758,
     759,   764,   768,   773,   774,   777,   779,   783,   789,   796,
     797,   799,   803,   805,   813,   818,   826,   827,   830,   832,
     834,   838,   839,   841,   845,   849,   855,   857,   861,   864,
     866,   870,   874,   876,   879,   883,   888,   894,   903,   905,
     907,   917,   927,   932,   937,   942,   943,   946,   950,   955,
     960,   963,   966,   971,   972,   976,   978,   982,   983,   985,
     988,   990,   992,  1007,  1012,  1018,  1023,  1027,  1032,  1034,
    1036,  1038,  1041,  1042,  1044,  1048,  1050,  1052,  1054,  1056,
    1058,  1060,  1062,  1064,  1066,  1068,  1070,  1072,  1073,  1075,
    1076,  1079,  1083,  1088,  1093,  1098,  1102,  1106,  1110,  1111,
    1115,  1117,  1118,  1122,  1124,  1128,  1131,  1132,  1134,  1136,
    1137,  1140,  1141,  1143,  1145,  1147,  1150,  1154,  1156,  1158,
    1162,  1164,  1168,  1170,  1174,  1177,  1181,  1184,  1186,  1192,
    1194,  1198,  1205,  1211,  1214,  1217,  1220,  1222,  1224,  1227,
    1230,  1232,  1233,  1237,  1239,  1241,  1243,  1245,  1247,  1248,
    1252,  1258,  1264,  1270,  1275,  1280,  1285,  1288,  1293,  1297,
    1301,  1305,  1309,  1313,  1317,  1321,  1325,  1330,  1333,  1334,
    1336,  1339,  1344,  1346,  1348,  1349,  1350,  1353,  1356,  1357,
    1359,  1360,  1362,  1366,  1368,  1372,  1377,  1379,  1381,  1385,
    1387,  1391,  1397,  1404,  1407,  1408,  1412,  1416,  1418,  1422,
    1427,  1429,  1431,  1433,  1434,  1438,  1439,  1442,  1446,  1449,
    1452,  1457,  1458,  1460,  1461,  1463,  1465,  1472,  1474,  1478,
    1481,  1483,  1485,  1488,  1490,  1492,  1494,  1497,  1499,  1501,
    1503,  1505,  1507,  1508,  1510,  1512,  1518,  1521,  1522,  1527,
    1529,  1531,  1533,  1535,  1537,  1540,  1544,  1546,  1550,  1554,
    1558,  1563,  1568,  1574,  1580,  1584,  1588,  1592,  1594,  1596,
    1598,  1600,  1604,  1606,  1610,  1614,  1617,  1618,  1620,  1624,
    1628,  1630,  1632,  1637,  1644,  1646,  1650,  1654,  1659,  1664,
    1670,  1672,  1673,  1675,  1677,  1678,  1682,  1686,  1690,  1693,
    1698,  1706,  1714,  1721,  1728,  1729,  1731,  1733,  1737,  1747,
    1750,  1751,  1755,  1759,  1763,  1764,  1766,  1768,  1770,  1772,
    1776,  1783,  1784,  1786,  1788,  1790,  1792,  1794,  1796,  1798,
    1800,  1802,  1804,  1806,  1808,  1810,  1812,  1814,  1816,  1818,
    1820,  1822,  1824,  1826,  1828,  1830,  1832,  1834,  1836,  1838,
    1840,  1842,  1844,  1846,  1848,  1851,  1858,  1864,  1868,  1870,
    1873,  1877,  1881,  1885,  1889,  1894,  1899,  1904,  1908,  1909,
    1911,  1914,  1916,  1918,  1920,  1922,  1924,  1926,  1928,  1931,
    1934,  1937,  1940,  1943,  1946,  1949,  1952,  1955,  1958,  1961,
    1964,  1967,  1970,  1973,  1975,  1977,  1978,  1980,  1983,  1985,
    1987,  1989,  1991,  1993,  1995,  1998,  2001,  2004,  2007,  2010,
    2013,  2016,  2019,  2022,  2025,  2028,  2031,  2034,  2037,  2040,
    2042,  2044,  2049,  2056,  2062,  2066,  2076,  2085,  2094,  2102,
    2104,  2107,  2112,  2115,  2123,  2126,  2128,  2133,  2134,  2137,
    2143,  2155,  2168,  2176,  2178,  2181,  2186,  2193,  2199,  2205,
    2209
=======
      62,    63,    65,    69,    71,    75,    79,    81,    83,    85,
      87,    89,    91,    93,    95,    97,   101,   103,   105,   109,
     115,   117,   119,   121,   123,   126,   128,   131,   134,   138,
     142,   146,   150,   154,   158,   162,   164,   167,   170,   174,
     178,   182,   186,   190,   194,   198,   202,   206,   210,   214,
     218,   222,   226,   231,   235,   239,   242,   246,   251,   255,
     260,   264,   269,   275,   282,   286,   291,   295,   297,   301,
     307,   309,   310,   312,   315,   320,   323,   324,   329,   335,
     340,   347,   352,   356,   361,   363,   365,   367,   369,   371,
     373,   375,   381,   389,   391,   395,   399,   408,   412,   413,
     415,   419,   421,   427,   431,   433,   435,   437,   439,   441,
     444,   447,   449,   452,   454,   457,   460,   462,   465,   468,
     471,   474,   476,   478,   480,   483,   489,   493,   494,   498,
     499,   501,   502,   506,   507,   511,   512,   515,   516,   519,
     521,   524,   526,   529,   531,   535,   536,   540,   544,   548,
     552,   556,   560,   564,   568,   572,   576,   578,   579,   584,
     585,   588,   590,   594,   602,   607,   615,   616,   619,   621,
     623,   627,   628,   630,   634,   638,   644,   646,   650,   653,
     655,   659,   663,   665,   668,   672,   677,   683,   692,   694,
     696,   706,   711,   716,   721,   722,   725,   729,   734,   739,
     742,   745,   750,   751,   755,   757,   761,   762,   764,   767,
     769,   771,   776,   780,   783,   784,   786,   788,   790,   792,
     794,   796,   797,   799,   800,   803,   807,   812,   817,   822,
     826,   830,   834,   835,   839,   841,   842,   846,   848,   852,
     855,   856,   858,   860,   861,   864,   865,   867,   869,   871,
     874,   878,   880,   882,   886,   888,   892,   894,   898,   901,
     905,   908,   910,   916,   918,   922,   929,   935,   938,   941,
     944,   946,   948,   949,   953,   955,   957,   959,   961,   963,
     964,   968,   974,   980,   985,   990,   995,   998,  1003,  1007,
    1011,  1015,  1019,  1023,  1027,  1031,  1036,  1039,  1040,  1042,
    1045,  1050,  1052,  1054,  1055,  1056,  1059,  1062,  1063,  1065,
    1066,  1068,  1072,  1074,  1078,  1083,  1085,  1087,  1091,  1093,
    1097,  1103,  1110,  1113,  1114,  1118,  1122,  1124,  1128,  1133,
    1135,  1137,  1139,  1140,  1144,  1145,  1148,  1152,  1155,  1158,
    1163,  1164,  1166,  1167,  1169,  1171,  1178,  1180,  1184,  1187,
    1189,  1191,  1194,  1196,  1198,  1201,  1203,  1205,  1207,  1209,
    1211,  1212,  1214,  1216,  1222,  1225,  1226,  1231,  1233,  1235,
    1237,  1239,  1241,  1244,  1246,  1250,  1254,  1258,  1263,  1268,
    1274,  1280,  1284,  1286,  1288,  1290,  1294,  1297,  1298,  1300,
    1304,  1308,  1310,  1312,  1317,  1324,  1326,  1330,  1334,  1339,
    1344,  1350,  1352,  1353,  1355,  1357,  1358,  1362,  1366,  1370,
    1373,  1378,  1386,  1394,  1401,  1408,  1409,  1411,  1413,  1417,
    1427,  1430,  1431,  1435,  1439,  1443,  1444,  1446,  1448,  1450,
    1452,  1456,  1463,  1464,  1466,  1468,  1470,  1472,  1474,  1476,
    1478,  1480,  1482,  1484,  1486,  1488,  1490,  1492,  1494,  1496,
    1498,  1500,  1502,  1504,  1506,  1508,  1510,  1512,  1514,  1516,
    1518,  1520,  1522,  1524,  1526
>>>>>>> refs/remotes/origin/master
};

/* YYRHS -- A `-1'-separated list of the rules' RHS.  */
static const yytype_int16 yyrhs[] =
{
<<<<<<< HEAD
     249,     0,    -1,   250,   107,    -1,   250,   246,   251,    -1,
     251,    -1,   443,    -1,   468,    -1,   470,    -1,   469,    -1,
     280,    -1,   282,    -1,   283,    -1,   298,    -1,   291,    -1,
     292,    -1,   293,    -1,   294,    -1,   296,    -1,   297,    -1,
     331,    -1,   324,    -1,   299,    -1,   274,    -1,   271,    -1,
     270,    -1,   317,    -1,   321,    -1,   368,    -1,   371,    -1,
     408,    -1,   411,    -1,   419,    -1,   424,    -1,   430,    -1,
     422,    -1,   379,    -1,   384,    -1,   386,    -1,   388,    -1,
     391,    -1,   401,    -1,   406,    -1,   395,    -1,   396,    -1,
     397,    -1,   398,    -1,   372,    -1,   315,    -1,   320,    -1,
      -1,   257,    -1,   252,   247,   257,    -1,   438,    -1,   439,
      42,   438,    -1,   439,    42,    34,    -1,     4,    -1,     6,
      -1,     5,    -1,     9,    -1,     8,    -1,    10,    -1,    12,
      -1,    14,    -1,    13,    -1,   173,    42,   438,    -1,   253,
      -1,   254,    -1,    40,   257,    41,    -1,    40,   252,   247,
     257,    41,    -1,   259,    -1,   265,    -1,   266,    -1,   332,
      -1,   110,   332,    -1,   264,    -1,   255,    -1,    32,   256,
      -1,    33,   256,    -1,   256,    32,   256,    -1,   256,    33,
     256,    -1,   256,    34,   256,    -1,   256,    35,   256,    -1,
     256,    36,   256,    -1,   256,    38,   256,    -1,   256,    37,
     256,    -1,   255,    -1,    32,   257,    -1,    33,   257,    -1,
     257,    32,   257,    -1,   257,    33,   257,    -1,   257,    34,
     257,    -1,   257,    35,   257,    -1,   257,    36,   257,    -1,
     257,    38,   257,    -1,   257,    37,   257,    -1,   257,    26,
     257,    -1,   257,    25,   257,    -1,   257,    24,   257,    -1,
     257,    22,   257,    -1,   257,    23,   257,    -1,   257,    21,
     257,    -1,   257,    28,   257,    -1,   257,    20,    28,   257,
      -1,   257,    19,   257,    -1,   257,    18,   257,    -1,    20,
     257,    -1,   257,    31,    10,    -1,   257,    31,    20,    10,
      -1,   257,    31,     8,    -1,   257,    31,    20,     8,    -1,
     257,    31,    11,    -1,   257,    31,    20,    11,    -1,   257,
      29,   256,    19,   256,    -1,   257,    20,    29,   256,    19,
     256,    -1,   257,    30,   258,    -1,   257,    20,    30,   258,
      -1,   257,    27,   257,    -1,   332,    -1,    40,   252,    41,
      -1,    79,   260,   261,   263,   106,    -1,   257,    -1,    -1,
     262,    -1,   261,   262,    -1,   202,   257,   188,   257,    -1,
     105,   257,    -1,    -1,    14,    40,     5,    41,    -1,    14,
      40,    14,    41,    -1,   440,    40,    34,    41,    -1,   440,
      40,   269,   257,    41,    -1,   440,    40,   252,    41,    -1,
     440,    40,   257,    70,   304,    41,    -1,   440,    40,   351,
      41,    -1,   440,    40,    41,    -1,   267,    40,   268,    41,
      -1,   231,    -1,   331,    -1,   324,    -1,   271,    -1,   270,
      -1,    68,    -1,   100,    -1,    97,   114,   364,   338,   325,
      -1,   195,   340,   364,   174,   272,   338,   325,    -1,   273,
      -1,   272,   247,   273,    -1,   438,    24,   257,    -1,    87,
     209,   300,   364,   147,   364,   275,   276,   278,    -1,    40,
     328,    41,    -1,   210,   277,    -1,    -1,    40,   328,    41,
      -1,   279,    -1,   278,   247,   279,    -1,    -1,   313,    -1,
      44,   281,    56,   113,   331,    -1,   441,    -1,    57,   281,
      -1,    58,   281,    -1,    58,   281,    60,    -1,    58,   281,
     131,   421,    -1,    58,   281,    60,   131,   421,    -1,    58,
     281,    62,   131,   421,    -1,    58,   281,    61,   131,   421,
      -1,    58,   281,    63,   131,   421,    -1,    58,   281,    64,
       5,   131,   421,    -1,    58,   281,   295,    65,     5,   131,
     421,    -1,    58,   281,    61,    -1,    58,   281,    62,    -1,
      58,   281,    63,    -1,    58,   281,   295,    65,     5,    -1,
      60,    -1,    61,    -1,    58,   281,    64,     5,    -1,    58,
     281,   114,     5,   194,     5,    -1,    59,   281,    -1,    87,
     186,   300,   364,    40,   301,    41,   312,    -1,   127,    20,
     110,    -1,    -1,   302,    -1,   301,   247,   302,    -1,   303,
      -1,   155,   134,    40,   328,    41,    -1,   438,   304,   310,
      -1,   191,    -1,   176,    -1,   141,    -1,   129,    -1,    73,
      -1,    95,   305,    -1,   145,   305,    -1,    75,    -1,   112,
     306,    -1,   157,    -1,   101,   307,    -1,   190,   308,    -1,
      93,    -1,    80,   309,    -1,    74,   309,    -1,   199,   309,
      -1,   200,   309,    -1,    88,    -1,   143,    -1,    92,    -1,
     189,   308,    -1,    40,     5,   247,     5,    41,    -1,    40,
       5,    41,    -1,    -1,    40,     5,    41,    -1,    -1,   153,
      -1,    -1,    40,     5,    41,    -1,    -1,    40,     5,    41,
      -1,    -1,   310,   311,    -1,    -1,    20,    10,    -1,    10,
      -1,    96,   254,    -1,   221,    -1,   155,   134,    -1,   313,
      -1,   312,   247,   313,    -1,    -1,   227,   314,     4,    -1,
     225,   314,     4,    -1,   239,   314,     5,    -1,   238,   314,
       5,    -1,   237,   314,     5,    -1,   229,   314,     5,    -1,
     223,   314,     4,    -1,   242,   314,     8,    -1,   224,   314,
     180,    -1,    82,   314,     4,    -1,    24,    -1,    -1,   218,
     219,   364,   316,    -1,    40,   328,    41,    -1,   102,   186,
     318,   319,    -1,    -1,   127,   110,    -1,   363,    -1,   319,
     247,   363,    -1,   104,   186,   318,   319,   434,    -1,   102,
     209,   318,   322,   147,   323,    -1,    -1,   364,    -1,   322,
     247,   364,    -1,   364,    -1,   326,   131,   364,   327,   198,
     329,   325,    -1,   326,   131,   364,   331,    -1,   326,   131,
     364,    40,   328,    41,   331,    -1,    -1,   202,   257,    -1,
     159,    -1,   130,    -1,    40,   328,    41,    -1,    -1,   438,
      -1,   328,   247,   438,    -1,    40,   330,    41,    -1,   329,
     247,    40,   330,    41,    -1,   257,    -1,   330,   247,   257,
      -1,   333,   325,    -1,   332,    -1,    40,   333,    41,    -1,
      40,   332,    41,    -1,   334,    -1,   337,   350,    -1,   335,
     354,   350,    -1,   335,   353,   339,   350,    -1,   171,   340,
     359,   361,   349,    -1,   171,   340,   359,   361,   114,   103,
     338,   349,    -1,   337,    -1,   332,    -1,   171,   340,   361,
     131,   421,   114,   362,   338,   350,    -1,   171,   340,   359,
     361,   114,   362,   338,   352,   358,    -1,   335,    16,   359,
     335,    -1,   335,    17,   359,   335,    -1,   335,    15,   359,
     335,    -1,    -1,   201,   257,    -1,   201,     7,   257,    -1,
     137,   348,   146,   348,    -1,   146,   348,   137,   348,    -1,
     137,   348,    -1,   146,   348,    -1,   137,   348,   247,   348,
      -1,    -1,   123,   341,   124,    -1,   342,    -1,   341,   247,
     342,    -1,    -1,   343,    -1,   342,   343,    -1,   156,    -1,
     125,    -1,   133,    40,   364,   247,   364,   247,   364,    42,
     364,   247,   364,    42,   364,    41,    -1,   166,    40,   347,
      41,    -1,   209,    40,   364,   364,    41,    -1,     3,    40,
       3,    41,    -1,    40,   344,    41,    -1,   132,    40,   345,
      41,    -1,   167,    -1,   168,    -1,   217,    -1,   344,   247,
      -1,    -1,   346,    -1,   345,   247,   346,    -1,   211,    -1,
     212,    -1,   213,    -1,   214,    -1,   215,    -1,   220,    -1,
     208,    -1,   182,    -1,   180,    -1,   116,    -1,     5,    -1,
      12,    -1,    -1,   339,    -1,    -1,   113,   195,    -1,   257,
     114,   257,    -1,    76,   257,   114,   257,    -1,   135,   257,
     114,   257,    -1,   192,   257,   114,   257,    -1,    76,   114,
     257,    -1,   135,   114,   257,    -1,   192,   114,   257,    -1,
      -1,   121,    77,   252,    -1,   354,    -1,    -1,   148,    77,
     355,    -1,   356,    -1,   355,   247,   356,    -1,   257,   357,
      -1,    -1,    71,    -1,    98,    -1,    -1,   122,   257,    -1,
      -1,    68,    -1,   100,    -1,   257,    -1,   257,   441,    -1,
     257,    70,   441,    -1,    34,    -1,   360,    -1,   361,   247,
     360,    -1,   363,    -1,   362,   247,   363,    -1,   364,    -1,
     364,    70,   439,    -1,   364,   439,    -1,   332,    70,   439,
      -1,   332,   439,    -1,   365,    -1,    40,   365,    41,    70,
     439,    -1,   439,    -1,    40,   365,    41,    -1,   363,   366,
     132,   363,   147,   257,    -1,   363,   132,   363,   147,   257,
      -1,   115,   367,    -1,   136,   367,    -1,   163,   367,    -1,
     128,    -1,   216,    -1,   136,   216,    -1,   163,   216,    -1,
     150,    -1,    -1,   111,   370,   369,    -1,   331,    -1,   270,
      -1,   324,    -1,   271,    -1,   244,    -1,    -1,   175,   187,
     376,    -1,   175,   209,   147,   364,   376,    -1,   175,    86,
     114,   364,   376,    -1,   175,    86,    30,   364,   376,    -1,
     175,   186,   236,   376,    -1,   175,   232,   236,   376,    -1,
     175,   375,   243,   376,    -1,   175,   169,    -1,   175,    87,
     186,   364,    -1,    99,   364,   377,    -1,    98,   364,   377,
      -1,   175,   245,   373,    -1,   175,   265,   245,    -1,   175,
     226,   374,    -1,   175,   151,   376,    -1,   175,   378,   205,
      -1,   140,   186,   364,    -1,   137,     5,   247,     5,    -1,
     137,     5,    -1,    -1,   387,    -1,   113,    89,    -1,   113,
      89,    40,    41,    -1,   118,    -1,   172,    -1,    -1,    -1,
      28,     4,    -1,   201,   257,    -1,    -1,     4,    -1,    -1,
     115,    -1,    87,   196,   380,    -1,   381,    -1,   380,   247,
     381,    -1,   382,   126,    77,   383,    -1,     4,    -1,     4,
      -1,   102,   196,   385,    -1,   382,    -1,   385,   247,   382,
      -1,   174,   152,   387,    24,   383,    -1,    69,   196,   382,
     126,    77,   383,    -1,   113,   382,    -1,    -1,   158,   196,
     390,    -1,   382,   194,   382,    -1,   389,    -1,   390,   247,
     389,    -1,    69,   196,   382,   392,    -1,   139,    -1,   240,
      -1,   204,    -1,    -1,   203,    84,   177,    -1,    -1,    72,
     393,    -1,   179,   193,   394,    -1,    83,   393,    -1,   164,
     393,    -1,   165,   399,   400,     5,    -1,    -1,   118,    -1,
      -1,   206,    -1,   207,    -1,   120,   402,   147,   405,   194,
     385,    -1,   403,    -1,   402,   247,   403,    -1,    68,   404,
      -1,    69,    -1,    87,    -1,    87,   196,    -1,    97,    -1,
     102,    -1,   104,    -1,   120,   149,    -1,   130,    -1,   195,
      -1,   171,    -1,   159,    -1,   161,    -1,    -1,    34,    -1,
     439,    -1,   162,   402,   407,   114,   385,    -1,   147,   405,
      -1,    -1,   154,   409,   114,   410,    -1,   441,    -1,   331,
      -1,   324,    -1,   271,    -1,   270,    -1,   174,   412,    -1,
     174,   174,   414,    -1,   413,    -1,   412,   247,   413,    -1,
      14,   415,   257,    -1,   438,   415,   257,    -1,   118,   438,
     415,   257,    -1,   172,   438,   415,   257,    -1,   119,    42,
     438,   415,   257,    -1,   173,    42,   438,   415,   257,    -1,
      13,   415,   257,    -1,   264,   415,   257,    -1,    14,   415,
     417,    -1,   194,    -1,    24,    -1,    14,    -1,   264,    -1,
      40,   418,    41,    -1,   254,    -1,   418,   247,   254,    -1,
     109,   409,   420,    -1,   197,   421,    -1,    -1,   416,    -1,
     421,   247,   416,    -1,   423,   154,   409,    -1,    94,    -1,
     102,    -1,    69,   186,   364,   425,    -1,    69,   186,   364,
     158,   194,   364,    -1,   426,    -1,   425,   247,   426,    -1,
      66,   427,   303,    -1,   102,   427,   438,   428,    -1,    69,
     427,   438,   429,    -1,   158,   427,   438,   194,   441,    -1,
      85,    -1,    -1,    78,    -1,   160,    -1,    -1,   174,    20,
      10,    -1,   102,    20,    10,    -1,   174,    96,   254,    -1,
     102,    96,    -1,    69,   181,   174,   432,    -1,    69,   181,
     431,    90,   144,    24,     4,    -1,    69,   181,   431,    91,
     144,    24,     4,    -1,    69,   181,   183,   144,    24,     4,
      -1,    69,   181,   184,   185,    24,     4,    -1,    -1,   117,
      -1,   433,    -1,   432,   247,   433,    -1,   438,    24,   254,
     434,   435,   235,    24,   436,   437,    -1,    82,     4,    -1,
      -1,   170,    24,   142,    -1,   170,    24,   178,    -1,   170,
      24,    76,    -1,    -1,   230,    -1,   241,    -1,   222,    -1,
     228,    -1,    81,    24,     5,    -1,   233,    24,     4,   234,
      24,     5,    -1,    -1,     3,    -1,   442,    -1,     3,    -1,
     442,    -1,     3,    -1,     3,    -1,   442,    -1,   221,    -1,
     222,    -1,   223,    -1,   224,    -1,   225,    -1,   226,    -1,
     227,    -1,   228,    -1,   229,    -1,   230,    -1,   231,    -1,
     232,    -1,   233,    -1,   234,    -1,   235,    -1,   236,    -1,
     238,    -1,   237,    -1,   239,    -1,   240,    -1,   241,    -1,
     242,    -1,   243,    -1,   244,    -1,   245,    -1,   444,   447,
      -1,    87,    43,     3,    40,   445,    41,    -1,    87,    43,
       3,    40,    41,    -1,   445,   247,   446,    -1,   446,    -1,
      14,   304,    -1,    30,    14,   304,    -1,    46,    14,   304,
      -1,    47,    14,   304,    -1,    14,   304,    54,    -1,    30,
      14,   304,    54,    -1,    46,    14,   304,    54,    -1,    47,
      14,   304,    54,    -1,    72,   448,   106,    -1,    -1,   449,
      -1,   449,   450,    -1,   450,    -1,   456,    -1,   460,    -1,
     465,    -1,   464,    -1,   454,    -1,   455,    -1,   336,   246,
      -1,   324,   246,    -1,   271,   246,    -1,   270,   246,    -1,
     331,   246,    -1,   280,   246,    -1,   282,   246,    -1,   298,
     246,    -1,   284,   246,    -1,   285,   246,    -1,   286,   246,
      -1,   288,   246,    -1,   287,   246,    -1,   289,   246,    -1,
     290,   246,    -1,   467,    -1,   466,    -1,    -1,   452,    -1,
     452,   453,    -1,   453,    -1,   456,    -1,   460,    -1,   465,
      -1,   464,    -1,   455,    -1,   336,   246,    -1,   324,   246,
      -1,   271,   246,    -1,   270,   246,    -1,   331,   246,    -1,
     280,   246,    -1,   282,   246,    -1,   298,   246,    -1,   284,
     246,    -1,   285,   246,    -1,   286,   246,    -1,   288,   246,
      -1,   287,   246,    -1,   289,   246,    -1,   290,   246,    -1,
     467,    -1,   466,    -1,    44,   421,   304,   246,    -1,    44,
     421,   304,    96,   254,   246,    -1,    44,   421,   304,    54,
     246,    -1,   174,   412,   246,    -1,   127,   257,   188,   451,
     457,   459,   106,   127,   246,    -1,   127,   257,   188,   451,
     457,   106,   127,   246,    -1,   127,   257,   188,   451,   459,
     106,   127,   246,    -1,   127,   257,   188,   451,   106,   127,
     246,    -1,   458,    -1,   457,   458,    -1,    45,   257,   188,
     451,    -1,   105,   451,    -1,    79,   257,   461,   463,   106,
      79,   246,    -1,   461,   462,    -1,   462,    -1,   202,   257,
     188,   451,    -1,    -1,   105,   451,    -1,    49,   451,   106,
      49,   246,    -1,   113,    14,    30,   256,   194,   256,    49,
     451,   106,    49,   246,    -1,   113,    14,    30,    55,   256,
     194,   256,    49,   451,   106,    49,   246,    -1,    48,   257,
      52,   451,   106,    48,   246,    -1,   246,    -1,    50,   246,
      -1,    50,   202,   257,   246,    -1,    53,     3,    40,   252,
      41,   340,    -1,    53,     3,    40,    41,   340,    -1,   102,
      43,   127,   110,     3,    -1,   102,    43,     3,    -1,   175,
      43,     3,    -1
=======
     209,     0,    -1,   210,    83,    -1,   210,   206,   211,    -1,
     211,    -1,   259,    -1,   252,    -1,   233,    -1,   230,    -1,
     229,    -1,   249,    -1,   293,    -1,   296,    -1,   332,    -1,
     335,    -1,   340,    -1,   345,    -1,   351,    -1,   343,    -1,
     303,    -1,   308,    -1,   310,    -1,   312,    -1,   315,    -1,
     325,    -1,   330,    -1,   319,    -1,   320,    -1,   321,    -1,
     322,    -1,    -1,   217,    -1,   212,   207,   217,    -1,   359,
      -1,   360,    42,   359,    -1,   360,    42,    34,    -1,     4,
      -1,     6,    -1,     5,    -1,     9,    -1,     8,    -1,    10,
      -1,    12,    -1,    14,    -1,    13,    -1,   145,    42,   359,
      -1,   213,    -1,   214,    -1,    40,   217,    41,    -1,    40,
     212,   207,   217,    41,    -1,   219,    -1,   224,    -1,   225,
      -1,   260,    -1,    86,   260,    -1,   215,    -1,    32,   216,
      -1,    33,   216,    -1,   216,    32,   216,    -1,   216,    33,
     216,    -1,   216,    34,   216,    -1,   216,    35,   216,    -1,
     216,    36,   216,    -1,   216,    38,   216,    -1,   216,    37,
     216,    -1,   215,    -1,    32,   217,    -1,    33,   217,    -1,
     217,    32,   217,    -1,   217,    33,   217,    -1,   217,    34,
     217,    -1,   217,    35,   217,    -1,   217,    36,   217,    -1,
     217,    38,   217,    -1,   217,    37,   217,    -1,   217,    26,
     217,    -1,   217,    25,   217,    -1,   217,    24,   217,    -1,
     217,    22,   217,    -1,   217,    23,   217,    -1,   217,    21,
     217,    -1,   217,    28,   217,    -1,   217,    20,    28,   217,
      -1,   217,    19,   217,    -1,   217,    18,   217,    -1,    20,
     217,    -1,   217,    31,    10,    -1,   217,    31,    20,    10,
      -1,   217,    31,     8,    -1,   217,    31,    20,     8,    -1,
     217,    31,    11,    -1,   217,    31,    20,    11,    -1,   217,
      29,   216,    19,   216,    -1,   217,    20,    29,   216,    19,
     216,    -1,   217,    30,   218,    -1,   217,    20,    30,   218,
      -1,   217,    27,   217,    -1,   260,    -1,    40,   212,    41,
      -1,    56,   220,   221,   223,    82,    -1,   217,    -1,    -1,
     222,    -1,   221,   222,    -1,   174,   217,   160,   217,    -1,
      81,   217,    -1,    -1,   361,    40,    34,    41,    -1,   361,
      40,   228,   217,    41,    -1,   361,    40,   212,    41,    -1,
     361,    40,   217,    47,   238,    41,    -1,   361,    40,   276,
      41,    -1,   361,    40,    41,    -1,   226,    40,   227,    41,
      -1,   191,    -1,   259,    -1,   252,    -1,   230,    -1,   229,
      -1,    45,    -1,    77,    -1,    74,    90,   289,   265,   253,
      -1,   167,   267,   289,   146,   231,   265,   253,    -1,   232,
      -1,   231,   207,   232,    -1,   359,    24,   217,    -1,    64,
     158,   234,   289,    40,   235,    41,   246,    -1,   103,    20,
      86,    -1,    -1,   236,    -1,   235,   207,   236,    -1,   237,
      -1,   129,   109,    40,   256,    41,    -1,   359,   238,   244,
      -1,   163,    -1,   148,    -1,   115,    -1,   105,    -1,    50,
      -1,    72,   239,    -1,   119,   239,    -1,    52,    -1,    88,
     240,    -1,   131,    -1,    78,   241,    -1,   162,   242,    -1,
      70,    -1,    57,   243,    -1,    51,   243,    -1,   171,   243,
      -1,   172,   243,    -1,    65,    -1,   117,    -1,    69,    -1,
     161,   242,    -1,    40,     5,   207,     5,    41,    -1,    40,
       5,    41,    -1,    -1,    40,     5,    41,    -1,    -1,   127,
      -1,    -1,    40,     5,    41,    -1,    -1,    40,     5,    41,
      -1,    -1,   244,   245,    -1,    -1,    20,    10,    -1,    10,
      -1,    73,   214,    -1,   181,    -1,   129,   109,    -1,   247,
      -1,   246,   207,   247,    -1,    -1,   187,   248,     4,    -1,
     185,   248,     4,    -1,   199,   248,     5,    -1,   198,   248,
       5,    -1,   197,   248,     5,    -1,   189,   248,     5,    -1,
     183,   248,     4,    -1,   202,   248,     8,    -1,   184,   248,
     152,    -1,    59,   248,     4,    -1,    24,    -1,    -1,    79,
     158,   250,   251,    -1,    -1,   103,    86,    -1,   288,    -1,
     251,   207,   288,    -1,   254,   107,   289,   255,   170,   257,
     253,    -1,   254,   107,   289,   259,    -1,   254,   107,   289,
      40,   256,    41,   259,    -1,    -1,   174,   217,    -1,   133,
      -1,   106,    -1,    40,   256,    41,    -1,    -1,   359,    -1,
     256,   207,   359,    -1,    40,   258,    41,    -1,   257,   207,
      40,   258,    41,    -1,   217,    -1,   258,   207,   217,    -1,
     261,   253,    -1,   260,    -1,    40,   261,    41,    -1,    40,
     260,    41,    -1,   262,    -1,   264,   275,    -1,   263,   279,
     275,    -1,   263,   278,   266,   275,    -1,   143,   267,   284,
     286,   274,    -1,   143,   267,   284,   286,    90,    80,   265,
     274,    -1,   264,    -1,   260,    -1,   143,   267,   284,   286,
      90,   287,   265,   277,   283,    -1,   263,    16,   284,   263,
      -1,   263,    17,   284,   263,    -1,   263,    15,   284,   263,
      -1,    -1,   173,   217,    -1,   173,     7,   217,    -1,   112,
     273,   120,   273,    -1,   120,   273,   112,   273,    -1,   112,
     273,    -1,   120,   273,    -1,   112,   273,   207,   273,    -1,
      -1,    99,   268,   100,    -1,   269,    -1,   268,   207,   269,
      -1,    -1,   270,    -1,   269,   270,    -1,   130,    -1,   101,
      -1,   140,    40,   272,    41,    -1,    40,   271,    41,    -1,
     271,   207,    -1,    -1,   180,    -1,   154,    -1,   152,    -1,
      92,    -1,     5,    -1,    12,    -1,    -1,   266,    -1,    -1,
      89,   167,    -1,   217,    90,   217,    -1,    53,   217,    90,
     217,    -1,   110,   217,    90,   217,    -1,   164,   217,    90,
     217,    -1,    53,    90,   217,    -1,   110,    90,   217,    -1,
     164,    90,   217,    -1,    -1,    97,    54,   212,    -1,   279,
      -1,    -1,   122,    54,   280,    -1,   281,    -1,   280,   207,
     281,    -1,   217,   282,    -1,    -1,    48,    -1,    75,    -1,
      -1,    98,   217,    -1,    -1,    45,    -1,    77,    -1,   217,
      -1,   217,   362,    -1,   217,    47,   362,    -1,    34,    -1,
     285,    -1,   286,   207,   285,    -1,   288,    -1,   287,   207,
     288,    -1,   289,    -1,   289,    47,   360,    -1,   289,   360,
      -1,   260,    47,   360,    -1,   260,   360,    -1,   290,    -1,
      40,   290,    41,    47,   360,    -1,   360,    -1,    40,   290,
      41,    -1,   288,   291,   108,   288,   121,   217,    -1,   288,
     108,   288,   121,   217,    -1,    91,   292,    -1,   111,   292,
      -1,   137,   292,    -1,   104,    -1,   124,    -1,    -1,    87,
     295,   294,    -1,   259,    -1,   229,    -1,   252,    -1,   230,
      -1,   204,    -1,    -1,   147,   159,   300,    -1,   147,    63,
      90,   289,   300,    -1,   147,    63,    30,   289,   300,    -1,
     147,   158,   196,   300,    -1,   147,   192,   196,   300,    -1,
     147,   299,   203,   300,    -1,   147,   141,    -1,   147,    64,
     158,   289,    -1,    76,   289,   301,    -1,    75,   289,   301,
      -1,   147,   205,   297,    -1,   147,   224,   205,    -1,   147,
     186,   298,    -1,   147,   125,   300,    -1,   147,   302,   177,
      -1,   112,     5,   207,     5,    -1,   112,     5,    -1,    -1,
     311,    -1,    89,    66,    -1,    89,    66,    40,    41,    -1,
      94,    -1,   144,    -1,    -1,    -1,    28,     4,    -1,   173,
     217,    -1,    -1,     4,    -1,    -1,    91,    -1,    64,   168,
     304,    -1,   305,    -1,   304,   207,   305,    -1,   306,   102,
      54,   307,    -1,     4,    -1,     4,    -1,    79,   168,   309,
      -1,   306,    -1,   309,   207,   306,    -1,   146,   126,   311,
      24,   307,    -1,    46,   168,   306,   102,    54,   307,    -1,
      89,   306,    -1,    -1,   132,   168,   314,    -1,   306,   166,
     306,    -1,   313,    -1,   314,   207,   313,    -1,    46,   168,
     306,   316,    -1,   114,    -1,   200,    -1,   176,    -1,    -1,
     175,    61,   149,    -1,    -1,    49,   317,    -1,   151,   165,
     318,    -1,    60,   317,    -1,   138,   317,    -1,   139,   323,
     324,     5,    -1,    -1,    94,    -1,    -1,   178,    -1,   179,
      -1,    96,   326,   121,   329,   166,   309,    -1,   327,    -1,
     326,   207,   327,    -1,    45,   328,    -1,    46,    -1,    64,
      -1,    64,   168,    -1,    74,    -1,    79,    -1,    96,   123,
      -1,   106,    -1,   167,    -1,   143,    -1,   133,    -1,   135,
      -1,    -1,    34,    -1,   360,    -1,   136,   326,   331,    90,
     309,    -1,   121,   329,    -1,    -1,   128,   333,    90,   334,
      -1,   362,    -1,   259,    -1,   252,    -1,   230,    -1,   229,
      -1,   146,   336,    -1,   337,    -1,   336,   207,   337,    -1,
      14,   338,   217,    -1,   359,   338,   217,    -1,    94,   359,
     338,   217,    -1,   144,   359,   338,   217,    -1,    95,    42,
     359,   338,   217,    -1,   145,    42,   359,   338,   217,    -1,
      13,   338,   217,    -1,   166,    -1,    24,    -1,    14,    -1,
      85,   333,   341,    -1,   169,   342,    -1,    -1,   339,    -1,
     342,   207,   339,    -1,   344,   128,   333,    -1,    71,    -1,
      79,    -1,    46,   158,   289,   346,    -1,    46,   158,   289,
     132,   166,   289,    -1,   347,    -1,   346,   207,   347,    -1,
      43,   348,   237,    -1,    79,   348,   359,   349,    -1,    46,
     348,   359,   350,    -1,   132,   348,   359,   166,   362,    -1,
      62,    -1,    -1,    55,    -1,   134,    -1,    -1,   146,    20,
      10,    -1,    79,    20,    10,    -1,   146,    73,   214,    -1,
      79,    73,    -1,    46,   153,   146,   353,    -1,    46,   153,
     352,    67,   118,    24,     4,    -1,    46,   153,   352,    68,
     118,    24,     4,    -1,    46,   153,   155,   118,    24,     4,
      -1,    46,   153,   156,   157,    24,     4,    -1,    -1,    93,
      -1,   354,    -1,   353,   207,   354,    -1,   359,    24,   214,
     355,   356,   195,    24,   357,   358,    -1,    59,     4,    -1,
      -1,   142,    24,   116,    -1,   142,    24,   150,    -1,   142,
      24,    53,    -1,    -1,   190,    -1,   201,    -1,   182,    -1,
     188,    -1,    58,    24,     5,    -1,   193,    24,     4,   194,
      24,     5,    -1,    -1,     3,    -1,   363,    -1,     3,    -1,
     363,    -1,     3,    -1,     3,    -1,   363,    -1,   181,    -1,
     182,    -1,   183,    -1,   184,    -1,   185,    -1,   186,    -1,
     187,    -1,   188,    -1,   189,    -1,   190,    -1,   191,    -1,
     192,    -1,   193,    -1,   194,    -1,   195,    -1,   196,    -1,
     198,    -1,   197,    -1,   199,    -1,   200,    -1,   201,    -1,
     202,    -1,   203,    -1,   204,    -1,   205,    -1
>>>>>>> refs/remotes/origin/master
};

/* YYRLINE[YYN] -- source line where rule number YYN was defined.  */
static const yytype_uint16 yyrline[] =
{
<<<<<<< HEAD
       0,   277,   277,   286,   293,   300,   301,   302,   303,   305,
     306,   307,   308,   309,   311,   312,   313,   314,   315,   317,
     318,   319,   320,   321,   322,   323,   324,   325,   326,   327,
     328,   329,   330,   331,   332,   333,   334,   335,   336,   337,
     338,   339,   340,   341,   342,   343,   344,   346,   349,   351,
     361,   365,   372,   374,   380,   389,   390,   391,   392,   393,
     394,   395,   396,   397,   398,   402,   404,   406,   408,   414,
     422,   426,   430,   434,   438,   446,   447,   451,   455,   456,
     457,   458,   459,   460,   461,   464,   465,   469,   473,   474,
     475,   476,   477,   478,   479,   480,   481,   482,   483,   484,
     485,   486,   487,   488,   492,   496,   500,   504,   508,   512,
     516,   520,   524,   528,   532,   536,   540,   547,   551,   556,
     564,   565,   569,   571,   576,   583,   584,   589,   593,   601,
     615,   643,   718,   741,   745,   767,   775,   782,   783,   784,
     785,   789,   793,   807,   821,   831,   835,   842,   856,   872,
     879,   885,   891,   898,   902,   907,   913,   930,   941,   954,
     970,   976,   990,  1005,  1020,  1036,  1052,  1068,  1085,  1101,
    1122,  1141,  1158,  1171,  1176,  1191,  1208,  1228,  1248,  1265,
    1268,  1272,  1276,  1283,  1287,  1296,  1305,  1307,  1309,  1311,
    1313,  1315,  1327,  1336,  1338,  1340,  1342,  1347,  1354,  1356,
    1363,  1370,  1377,  1384,  1386,  1388,  1394,  1406,  1408,  1411,
    1415,  1416,  1420,  1421,  1425,  1426,  1430,  1431,  1435,  1438,
    1442,  1447,  1452,  1454,  1456,  1461,  1465,  1470,  1476,  1481,
    1486,  1491,  1496,  1501,  1506,  1511,  1516,  1522,  1530,  1531,
    1542,  1551,  1563,  1573,  1574,  1579,  1583,  1595,  1612,  1622,
    1624,  1628,  1635,  1646,  1660,  1671,  1689,  1690,  1697,  1702,
    1710,  1715,  1719,  1720,  1727,  1731,  1737,  1738,  1754,  1764,
    1769,  1770,  1774,  1778,  1783,  1793,  1814,  1836,  1862,  1863,
    1871,  1903,  1929,  1951,  1973,  1999,  2000,  2004,  2011,  2019,
    2027,  2031,  2035,  2047,  2050,  2064,  2068,  2073,  2079,  2083,
    2090,  2094,  2098,  2102,  2108,  2114,  2121,  2126,  2131,  2135,
    2139,  2146,  2151,  2158,  2162,  2169,  2173,  2178,  2182,  2186,
    2192,  2200,  2204,  2208,  2212,  2218,  2220,  2226,  2227,  2233,
    2234,  2242,  2249,  2256,  2263,  2270,  2281,  2292,  2307,  2308,
    2315,  2316,  2320,  2327,  2329,  2334,  2342,  2343,  2345,  2351,
    2352,  2360,  2363,  2367,  2374,  2379,  2387,  2395,  2405,  2409,
    2416,  2418,  2423,  2427,  2431,  2435,  2439,  2443,  2447,  2456,
    2464,  2468,  2472,  2481,  2487,  2493,  2499,  2504,  2508,  2512,
    2520,  2521,  2531,  2539,  2540,  2541,  2542,  2546,  2547,  2558,
    2560,  2562,  2564,  2566,  2568,  2570,  2575,  2577,  2579,  2581,
    2583,  2587,  2600,  2604,  2608,  2615,  2621,  2630,  2640,  2644,
    2646,  2648,  2653,  2654,  2655,  2660,  2661,  2663,  2669,  2670,
    2675,  2676,  2686,  2692,  2696,  2702,  2708,  2714,  2726,  2732,
    2736,  2748,  2752,  2758,  2763,  2774,  2780,  2786,  2790,  2802,
    2808,  2813,  2827,  2832,  2836,  2841,  2845,  2851,  2863,  2875,
    2887,  2894,  2898,  2906,  2910,  2915,  2929,  2940,  2944,  2950,
    2956,  2961,  2966,  2971,  2976,  2982,  2987,  2992,  2997,  3002,
    3007,  3014,  3019,  3024,  3029,  3041,  3081,  3086,  3098,  3105,
    3110,  3112,  3114,  3116,  3129,  3135,  3143,  3147,  3154,  3160,
    3167,  3174,  3181,  3188,  3195,  3202,  3213,  3223,  3224,  3228,
    3231,  3237,  3244,  3245,  3260,  3267,  3272,  3277,  3281,  3294,
    3302,  3304,  3315,  3321,  3332,  3336,  3343,  3348,  3354,  3359,
    3368,  3369,  3373,  3374,  3375,  3379,  3384,  3389,  3393,  3407,
    3413,  3420,  3427,  3434,  3444,  3447,  3455,  3459,  3466,  3481,
    3484,  3488,  3490,  3492,  3495,  3499,  3504,  3509,  3514,  3522,
    3526,  3531,  3542,  3544,  3561,  3563,  3580,  3584,  3586,  3599,
    3600,  3601,  3602,  3603,  3604,  3605,  3606,  3607,  3608,  3609,
    3610,  3611,  3612,  3613,  3614,  3615,  3616,  3617,  3618,  3619,
    3620,  3621,  3622,  3623,  3633,  3638,  3645,  3651,  3655,  3660,
    3664,  3668,  3672,  3676,  3683,  3690,  3697,  3705,  3712,  3715,
    3720,  3724,  3729,  3731,  3733,  3735,  3737,  3739,  3741,  3743,
    3745,  3747,  3749,  3751,  3753,  3755,  3757,  3759,  3761,  3763,
    3765,  3767,  3769,  3772,  3774,  3781,  3784,  3789,  3793,  3798,
    3800,  3802,  3804,  3806,  3808,  3810,  3812,  3814,  3816,  3818,
    3820,  3822,  3824,  3826,  3828,  3830,  3832,  3834,  3836,  3839,
    3841,  3849,  3855,  3861,  3881,  3893,  3899,  3905,  3909,  3914,
    3918,  3923,  3929,  3940,  3947,  3951,  3956,  3962,  3965,  3976,
    3985,  3994,  4012,  4018,  4028,  4038,  4049,  4057,  4070,  4074,
    4088
=======
       0,   221,   221,   230,   237,   244,   245,   246,   247,   248,
     249,   250,   251,   252,   253,   254,   255,   256,   257,   258,
     259,   260,   261,   262,   263,   264,   265,   266,   267,   268,
     269,   279,   283,   290,   292,   298,   307,   308,   309,   310,
     311,   312,   313,   314,   315,   316,   320,   322,   324,   326,
     332,   340,   344,   348,   352,   360,   361,   365,   369,   370,
     371,   372,   373,   374,   375,   378,   379,   383,   387,   388,
     389,   390,   391,   392,   393,   394,   395,   396,   397,   398,
     399,   400,   401,   402,   406,   410,   414,   418,   422,   426,
     430,   434,   438,   442,   446,   450,   454,   461,   465,   470,
     478,   479,   483,   485,   490,   497,   498,   502,   516,   544,
     619,   635,   639,   661,   669,   676,   677,   678,   679,   683,
     687,   701,   715,   725,   729,   736,   750,   767,   770,   774,
     778,   785,   789,   798,   807,   809,   811,   813,   815,   817,
     826,   835,   837,   839,   841,   846,   853,   855,   862,   869,
     876,   883,   885,   887,   893,   905,   907,   910,   914,   915,
     919,   920,   924,   925,   929,   930,   934,   937,   941,   946,
     951,   953,   955,   960,   964,   969,   975,   980,   985,   990,
     995,  1000,  1005,  1010,  1015,  1021,  1029,  1030,  1041,  1051,
    1052,  1057,  1061,  1074,  1088,  1099,  1117,  1118,  1125,  1130,
    1138,  1143,  1147,  1148,  1155,  1159,  1165,  1166,  1180,  1190,
    1195,  1196,  1200,  1204,  1209,  1219,  1240,  1262,  1288,  1289,
    1293,  1319,  1341,  1363,  1389,  1390,  1394,  1401,  1409,  1417,
    1421,  1425,  1437,  1440,  1454,  1458,  1463,  1469,  1473,  1480,
    1484,  1488,  1493,  1500,  1505,  1511,  1515,  1519,  1523,  1529,
    1531,  1537,  1538,  1544,  1545,  1553,  1560,  1567,  1574,  1581,
    1592,  1603,  1618,  1619,  1626,  1627,  1631,  1638,  1640,  1645,
    1653,  1654,  1656,  1662,  1663,  1671,  1674,  1678,  1685,  1690,
    1698,  1706,  1716,  1720,  1727,  1729,  1734,  1738,  1742,  1746,
    1750,  1754,  1758,  1767,  1775,  1779,  1783,  1792,  1798,  1804,
    1810,  1817,  1818,  1828,  1836,  1837,  1838,  1839,  1843,  1844,
    1854,  1856,  1858,  1860,  1862,  1864,  1869,  1871,  1873,  1875,
    1877,  1881,  1894,  1898,  1902,  1910,  1919,  1929,  1933,  1935,
    1937,  1942,  1943,  1944,  1949,  1950,  1952,  1958,  1959,  1964,
    1965,  1974,  1980,  1984,  1990,  1996,  2002,  2014,  2020,  2024,
    2036,  2040,  2046,  2051,  2061,  2067,  2073,  2077,  2088,  2094,
    2099,  2112,  2117,  2121,  2126,  2130,  2136,  2147,  2159,  2171,
    2178,  2182,  2190,  2194,  2199,  2213,  2224,  2228,  2234,  2240,
    2245,  2250,  2255,  2260,  2265,  2270,  2275,  2280,  2285,  2292,
    2297,  2302,  2307,  2318,  2358,  2363,  2374,  2381,  2386,  2388,
    2390,  2392,  2403,  2411,  2415,  2422,  2428,  2435,  2442,  2449,
    2456,  2463,  2472,  2473,  2477,  2488,  2495,  2500,  2505,  2509,
    2522,  2530,  2532,  2543,  2549,  2560,  2564,  2571,  2576,  2582,
    2587,  2596,  2597,  2601,  2602,  2603,  2607,  2612,  2617,  2621,
    2635,  2641,  2648,  2655,  2662,  2672,  2675,  2683,  2687,  2694,
    2709,  2712,  2716,  2718,  2720,  2723,  2727,  2732,  2737,  2742,
    2750,  2754,  2759,  2770,  2772,  2789,  2791,  2808,  2812,  2814,
    2827,  2828,  2829,  2830,  2831,  2832,  2833,  2834,  2835,  2836,
    2837,  2838,  2839,  2840,  2841,  2842,  2843,  2844,  2845,  2846,
    2847,  2848,  2849,  2850,  2851
>>>>>>> refs/remotes/origin/master
};
#endif

#if YYDEBUG || YYERROR_VERBOSE || YYTOKEN_TABLE
/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "$end", "error", "$undefined", "NAME", "STRING", "INTNUM", "DATE_VALUE",
  "HINT_VALUE", "BOOL", "APPROXNUM", "NULLX", "UNKNOWN", "QUESTIONMARK",
  "SYSTEM_VARIABLE", "TEMP_VARIABLE", "EXCEPT", "UNION", "INTERSECT", "OR",
  "AND", "NOT", "COMP_NE", "COMP_GE", "COMP_GT", "COMP_EQ", "COMP_LT",
  "COMP_LE", "CNNOP", "LIKE", "BETWEEN", "IN", "IS", "'+'", "'-'", "'*'",
<<<<<<< HEAD
  "'/'", "'%'", "MOD", "'^'", "UMINUS", "'('", "')'", "'.'", "PROCEDURE",
  "DECLARE", "ELSEIF", "OUT", "INOUT", "WHILE", "LOOP", "EXIT", "CONTINUE",
  "DO", "CALL", "ARRAY", "REVERSE", "CURSOR", "OPEN", "FETCH", "CLOSE",
  "NEXT", "PRIOR", "FIRST", "LAST", "ABSOLUTE", "RELATIVE", "ADD", "ANY",
=======
  "'/'", "'%'", "MOD", "'^'", "UMINUS", "'('", "')'", "'.'", "ADD", "ANY",
>>>>>>> refs/remotes/origin/master
  "ALL", "ALTER", "AS", "ASC", "BEGI", "BIGINT", "BINARY", "BOOLEAN",
  "BOTH", "BY", "CASCADE", "CASE", "CHARACTER", "CLUSTER", "COMMENT",
  "COMMIT", "CONSISTENT", "COLUMN", "COLUMNS", "CREATE", "CREATETIME",
  "CURRENT_USER", "CHANGE_OBI", "SWITCH_CLUSTER", "DATE", "DATETIME",
  "DEALLOCATE", "DECIMAL", "DEFAULT", "DELETE", "DESC", "DESCRIBE",
<<<<<<< HEAD
  "DISTINCT", "DOUBLE", "DROP", "DUAL", "TRUNCATE", "ELSE", "END", "END_P",
  "ERROR", "EXECUTE", "EXISTS", "EXPLAIN", "FLOAT", "FOR", "FROM", "FULL",
  "FROZEN", "FORCE", "GLOBAL", "GLOBAL_ALIAS", "GRANT", "GROUP", "HAVING",
  "HINT_BEGIN", "HINT_END", "HOTSPOT", "IDENTIFIED", "IF", "INNER",
  "INTEGER", "INSERT", "INTO", "JOIN", "SEMI_JOIN", "KEY", "LEADING",
  "LEFT", "LIMIT", "LOCAL", "LOCKED", "LOCKWJH", "MEDIUMINT", "MEMORY",
  "MODIFYTIME", "MASTER", "NUMERIC", "OFFSET", "ON", "ORDER", "OPTION",
  "OUTER", "PARAMETERS", "PASSWORD", "PRECISION", "PREPARE", "PRIMARY",
  "READ_STATIC", "REAL", "RENAME", "REPLACE", "RESTRICT", "PRIVILEGES",
  "REVOKE", "RIGHT", "ROLLBACK", "KILL", "READ_CONSISTENCY", "NO_GROUP",
  "LONG_TRANS", "SCHEMA", "SCOPE", "SELECT", "SESSION", "SESSION_ALIAS",
  "SET", "SHOW", "SMALLINT", "SNAPSHOT", "SPFILE", "START", "STATIC",
  "SYSTEM", "STRONG", "SET_MASTER_CLUSTER", "SET_SLAVE_CLUSTER", "SLAVE",
  "TABLE", "TABLES", "THEN", "TIME", "TIMESTAMP", "TINYINT", "TRAILING",
  "TRANSACTION", "TO", "UPDATE", "USER", "USING", "VALUES", "VARCHAR",
  "VARBINARY", "WHERE", "WHEN", "WITH", "WORK", "PROCESSLIST", "QUERY",
  "CONNECTION", "WEAK", "INDEX", "STORING", "BLOOMFILTER_JOIN",
  "MERGE_JOIN", "SI", "SIB", "SM", "SEMI", "NO_QUERY_OPT", "GATHER",
  "STATISTICS", "HASH_JOIN", "AUTO_INCREMENT", "CHUNKSERVER",
  "COMPRESS_METHOD", "CONSISTENT_MODE", "EXPIRE_INFO", "GRANTS",
  "JOIN_INFO", "MERGESERVER", "REPLICA_NUM", "ROOTSERVER", "ROW_COUNT",
  "SERVER", "SERVER_IP", "SERVER_PORT", "SERVER_TYPE", "STATUS",
  "TABLE_ID", "TABLET_BLOCK_SIZE", "TABLET_MAX_SIZE", "UNLOCKED",
  "UPDATESERVER", "USE_BLOOM_FILTER", "VARIABLES", "VERBOSE", "WARNINGS",
  "';'", "','", "$accept", "sql_stmt", "stmt_list", "stmt", "expr_list",
  "column_ref", "expr_const", "simple_expr", "arith_expr", "expr",
  "in_expr", "case_expr", "case_arg", "when_clause_list", "when_clause",
  "case_default", "array_expr", "func_expr", "when_func", "when_func_name",
  "when_func_stmt", "distinct_or_all", "delete_stmt", "update_stmt",
  "update_asgn_list", "update_asgn_factor", "create_index_stmt",
  "opt_index_columns", "opt_storing", "opt_storing_columns",
  "opt_index_option_list", "index_option", "cursor_declare_stmt",
  "cursor_name", "cursor_open_stmt", "cursor_fetch_stmt",
  "cursor_fetch_into_stmt", "cursor_fetch_next_into_stmt",
  "cursor_fetch_first_into_stmt", "cursor_fetch_prior_into_stmt",
  "cursor_fetch_last_into_stmt", "cursor_fetch_absolute_into_stmt",
  "cursor_fetch_relative_into_stmt", "fetch_prior_stmt",
  "fetch_first_stmt", "fetch_last_stmt", "fetch_relative_stmt",
  "next_or_prior", "fetch_absolute_stmt", "fetch_fromto_stmt",
  "cursor_close_stmt", "create_table_stmt", "opt_if_not_exists",
  "table_element_list", "table_element", "column_definition", "data_type",
  "opt_decimal", "opt_float", "opt_precision", "opt_time_precision",
  "opt_char_length", "opt_column_attribute_list", "column_attribute",
  "opt_table_option_list", "table_option", "opt_equal_mark",
  "gather_statistics_stmt", "opt_gather_columns", "drop_table_stmt",
  "opt_if_exists", "table_list", "truncate_table_stmt", "drop_index_stmt",
  "index_list", "table_name", "insert_stmt", "opt_when",
  "replace_or_insert", "opt_insert_columns", "column_list",
  "insert_vals_list", "insert_vals", "select_stmt", "select_with_parens",
  "select_no_parens", "no_table_select", "select_clause",
  "select_into_clause", "simple_select", "opt_where", "select_limit",
  "opt_hint", "opt_hint_list", "hint_options", "hint_option",
  "opt_comma_list", "join_op_type_list", "join_op_type",
  "consistency_level", "limit_expr", "opt_select_limit", "opt_for_update",
  "parameterized_trim", "opt_groupby", "opt_order_by", "order_by",
  "sort_list", "sort_key", "opt_asc_desc", "opt_having", "opt_distinct",
  "projection", "select_expr_list", "from_list", "table_factor",
  "relation_factor", "joined_table", "join_type", "join_outer",
  "explain_stmt", "explainable_stmt", "opt_verbose", "show_stmt",
  "lock_table_stmt", "opt_limit", "opt_for_grant_user", "opt_scope",
  "opt_show_condition", "opt_like_condition", "opt_full",
  "create_user_stmt", "user_specification_list", "user_specification",
  "user", "password", "drop_user_stmt", "user_list", "set_password_stmt",
  "opt_for_user", "rename_user_stmt", "rename_info", "rename_list",
  "lock_user_stmt", "lock_spec", "opt_work",
  "opt_with_consistent_snapshot", "begin_stmt", "commit_stmt",
  "rollback_stmt", "kill_stmt", "opt_is_global", "opt_flag", "grant_stmt",
  "priv_type_list", "priv_type", "opt_privilege", "priv_level",
  "revoke_stmt", "opt_on_priv_level", "prepare_stmt", "stmt_name",
  "preparable_stmt", "variable_set_stmt", "var_and_val_list",
  "var_and_val", "var_and_array_val", "to_or_eq", "argument",
  "array_vals_list", "array_val_list", "execute_stmt", "opt_using_args",
=======
  "DISTINCT", "DOUBLE", "DROP", "DUAL", "ELSE", "END", "END_P", "ERROR",
  "EXECUTE", "EXISTS", "EXPLAIN", "FLOAT", "FOR", "FROM", "FULL", "FROZEN",
  "FORCE", "GLOBAL", "GLOBAL_ALIAS", "GRANT", "GROUP", "HAVING",
  "HINT_BEGIN", "HINT_END", "HOTSPOT", "IDENTIFIED", "IF", "INNER",
  "INTEGER", "INSERT", "INTO", "JOIN", "KEY", "LEADING", "LEFT", "LIMIT",
  "LOCAL", "LOCKED", "MEDIUMINT", "MEMORY", "MODIFYTIME", "MASTER",
  "NUMERIC", "OFFSET", "ON", "ORDER", "OPTION", "OUTER", "PARAMETERS",
  "PASSWORD", "PRECISION", "PREPARE", "PRIMARY", "READ_STATIC", "REAL",
  "RENAME", "REPLACE", "RESTRICT", "PRIVILEGES", "REVOKE", "RIGHT",
  "ROLLBACK", "KILL", "READ_CONSISTENCY", "SCHEMA", "SCOPE", "SELECT",
  "SESSION", "SESSION_ALIAS", "SET", "SHOW", "SMALLINT", "SNAPSHOT",
  "SPFILE", "START", "STATIC", "SYSTEM", "STRONG", "SET_MASTER_CLUSTER",
  "SET_SLAVE_CLUSTER", "SLAVE", "TABLE", "TABLES", "THEN", "TIME",
  "TIMESTAMP", "TINYINT", "TRAILING", "TRANSACTION", "TO", "UPDATE",
  "USER", "USING", "VALUES", "VARCHAR", "VARBINARY", "WHERE", "WHEN",
  "WITH", "WORK", "PROCESSLIST", "QUERY", "CONNECTION", "WEAK",
  "AUTO_INCREMENT", "CHUNKSERVER", "COMPRESS_METHOD", "CONSISTENT_MODE",
  "EXPIRE_INFO", "GRANTS", "JOIN_INFO", "MERGESERVER", "REPLICA_NUM",
  "ROOTSERVER", "ROW_COUNT", "SERVER", "SERVER_IP", "SERVER_PORT",
  "SERVER_TYPE", "STATUS", "TABLE_ID", "TABLET_BLOCK_SIZE",
  "TABLET_MAX_SIZE", "UNLOCKED", "UPDATESERVER", "USE_BLOOM_FILTER",
  "VARIABLES", "VERBOSE", "WARNINGS", "';'", "','", "$accept", "sql_stmt",
  "stmt_list", "stmt", "expr_list", "column_ref", "expr_const",
  "simple_expr", "arith_expr", "expr", "in_expr", "case_expr", "case_arg",
  "when_clause_list", "when_clause", "case_default", "func_expr",
  "when_func", "when_func_name", "when_func_stmt", "distinct_or_all",
  "delete_stmt", "update_stmt", "update_asgn_list", "update_asgn_factor",
  "create_table_stmt", "opt_if_not_exists", "table_element_list",
  "table_element", "column_definition", "data_type", "opt_decimal",
  "opt_float", "opt_precision", "opt_time_precision", "opt_char_length",
  "opt_column_attribute_list", "column_attribute", "opt_table_option_list",
  "table_option", "opt_equal_mark", "drop_table_stmt", "opt_if_exists",
  "table_list", "insert_stmt", "opt_when", "replace_or_insert",
  "opt_insert_columns", "column_list", "insert_vals_list", "insert_vals",
  "select_stmt", "select_with_parens", "select_no_parens",
  "no_table_select", "select_clause", "simple_select", "opt_where",
  "select_limit", "opt_hint", "opt_hint_list", "hint_options",
  "hint_option", "opt_comma_list", "consistency_level", "limit_expr",
  "opt_select_limit", "opt_for_update", "parameterized_trim",
  "opt_groupby", "opt_order_by", "order_by", "sort_list", "sort_key",
  "opt_asc_desc", "opt_having", "opt_distinct", "projection",
  "select_expr_list", "from_list", "table_factor", "relation_factor",
  "joined_table", "join_type", "join_outer", "explain_stmt",
  "explainable_stmt", "opt_verbose", "show_stmt", "opt_limit",
  "opt_for_grant_user", "opt_scope", "opt_show_condition",
  "opt_like_condition", "opt_full", "create_user_stmt",
  "user_specification_list", "user_specification", "user", "password",
  "drop_user_stmt", "user_list", "set_password_stmt", "opt_for_user",
  "rename_user_stmt", "rename_info", "rename_list", "lock_user_stmt",
  "lock_spec", "opt_work", "opt_with_consistent_snapshot", "begin_stmt",
  "commit_stmt", "rollback_stmt", "kill_stmt", "opt_is_global", "opt_flag",
  "grant_stmt", "priv_type_list", "priv_type", "opt_privilege",
  "priv_level", "revoke_stmt", "opt_on_priv_level", "prepare_stmt",
  "stmt_name", "preparable_stmt", "variable_set_stmt", "var_and_val_list",
  "var_and_val", "to_or_eq", "argument", "execute_stmt", "opt_using_args",
>>>>>>> refs/remotes/origin/master
  "argument_list", "deallocate_prepare_stmt", "deallocate_or_drop",
  "alter_table_stmt", "alter_column_actions", "alter_column_action",
  "opt_column", "opt_drop_behavior", "alter_column_behavior",
  "alter_system_stmt", "opt_force", "alter_system_actions",
  "alter_system_action", "opt_comment", "opt_config_scope", "server_type",
  "opt_cluster_or_address", "column_name", "relation_name",
<<<<<<< HEAD
  "function_name", "column_label", "unreserved_keyword",
  "create_procedure_stmt", "proc_decl", "proc_parameter_list",
  "proc_parameter", "proc_block", "proc_sect", "proc_stmts", "proc_stmt",
  "control_sect", "control_stmts", "control_stmt", "stmt_declare",
  "stmt_assign", "stmt_if", "stmt_elsifs", "stmt_elsif", "stmt_else",
  "stmt_case", "case_when_list", "case_when", "case_else", "stmt_loop",
  "stmt_while", "stmt_null", "stmt_exit", "exec_procedure_stmt",
  "drop_procedure_stmt", "show_procedure_stmt", 0
=======
  "function_name", "column_label", "unreserved_keyword", 0
>>>>>>> refs/remotes/origin/master
};
#endif

# ifdef YYPRINT
/* YYTOKNUM[YYLEX-NUM] -- Internal token number corresponding to
   token YYLEX-NUM.  */
static const yytype_uint16 yytoknum[] =
{
       0,   256,   257,   258,   259,   260,   261,   262,   263,   264,
     265,   266,   267,   268,   269,   270,   271,   272,   273,   274,
     275,   276,   277,   278,   279,   280,   281,   282,   283,   284,
     285,   286,    43,    45,    42,    47,    37,   287,    94,   288,
      40,    41,    46,   289,   290,   291,   292,   293,   294,   295,
     296,   297,   298,   299,   300,   301,   302,   303,   304,   305,
     306,   307,   308,   309,   310,   311,   312,   313,   314,   315,
     316,   317,   318,   319,   320,   321,   322,   323,   324,   325,
     326,   327,   328,   329,   330,   331,   332,   333,   334,   335,
     336,   337,   338,   339,   340,   341,   342,   343,   344,   345,
     346,   347,   348,   349,   350,   351,   352,   353,   354,   355,
     356,   357,   358,   359,   360,   361,   362,   363,   364,   365,
     366,   367,   368,   369,   370,   371,   372,   373,   374,   375,
     376,   377,   378,   379,   380,   381,   382,   383,   384,   385,
     386,   387,   388,   389,   390,   391,   392,   393,   394,   395,
     396,   397,   398,   399,   400,   401,   402,   403,   404,   405,
     406,   407,   408,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   430,   431,   432,   433,   434,   435,
     436,   437,   438,   439,   440,   441,   442,   443,   444,   445,
<<<<<<< HEAD
     446,   447,   448,   449,   450,   451,   452,   453,   454,   455,
     456,   457,   458,   459,   460,   461,   462,   463,   464,   465,
     466,   467,   468,   469,   470,   471,   472,   473,   474,   475,
     476,   477,   478,   479,   480,   481,   482,   483,   484,   485,
     486,   487,   488,   489,   490,   491,    59,    44
=======
     446,   447,   448,   449,   450,   451,    59,    44
>>>>>>> refs/remotes/origin/master
};
# endif

/* YYR1[YYN] -- Symbol number of symbol that rule YYN derives.  */
static const yytype_uint16 yyr1[] =
{
<<<<<<< HEAD
       0,   248,   249,   250,   250,   251,   251,   251,   251,   251,
     251,   251,   251,   251,   251,   251,   251,   251,   251,   251,
     251,   251,   251,   251,   251,   251,   251,   251,   251,   251,
     251,   251,   251,   251,   251,   251,   251,   251,   251,   251,
     251,   251,   251,   251,   251,   251,   251,   251,   251,   251,
     252,   252,   253,   253,   253,   254,   254,   254,   254,   254,
     254,   254,   254,   254,   254,   255,   255,   255,   255,   255,
     255,   255,   255,   255,   255,   256,   256,   256,   256,   256,
     256,   256,   256,   256,   256,   257,   257,   257,   257,   257,
     257,   257,   257,   257,   257,   257,   257,   257,   257,   257,
     257,   257,   257,   257,   257,   257,   257,   257,   257,   257,
     257,   257,   257,   257,   257,   257,   257,   258,   258,   259,
     260,   260,   261,   261,   262,   263,   263,   264,   264,   265,
     265,   265,   265,   265,   265,   266,   267,   268,   268,   268,
     268,   269,   269,   270,   271,   272,   272,   273,   274,   275,
     276,   276,   277,   278,   278,   278,   279,   280,   281,   282,
     283,   283,   284,   285,   286,   287,   288,   289,   290,   291,
     292,   293,   294,   295,   295,   296,   297,   298,   299,   300,
     300,   301,   301,   302,   302,   303,   304,   304,   304,   304,
     304,   304,   304,   304,   304,   304,   304,   304,   304,   304,
     304,   304,   304,   304,   304,   304,   304,   305,   305,   305,
     306,   306,   307,   307,   308,   308,   309,   309,   310,   310,
     311,   311,   311,   311,   311,   312,   312,   312,   313,   313,
     313,   313,   313,   313,   313,   313,   313,   313,   314,   314,
     315,   316,   317,   318,   318,   319,   319,   320,   321,   322,
     322,   322,   323,   324,   324,   324,   325,   325,   326,   326,
     327,   327,   328,   328,   329,   329,   330,   330,   331,   331,
     332,   332,   333,   333,   333,   333,   334,   334,   335,   335,
     336,   337,   337,   337,   337,   338,   338,   338,   339,   339,
     339,   339,   339,   340,   340,   341,   341,   341,   342,   342,
     343,   343,   343,   343,   343,   343,   343,   343,   343,   343,
     343,   344,   344,   345,   345,   346,   346,   346,   346,   346,
     346,   347,   347,   347,   347,   348,   348,   349,   349,   350,
     350,   351,   351,   351,   351,   351,   351,   351,   352,   352,
     353,   353,   354,   355,   355,   356,   357,   357,   357,   358,
     358,   359,   359,   359,   360,   360,   360,   360,   361,   361,
     362,   362,   363,   363,   363,   363,   363,   363,   363,   364,
     365,   365,   365,   366,   366,   366,   366,   366,   366,   366,
     367,   367,   368,   369,   369,   369,   369,   370,   370,   371,
     371,   371,   371,   371,   371,   371,   371,   371,   371,   371,
     371,   371,   371,   371,   371,   372,   373,   373,   373,   374,
     374,   374,   375,   375,   375,   376,   376,   376,   377,   377,
     378,   378,   379,   380,   380,   381,   382,   383,   384,   385,
     385,   386,   386,   387,   387,   388,   389,   390,   390,   391,
     392,   392,   393,   393,   394,   394,   395,   395,   396,   397,
     398,   399,   399,   400,   400,   400,   401,   402,   402,   403,
     403,   403,   403,   403,   403,   403,   403,   403,   403,   403,
     403,   404,   404,   405,   405,   406,   407,   407,   408,   409,
     410,   410,   410,   410,   411,   411,   412,   412,   413,   413,
     413,   413,   413,   413,   413,   413,   414,   415,   415,   416,
     416,   417,   418,   418,   419,   420,   420,   421,   421,   422,
     423,   423,   424,   424,   425,   425,   426,   426,   426,   426,
     427,   427,   428,   428,   428,   429,   429,   429,   429,   430,
     430,   430,   430,   430,   431,   431,   432,   432,   433,   434,
     434,   435,   435,   435,   435,   436,   436,   436,   436,   437,
     437,   437,   438,   438,   439,   439,   440,   441,   441,   442,
     442,   442,   442,   442,   442,   442,   442,   442,   442,   442,
     442,   442,   442,   442,   442,   442,   442,   442,   442,   442,
     442,   442,   442,   442,   443,   444,   444,   445,   445,   446,
     446,   446,   446,   446,   446,   446,   446,   447,   448,   448,
     449,   449,   450,   450,   450,   450,   450,   450,   450,   450,
     450,   450,   450,   450,   450,   450,   450,   450,   450,   450,
     450,   450,   450,   450,   450,   451,   451,   452,   452,   453,
     453,   453,   453,   453,   453,   453,   453,   453,   453,   453,
     453,   453,   453,   453,   453,   453,   453,   453,   453,   453,
     453,   454,   454,   454,   455,   456,   456,   456,   456,   457,
     457,   458,   459,   460,   461,   461,   462,   463,   463,   464,
     464,   464,   465,   466,   467,   467,   468,   468,   469,   469,
     470
=======
       0,   208,   209,   210,   210,   211,   211,   211,   211,   211,
     211,   211,   211,   211,   211,   211,   211,   211,   211,   211,
     211,   211,   211,   211,   211,   211,   211,   211,   211,   211,
     211,   212,   212,   213,   213,   213,   214,   214,   214,   214,
     214,   214,   214,   214,   214,   214,   215,   215,   215,   215,
     215,   215,   215,   215,   215,   216,   216,   216,   216,   216,
     216,   216,   216,   216,   216,   217,   217,   217,   217,   217,
     217,   217,   217,   217,   217,   217,   217,   217,   217,   217,
     217,   217,   217,   217,   217,   217,   217,   217,   217,   217,
     217,   217,   217,   217,   217,   217,   217,   218,   218,   219,
     220,   220,   221,   221,   222,   223,   223,   224,   224,   224,
     224,   224,   224,   225,   226,   227,   227,   227,   227,   228,
     228,   229,   230,   231,   231,   232,   233,   234,   234,   235,
     235,   236,   236,   237,   238,   238,   238,   238,   238,   238,
     238,   238,   238,   238,   238,   238,   238,   238,   238,   238,
     238,   238,   238,   238,   238,   239,   239,   239,   240,   240,
     241,   241,   242,   242,   243,   243,   244,   244,   245,   245,
     245,   245,   245,   246,   246,   246,   247,   247,   247,   247,
     247,   247,   247,   247,   247,   247,   248,   248,   249,   250,
     250,   251,   251,   252,   252,   252,   253,   253,   254,   254,
     255,   255,   256,   256,   257,   257,   258,   258,   259,   259,
     260,   260,   261,   261,   261,   261,   262,   262,   263,   263,
     264,   264,   264,   264,   265,   265,   265,   266,   266,   266,
     266,   266,   267,   267,   268,   268,   268,   269,   269,   270,
     270,   270,   270,   271,   271,   272,   272,   272,   272,   273,
     273,   274,   274,   275,   275,   276,   276,   276,   276,   276,
     276,   276,   277,   277,   278,   278,   279,   280,   280,   281,
     282,   282,   282,   283,   283,   284,   284,   284,   285,   285,
     285,   285,   286,   286,   287,   287,   288,   288,   288,   288,
     288,   288,   288,   289,   290,   290,   290,   291,   291,   291,
     291,   292,   292,   293,   294,   294,   294,   294,   295,   295,
     296,   296,   296,   296,   296,   296,   296,   296,   296,   296,
     296,   296,   296,   296,   296,   297,   297,   297,   298,   298,
     298,   299,   299,   299,   300,   300,   300,   301,   301,   302,
     302,   303,   304,   304,   305,   306,   307,   308,   309,   309,
     310,   310,   311,   311,   312,   313,   314,   314,   315,   316,
     316,   317,   317,   318,   318,   319,   319,   320,   321,   322,
     323,   323,   324,   324,   324,   325,   326,   326,   327,   327,
     327,   327,   327,   327,   327,   327,   327,   327,   327,   328,
     328,   329,   329,   330,   331,   331,   332,   333,   334,   334,
     334,   334,   335,   336,   336,   337,   337,   337,   337,   337,
     337,   337,   338,   338,   339,   340,   341,   341,   342,   342,
     343,   344,   344,   345,   345,   346,   346,   347,   347,   347,
     347,   348,   348,   349,   349,   349,   350,   350,   350,   350,
     351,   351,   351,   351,   351,   352,   352,   353,   353,   354,
     355,   355,   356,   356,   356,   356,   357,   357,   357,   357,
     358,   358,   358,   359,   359,   360,   360,   361,   362,   362,
     363,   363,   363,   363,   363,   363,   363,   363,   363,   363,
     363,   363,   363,   363,   363,   363,   363,   363,   363,   363,
     363,   363,   363,   363,   363
>>>>>>> refs/remotes/origin/master
};

/* YYR2[YYN] -- Number of symbols composing right hand side of rule YYN.  */
static const yytype_uint8 yyr2[] =
{
       0,     2,     2,     3,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
<<<<<<< HEAD
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     0,
       1,     3,     1,     3,     3,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     3,     1,     1,     3,     5,     1,
       1,     1,     1,     2,     1,     1,     2,     2,     3,     3,
=======
       0,     1,     3,     1,     3,     3,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     3,     1,     1,     3,     5,
       1,     1,     1,     1,     2,     1,     2,     2,     3,     3,
>>>>>>> refs/remotes/origin/master
       3,     3,     3,     3,     3,     1,     2,     2,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     3,     3,
       3,     3,     4,     3,     3,     2,     3,     4,     3,     4,
       3,     4,     5,     6,     3,     4,     3,     1,     3,     5,
<<<<<<< HEAD
       1,     0,     1,     2,     4,     2,     0,     4,     4,     4,
       5,     4,     6,     4,     3,     4,     1,     1,     1,     1,
       1,     1,     1,     5,     7,     1,     3,     3,     9,     3,
       2,     0,     3,     1,     3,     0,     1,     5,     1,     2,
       2,     3,     4,     5,     5,     5,     5,     6,     7,     3,
       3,     3,     5,     1,     1,     4,     6,     2,     8,     3,
       0,     1,     3,     1,     5,     3,     1,     1,     1,     1,
       1,     2,     2,     1,     2,     1,     2,     2,     1,     2,
       2,     2,     2,     1,     1,     1,     2,     5,     3,     0,
       3,     0,     1,     0,     3,     0,     3,     0,     2,     0,
       2,     1,     2,     1,     2,     1,     3,     0,     3,     3,
       3,     3,     3,     3,     3,     3,     3,     3,     1,     0,
       4,     3,     4,     0,     2,     1,     3,     5,     6,     0,
       1,     3,     1,     7,     4,     7,     0,     2,     1,     1,
       3,     0,     1,     3,     3,     5,     1,     3,     2,     1,
       3,     3,     1,     2,     3,     4,     5,     8,     1,     1,
       9,     9,     4,     4,     4,     0,     2,     3,     4,     4,
       2,     2,     4,     0,     3,     1,     3,     0,     1,     2,
       1,     1,    14,     4,     5,     4,     3,     4,     1,     1,
       1,     2,     0,     1,     3,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     0,     1,     0,
       2,     3,     4,     4,     4,     3,     3,     3,     0,     3,
       1,     0,     3,     1,     3,     2,     0,     1,     1,     0,
       2,     0,     1,     1,     1,     2,     3,     1,     1,     3,
       1,     3,     1,     3,     2,     3,     2,     1,     5,     1,
       3,     6,     5,     2,     2,     2,     1,     1,     2,     2,
       1,     0,     3,     1,     1,     1,     1,     1,     0,     3,
       5,     5,     5,     4,     4,     4,     2,     4,     3,     3,
       3,     3,     3,     3,     3,     3,     4,     2,     0,     1,
       2,     4,     1,     1,     0,     0,     2,     2,     0,     1,
       0,     1,     3,     1,     3,     4,     1,     1,     3,     1,
       3,     5,     6,     2,     0,     3,     3,     1,     3,     4,
       1,     1,     1,     0,     3,     0,     2,     3,     2,     2,
       4,     0,     1,     0,     1,     1,     6,     1,     3,     2,
       1,     1,     2,     1,     1,     1,     2,     1,     1,     1,
       1,     1,     0,     1,     1,     5,     2,     0,     4,     1,
       1,     1,     1,     1,     2,     3,     1,     3,     3,     3,
       4,     4,     5,     5,     3,     3,     3,     1,     1,     1,
       1,     3,     1,     3,     3,     2,     0,     1,     3,     3,
       1,     1,     4,     6,     1,     3,     3,     4,     4,     5,
       1,     0,     1,     1,     0,     3,     3,     3,     2,     4,
       7,     7,     6,     6,     0,     1,     1,     3,     9,     2,
       0,     3,     3,     3,     0,     1,     1,     1,     1,     3,
       6,     0,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     2,     6,     5,     3,     1,     2,
       3,     3,     3,     3,     4,     4,     4,     3,     0,     1,
       2,     1,     1,     1,     1,     1,     1,     1,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     1,     1,     0,     1,     2,     1,     1,
       1,     1,     1,     1,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     1,
       1,     4,     6,     5,     3,     9,     8,     8,     7,     1,
       2,     4,     2,     7,     2,     1,     4,     0,     2,     5,
      11,    12,     7,     1,     2,     4,     6,     5,     5,     3,
       3
};

/* YYDEFACT[STATE-NAME] -- Default rule to reduce with in state
   STATE-NUM when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
      49,     0,     0,     0,     0,     0,     0,     0,   443,   443,
       0,   510,     0,     0,     0,   511,     0,     0,   388,     0,
     259,     0,     0,     0,   258,     0,   443,   451,   293,     0,
     414,     0,   293,     0,     0,     0,     4,    24,    23,    22,
       9,    10,    11,    13,    14,    15,    16,    17,    18,    12,
      21,    47,    25,    48,    26,    20,     0,    19,   279,   256,
     272,   341,   278,    27,    28,    46,    35,    36,    37,    38,
      39,    42,    43,    44,    45,    40,    41,    29,    30,    31,
      34,     0,    32,    33,     5,     0,     6,     8,     7,   279,
       0,   557,   559,   560,   561,   562,   563,   564,   565,   566,
     567,   568,   569,   570,   571,   572,   573,   574,   576,   575,
     577,   578,   579,   580,   581,   582,   583,     0,   158,   558,
       0,   159,   160,   177,   534,     0,     0,   442,   446,   448,
       0,   180,     0,   180,     0,   554,   418,   369,   555,   418,
       0,   243,     0,   243,   243,   506,   479,   387,     0,   472,
     460,   461,   463,   464,   465,     0,   467,   470,   469,   468,
       0,   457,     0,     0,     0,   477,   449,   452,   453,   297,
     351,   552,     0,     0,     0,     0,   434,     0,     0,     0,
       0,   484,   486,     0,   553,   556,     0,     0,     0,   421,
     412,   415,   396,   413,     0,   415,     0,   434,     0,   408,
       0,     0,     0,     0,   445,     0,     0,     1,     2,    49,
       0,     0,   268,   351,   351,   351,     0,     0,   329,     0,
     273,     0,   598,   584,   271,   270,     0,     0,   161,   169,
     170,   171,     0,     0,     0,   535,     0,     0,     0,     0,
       0,   426,     0,     0,     0,     0,   422,   423,     0,     0,
     285,   419,   399,   398,   679,     0,     0,     0,   429,   428,
     249,     0,     0,   504,   384,   386,   385,   383,   382,   471,
     459,   462,   466,     0,     0,   405,     0,     0,   437,   435,
       0,     0,   454,   455,     0,     0,   312,   301,     0,     0,
     300,     0,   308,   309,     0,   310,     0,   295,   298,   352,
     353,     0,   498,   497,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   485,     0,     0,     0,   680,     0,
       0,     0,     0,     0,   403,   415,   389,     0,     0,   402,
     409,   415,     0,   400,   401,   415,   404,     0,     0,   447,
       0,     0,     3,   261,   552,    55,    57,    56,    59,    58,
      60,    61,    63,    62,     0,     0,     0,     0,   121,     0,
       0,   569,    65,    66,    85,   257,    69,    74,    70,    71,
       0,    72,    52,     0,   553,     0,     0,     0,     0,     0,
       0,   329,   274,   330,   509,     0,     0,   625,     0,     0,
       0,     0,     0,   293,     0,   673,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   599,   601,   606,   607,   602,   603,   605,   604,
     624,   623,     0,   293,     0,    50,   175,     0,     0,   529,
     536,     0,     0,     0,     0,     0,   521,   521,   521,   521,
     512,   514,     0,   440,   441,   439,     0,     0,     0,     0,
       0,     0,     0,   256,     0,   244,     0,   242,     0,   245,
     362,   367,     0,     0,   250,   540,   499,   500,   507,   505,
     473,     0,   474,   458,   483,   482,   481,   480,   478,     0,
       0,   476,     0,   450,     0,     0,     0,     0,     0,     0,
     294,     0,   299,   357,   354,   358,   327,   494,     0,     0,
     488,     0,     0,   433,     0,     0,     0,     0,   495,   487,
     489,   415,   415,   397,   416,   417,   393,   415,   410,   394,
     407,   395,     0,   134,   141,     0,   142,     0,     0,     0,
      50,     0,     0,     0,     0,     0,   240,     0,     0,   254,
     105,    86,    87,     0,    50,    72,   120,     0,    73,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   293,   279,   284,   278,   282,   283,   346,
     342,   343,   325,   326,   290,   291,   275,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   626,   628,   633,   629,   630,
     632,   631,   650,   649,     0,   674,     0,     0,     0,     0,
       0,     0,   611,   610,   613,   614,   616,   617,   618,   620,
     619,   621,   622,   615,   609,   612,   608,   597,   600,   157,
     677,   293,     0,     0,   172,     0,     0,     0,     0,     0,
       0,   520,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   586,     0,     0,     0,   588,   179,     0,   424,     0,
       0,     0,   286,   143,   678,   279,     0,   367,     0,     0,
     366,   381,   376,     0,   381,   381,   377,     0,     0,   364,
     430,     0,     0,     0,   247,     0,     0,   436,   438,   475,
       0,   306,   311,   315,   316,   317,   318,   319,   320,     0,
     313,     0,   324,   323,   322,   321,     0,     0,   296,     0,
     355,     0,     0,   328,   276,   127,   128,   490,     0,   427,
     431,   491,     0,     0,   496,   392,   391,   390,     0,     0,
     129,     0,     0,     0,     0,     0,     0,   131,     0,     0,
       0,   133,   444,   285,   145,     0,     0,   262,     0,     0,
       0,    67,     0,   126,   122,    64,   104,   103,     0,     0,
       0,   100,    98,    99,    97,    96,    95,   116,   101,     0,
       0,    75,     0,     0,   114,   117,   108,   106,   110,     0,
      88,    89,    90,    91,    92,    94,    93,     0,   140,   139,
     138,   137,    54,    53,   351,   347,   348,   345,     0,     0,
       0,     0,   190,   217,   193,   217,   203,   205,   198,   209,
     213,   211,   189,   188,   204,   209,   195,   187,   215,   215,
     186,   217,   217,     0,   625,   637,   636,   639,   640,   642,
     643,   644,   646,   645,   647,   648,   641,   635,   638,   634,
       0,   627,     0,   173,   174,     0,     0,     0,     0,     0,
       0,   667,   665,     0,   625,     0,   654,   676,    51,   176,
     537,    62,   540,   532,   533,     0,     0,   516,     0,     0,
     524,   513,     0,   521,   515,   432,   589,     0,     0,     0,
     585,     0,     0,     0,   181,   183,   425,     0,   287,   370,
     246,   365,   380,   373,     0,   378,   374,   379,   375,     0,
     363,   248,   252,   251,   539,   508,   456,   305,   307,     0,
       0,   303,     0,   356,   285,   285,   360,   359,   492,   493,
     502,     0,   411,   406,   335,     0,   336,     0,   337,     0,
       0,   331,   130,     0,   256,     0,   241,     0,   260,     0,
     256,    51,     0,     0,   123,     0,   102,     0,   115,    76,
      77,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     109,   107,   111,   135,     0,   344,   288,   292,   289,     0,
     200,   199,     0,   191,   212,   196,     0,   194,   192,     0,
     206,   197,   201,   202,     0,     0,   651,     0,     0,   675,
       0,     0,     0,     0,     0,   162,     0,     0,   625,   664,
       0,     0,     0,     0,     0,   544,   530,   531,   219,     0,
       0,   518,   522,   523,   517,     0,   593,   590,   591,   592,
     587,     0,   227,     0,     0,   151,     0,     0,     0,   314,
       0,   304,   327,     0,   338,   501,     0,   332,   333,   334,
     132,   146,   144,   147,   263,   255,   266,     0,     0,   253,
      68,     0,   125,   119,     0,   112,    78,    79,    80,    81,
      82,    84,    83,   118,     0,     0,     0,     0,     0,   653,
       0,     0,   669,   163,   165,   164,   166,     0,     0,   625,
     668,     0,     0,     0,     0,   625,     0,     0,   659,     0,
       0,     0,     0,   185,     0,   528,     0,     0,   519,   594,
     595,   596,     0,   239,   239,   239,   239,   239,   239,   239,
     239,   239,   239,   178,   225,   182,     0,     0,   155,   368,
     372,     0,     0,   277,   361,     0,   349,   503,   264,     0,
       0,   124,   113,     0,   216,   208,     0,   210,   214,   652,
       0,   167,     0,   666,     0,     0,     0,     0,   662,     0,
       0,   660,     0,     0,     0,     0,     0,   221,     0,     0,
       0,   223,   218,   526,   525,   527,     0,   238,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   149,
       0,   150,   148,   153,   156,   371,     0,     0,     0,   281,
     267,     0,     0,   672,   168,   663,     0,   625,   625,   658,
       0,     0,     0,   285,   543,   541,   542,     0,   220,   222,
     224,   184,   237,   234,   236,   229,   228,   233,   232,   231,
     230,   235,   226,     0,     0,     0,   339,   350,   265,   207,
     625,     0,   661,   656,     0,   657,   329,   547,   548,   545,
     546,   551,   152,   154,     0,     0,     0,   655,   280,     0,
       0,   538,     0,     0,     0,     0,     0,     0,     0,   670,
     549,     0,     0,   671,     0,     0,     0,   302,   550
=======
       1,     0,     1,     2,     4,     2,     0,     4,     5,     4,
       6,     4,     3,     4,     1,     1,     1,     1,     1,     1,
       1,     5,     7,     1,     3,     3,     8,     3,     0,     1,
       3,     1,     5,     3,     1,     1,     1,     1,     1,     2,
       2,     1,     2,     1,     2,     2,     1,     2,     2,     2,
       2,     1,     1,     1,     2,     5,     3,     0,     3,     0,
       1,     0,     3,     0,     3,     0,     2,     0,     2,     1,
       2,     1,     2,     1,     3,     0,     3,     3,     3,     3,
       3,     3,     3,     3,     3,     3,     1,     0,     4,     0,
       2,     1,     3,     7,     4,     7,     0,     2,     1,     1,
       3,     0,     1,     3,     3,     5,     1,     3,     2,     1,
       3,     3,     1,     2,     3,     4,     5,     8,     1,     1,
       9,     4,     4,     4,     0,     2,     3,     4,     4,     2,
       2,     4,     0,     3,     1,     3,     0,     1,     2,     1,
       1,     4,     3,     2,     0,     1,     1,     1,     1,     1,
       1,     0,     1,     0,     2,     3,     4,     4,     4,     3,
       3,     3,     0,     3,     1,     0,     3,     1,     3,     2,
       0,     1,     1,     0,     2,     0,     1,     1,     1,     2,
       3,     1,     1,     3,     1,     3,     1,     3,     2,     3,
       2,     1,     5,     1,     3,     6,     5,     2,     2,     2,
       1,     1,     0,     3,     1,     1,     1,     1,     1,     0,
       3,     5,     5,     4,     4,     4,     2,     4,     3,     3,
       3,     3,     3,     3,     3,     4,     2,     0,     1,     2,
       4,     1,     1,     0,     0,     2,     2,     0,     1,     0,
       1,     3,     1,     3,     4,     1,     1,     3,     1,     3,
       5,     6,     2,     0,     3,     3,     1,     3,     4,     1,
       1,     1,     0,     3,     0,     2,     3,     2,     2,     4,
       0,     1,     0,     1,     1,     6,     1,     3,     2,     1,
       1,     2,     1,     1,     2,     1,     1,     1,     1,     1,
       0,     1,     1,     5,     2,     0,     4,     1,     1,     1,
       1,     1,     2,     1,     3,     3,     3,     4,     4,     5,
       5,     3,     1,     1,     1,     3,     2,     0,     1,     3,
       3,     1,     1,     4,     6,     1,     3,     3,     4,     4,
       5,     1,     0,     1,     1,     0,     3,     3,     3,     2,
       4,     7,     7,     6,     6,     0,     1,     1,     3,     9,
       2,     0,     3,     3,     3,     0,     1,     1,     1,     1,
       3,     6,     0,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1
};

/* YYDEFACT[STATE-NAME] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE doesn't specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint16 yydefact[] =
{
      30,     0,     0,   362,   362,     0,   421,     0,     0,     0,
     422,     0,   309,     0,   199,     0,     0,   198,     0,   362,
     370,   232,     0,   333,     0,   232,     0,     0,     4,     9,
       8,     7,    10,     6,     0,     5,   219,   196,   212,   265,
     218,    11,    12,    19,    20,    21,    22,    23,    26,    27,
      28,    29,    24,    25,    13,    14,    15,    18,     0,    16,
      17,   219,     0,   445,     0,     0,   361,   365,   367,   128,
       0,     0,   465,   470,   471,   472,   473,   474,   475,   476,
     477,   478,   479,   480,   481,   482,   483,   484,   485,   487,
     486,   488,   489,   490,   491,   492,   493,   494,   337,   293,
     466,   337,   189,     0,   468,   417,   397,   469,   308,     0,
     390,   379,   380,   382,   383,     0,   385,   388,   387,   386,
       0,   376,     0,     0,   395,   368,   371,   372,   236,   275,
     463,     0,     0,     0,     0,   353,     0,     0,   402,   403,
       0,   464,   467,     0,     0,   340,   331,   334,   316,   332,
       0,   334,   353,     0,   327,     0,     0,     0,     0,   364,
       0,     1,     2,    30,     0,     0,   208,   275,   275,   275,
       0,     0,   253,     0,   213,     0,   211,   210,   446,     0,
       0,     0,     0,     0,   345,     0,     0,     0,   341,   342,
       0,   224,   338,   319,   318,     0,     0,   348,   347,     0,
     415,   305,   307,   306,   304,   303,   389,   378,   381,   384,
       0,     0,     0,     0,   356,   354,     0,     0,   373,   374,
       0,   244,   240,   239,     0,     0,   234,   237,   276,   277,
       0,   413,   412,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   323,   334,
     310,     0,   322,   328,   334,     0,   320,   321,   334,   324,
       0,     0,   366,     0,     3,   201,   463,    36,    38,    37,
      40,    39,    41,    42,    44,    43,     0,     0,     0,     0,
     101,     0,     0,   480,    46,    47,    65,   197,    50,    51,
      52,     0,    53,    33,     0,   464,     0,     0,     0,     0,
       0,     0,   253,   214,   254,   420,   440,   447,     0,     0,
       0,     0,     0,   432,   432,   432,   432,   423,   425,     0,
     359,   360,   358,     0,     0,     0,     0,     0,   196,   190,
       0,   188,     0,   191,   286,   291,     0,   414,   418,   416,
     391,     0,   392,   377,   401,   400,   399,   398,   396,     0,
       0,   394,     0,   369,     0,     0,   233,     0,   238,   281,
     278,   282,   251,   411,   405,     0,     0,   352,     0,     0,
       0,   404,   406,   334,   334,   317,   335,   336,   313,   329,
     314,   326,   315,     0,   112,   119,     0,   120,     0,     0,
       0,    31,     0,     0,     0,     0,     0,     0,   194,    85,
      66,    67,     0,    31,    53,   100,     0,    54,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   232,   219,   223,   218,   221,   222,   270,   266,
     267,   249,   250,   229,   230,   215,     0,     0,     0,     0,
       0,     0,   431,     0,     0,     0,     0,     0,     0,     0,
     127,     0,   343,     0,     0,   225,   121,   219,     0,   291,
       0,     0,   290,   302,   300,     0,   302,   302,     0,     0,
     288,   349,     0,     0,   355,   357,   393,   242,   243,   248,
     247,   246,   245,     0,   235,     0,   279,     0,     0,   252,
     216,   407,     0,   346,   350,   408,     0,   312,   311,     0,
       0,   107,     0,     0,     0,     0,     0,     0,   109,     0,
       0,     0,     0,   111,   363,   224,   123,     0,     0,   202,
       0,     0,    48,     0,   106,   102,    45,    84,    83,     0,
       0,     0,    80,    78,    79,    77,    76,    75,    96,    81,
       0,     0,    55,     0,     0,    94,    97,    88,    86,    90,
       0,    68,    69,    70,    71,    72,    74,    73,     0,   118,
     117,   116,   115,    35,    34,   275,   271,   272,   269,     0,
       0,     0,     0,   448,   451,   443,   444,     0,     0,   427,
       0,     0,   435,   424,     0,   432,   426,   351,     0,     0,
     129,   131,   344,   226,   294,   192,   289,   301,   297,     0,
     298,   299,     0,   287,   419,   375,   241,   280,   224,   224,
     284,   283,   409,   410,   330,   325,   259,     0,   260,     0,
     261,     0,    32,   138,   165,   141,   165,   151,   153,   146,
     157,   161,   159,   137,   136,   152,   157,   143,   135,   163,
     163,   134,   165,   165,     0,   255,   108,     0,   196,     0,
     200,     0,     0,   196,    32,     0,     0,   103,     0,    82,
       0,    95,    56,    57,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    31,    89,    87,    91,   113,     0,   268,
     227,   231,   228,     0,   455,   441,   442,   167,     0,     0,
     429,   433,   434,   428,     0,     0,   175,     0,     0,     0,
       0,   251,     0,   262,   256,   257,   258,     0,   148,   147,
       0,   139,   160,   144,     0,   142,   140,     0,   154,   145,
     149,   150,   110,   124,   122,   125,   195,   203,   206,     0,
       0,   193,    49,     0,   105,    99,     0,    92,    58,    59,
      60,    61,    62,    64,    63,    98,     0,   450,     0,     0,
     133,     0,   439,     0,     0,   430,     0,   187,   187,   187,
     187,   187,   187,   187,   187,   187,   187,   126,   173,   130,
     292,   296,     0,   217,   285,     0,   273,     0,     0,     0,
       0,   204,     0,     0,   104,    93,     0,     0,     0,   169,
       0,     0,     0,   171,   166,   437,   436,   438,     0,   186,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   295,     0,     0,   220,   164,   156,     0,   158,   162,
     207,     0,   454,   452,   453,     0,   168,   170,   172,   132,
     185,   182,   184,   177,   176,   181,   180,   179,   178,   183,
     174,   263,   274,     0,   205,   458,   459,   456,   457,   462,
     155,     0,     0,   449,     0,     0,   460,     0,     0,     0,
     461
>>>>>>> refs/remotes/origin/master
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int16 yydefgoto[] =
{
<<<<<<< HEAD
      -1,    34,    35,    36,   424,   362,   363,   364,   782,   494,
     784,   366,   547,   763,   764,   955,   367,   368,   369,   370,
     797,   531,   589,   590,   753,   754,    39,  1035,  1128,  1191,
    1192,  1193,   591,   117,   592,    42,   593,   594,   595,   596,
     597,   598,   599,    43,    44,    45,    46,   234,    47,    48,
     600,    50,   245,   893,   894,   895,   833,   983,   987,   985,
     990,   980,  1103,  1172,  1123,  1194,  1178,    51,   536,    52,
     257,   457,    53,    54,   463,   911,   601,   212,    56,   538,
     756,   950,  1057,   602,   371,    59,    60,    61,   603,    62,
     453,   723,   170,   296,   297,   298,   485,   709,   710,   716,
     584,   724,   220,   532,  1136,   217,   218,   580,   581,   807,
    1199,   301,   495,   496,   925,   926,   460,   461,   687,   903,
      63,   268,   148,    64,    65,   333,   329,   201,   324,   252,
     202,    66,   246,   247,   258,   730,    67,   259,    68,   310,
      69,   278,   279,    70,   445,   128,   339,    71,    72,    73,
      74,   168,   284,    75,   160,   161,   270,   471,    76,   281,
      77,   145,   478,    78,   181,   182,   314,   304,   468,   734,
     931,    79,   263,   469,    80,    81,    82,   440,   441,   656,
    1024,  1021,    83,   239,   429,   430,   694,  1102,  1251,  1261,
     372,   373,   203,   118,   374,    84,    85,   664,   665,   223,
     411,   412,   413,   604,   605,   606,   414,   607,   608,  1097,
    1098,  1099,   609,   861,   862,  1010,   610,   611,   612,   613,
      86,    87,    88
=======
      -1,    26,    27,    28,   390,   284,   285,   286,   553,   360,
     555,   288,   406,   534,   535,   668,   289,   290,   291,   568,
     392,    29,    30,   525,   526,    31,   187,   599,   600,   601,
     654,   721,   725,   723,   728,   718,   760,   804,   777,   778,
     810,    32,   196,   331,    33,   166,    34,   397,   528,   663,
     739,    35,   292,    37,    38,    39,    40,   328,   499,   129,
     225,   226,   227,   354,   493,   443,   500,   174,   393,   786,
     171,   172,   439,   440,   578,   824,   230,   361,   362,   619,
     620,   334,   335,   478,   608,    41,   205,   109,    42,   256,
     252,   156,   248,   193,   157,    43,   188,   189,   197,   504,
      44,   198,    45,   238,    46,   214,   215,    47,   322,    67,
     262,    48,    49,    50,    51,   127,   220,    52,   120,   121,
     207,   341,    53,   217,    54,   105,   348,    55,   138,   139,
     233,   338,    56,   200,   339,    57,    58,    59,   317,   318,
     457,   703,   700,    60,   182,   306,   307,   694,   759,   859,
     863,   293,   294,   158,   106,   295
>>>>>>> refs/remotes/origin/master
};

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
<<<<<<< HEAD
#define YYPACT_NINF -973
static const yytype_int16 yypact[] =
{
    3298,    49,  3042,    65,  3042,  3042,  3042,   181,   -96,   -96,
     148,  -973,    97,  3067,  3067,   154,   -52,  3042,  -128,   742,
    -973,    -3,  3042,   -41,  -973,   742,   -96,   106,     8,  1641,
     694,    -6,     8,   126,   236,   -60,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  -973,  -973,  -973,   217,  -973,    26,   172,
    -973,   103,    86,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,   225,  -973,  -973,  -973,   344,  -973,  -973,  -973,   357,
     361,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  -973,  -973,  -973,  -973,   363,  -973,  -973,
     381,  -973,   523,  -973,   243,  3067,   419,  -973,  -973,  -973,
     427,   298,   419,   298,  3067,  -973,   437,  -973,  -973,   437,
      50,   317,   419,   317,   317,   258,  -973,  -973,   303,   310,
    -973,   277,  -973,  -973,  -973,   329,  -973,  -973,  -973,  -973,
     -65,  -973,  3067,   369,   419,   -55,  -973,  -973,   232,   652,
      61,  -973,    24,   129,  3092,   448,   395,  3092,   478,   508,
      24,   280,  -973,    24,  -973,  -973,   521,    76,   343,  -973,
    -973,    16,  -973,  -973,   300,    16,   387,   426,   312,   420,
     327,   328,   371,   540,   386,  3067,  3067,  -973,  -973,  3298,
    3067,  2427,  -973,    61,    61,    61,   514,   210,   209,   408,
    -973,  3042,   792,  -973,  -973,  -973,   502,  1778,   551,   553,
    -973,  -973,   623,   624,   566,  -973,  3092,   488,   450,   385,
     330,  -973,   -15,   602,   626,  3067,   397,  -973,   530,  3067,
     456,  -973,  -973,  -973,  -973,   539,   549,  2380,  -973,   414,
    3067,  2380,   649,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  2728,   742,  -973,   303,   470,  -973,   433,
    2728,   564,  -973,  -973,   680,   651,  -973,  -973,   656,   660,
    -973,   661,  -973,  -973,   663,  -973,   -49,   652,  -973,  -973,
    -973,  1819,  -973,  -973,  2427,   376,  2427,    24,  3092,   419,
     683,    24,  3092,    24,  -973,  2427,  2623,  2427,  -973,  3067,
    3067,  3067,   706,  2427,  -973,    16,  -973,  3067,    69,  -973,
    -973,    16,   708,  -973,  -973,    16,  -973,  1388,   630,  -973,
     542,   678,  -973,   133,   326,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,   682,  2427,  2427,  2427,  1914,  2427,   685,
     686,   695,  -973,  -973,  -973,  3949,  -973,  -973,  -973,  -973,
     696,  -973,  -973,   698,   699,   183,   183,   183,  2427,   389,
     389,   625,  -973,  -973,  -973,  2753,  2427,   835,   -72,  3042,
    2427,   734,  2427,     8,  2623,  -973,   504,   519,   524,   525,
     528,   529,   532,   533,   537,   559,   561,   568,   570,   571,
     575,   648,   792,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,    49,     8,   -17,  3949,  -973,   573,   759,   522,
    -973,   762,   764,   798,   679,   684,   739,   739,   739,   -14,
     579,  -973,   750,  -973,  -973,  -973,   658,   720,   791,   419,
     757,   688,  2085,   172,   830,  -973,  1991,   591,  2799,   374,
    2996,  -973,   419,   -35,  -973,   -40,   682,  -973,  -973,   596,
    -973,   653,  -973,  -973,  -973,  -973,  -973,  -973,  -973,   419,
     419,  -973,   419,  -973,   845,    13,   697,  3067,   304,  3067,
    -973,   652,  -973,  -973,  1481,  -973,   -20,  3949,   811,   817,
    3949,  2427,    24,  -973,   856,  2427,    24,   824,  3949,  -973,
    3949,    16,    16,  -973,  -973,  3949,  -973,    16,   827,  -973,
     621,  -973,   829,  -973,  -973,  2164,  -973,  2256,  2335,    18,
    3670,  2427,   832,   700,  3092,  3092,  -973,  2291,   692,  -973,
    2980,  -973,  -973,   629,  3877,   667,  3949,   676,  -973,  3092,
    2427,  2427,   592,  2427,  2427,  2427,  2427,  2427,  2427,  2427,
    2427,  2703,   842,   659,  2427,  2427,  2427,  2427,  2427,  2427,
    2427,   303,  2882,     8,  -973,   869,  -973,   869,  -973,  3788,
     644,  -973,  -973,  -973,   -69,   758,  -973,  1130,  3842,   650,
     654,   669,   670,   675,   677,   681,   687,   689,   690,   701,
     703,   707,   709,   710,   796,   835,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  -973,  2427,  -973,   793,  3053,   867,  3151,
    1738,   218,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,     8,  2427,   893,  -973,  3092,   600,   900,   902,   901,
     904,  -973,  3092,  3092,  3092,  3067,  3092,   459,   856,  3466,
     915,  -973,   916,   917,    20,  -973,  -973,  2914,  -973,   856,
    3067,  2427,  3949,  -973,  -973,  1450,   374,   899,  2380,  3067,
    -973,   784,  -973,  2380,   -80,   -48,  -973,   810,  3067,  -973,
    -973,  3067,  3067,   939,  -973,   649,   419,  -973,  -973,   414,
     903,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,    42,
    -973,   711,  -973,  -973,  -973,  -973,   905,  3067,   652,  3042,
    -973,  2593,  1819,  -973,  -973,  -973,  -973,  3949,  2427,  -973,
    -973,  3949,  2427,   600,  -973,  -973,  -973,  -973,   920,   952,
    -973,  2427,  3691,  2427,  3723,  2427,  3744,  -973,  3466,  2427,
    3901,  -973,  -973,   -88,  -973,   943,    47,  -973,    52,   924,
    2427,  -973,  2427,   -31,  -973,  -973,  2046,  2980,  2427,  2703,
     842,  1138,  1138,  1138,  1138,  1138,  1138,  1304,  1042,  2703,
    2703,  -973,   964,  1914,  -973,  -973,  -973,  -973,  -973,   449,
     507,   507,   931,   931,   931,   931,  -973,   929,  -973,  -973,
    -973,  -973,  -973,  -973,    61,  -973,  -973,  -973,  2427,   389,
     389,   389,  -973,   944,  -973,   944,  -973,  -973,  -973,   945,
     833,   948,  -973,  -973,  -973,   945,  -973,  -973,   950,   950,
    -973,   944,   944,     2,   835,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
     942,  -973,  1566,   861,   864,   873,   874,  1002,   649,   947,
    2427,    43,  -973,  2670,   835,   -68,  -973,  -973,  3949,  -973,
    -973,  -973,   926,  -973,  -973,  1006,  1010,  -973,  3466,   -18,
      29,  -973,   821,   739,  -973,  -973,   970,  3466,  3466,  3466,
    -973,   593,   891,    54,  -973,  -973,  -973,   988,  3949,   963,
     374,  -973,  -973,  -973,   261,  -973,  -973,  -973,  -973,  2380,
    -973,  -973,  -973,  -973,  -973,  -973,   414,  -973,  -973,   697,
    3067,  -973,   994,  -973,   456,   -66,   374,  -973,  3949,  3949,
    -973,    55,  -973,  -973,  3949,  2427,  3949,  2427,  3949,  2427,
     995,  3949,  -973,  3092,   172,  2427,  -973,  3092,    49,  2427,
    -102,  3925,  3172,  2427,  -973,   933,  1042,   984,  -973,  -973,
    -973,  2703,  2703,  2703,  2703,  2703,  2703,  2703,  2703,    56,
    -973,  -973,  -973,  -973,  1819,  -973,  -973,  -973,  -973,  1032,
    -973,  -973,  1035,  -973,  -973,  -973,  1038,  -973,  -973,  1039,
    -973,  -973,  -973,  -973,   799,   600,  -973,   941,   802,  -973,
     649,   649,   649,   649,   918,   596,  1045,  3476,   835,  -973,
     954,  2703,   518,   340,   649,   881,  -973,  -973,  -973,    67,
      71,  -973,  -973,  -973,  -973,  3042,  -973,  1007,  1008,  1009,
    -973,  1024,   276,  2914,  3092,   855,  3067,  2427,   431,  -973,
     820,  -973,   210,  2380,   961,  -973,   600,  3949,  3949,  3949,
    -973,  -973,  -973,  3949,  -973,  -973,  3949,    58,  1028,  -973,
    -973,  2427,  3949,  -973,  2703,  1202,   756,   756,  1031,  1031,
    1031,  1031,  -973,  -973,   -63,  1043,    60,  1044,  1047,  -973,
     837,  1041,  -973,   596,   596,   596,   596,   649,   955,   835,
    -973,  1011,   765,  2703,  2427,   835,   965,   425,  -973,   985,
     -33,  1069,   870,    66,  1097,  -973,  1098,   600,  -973,  -973,
    -973,  -973,  3092,  1085,  1085,  1085,  1085,  1085,  1085,  1085,
    1085,  1085,  1085,   866,  -973,  -973,    62,  1071,   276,  -973,
    3949,  2427,  3067,  -973,   374,  1037,   996,  -973,  -973,  2427,
    2427,  3949,  1202,  2380,  -973,  -973,  1111,  -973,  -973,  -973,
     875,   596,   649,  -973,   876,  2703,   940,  3499,  -973,   878,
     992,  -973,  1014,   998,  2380,   262,  1102,  -973,  1117,   600,
    1000,  -973,  -973,  -973,  -973,  -973,    68,  -973,  1124,  1125,
     956,  1131,  1134,  1135,  1137,  1139,  1140,  1141,   276,  -973,
    3092,  -973,   892,  -973,  -973,  3949,  1101,  2427,  2427,  -973,
    3949,    73,  1105,  -973,   596,  -973,  1021,   835,   835,  -973,
     906,  1023,   907,   -66,  -973,  -973,  -973,   351,  -973,  -973,
    -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,    74,   276,  3067,   912,  3949,  -973,  -973,
     835,  1054,  -973,  -973,   934,  -973,   625,  -973,  -973,  -973,
    -973,   -32,  -973,  -973,   914,  1057,  1099,  -973,  -973,  1127,
    1153,  -973,  3067,  1132,   936,  1174,  1180,  1143,   949,  -973,
    -973,   953,  3067,  -973,  1165,  1150,  1191,  -973,  -973
=======
#define YYPACT_NINF -547
static const yytype_int16 yypact[] =
{
     829,    19,   -64,   -93,   -93,    55,  -547,    18,  2091,  2091,
      75,  2123,  -112,   471,  -547,  2123,   -51,  -547,   471,   -93,
      25,    41,   481,   380,   -18,    41,   217,   -42,  -547,  -547,
    -547,  -547,  -547,  -547,   113,  -547,    -7,    48,  -547,    62,
      12,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,    96,  -547,
    -547,   185,   190,   253,  2091,   258,  -547,  -547,  -547,   133,
     258,  2091,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   264,  -547,
    -547,   264,   170,   258,  -547,   107,  -547,  -547,  -547,   249,
     156,  -547,   125,  -547,  -547,   177,  -547,  -547,  -547,  -547,
     -59,  -547,   230,   258,   -22,  -547,  -547,   171,   179,    60,
    -547,    22,    22,  2173,   280,   236,  2173,   291,   131,  -547,
      22,  -547,  -547,    44,   195,  -547,  -547,    10,  -547,  -547,
     163,    10,   292,   192,   279,   188,   197,   229,   370,   237,
    2091,  -547,  -547,   829,  2091,  1684,  -547,    60,    60,    60,
     360,   146,   152,   248,  -547,  2123,  -547,  -547,  -547,  2173,
     299,   262,   318,   231,  -547,   -54,   406,  2091,   221,  -547,
     327,   265,  -547,  -547,  -547,   353,   580,  -547,   239,   431,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    1264,   471,   249,   286,  -547,   255,  1264,   363,  -547,  -547,
     458,  -547,  -547,  -547,   426,   -16,   179,  -547,  -547,  -547,
    1119,  -547,  -547,  1684,  1684,    22,  2173,   258,   444,    22,
    2173,  1748,  1684,  2091,  2091,  2091,   472,  1684,  -547,    10,
    -547,    67,  -547,  -547,    10,   474,  -547,  -547,    10,  -547,
     854,   419,  -547,   337,  -547,    27,    30,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  1684,  1684,  1684,  1152,
    1684,   445,   446,   453,  -547,  -547,  -547,  2665,  -547,  -547,
    -547,   462,  -547,  -547,   456,   464,    51,    51,    51,  1684,
      91,    91,   415,  -547,  -547,  -547,   315,  -547,   499,   501,
     502,   418,   434,   475,   475,   475,    -1,   333,  -547,   495,
    -547,  -547,  -547,   467,   518,   258,   506,  1355,    48,  -547,
    1030,   355,  1920,   276,  1970,  -547,   258,  -547,  -547,   356,
    -547,   402,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   258,
     258,  -547,   258,  -547,   -14,   225,  -547,   179,  -547,  -547,
    1058,  -547,   -39,  2665,  2665,  1684,    22,  -547,   561,  1684,
      22,  -547,  2665,    10,    10,  -547,  -547,  2665,  -547,   530,
    -547,   364,  -547,   532,  -547,  -547,  1388,  -547,  1476,  1596,
     -10,  2484,  1684,   533,   429,  2173,  1811,   409,  -547,  2684,
    -547,  -547,   373,  2593,   254,  2665,   407,  -547,  2173,  1684,
    1684,   394,  1684,  1684,  1684,  1684,  1684,  1684,  1684,  1684,
    1717,   544,   387,  1684,  1684,  1684,  1684,  1684,  1684,  1684,
     249,  2021,    41,  -547,   586,  -547,   586,  -547,  2535,   398,
    -547,  -547,  -547,   -68,   497,  -547,  2173,   147,   604,   606,
     587,   589,  -547,  2173,  2173,  2173,  2091,  2173,   232,   561,
    -547,  2053,  -547,   561,  1684,  2665,  -547,  1849,   276,   583,
     580,  2091,  -547,   498,  -547,   580,   498,   498,   542,  2091,
    -547,  -547,   431,   258,  -547,  -547,   239,  -547,  -547,  -547,
    -547,  -547,  -547,   611,   179,  2123,  -547,  1888,  1119,  -547,
    -547,  2665,  1684,  -547,  -547,  2665,  1684,  -547,  -547,   612,
     649,  -547,  1684,  2109,  1684,  2312,  1684,  2514,  -547,  1684,
    2329,  1684,  2617,  -547,  -547,  -110,  -547,   632,    -5,  -547,
     617,  1684,  -547,  1684,   -24,  -547,  -547,  2565,  2684,  1684,
    1717,   544,   559,   559,   559,   559,   559,   559,   599,   610,
    1717,  1717,  -547,   399,  1152,  -547,  -547,  -547,  -547,  -547,
     326,   422,   422,   620,   620,   620,   620,  -547,   618,  -547,
    -547,  -547,  -547,  -547,  -547,    60,  -547,  -547,  -547,  1684,
      91,    91,    91,  -547,   602,  -547,  -547,   684,   685,  -547,
    2329,   -15,    61,  -547,   524,   475,  -547,  -547,   584,    -2,
    -547,  -547,  -547,  2665,   645,   276,  -547,  -547,  -547,     7,
    -547,  -547,   580,  -547,  -547,   239,  -547,  -547,   265,  -107,
     276,  -547,  2665,  2665,  -547,  -547,  2665,  1684,  2665,  1684,
    2665,  1684,  2665,  -547,   654,  -547,   654,  -547,  -547,  -547,
     655,   569,   657,  -547,  -547,  -547,   655,  -547,  -547,   659,
     659,  -547,   654,   654,   663,  2665,  -547,  2173,    48,  1684,
      19,  2173,  1684,  -105,  2641,  1250,  1684,  -547,   623,   610,
     477,  -547,  -547,  -547,  1717,  1717,  1717,  1717,  1717,  1717,
    1717,  1717,     1,  2665,  -547,  -547,  -547,  -547,  1119,  -547,
    -547,  -547,  -547,   696,   567,  -547,  -547,  -547,    65,    68,
    -547,  -547,  -547,  -547,  2123,   673,   173,  2053,  2091,  1684,
     257,   146,   580,   619,  2665,  2665,  2665,   710,  -547,  -547,
     712,  -547,  -547,  -547,   713,  -547,  -547,   715,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  2665,  -547,  -547,  2665,     2,
     681,  -547,  -547,  1684,  2665,  -547,  1717,   496,   581,   581,
     686,   686,   686,   686,  -547,  -547,   -35,  -547,   698,   531,
      20,   717,  -547,   718,   147,  -547,  2173,   701,   701,   701,
     701,   701,   701,   701,   701,   701,   701,   522,  -547,  -547,
    -547,  2665,  1684,  -547,   276,   676,   635,   702,     4,   703,
     711,  -547,  1684,  1684,  2665,   496,   580,    90,   730,  -547,
     732,   147,   646,  -547,  -547,  -547,  -547,  -547,     8,  -547,
     752,   753,   607,   754,   783,   784,   786,   787,   788,   780,
     173,  2665,  1684,  1684,  -547,  -547,  -547,   789,  -547,  -547,
    2665,     9,  -547,  -547,  -547,   139,  -547,  -547,  -547,  -547,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,
    -547,   588,  2665,   765,  -547,  -547,  -547,  -547,  -547,    -4,
    -547,   785,   790,  -547,   803,   806,  -547,   621,   792,   808,
    -547
>>>>>>> refs/remotes/origin/master
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
<<<<<<< HEAD
    -973,  -973,  -973,   989,  -332,  -973,  -611,  -366,  -205,   800,
     430,  -973,  -973,  -973,   434,  -973,   -21,  1171,  -973,  -973,
    -973,  -973,    17,    22,  -973,   259,  -973,  -973,  -973,  -973,
    -973,   -28,    31,    33,    41,  -973,  -179,  -144,  -143,  -142,
    -137,   -85,   -84,  -973,  -973,  -973,  -973,   597,  -973,  -973,
      57,  -973,  1074,  -973,   175,   557,  -604,   391,  -973,  -973,
     383,  -690,  -973,  -973,  -973,  -972,   -19,  -973,  -973,  -973,
     422,   958,  -973,  -973,  -973,  -973,    40,  -427,  -973,  -973,
    -530,  -973,    77,     6,   104,    12,  -973,   318,   -83,   356,
    -719,   997,   -11,  -973,   729,  -281,  -973,  -973,   302,  -973,
    -361,   182,  -217,  -973,  -973,  -973,  -973,  -973,   418,  -973,
    -973,  -185,   505,  -588,    64,  -247,    32,   773,  -973,  -116,
    -973,  -973,  -973,  -973,  -973,  -973,  -973,  -973,   -45,  1091,
    -973,  -973,  -973,   783,   -74,  -548,  -973,  -432,  -973,  1046,
    -973,   761,  -973,  -973,  -973,   220,  -973,  -973,  -973,  -973,
    -973,  -973,  -973,  -973,  1219,   971,  -973,   966,  -973,  -973,
    -973,    11,  -973,  -973,   854,   935,  -973,  -111,   554,  -973,
    -973,  -973,  -973,  -376,  -973,  -973,  -973,  -973,   598,   309,
    -973,  -973,  -973,  -973,  -973,   605,   382,  -973,  -973,  -973,
      99,    51,  -973,     1,    -2,  -973,  -973,  -973,   362,  -973,
    -973,  -973,   844,  -798,  -973,   657,  -973,   -82,   -79,  -973,
     160,   161,   -76,  -973,   400,  -973,   -75,   -73,   -71,   -70,
    -973,  -973,  -973
=======
    -547,  -547,  -547,   648,  -271,  -547,  -432,  -373,  -428,   322,
     277,  -547,  -547,  -547,   283,  -547,   796,  -547,  -547,  -547,
    -547,   -91,   -88,  -547,   164,  -547,  -547,  -547,   115,   367,
     235,   180,  -547,  -547,   181,  -546,  -547,  -547,  -547,     3,
      29,  -547,  -547,  -547,   -86,  -311,  -547,  -547,    63,  -547,
      34,   -99,     0,     5,  -547,   222,   246,  -390,   661,   -21,
    -547,   473,  -200,  -547,  -547,  -266,   122,  -128,  -547,  -547,
    -547,  -547,  -547,   256,  -547,  -547,  -155,   339,   151,  -547,
    -185,    16,   510,  -547,   -73,  -547,  -547,  -547,  -547,  -547,
    -547,  -547,  -129,   741,  -547,  -547,  -547,   519,   -37,  -349,
    -547,  -296,  -547,   693,  -547,   500,  -547,  -547,  -547,   238,
    -547,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   828,   636,
    -547,   633,  -547,  -547,  -547,    17,  -547,  -547,  -547,   613,
    -103,   366,  -547,  -547,  -547,  -547,  -547,  -547,  -547,   393,
     233,  -547,  -547,  -547,  -547,  -547,   410,  -547,  -547,  -547,
    -547,    46,    11,  -547,  -353,    -6
>>>>>>> refs/remotes/origin/master
};

/* YYTABLE[YYPACT[STATE-NUM]].  What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule which
<<<<<<< HEAD
   number is the opposite.  If zero, do what YYDEFACT says.
   If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -557
static const yytype_int16 yytable[] =
{
     119,   382,   119,   119,   119,   529,    57,   758,   180,   587,
     459,   138,   138,    90,   459,   119,   492,    37,   146,   585,
     119,   205,    38,   146,   641,   543,   673,   184,   375,   376,
     377,    40,   865,   163,   944,   872,   997,   121,   122,   123,
      55,    41,   693,   400,   322,   136,   139,   208,   302,  1259,
     699,  1143,   242,   254,   701,   886,   994,    49,   248,   747,
    1124,   890,   306,  1014,   137,   137,  1013,  -269,   120,   315,
     902,   651,   317,   241,   953,   490,  1167,   809,   401,   402,
     403,  1164,   273,   918,  1019,   404,  1168,  1104,   946,     1,
     277,  1106,   280,   948,   721,  1032,  1045,  1073,   995,  1138,
     211,  1145,   902,  1189,    58,    89,   319,  1022,   127,  1221,
     885,   442,   691,   452,  1238,  1252,   147,   379,   213,   214,
     215,   896,   930,   138,   443,   981,   380,  -329,   183,   299,
     614,   169,   138,  -269,   144,   452,   905,   405,   406,   410,
     415,   992,   993,   416,   940,  1058,   417,   418,  1008,   419,
     326,   420,   421,   302,   267,   164,  1020,   240,   518,   943,
     138,   300,  1169,  1105,   586,   264,   250,  1107,   907,   305,
     265,   762,   184,   537,   615,   184,   137,   255,   810,   722,
     655,  1043,   274,   162,   722,   137,   209,   204,   266,  1023,
     320,   130,   274,  -329,   275,   781,   501,   140,   491,   219,
     505,  1260,   507,   138,   138,  1042,  1044,   678,   138,   676,
    1090,   134,   692,   137,   695,    57,  1232,   323,   303,   119,
      28,  1170,   146,     1,   167,   444,    37,   722,   409,   129,
     642,    38,   384,   400,   184,   503,   207,   340,   341,   396,
      40,   467,   343,   138,   397,   860,   166,   138,   996,    55,
      41,   216,    58,   398,   503,   138,   137,   137,   138,   138,
     702,   137,   408,   399,   916,   642,    49,   891,   401,   402,
     403,   138,  -269,   307,  1018,   404,   311,   448,   138,   407,
     516,   451,   477,  1027,  1028,  1029,   519,  1171,  -329,   919,
     521,  1153,   464,   474,   947,   180,   137,  1158,   475,   947,
     137,  1033,  1046,   642,    28,  1139,   184,  1146,   137,   947,
     184,   137,   137,    58,   184,   947,   476,   138,   138,   138,
    1139,   947,   219,   303,   472,   138,    58,   405,   406,   410,
     415,   472,  -329,   416,   131,   431,   417,   418,  1214,   419,
     141,   420,   421,     1,   132,   206,  -340,   379,   210,   539,
     142,   511,   512,   513,   573,  -340,   380,   133,  1113,   517,
     235,   458,   124,   143,   467,   458,  -556,   125,  -554,    90,
     137,   137,   137,   180,   211,   248,   681,   126,   137,   221,
      58,   498,   620,   119,  1080,  1094,  1074,   119,   690,   682,
     499,   728,   184,   683,   582,   732,   436,   684,   224,   437,
      12,   583,   225,   781,  1215,   697,   277,   502,  1037,  1241,
    1242,   506,   640,   781,   781,   183,   222,   236,   409,   226,
     712,   227,   616,   241,   685,   244,   237,   238,   639,   396,
     243,   900,   438,    20,   397,  1137,   904,   492,   282,   283,
    1216,   251,  1255,   398,   256,  1095,  1096,    58,   976,   977,
     978,   969,   408,   399,   138,   262,   138,   970,   138,   971,
     972,   545,    24,   548,   866,   316,   735,   736,    90,   407,
    1094,   269,   737,   271,    28,   434,   435,   686,   272,   574,
     574,   574,  1005,   276,   713,   138,   714,   138,   439,   681,
     308,    58,   119,   183,  1246,   720,  1175,   781,    32,  1114,
    1115,  1116,   682,  1117,  1126,  1118,   683,   137,   309,   680,
     684,   689,   715,  1119,  1120,  1121,    58,  1052,  1122,   711,
     312,   717,   313,  1059,   318,   436,    58,   316,   437,   321,
    1095,  1160,   184,   184,   327,   184,   325,   685,   137,   328,
     137,   566,   567,   568,   569,   570,   681,   184,   331,    90,
     962,   963,   964,   965,   966,   967,   968,   332,  1219,   682,
     675,   438,   804,   683,   957,   260,   261,   684,   906,   908,
     184,   335,   334,  1247,   959,   960,   336,   801,  1131,  1248,
     337,  1249,  1176,   228,   229,   230,   231,   232,   798,   338,
     686,   378,  1250,   799,   685,   781,   781,   781,   781,   781,
     781,   781,   781,   383,   345,   346,   347,   659,   348,   349,
     350,   800,   351,   352,   871,   422,  -173,   883,  -174,   974,
     768,   769,   770,   660,  1083,  1084,  1085,  1086,   426,   427,
     867,   428,   432,   755,   757,   433,   757,   233,  1100,   662,
     663,    89,   446,   184,   449,   781,   447,   686,   765,   454,
     184,   184,   184,   138,   184,   285,   450,   452,  1012,   455,
    1233,   462,  1038,   466,   479,   184,   785,   786,   138,   787,
     788,   803,   659,   138,   467,    58,   138,   138,   482,   789,
     480,   138,  -279,  -279,  -279,   483,   138,   881,   660,   138,
     138,   484,   286,   575,   577,   578,   486,   185,   781,   661,
     487,   488,   897,   489,   662,   663,   137,   504,   224,    58,
     514,  1151,  1093,   520,   533,   138,   534,   119,   535,   138,
     923,   137,   305,   912,   913,     1,   680,   781,   549,   137,
     901,   576,   576,   576,   137,  -136,   571,   186,   219,   910,
     572,  -555,   137,   137,   431,   652,   653,   654,   618,   922,
     622,   878,   879,   880,   637,   882,  1065,  1066,  1067,  1068,
    1069,  1070,  1071,  1072,   644,   623,   878,   643,   137,   645,
     624,   625,   137,   360,   626,   627,  1204,   287,   628,   629,
     187,   188,   458,   630,   288,   289,   646,   458,   647,   781,
     964,   965,   966,   967,   968,    90,  1134,   962,   963,   964,
     965,   966,   967,   968,  -279,   631,  1092,   632,   290,   189,
     149,   150,   190,  -279,   633,  -279,   634,   635,   291,   292,
     293,   636,   648,   649,   651,   458,   657,   658,   650,   151,
     666,   667,     1,   674,   669,   670,   385,   467,   678,   152,
     386,   387,   388,   695,   153,   191,   154,   696,   700,     4,
     389,     6,   725,   853,   854,   855,   856,   857,   726,  1142,
     729,   294,   155,   192,   733,  1236,   193,   738,   739,   295,
     740,   390,   156,   751,   785,     1,   760,   752,   762,     2,
     194,   195,   783,   386,   387,   388,   215,   545,  1156,    12,
     759,   808,     4,   389,     6,   811,   835,   863,   869,  -420,
     836,   157,   850,   196,   873,   391,   874,   138,   703,   704,
     705,   706,   707,   158,   390,   837,   838,   708,   138,   392,
     197,   839,    20,   840,   858,   875,   198,   841,   876,   887,
     888,   889,    12,   842,   902,   843,   844,   159,    58,   199,
     899,   184,   909,   914,   917,   184,   921,   845,   391,   846,
    1206,    24,  1040,   847,  1055,   848,   849,   933,   920,  1155,
     137,   932,   392,   393,   949,    20,   394,   945,    58,   570,
     973,   137,   962,   963,   964,   965,   966,   967,   968,   467,
     467,   467,   467,   961,   979,   982,   984,    32,   986,  1207,
     989,   998,  1000,   467,    24,  1001,   962,   963,   964,   965,
     966,   967,   968,  1064,  1002,  1003,   393,  1004,   693,   394,
    1016,   365,  1006,   458,  1017,  1025,   962,   963,   964,   965,
     966,   967,   968,   119,  1026,  1031,  1108,   425,  1034,  1258,
      32,   184,   184,  1036,   138,  1041,  1050,  1075,   395,  1063,
    1076,   138,   755,  1077,  1078,  1079,  1054,  1081,  1082,  1087,
    1088,  1101,    58,   962,   963,   964,   965,   966,   967,   968,
    1091,  1109,  1110,  1111,  1112,  1127,   467,  1132,  1140,   968,
    1240,   561,   562,   563,   564,   565,   566,   567,   568,   569,
     570,   395,  1135,  1149,  1144,  1147,  1152,  1129,  1148,  1150,
    1154,  1163,  1159,  1165,   137,  1179,  1180,  1181,  1182,  1183,
    1184,  1185,  1186,  1187,   497,  1166,   500,  1173,  1174,  1177,
     184,  1190,    58,  1188,  1197,   508,  1202,   510,  1198,  1210,
    1211,  1203,  1205,   515,  1209,  1212,  1217,  1218,  1222,  1223,
     138,   467,   878,   757,  1220,  1225,  1224,   530,  1226,  1234,
    1227,   138,  1228,  1235,  1229,  1230,  1239,   458,  1264,  1231,
    1244,  1265,  1243,  1245,   540,   541,   542,   544,   546,   642,
    1256,  1262,   138,  1263,  1196,   559,   560,   561,   562,   563,
     564,   565,   566,   567,   568,   569,   570,  1266,   579,  1270,
    1257,  1268,  1269,   137,  1271,  1272,   588,  1274,   184,  1276,
     617,  1277,   619,    58,   137,  1273,  1278,   954,   342,    58,
     958,   200,  1051,   812,   813,   814,  1253,   249,  1125,   877,
     815,   757,   991,   859,   381,   137,   988,  1201,   816,   465,
     718,  1039,   817,   818,  1133,   819,   975,   927,  1213,   677,
     253,   820,   668,   138,   962,   963,   964,   965,   966,   967,
     968,   698,   821,   330,   165,   473,   481,   458,   621,   915,
     870,   509,   672,  1030,  1015,   884,   638,  1161,  1162,   822,
     138,  1009,   851,     0,     0,     0,     0,  1254,   458,     0,
     138,   823,     0,   824,     0,   825,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   137,   826,     0,   757,
       0,     0,     0,     0,  1267,     0,     0,     0,     0,     0,
       0,   727,     0,     0,  1275,   731,   827,     0,     0,     0,
       0,    58,    58,   137,     0,     0,     0,     0,     0,   828,
     829,   830,     0,   137,     0,   742,     0,   744,   746,   831,
     832,   750,   560,   561,   562,   563,   564,   565,   566,   567,
     568,   569,   570,     0,    58,     0,     0,     0,     0,     0,
     766,   767,     0,   771,   772,   773,   774,   775,   776,   777,
     778,     0,     0,     0,   790,   791,   792,   793,   794,   795,
     796,     0,     0,     0,     0,     0,     0,   695,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   344,   345,   346,   347,     0,   348,   349,   350,     0,
     351,   352,   353,     0,     0,     0,     0,     0,   354,     0,
       0,     0,     0,     0,   852,     0,     0,     0,     0,     0,
     355,   356,   522,     0,     0,     0,     0,     0,   357,   523,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   868,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   135,     0,     0,   524,     0,     0,     0,
       0,     0,     0,     0,   525,     0,     0,   358,     0,     0,
       0,   898,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,    91,     0,     0,     0,   526,     0,
       0,   224,     0,     0,     0,     0,     0,     0,   359,   550,
     551,   552,   553,   554,   555,   556,   557,   558,   559,   560,
     561,   562,   563,   564,   565,   566,   567,   568,   569,   570,
     679,     0,     0,   527,     0,     0,     0,     0,   928,     0,
       0,     0,   929,     0,     0,     0,     0,     0,     0,     0,
       0,   934,     0,   936,     0,   938,     0,     0,     0,   941,
       0,   719,     0,     0,     0,     0,     0,     0,     0,     0,
     951,   360,   952,     0,     0,     0,     0,     0,   956,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     528,     0,     0,   425,   550,   551,   552,   553,   554,   555,
     556,   557,   558,   559,   560,   561,   562,   563,   564,   565,
     566,   567,   568,   569,   570,     0,     0,     0,   579,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   361,
     103,   104,   105,   106,   107,   108,   109,   110,   111,   112,
     113,   114,   115,   116,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   171,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   172,   173,     0,     0,     0,     0,
    1007,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,    92,    93,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   112,   113,   114,   115,   116,     0,     0,     0,     0,
       0,     0,    92,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   115,   116,     0,     0,     0,
       0,     0,     0,     0,     0,  1047,     0,  1048,     0,  1049,
       0,   344,   345,   346,   347,  1053,   348,   349,   350,  1056,
     351,   352,   353,  1062,     0,     0,     0,     0,   354,   174,
     175,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     355,   356,   493,     0,     0,     0,     0,     0,   357,     0,
       0,   344,   345,   346,   347,     0,   348,   349,   350,     0,
     351,   352,   353,   176,     0,     0,     0,     0,   354,     0,
       0,     0,     0,     0,     0,     0,   299,     0,     0,     0,
     355,   356,   999,   177,   178,   179,     0,   358,   357,   423,
       0,     0,   344,   345,   346,   347,     0,   348,   349,   350,
       0,   351,   352,   353,     0,     0,     0,  1130,   300,   354,
       0,     0,     0,     0,     0,     0,     0,     0,   359,     0,
       0,   355,   356,   493,     0,     0,     0,   358,     0,   357,
       0,  1141,    92,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   115,   116,     0,   359,     0,
       0,     0,     0,     0,  1157,     0,     0,     0,   358,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   360,     0,     0,     0,     0,     0,   344,   345,   346,
     347,     0,   348,   349,   350,     0,   351,   352,   353,   359,
       0,  1195,     0,     0,   354,     0,     0,     0,     0,  1200,
    1056,     0,     0,     0,     0,     0,   355,   356,     0,     0,
       0,   360,     0,     0,   357,     0,     0,     0,     0,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   361,
     103,   104,   105,   106,   107,   108,   109,   110,   111,   112,
     113,   114,   115,   116,     0,     0,     0,     0,     0,     0,
       0,     0,   360,   358,   135,     0,     0,   425,  1237,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   361,
     103,   104,   105,   106,   107,   108,   109,   110,   111,   112,
     113,   114,   115,   116,   359,     0,     0,     0,     0,     0,
       0,   456,     0,     0,     0,     0,     0,     0,     0,     0,
      92,    93,    94,    95,    96,    97,    98,    99,   100,   101,
     361,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,   113,   114,   115,   116,   551,   552,   553,   554,   555,
     556,   557,   558,   559,   560,   561,   562,   563,   564,   565,
     566,   567,   568,   569,   570,    28,     0,   360,   344,   345,
     346,   347,   671,   348,   349,   350,     0,   351,   352,   353,
       0,     0,     0,     0,     0,   354,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   355,   356,     0,
       0,     0,     0,     0,     0,   357,     0,     0,     0,     0,
       0,     0,     0,     0,     0,    92,    93,    94,    95,    96,
      97,    98,    99,   100,   101,   361,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   112,   113,   114,   115,   116,
       0,     0,    28,     0,   358,     0,     0,   344,   345,   346,
     347,     0,   348,   349,   350,     0,   351,   352,   353,     0,
       0,     0,     0,     0,   354,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   359,   355,   356,     0,     0,
       0,     0,     0,     0,   357,     0,     0,     0,     0,     0,
       0,     0,    92,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   115,   116,     0,     0,     0,
       0,     0,     0,   358,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   360,   344,
     345,   346,   347,     0,   348,   349,   350,     0,   351,   352,
     353,     0,     0,     0,   359,     0,   354,     0,   741,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   355,   356,
       0,     0,     0,     0,   171,     0,   357,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    92,    93,    94,    95,
      96,    97,    98,    99,   100,   101,   361,   103,   104,   105,
     106,   107,   108,   109,   110,   111,   112,   113,   114,   115,
     116,     1,     0,     0,     0,   358,     0,   360,   344,   345,
     346,   347,     0,   348,   349,   350,     0,   351,   352,   353,
       0,     0,     0,     0,     0,   354,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   359,   355,   356,     0,
     743,     0,     0,     0,     0,   357,     0,     0,     0,     0,
       0,     0,     0,   135,     0,    92,    93,    94,    95,    96,
      97,    98,    99,   100,   101,   361,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   112,   113,   114,   115,   116,
       0,     0,     0,     0,   358,     0,     0,     0,     0,     0,
     456,     0,     0,     0,     0,     0,     0,     0,     0,   360,
     344,   345,   346,   347,     0,   348,   349,   350,     0,   351,
     352,   353,     0,     0,     0,   359,     0,   354,     0,   745,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   355,
     356,     0,    28,     0,     0,     0,     0,   357,     0,     0,
       0,     0,     0,     0,     0,     0,     0,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   361,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,     0,     0,     0,     0,   358,     0,   360,     0,
       0,     0,    92,    93,    94,    95,    96,    97,    98,    99,
     100,   101,   102,   103,   104,   105,   106,   107,   108,   109,
     110,   111,   112,   113,   114,   115,   116,   359,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,    92,    93,    94,    95,
      96,    97,    98,    99,   100,   101,   361,   103,   104,   105,
     106,   107,   108,   109,   110,   111,   112,   113,   114,   115,
     116,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   135,     0,     0,     0,
     360,    92,    93,    94,    95,    96,    97,    98,    99,   100,
     101,   102,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   112,   113,   114,   115,   116,   171,     0,     0,     0,
       0,     0,     0,   456,     0,     0,   172,   173,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,   361,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     114,   115,   116,   344,   345,   346,   347,     0,   348,   349,
     350,     0,   351,   352,   353,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   924,     0,     0,     0,
       0,     0,   779,   780,     0,     0,   344,   345,   346,   347,
     357,   348,   349,   350,     0,   351,   352,   353,     0,     0,
       0,     0,     0,     0,     0,  1011,     0,     0,     0,     0,
       0,   135,     0,     0,     0,   779,   780,     0,     0,     0,
       0,   174,   175,   357,     0,     0,     0,     0,     0,   358,
       0,     0,     0,     0,     0,     0,    91,     0,     0,     0,
       0,     0,   470,     0,     0,     0,     0,   466,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     359,     0,   358,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   177,   178,     0,     0,     0,
       0,     0,   135,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   359,    92,    93,    94,    95,    96,    97,
      98,    99,   100,   101,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,   113,   114,   115,   116,     0,
       0,     0,     0,   360,    92,    93,    94,    95,    96,    97,
      98,    99,   100,   101,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,   113,   114,   115,   116,   679,
       0,     0,     0,     0,     0,     0,   360,     0,     0,     0,
       0,     0,     0,     0,     0,   171,     0,     0,     0,     0,
       0,    92,    93,    94,    95,    96,    97,    98,    99,   100,
     101,   361,   103,   104,   105,   106,   107,   108,   109,   110,
     111,   112,   113,   114,   115,   116,   802,   171,     0,     0,
       0,     0,     0,     0,    92,    93,    94,    95,    96,    97,
      98,    99,   100,   101,   361,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,   113,   114,   115,   116,    92,
      93,    94,    95,    96,    97,    98,    99,   100,   101,   102,
     103,   104,   105,   106,   107,   108,   109,   110,   111,   112,
     113,   114,   115,   116,    92,    93,    94,    95,    96,    97,
      98,    99,   100,   101,   102,   103,   104,   105,   106,   107,
     108,   109,   110,   111,   112,   113,   114,   115,   116,   135,
     552,   553,   554,   555,   556,   557,   558,   559,   560,   561,
     562,   563,   564,   565,   566,   567,   568,   569,   570,     0,
      92,    93,    94,    95,    96,    97,    98,    99,   100,   101,
     102,   103,   104,   105,   106,   107,   108,   109,   110,   111,
     112,   113,   114,   115,   116,    91,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   688,     0,     0,   892,
     135,   550,   551,   552,   553,   554,   555,   556,   557,   558,
     559,   560,   561,   562,   563,   564,   565,   566,   567,   568,
     569,   570,     0,     0,     0,   171,     0,     0,     0,     0,
       0,     0,     0,    92,    93,    94,    95,    96,    97,    98,
      99,   100,   101,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   112,   113,   114,   115,   116,     0,     0,
       0,     0,     0,     0,     0,    92,    93,    94,    95,    96,
      97,    98,    99,   100,   101,   102,   103,   104,   105,   106,
     107,   108,   109,   110,   111,   112,   113,   114,   115,   116,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   550,
     551,   552,   553,   554,   555,   556,   557,   558,   559,   560,
     561,   562,   563,   564,   565,   566,   567,   568,   569,   570,
     550,   551,   552,   553,   554,   555,   556,   557,   558,   559,
     560,   561,   562,   563,   564,   565,   566,   567,   568,   569,
     570,     0,     0,     0,     0,     0,     0,    92,    93,    94,
      95,    96,    97,    98,    99,   100,   101,   102,   103,   104,
     105,   106,   107,   108,   109,   110,   111,   112,   113,   114,
     115,   116,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   860,     0,     0,     0,     0,
       0,     0,     0,    92,    93,    94,    95,    96,    97,    98,
      99,   100,   101,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   112,   113,   114,   115,   116,    92,    93,
      94,    95,    96,    97,    98,    99,   100,   101,   102,   103,
     104,   105,   106,   107,   108,   109,   110,   111,   112,   113,
     114,   115,   116,    92,    93,    94,    95,    96,    97,    98,
      99,   100,   101,   102,   103,   104,   105,   106,   107,   108,
     109,   110,   111,   112,   113,   114,   115,   116,     1,   864,
       0,     0,     2,     0,     0,     0,     0,     0,     0,     0,
       0,     3,     0,     0,     0,     4,     5,     6,     0,     0,
    1061,     0,     0,     0,     0,     0,     0,     7,     0,     0,
       8,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     9,     0,     0,     0,    10,     0,     0,     0,     0,
       0,     0,    11,     0,     0,    12,    13,    14,     0,     0,
      15,     0,    16,     0,     0,     0,     0,    17,     0,    18,
       0,     0,     0,     0,     0,     0,     0,     0,    19,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    20,     0,
       0,     0,     0,     0,     0,     0,     0,     0,    21,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    22,     0,     0,     0,    23,    24,     0,     0,
      25,     0,    26,    27,     0,     0,     0,     0,     0,    28,
       0,     0,    29,    30,     0,     0,     0,    31,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,    32,   550,   551,   552,   553,   554,   555,
     556,   557,   558,   559,   560,   561,   562,   563,   564,   565,
     566,   567,   568,   569,   570,     0,    33,   550,   551,   552,
     553,   554,   555,   556,   557,   558,   559,   560,   561,   562,
     563,   564,   565,   566,   567,   568,   569,   570,     0,   812,
     813,   814,     0,     0,     0,     0,   815,     0,     0,     0,
       0,     0,     0,     0,   816,     0,     0,     0,   817,   818,
       0,   819,     0,     0,     0,     0,     0,   820,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   821,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   822,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   823,     0,   824,
       0,   825,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,   826,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   827,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   828,   829,   830,     0,     0,
       0,     0,     0,     0,  1089,   831,   832,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,  1208,   550,   551,
     552,   553,   554,   555,   556,   557,   558,   559,   560,   561,
     562,   563,   564,   565,   566,   567,   568,   569,   570,   550,
     551,   552,   553,   554,   555,   556,   557,   558,   559,   560,
     561,   562,   563,   564,   565,   566,   567,   568,   569,   570,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     748,   550,   551,   552,   553,   554,   555,   556,   557,   558,
     559,   560,   561,   562,   563,   564,   565,   566,   567,   568,
     569,   570,   550,   551,   552,   553,   554,   555,   556,   557,
     558,   559,   560,   561,   562,   563,   564,   565,   566,   567,
     568,   569,   570,     0,   749,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   935,   550,   551,   552,   553,
     554,   555,   556,   557,   558,   559,   560,   561,   562,   563,
     564,   565,   566,   567,   568,   569,   570,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   937,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   939,   805,
     550,   551,   552,   553,   554,   555,   556,   557,   558,   559,
     560,   561,   562,   563,   564,   565,   566,   567,   568,   569,
     570,     0,     0,     0,     0,     0,   806,     0,     0,     0,
       0,     0,     0,     0,   834,   550,   551,   552,   553,   554,
     555,   556,   557,   558,   559,   560,   561,   562,   563,   564,
     565,   566,   567,   568,   569,   570,     0,     0,   761,   550,
     551,   552,   553,   554,   555,   556,   557,   558,   559,   560,
     561,   562,   563,   564,   565,   566,   567,   568,   569,   570,
       0,     0,   942,   550,   551,   552,   553,   554,   555,   556,
     557,   558,   559,   560,   561,   562,   563,   564,   565,   566,
     567,   568,   569,   570,     0,     0,  1060,   550,   551,   552,
     553,   554,   555,   556,   557,   558,   559,   560,   561,   562,
     563,   564,   565,   566,   567,   568,   569,   570
};

static const yytype_int16 yycheck[] =
{
       2,   218,     4,     5,     6,   337,     0,   537,    29,   385,
     257,    13,    14,     1,   261,    17,   297,     0,    17,   380,
      22,    32,     0,    22,    41,   357,   453,    29,   213,   214,
     215,     0,   620,    22,   753,   646,   834,     4,     5,     6,
       0,     0,    82,   222,    28,    13,    14,   107,    24,    81,
     482,   114,   126,     3,    41,   659,    54,     0,   132,    41,
    1032,    41,   173,   131,    13,    14,   864,    41,     3,   180,
     150,    85,   183,     4,   105,   124,    10,   146,   222,   222,
     222,   114,   147,    41,   102,   222,    20,    20,    41,    40,
     164,    20,   147,    41,   114,    41,    41,    41,    96,    41,
     202,    41,   150,    41,     0,     1,    30,    78,   204,    41,
     658,   126,   147,   201,    41,    41,   244,   137,    15,    16,
      17,   669,   733,   125,   139,   815,   146,    41,    29,    68,
     202,   123,   134,   107,   186,   201,   216,   222,   222,   222,
     222,   831,   832,   222,   748,   247,   222,   222,   105,   222,
     195,   222,   222,    24,   148,   196,   174,   125,    89,   247,
     162,   100,    96,    96,   381,   148,   134,    96,   216,    40,
     148,   202,   174,    40,   246,   177,   125,   127,   247,   247,
     194,   247,   247,   186,   247,   134,   246,   193,   148,   160,
     114,    43,   247,   107,   162,   561,   307,    43,   247,   113,
     311,   233,   313,   205,   206,   924,   925,   247,   210,   456,
    1008,   114,   247,   162,   247,   209,  1188,   201,   194,   221,
     171,   155,   221,    40,   118,   240,   209,   247,   222,     9,
     247,   209,   221,   412,   236,   309,     0,   205,   206,   222,
     209,   262,   210,   245,   222,   202,    26,   249,   246,   209,
     209,   148,   148,   222,   328,   257,   205,   206,   260,   261,
     247,   210,   222,   222,   696,   247,   209,   247,   412,   412,
     412,   273,   246,   174,   878,   412,   177,   245,   280,   222,
     325,   249,   276,   887,   888,   889,   331,   221,   202,   247,
     335,  1089,   260,   276,   247,   316,   245,  1095,   276,   247,
     249,   247,   247,   247,   171,   247,   308,   247,   257,   247,
     312,   260,   261,   209,   316,   247,   276,   319,   320,   321,
     247,   247,   113,   194,   273,   327,   222,   412,   412,   412,
     412,   280,   246,   412,   186,   236,   412,   412,    76,   412,
     186,   412,   412,    40,   196,   219,   137,   137,   131,   343,
     196,   319,   320,   321,   171,   146,   146,   209,    82,   327,
     117,   257,   181,   209,   385,   261,    40,   186,    42,   357,
     319,   320,   321,   394,   202,   449,   115,   196,   327,   154,
     276,     5,   393,   385,   995,    45,   974,   389,   462,   128,
      14,   502,   394,   132,     5,   506,    66,   136,    41,    69,
      97,    12,    41,   769,   142,   479,   480,   308,   147,  1207,
    1208,   312,   423,   779,   780,   316,    72,   174,   412,    56,
     116,    40,   389,     4,   163,   127,   183,   184,   422,   412,
       3,   678,   102,   130,   412,  1046,   683,   718,   206,   207,
     178,     4,  1240,   412,   127,   105,   106,   343,   809,   810,
     811,   783,   412,   412,   456,   197,   458,     8,   460,    10,
      11,   357,   159,   359,   246,   247,   511,   512,   456,   412,
      45,   161,   517,   196,   171,    90,    91,   216,   149,   375,
     376,   377,   858,   114,   180,   487,   182,   489,   158,   115,
      42,   387,   494,   394,  1213,   494,  1107,   863,   195,   223,
     224,   225,   128,   227,  1034,   229,   132,   456,   113,   458,
     136,   460,   208,   237,   238,   239,   412,   944,   242,   487,
      42,   489,    14,   950,     3,    66,   422,   247,    69,   186,
     105,   106,   534,   535,   147,   537,   236,   163,   487,   113,
     489,    34,    35,    36,    37,    38,   115,   549,   236,   537,
      32,    33,    34,    35,    36,    37,    38,   137,  1169,   128,
     456,   102,   573,   132,   769,   143,   144,   136,   684,   685,
     572,   243,   245,   222,   779,   780,   205,   571,   147,   228,
      40,   230,  1112,    60,    61,    62,    63,    64,   571,   203,
     216,    77,   241,   571,   163,   961,   962,   963,   964,   965,
     966,   967,   968,   195,     4,     5,     6,    14,     8,     9,
      10,   571,    12,    13,    14,   113,    65,   158,    65,   804,
      28,    29,    30,    30,  1000,  1001,  1002,  1003,     5,     5,
     641,    65,   144,   534,   535,   185,   537,   114,  1014,    46,
      47,   537,    40,   645,   247,  1011,    20,   216,   549,   110,
     652,   653,   654,   655,   656,     3,   126,   201,   863,   110,
    1190,   247,   909,    14,   194,   667,   562,     8,   670,    10,
      11,   572,    14,   675,   695,   571,   678,   679,   114,    20,
     247,   683,    15,    16,    17,     5,   688,   655,    30,   691,
     692,    40,    40,   375,   376,   377,    40,     3,  1064,    41,
      40,    40,   670,    40,    46,    47,   655,    24,    41,   605,
       4,  1087,   194,     5,    84,   717,   174,   719,    40,   721,
     719,   670,    40,   691,   692,    40,   675,  1093,    42,   678,
     679,   375,   376,   377,   683,    40,    40,    43,   113,   688,
      42,    42,   691,   692,   645,   436,   437,   438,    14,   717,
     246,   652,   653,   654,   106,   656,   961,   962,   963,   964,
     965,   966,   967,   968,     5,   246,   667,   194,   717,   247,
     246,   246,   721,   173,   246,   246,  1152,   125,   246,   246,
      86,    87,   678,   246,   132,   133,    24,   683,    24,  1155,
      34,    35,    36,    37,    38,   783,  1043,    32,    33,    34,
      35,    36,    37,    38,   137,   246,  1011,   246,   156,   115,
      68,    69,   118,   146,   246,   148,   246,   246,   166,   167,
     168,   246,    24,   144,    85,   721,   247,    77,   144,    87,
     110,    40,    40,     3,    77,   147,    44,   858,   247,    97,
      48,    49,    50,   247,   102,   151,   104,   194,     3,    57,
      58,    59,    41,    60,    61,    62,    63,    64,    41,  1064,
       4,   209,   120,   169,    40,  1197,   172,    40,   247,   217,
      41,    79,   130,    41,   770,    40,   247,   177,   202,    44,
     186,   187,    40,    48,    49,    50,    17,   783,  1093,    97,
     198,   247,    57,    58,    59,   137,   246,    30,     5,   205,
     246,   159,   106,   209,     4,   113,     4,   909,   211,   212,
     213,   214,   215,   171,    79,   246,   246,   220,   920,   127,
     226,   246,   130,   246,   131,    24,   232,   246,    24,    14,
      14,    14,    97,   246,   150,   246,   246,   195,   834,   245,
      41,   943,   132,     4,    41,   947,    41,   246,   113,   246,
    1155,   159,   920,   246,   948,   246,   246,     5,   247,   194,
     909,    41,   127,   171,    40,   130,   174,    24,   864,    38,
      41,   920,    32,    33,    34,    35,    36,    37,    38,  1000,
    1001,  1002,  1003,    19,    40,    40,   153,   195,    40,    49,
      40,    49,   131,  1014,   159,   131,    32,    33,    34,    35,
      36,    37,    38,    19,   131,   131,   171,     5,    82,   174,
       4,   211,    65,   909,     4,   194,    32,    33,    34,    35,
      36,    37,    38,  1025,    54,   134,  1025,   227,    40,  1246,
     195,  1033,  1034,    70,  1036,    41,    41,     5,   246,   106,
       5,  1043,   943,     5,     5,   246,   947,   106,   246,   131,
       5,   170,   948,    32,    33,    34,    35,    36,    37,    38,
     106,    54,    54,    54,    40,   210,  1087,   247,    40,    38,
      49,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,   246,   121,   246,    41,    41,   131,  1036,    41,    48,
      79,   106,   127,    24,  1043,  1114,  1115,  1116,  1117,  1118,
    1119,  1120,  1121,  1122,   304,   235,   306,    10,    10,    24,
    1112,    40,  1008,   247,    77,   315,     5,   317,   122,   127,
     106,   246,   246,   323,   246,   127,    24,    10,     4,     4,
    1132,  1152,  1033,  1034,   134,     4,   180,   337,     4,   247,
       5,  1143,     5,    42,     5,     5,    41,  1043,    49,     8,
     127,    24,   246,   246,   354,   355,   356,   357,   358,   247,
     106,   247,  1164,   106,  1132,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    24,   378,     5,
     246,    49,   246,  1132,     4,    42,   386,   234,  1190,    24,
     390,    41,   392,  1089,  1143,   246,     5,   763,   209,  1095,
     770,    30,   943,    73,    74,    75,  1234,   133,  1033,   652,
      80,  1112,   829,   616,   217,  1164,   825,  1140,    88,   261,
     491,   919,    92,    93,  1042,    95,   808,   722,  1164,   456,
     139,   101,   449,  1235,    32,    33,    34,    35,    36,    37,
      38,   480,   112,   197,    25,   274,   280,  1143,   394,   695,
     645,   316,   452,   891,   872,   657,   412,  1097,  1097,   129,
    1262,   861,   605,    -1,    -1,    -1,    -1,  1235,  1164,    -1,
    1272,   141,    -1,   143,    -1,   145,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,  1235,   157,    -1,  1190,
      -1,    -1,    -1,    -1,  1262,    -1,    -1,    -1,    -1,    -1,
      -1,   501,    -1,    -1,  1272,   505,   176,    -1,    -1,    -1,
      -1,  1207,  1208,  1262,    -1,    -1,    -1,    -1,    -1,   189,
     190,   191,    -1,  1272,    -1,   525,    -1,   527,   528,   199,
     200,   531,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    -1,  1240,    -1,    -1,    -1,    -1,    -1,
     550,   551,    -1,   553,   554,   555,   556,   557,   558,   559,
     560,    -1,    -1,    -1,   564,   565,   566,   567,   568,   569,
     570,    -1,    -1,    -1,    -1,    -1,    -1,   247,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,     3,     4,     5,     6,    -1,     8,     9,    10,    -1,
      12,    13,    14,    -1,    -1,    -1,    -1,    -1,    20,    -1,
      -1,    -1,    -1,    -1,   614,    -1,    -1,    -1,    -1,    -1,
      32,    33,    34,    -1,    -1,    -1,    -1,    -1,    40,    41,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   642,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,     3,    -1,    -1,    68,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    76,    -1,    -1,    79,    -1,    -1,
      -1,   671,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,   100,    -1,
      -1,    41,    -1,    -1,    -1,    -1,    -1,    -1,   110,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      70,    -1,    -1,   135,    -1,    -1,    -1,    -1,   728,    -1,
      -1,    -1,   732,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   741,    -1,   743,    -1,   745,    -1,    -1,    -1,   749,
      -1,    70,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     760,   173,   762,    -1,    -1,    -1,    -1,    -1,   768,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     192,    -1,    -1,   783,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,    -1,    -1,    -1,   808,   221,
     222,   223,   224,   225,   226,   227,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    13,    14,    -1,    -1,    -1,    -1,
     860,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   221,   222,   223,   224,   225,   226,   227,   228,   229,
     230,   231,   232,   233,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,    -1,    -1,    -1,    -1,
      -1,    -1,   221,   222,   223,   224,   225,   226,   227,   228,
     229,   230,   231,   232,   233,   234,   235,   236,   237,   238,
     239,   240,   241,   242,   243,   244,   245,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   935,    -1,   937,    -1,   939,
      -1,     3,     4,     5,     6,   945,     8,     9,    10,   949,
      12,    13,    14,   953,    -1,    -1,    -1,    -1,    20,   118,
     119,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      32,    33,    34,    -1,    -1,    -1,    -1,    -1,    40,    -1,
      -1,     3,     4,     5,     6,    -1,     8,     9,    10,    -1,
      12,    13,    14,   152,    -1,    -1,    -1,    -1,    20,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    68,    -1,    -1,    -1,
      32,    33,   246,   172,   173,   174,    -1,    79,    40,    41,
      -1,    -1,     3,     4,     5,     6,    -1,     8,     9,    10,
      -1,    12,    13,    14,    -1,    -1,    -1,  1037,   100,    20,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   110,    -1,
      -1,    32,    33,    34,    -1,    -1,    -1,    79,    -1,    40,
      -1,  1061,   221,   222,   223,   224,   225,   226,   227,   228,
     229,   230,   231,   232,   233,   234,   235,   236,   237,   238,
     239,   240,   241,   242,   243,   244,   245,    -1,   110,    -1,
      -1,    -1,    -1,    -1,  1094,    -1,    -1,    -1,    79,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   173,    -1,    -1,    -1,    -1,    -1,     3,     4,     5,
       6,    -1,     8,     9,    10,    -1,    12,    13,    14,   110,
      -1,  1131,    -1,    -1,    20,    -1,    -1,    -1,    -1,  1139,
    1140,    -1,    -1,    -1,    -1,    -1,    32,    33,    -1,    -1,
      -1,   173,    -1,    -1,    40,    -1,    -1,    -1,    -1,   221,
     222,   223,   224,   225,   226,   227,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   173,    79,     3,    -1,    -1,  1197,  1198,   221,
     222,   223,   224,   225,   226,   227,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   110,    -1,    -1,    -1,    -1,    -1,
      -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     221,   222,   223,   224,   225,   226,   227,   228,   229,   230,
     231,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,   244,   245,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,   171,    -1,   173,     3,     4,
       5,     6,     7,     8,     9,    10,    -1,    12,    13,    14,
      -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,    33,    -1,
      -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   221,   222,   223,   224,   225,
     226,   227,   228,   229,   230,   231,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
      -1,    -1,   171,    -1,    79,    -1,    -1,     3,     4,     5,
       6,    -1,     8,     9,    10,    -1,    12,    13,    14,    -1,
      -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   110,    32,    33,    -1,    -1,
      -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   221,   222,   223,   224,   225,   226,   227,   228,
     229,   230,   231,   232,   233,   234,   235,   236,   237,   238,
     239,   240,   241,   242,   243,   244,   245,    -1,    -1,    -1,
      -1,    -1,    -1,    79,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   173,     3,
       4,     5,     6,    -1,     8,     9,    10,    -1,    12,    13,
      14,    -1,    -1,    -1,   110,    -1,    20,    -1,   114,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,    33,
      -1,    -1,    -1,    -1,     3,    -1,    40,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   221,   222,   223,   224,
     225,   226,   227,   228,   229,   230,   231,   232,   233,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,    40,    -1,    -1,    -1,    79,    -1,   173,     3,     4,
       5,     6,    -1,     8,     9,    10,    -1,    12,    13,    14,
      -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   110,    32,    33,    -1,
     114,    -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,     3,    -1,   221,   222,   223,   224,   225,
     226,   227,   228,   229,   230,   231,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
      -1,    -1,    -1,    -1,    79,    -1,    -1,    -1,    -1,    -1,
      40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   173,
       3,     4,     5,     6,    -1,     8,     9,    10,    -1,    12,
      13,    14,    -1,    -1,    -1,   110,    -1,    20,    -1,   114,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,
      33,    -1,   171,    -1,    -1,    -1,    -1,    40,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   221,   222,   223,
     224,   225,   226,   227,   228,   229,   230,   231,   232,   233,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     244,   245,    -1,    -1,    -1,    -1,    79,    -1,   173,    -1,
      -1,    -1,   221,   222,   223,   224,   225,   226,   227,   228,
     229,   230,   231,   232,   233,   234,   235,   236,   237,   238,
     239,   240,   241,   242,   243,   244,   245,   110,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   221,   222,   223,   224,
     225,   226,   227,   228,   229,   230,   231,   232,   233,   234,
     235,   236,   237,   238,   239,   240,   241,   242,   243,   244,
     245,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,
     173,   221,   222,   223,   224,   225,   226,   227,   228,   229,
     230,   231,   232,   233,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,     3,    -1,    -1,    -1,
      -1,    -1,    -1,    40,    -1,    -1,    13,    14,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   221,   222,
     223,   224,   225,   226,   227,   228,   229,   230,   231,   232,
     233,   234,   235,   236,   237,   238,   239,   240,   241,   242,
     243,   244,   245,     3,     4,     5,     6,    -1,     8,     9,
      10,    -1,    12,    13,    14,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   103,    -1,    -1,    -1,
      -1,    -1,    32,    33,    -1,    -1,     3,     4,     5,     6,
      40,     8,     9,    10,    -1,    12,    13,    14,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    55,    -1,    -1,    -1,    -1,
      -1,     3,    -1,    -1,    -1,    32,    33,    -1,    -1,    -1,
      -1,   118,   119,    40,    -1,    -1,    -1,    -1,    -1,    79,
      -1,    -1,    -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,
      -1,    -1,    34,    -1,    -1,    -1,    -1,    14,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     110,    -1,    79,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   172,   173,    -1,    -1,    -1,
      -1,    -1,     3,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   110,   221,   222,   223,   224,   225,   226,
     227,   228,   229,   230,   231,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   245,    -1,
      -1,    -1,    -1,   173,   221,   222,   223,   224,   225,   226,
     227,   228,   229,   230,   231,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   245,    70,
      -1,    -1,    -1,    -1,    -1,    -1,   173,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,
      -1,   221,   222,   223,   224,   225,   226,   227,   228,   229,
     230,   231,   232,   233,   234,   235,   236,   237,   238,   239,
     240,   241,   242,   243,   244,   245,    34,     3,    -1,    -1,
      -1,    -1,    -1,    -1,   221,   222,   223,   224,   225,   226,
     227,   228,   229,   230,   231,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   245,   221,
     222,   223,   224,   225,   226,   227,   228,   229,   230,   231,
     232,   233,   234,   235,   236,   237,   238,   239,   240,   241,
     242,   243,   244,   245,   221,   222,   223,   224,   225,   226,
     227,   228,   229,   230,   231,   232,   233,   234,   235,   236,
     237,   238,   239,   240,   241,   242,   243,   244,   245,     3,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    -1,
     221,   222,   223,   224,   225,   226,   227,   228,   229,   230,
     231,   232,   233,   234,   235,   236,   237,   238,   239,   240,
     241,   242,   243,   244,   245,     3,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    70,    -1,    -1,   155,
       3,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   221,   222,   223,   224,   225,   226,   227,
     228,   229,   230,   231,   232,   233,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   221,   222,   223,   224,   225,
     226,   227,   228,   229,   230,   231,   232,   233,   234,   235,
     236,   237,   238,   239,   240,   241,   242,   243,   244,   245,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,    -1,    -1,    -1,    -1,    -1,    -1,   221,   222,   223,
     224,   225,   226,   227,   228,   229,   230,   231,   232,   233,
     234,   235,   236,   237,   238,   239,   240,   241,   242,   243,
     244,   245,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   202,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   221,   222,   223,   224,   225,   226,   227,
     228,   229,   230,   231,   232,   233,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,   221,   222,
     223,   224,   225,   226,   227,   228,   229,   230,   231,   232,
     233,   234,   235,   236,   237,   238,   239,   240,   241,   242,
     243,   244,   245,   221,   222,   223,   224,   225,   226,   227,
     228,   229,   230,   231,   232,   233,   234,   235,   236,   237,
     238,   239,   240,   241,   242,   243,   244,   245,    40,   188,
      -1,    -1,    44,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    53,    -1,    -1,    -1,    57,    58,    59,    -1,    -1,
     188,    -1,    -1,    -1,    -1,    -1,    -1,    69,    -1,    -1,
      72,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    83,    -1,    -1,    -1,    87,    -1,    -1,    -1,    -1,
      -1,    -1,    94,    -1,    -1,    97,    98,    99,    -1,    -1,
     102,    -1,   104,    -1,    -1,    -1,    -1,   109,    -1,   111,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   120,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   130,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   140,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   154,    -1,    -1,    -1,   158,   159,    -1,    -1,
     162,    -1,   164,   165,    -1,    -1,    -1,    -1,    -1,   171,
      -1,    -1,   174,   175,    -1,    -1,    -1,   179,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   195,    18,    19,    20,    21,    22,    23,
      24,    25,    26,    27,    28,    29,    30,    31,    32,    33,
      34,    35,    36,    37,    38,    -1,   218,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    -1,    73,
      74,    75,    -1,    -1,    -1,    -1,    80,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    88,    -1,    -1,    -1,    92,    93,
      -1,    95,    -1,    -1,    -1,    -1,    -1,   101,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   112,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   129,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   141,    -1,   143,
      -1,   145,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   157,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   176,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   189,   190,   191,    -1,    -1,
      -1,    -1,    -1,    -1,   188,   199,   200,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   188,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      70,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    -1,   114,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,   114,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   114,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   114,    71,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,    -1,    -1,    -1,    -1,    -1,    98,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    52,    18,    19,    20,    21,    22,
=======
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
#define YYTABLE_NINF -468
static const yytype_int16 yytable[] =
{
      36,    61,   100,   100,   160,   107,    62,   496,   402,   107,
     204,   333,   296,   297,   298,   584,   141,   466,   201,    99,
      99,   202,   250,   203,    98,   101,   358,   487,   185,   234,
     799,   518,   122,   190,  -209,   444,   660,   242,   246,   706,
     800,   162,   755,   791,   303,   826,   231,   552,   319,   839,
     854,   497,   580,  -253,   861,   796,   486,   666,   100,     1,
     320,   452,   210,   327,   698,   100,   327,   396,   140,   165,
    -467,   184,  -465,   300,   243,    99,  -209,   167,   168,   169,
     183,   301,    99,    66,   356,   761,   213,   191,   763,    63,
     719,     1,   108,   801,    64,  -253,   441,   657,   473,   216,
     712,   173,   740,   442,    65,   228,   730,   731,    71,    36,
     597,   474,   670,   347,   602,   475,   701,   123,   476,   126,
     378,   344,   672,   673,   345,   380,   346,   141,   709,   382,
     141,   699,   365,   379,   244,   658,   369,   229,   762,   581,
     128,   764,   617,   832,   477,   468,   321,   159,   211,   802,
     533,   267,   268,   269,   100,   270,   271,   272,   100,   273,
     274,   275,    21,    36,   163,   456,   398,   552,   498,   107,
      21,    99,   498,   141,   445,    99,   263,   552,   552,   235,
     265,   100,   239,   247,   170,   211,  -253,   615,   232,   862,
     100,   357,   305,   488,   432,   702,   332,   519,    99,  -209,
     367,   803,   661,   324,   100,   707,   833,    99,   519,   792,
     100,   827,    36,    69,   367,   661,   792,   161,  -253,   221,
     164,   342,   165,    70,   175,   308,   176,   342,   711,   713,
     141,   177,   767,   102,   141,   141,   186,   100,   100,   100,
     834,   173,    68,   103,   507,   508,   747,   748,   749,   750,
     751,   752,   753,   754,    99,    99,    99,   125,   300,   373,
     374,   375,   184,   502,  -264,    36,   301,   506,   192,  -219,
    -219,  -219,  -264,   195,   313,   313,   199,   314,   314,   404,
     222,   407,   366,   682,    62,   605,   370,   140,   190,     1,
     609,   206,   282,   208,   358,   176,   433,   433,   433,   481,
     209,   552,   552,   552,   552,   552,   552,   552,   552,   223,
     315,   315,   484,   213,   690,   691,   692,   489,   795,   224,
     212,   855,   236,     7,   100,   237,   100,   856,   100,   857,
     467,   572,   807,   240,   684,    62,   685,   686,   241,   569,
     858,    99,   570,   472,   571,   480,   178,   734,   473,   218,
     219,   765,   741,   245,   107,    14,   768,   769,   770,   249,
     771,   474,   772,   316,   595,   475,  -219,   473,   476,   837,
     773,   774,   775,   552,  -219,   776,  -219,   490,   782,   491,
     474,   251,    17,   142,   475,   311,   312,   476,   254,   141,
     141,   255,    21,   257,   477,   557,    61,   558,   559,   179,
     258,    62,   141,   610,   611,   492,   259,   560,   180,   181,
     260,   575,   261,   477,   299,   304,    25,   309,   674,   310,
     688,   556,   539,   540,   541,   141,   323,   710,   325,   326,
      36,   675,   676,   677,   678,   679,   680,   681,   327,   329,
     141,   527,   529,   143,   144,   337,   336,   141,   141,   141,
     100,   141,   349,   352,   536,   141,   425,   426,   427,   428,
     429,   100,   350,   353,   100,   100,   355,    99,   368,   100,
     332,   145,   593,   100,   146,   332,   376,   574,   472,   381,
     394,    99,   606,   395,   130,     1,    99,   287,   408,   107,
     613,   100,   308,  -114,   131,   132,   746,   332,   431,   590,
     591,   592,   430,   594,   173,   147,  -466,   590,    99,   675,
     676,   677,   678,   679,   680,   681,   110,   111,   434,   436,
     437,   148,   446,   447,   149,   448,   449,   784,   675,   676,
     677,   678,   679,   680,   681,   112,   450,   452,   150,   151,
     458,   556,   435,   435,   435,   113,   453,   454,   455,   459,
     114,   851,   451,   460,   404,   363,   364,  -339,   461,    62,
     463,   736,   470,   482,   372,   503,   152,   115,   483,   377,
     509,   510,   153,   511,   523,   133,   134,   116,   524,   530,
     531,   533,   391,    72,   554,   154,   418,   419,   420,   421,
     422,   423,   424,   425,   426,   427,   428,   429,   399,   400,
     401,   403,   405,   169,   117,   579,   100,   135,   585,   582,
     586,   587,   332,   588,   118,   677,   678,   679,   680,   681,
     330,   438,   607,    99,   604,   136,   137,   419,   420,   421,
     422,   423,   424,   425,   426,   427,   428,   429,   119,   420,
     421,   422,   423,   424,   425,   426,   427,   428,   429,   465,
     612,   141,   616,   624,   625,   141,   659,   662,   429,   687,
      36,   693,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,   501,   695,   696,
     704,   505,   708,   705,   717,   720,   722,   724,   107,   727,
     757,   141,   100,   527,   732,   745,   100,   737,   513,   758,
     515,   517,   332,   766,   522,   787,   785,   788,   789,   780,
     790,   793,   797,    99,   681,   809,   798,   805,   806,   820,
     822,   537,   538,   823,   542,   543,   544,   545,   546,   547,
     548,   549,   836,   825,   828,   561,   562,   563,   564,   565,
     566,   567,   829,   590,   835,   838,   840,   841,   843,   842,
     141,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,   603,   844,   849,   845,
     100,   846,   847,   848,   853,   519,   332,   811,   812,   813,
     814,   815,   816,   817,   818,   819,   860,    99,   866,   864,
     867,   264,   529,   870,   865,   868,   869,   667,   671,   155,
     589,   733,   779,   850,   622,   697,   726,   831,   623,   808,
     494,   729,   302,   783,   626,   689,   628,   621,   630,   756,
     469,   632,   194,   655,   462,   253,   124,   343,   614,   351,
     485,   596,     0,   664,   371,   665,   583,   266,   267,   268,
     269,   669,   270,   271,   272,     0,   273,   274,   275,     1,
       0,     0,     0,     0,   276,     2,   683,     0,     3,     0,
       0,     0,     0,     0,     0,     0,   277,   278,   383,     4,
       0,     0,     0,     5,   279,   384,     0,     0,     0,   385,
       6,   438,     0,     7,     8,     9,     0,   386,    10,     0,
     280,     0,     0,     0,    11,     0,    12,     0,     0,     0,
       0,     0,     0,     0,     0,    13,     0,     0,     0,     0,
       0,   387,     0,     0,     0,    14,     0,     0,     0,     0,
     281,     0,     0,     0,     0,     0,     0,     0,     0,   714,
       0,   715,     0,   716,     0,     0,     0,    15,     0,     0,
       0,    16,    17,     0,   388,    18,     0,    19,    20,     0,
       0,     0,    21,     0,     0,    22,    23,     0,     0,     0,
      24,   735,     0,     0,   738,     0,     0,     0,   744,     0,
       0,     0,     0,     0,     0,     0,    25,     0,     0,   282,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,   389,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   781,     0,    72,     0,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,   283,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,    95,    96,    97,
       0,   104,     0,     0,     0,   794,     0,     0,     0,     0,
     330,     0,     0,     0,     0,     0,   409,   410,   411,   412,
     413,   414,   415,   416,   417,   418,   419,   420,   421,   422,
     423,   424,   425,   426,   427,   428,   429,     0,     0,     0,
       0,     0,     0,     0,   821,   495,     0,     0,     0,     0,
       0,     0,     0,     0,   830,   738,     0,     0,     0,     0,
       0,     0,   266,   267,   268,   269,     0,   270,   271,   272,
       0,   273,   274,   275,     0,     0,     0,     0,     0,   276,
       0,     0,     0,     0,   683,   852,     0,     0,     0,     0,
       0,   277,   278,   359,     0,   266,   267,   268,   269,   279,
     270,   271,   272,     0,   273,   274,   275,     0,     0,     0,
       0,     0,   276,    21,     0,   280,     0,     0,     0,     0,
       0,     0,     0,     0,   277,   278,     0,     0,     0,     0,
       0,     0,   279,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,   281,     0,     0,   280,     0,
       0,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,     0,     0,   281,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,   282,     0,     0,    72,   409,   410,
     411,   412,   413,   414,   415,   416,   417,   418,   419,   420,
     421,   422,   423,   424,   425,   426,   427,   428,   429,     0,
       0,     0,     0,     0,     0,    21,     0,   282,   340,     0,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
     283,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,     0,     0,     0,     0,     0,
       0,     0,     0,    73,    74,    75,    76,    77,    78,    79,
      80,    81,    82,   283,    84,    85,    86,    87,    88,    89,
      90,    91,    92,    93,    94,    95,    96,    97,   266,   267,
     268,   269,   464,   270,   271,   272,     0,   273,   274,   275,
       0,     0,     0,     0,     0,   276,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   277,   278,     0,
       0,   266,   267,   268,   269,   279,   270,   271,   272,     0,
     273,   274,   275,     0,     0,     0,     0,     0,   276,     0,
     743,   280,     0,     0,     0,     0,     0,     0,     0,     0,
     277,   278,     0,     0,     0,     0,     0,     0,   279,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,   281,     0,     0,   280,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,    95,    96,    97,
       0,     0,     0,     0,   281,     0,     0,     0,   512,   266,
     267,   268,   269,     0,   270,   271,   272,     0,   273,   274,
     275,     0,     0,     0,     0,     0,   276,     0,     0,     0,
     282,     0,     0,     0,     0,     0,     0,     0,   277,   278,
       0,     0,     0,     0,     0,     0,   279,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   280,   282,     0,     0,    73,    74,    75,    76,
      77,    78,    79,    80,    81,    82,   283,    84,    85,    86,
      87,    88,    89,    90,    91,    92,    93,    94,    95,    96,
      97,     0,   281,     0,     0,     0,   514,     0,     0,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,   283,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,     0,     0,     0,     0,     0,   266,
     267,   268,   269,     0,   270,   271,   272,     0,   273,   274,
     275,     0,     0,     0,     0,     0,   276,     0,     0,     0,
       0,   282,     0,     0,     0,     0,     0,     0,   277,   278,
       0,     0,     0,     0,     0,     0,   279,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   280,     0,     0,     0,     0,    73,    74,    75,
      76,    77,    78,    79,    80,    81,    82,   283,    84,    85,
      86,    87,    88,    89,    90,    91,    92,    93,    94,    95,
      96,    97,   281,     0,     0,     0,   516,   266,   267,   268,
     269,     0,   270,   271,   272,     0,   273,   274,   275,     0,
       0,     0,     0,     0,   276,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,   277,   278,     0,     0,
     266,   267,   268,   269,   279,   270,   271,   272,     0,   273,
     274,   275,     0,     0,     0,     0,     0,     0,     0,     0,
     280,   282,     0,     0,     0,     0,     0,     0,     0,   550,
     551,   130,     0,     0,     0,     0,     0,   279,     0,     0,
       0,   131,   132,     0,     0,     0,     0,     0,     0,     0,
     281,     0,     0,   280,     0,     0,     0,    73,    74,    75,
      76,    77,    78,    79,    80,    81,    82,   283,    84,    85,
      86,    87,    88,    89,    90,    91,    92,    93,    94,    95,
      96,    97,     0,   281,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   130,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   282,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   133,   134,     0,     0,     0,     0,     0,     0,
       0,     1,    72,     0,     0,     0,     0,     0,     0,     0,
       0,     0,   282,     0,     0,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,   283,    84,    85,    86,    87,
      88,    89,    90,    91,    92,    93,    94,    95,    96,    97,
     176,    72,   136,   137,     0,     0,   471,     0,    73,    74,
      75,    76,    77,    78,    79,    80,    81,    82,   283,    84,
      85,    86,    87,    88,    89,    90,    91,    92,    93,    94,
      95,    96,    97,    72,     0,     0,     0,     0,   330,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,    21,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   471,   618,     0,
       0,     0,     0,    72,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,   479,     0,     0,
       0,     0,     0,     0,   130,     0,     0,     0,     0,     0,
      73,    74,    75,    76,    77,    78,    79,    80,    81,    82,
      83,    84,    85,    86,    87,    88,    89,    90,    91,    92,
      93,    94,    95,    96,    97,   573,   130,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,    73,
      74,    75,    76,    77,    78,    79,    80,    81,    82,    83,
      84,    85,    86,    87,    88,    89,    90,    91,    92,    93,
      94,    95,    96,    97,    72,     0,     0,     0,     0,     0,
       0,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,   104,   409,   410,   411,
     412,   413,   414,   415,   416,   417,   418,   419,   420,   421,
     422,   423,   424,   425,   426,   427,   428,   429,     0,     0,
       0,    73,    74,    75,    76,    77,    78,    79,    80,    81,
      82,    83,    84,    85,    86,    87,    88,    89,    90,    91,
      92,    93,    94,    95,    96,    97,   130,     0,     0,     0,
       0,     0,   598,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,   627,
       0,     0,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,     0,     0,     0,
       0,     0,     0,     0,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,    73,    74,    75,    76,    77,    78,    79,    80,
      81,    82,    83,    84,    85,    86,    87,    88,    89,    90,
      91,    92,    93,    94,    95,    96,    97,     0,     0,     0,
       0,     0,     0,     0,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,     0,
     409,   410,   411,   412,   413,   414,   415,   416,   417,   418,
     419,   420,   421,   422,   423,   424,   425,   426,   427,   428,
     429,     0,     0,     0,    73,    74,    75,    76,    77,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    90,    91,    92,    93,    94,    95,    96,    97,   633,
     634,   635,     0,     0,     0,     0,   636,     0,     0,     0,
       0,     0,     0,     0,   637,     0,     0,     0,   638,   639,
       0,   640,   629,     0,     0,     0,     0,   641,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   642,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   643,     0,     0,     0,     0,     0,
       0,     0,     0,     0,   644,     0,   645,     0,   646,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     647,     0,     0,     0,     0,     0,     0,     0,     0,     0,
       0,     0,     0,     0,     0,     0,     0,   648,     0,     0,
       0,     0,     0,     0,     0,     0,     0,     0,     0,     0,
     649,   650,   651,     0,     0,     0,     0,     0,     0,     0,
     652,   653,   409,   410,   411,   412,   413,   414,   415,   416,
     417,   418,   419,   420,   421,   422,   423,   424,   425,   426,
     427,   428,   429,     0,     0,     0,     0,     0,     0,     0,
       0,   520,   409,   410,   411,   412,   413,   414,   415,   416,
     417,   418,   419,   420,   421,   422,   423,   424,   425,   426,
     427,   428,   429,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   521,     0,     0,     0,     0,     0,
       0,     0,     0,   576,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   631,     0,     0,     0,     0,     0,
     577,   409,   410,   411,   412,   413,   414,   415,   416,   417,
     418,   419,   420,   421,   422,   423,   424,   425,   426,   427,
     428,   429,     0,     0,   532,   409,   410,   411,   412,   413,
     414,   415,   416,   417,   418,   419,   420,   421,   422,   423,
     424,   425,   426,   427,   428,   429,     0,     0,   656,   409,
     410,   411,   412,   413,   414,   415,   416,   417,   418,   419,
     420,   421,   422,   423,   424,   425,   426,   427,   428,   429,
       0,     0,   742,   409,   410,   411,   412,   413,   414,   415,
     416,   417,   418,   419,   420,   421,   422,   423,   424,   425,
     426,   427,   428,   429,   411,   412,   413,   414,   415,   416,
     417,   418,   419,   420,   421,   422,   423,   424,   425,   426,
     427,   428,   429
};

#define yypact_value_is_default(yystate) \
  ((yystate) == (-547))

#define yytable_value_is_error(yytable_value) \
  YYID (0)

static const yytype_int16 yycheck[] =
{
       0,     1,     8,     9,    25,    11,     1,   360,   279,    15,
     109,   196,   167,   168,   169,   447,    22,   328,   109,     8,
       9,   109,   151,   109,     8,     9,   226,    41,    65,   132,
      10,    41,    15,    70,    41,   301,    41,   140,    28,    41,
      20,    83,    41,    41,   172,    41,    24,   420,   102,    41,
      41,    90,   120,    41,    58,    90,   352,    81,    64,    40,
     114,    62,   121,   173,    79,    71,   173,    40,    22,   174,
      40,     4,    42,   112,    30,    64,    83,    15,    16,    17,
      64,   120,    71,   176,   100,    20,   123,    71,    20,   153,
     636,    40,   204,    73,   158,    83,     5,   207,    91,   121,
     207,    89,   207,    12,   168,    45,   652,   653,    90,   109,
     459,   104,   540,   212,   463,   108,    55,   168,   111,    94,
     249,   212,   550,   551,   212,   254,   212,   133,   121,   258,
     136,   146,   235,    66,    90,   525,   239,    77,    73,   207,
      99,    73,   495,    53,   137,   330,   200,   165,   207,   129,
     174,     4,     5,     6,   160,     8,     9,    10,   164,    12,
      13,    14,   143,   163,   206,   166,   265,   540,   207,   175,
     143,   160,   207,   179,   302,   164,   160,   550,   551,   133,
     164,   187,   136,   173,   122,   207,   174,   483,   166,   193,
     196,   207,   175,   207,   143,   134,   196,   207,   187,   206,
     237,   181,   207,   187,   210,   207,   116,   196,   207,   207,
     216,   207,   212,   158,   251,   207,   207,     0,   206,    40,
     107,   210,   174,   168,   128,   179,    41,   216,   618,   619,
     236,    41,    59,   158,   240,   241,   103,   243,   244,   245,
     150,    89,     4,   168,   373,   374,   674,   675,   676,   677,
     678,   679,   680,   681,   243,   244,   245,    19,   112,   243,
     244,   245,     4,   366,   112,   265,   120,   370,     4,    15,
      16,    17,   120,   103,    43,    43,   169,    46,    46,   279,
     101,   281,   236,   554,   279,   470,   240,   241,   325,    40,
     475,   135,   145,   168,   494,    41,   296,   297,   298,   336,
     123,   674,   675,   676,   677,   678,   679,   680,   681,   130,
      79,    79,   349,   350,   580,   581,   582,    92,   746,   140,
      90,   182,    42,    74,   330,    89,   332,   188,   334,   190,
     330,   430,   764,    42,     8,   330,    10,    11,   207,   430,
     201,   330,   430,   332,   430,   334,    93,   658,    91,   178,
     179,   704,   663,   158,   360,   106,   183,   184,   185,   196,
     187,   104,   189,   132,   132,   108,   112,    91,   111,   801,
     197,   198,   199,   746,   120,   202,   122,   152,   121,   154,
     104,    89,   133,     3,   108,    67,    68,   111,   196,   395,
     396,   112,   143,   205,   137,     8,   396,    10,    11,   146,
     203,   396,   408,   476,   477,   180,   177,    20,   155,   156,
      40,   432,   175,   137,    54,   167,   167,   118,    19,   157,
     575,   421,    28,    29,    30,   431,    20,   612,   207,   102,
     430,    32,    33,    34,    35,    36,    37,    38,   173,    86,
     446,   395,   396,    63,    64,    14,   207,   453,   454,   455,
     456,   457,   166,    90,   408,   461,    34,    35,    36,    37,
      38,   467,   207,     5,   470,   471,    40,   456,    24,   475,
     470,    91,   456,   479,    94,   475,     4,   431,   467,     5,
      61,   470,   471,   146,     3,    40,   475,   165,    42,   495,
     479,   497,   446,    40,    13,    14,    19,   497,    42,   453,
     454,   455,    40,   457,    89,   125,    42,   461,   497,    32,
      33,    34,    35,    36,    37,    38,    45,    46,   296,   297,
     298,   141,   207,    24,   144,    24,    24,   712,    32,    33,
      34,    35,    36,    37,    38,    64,   118,    62,   158,   159,
     207,   541,   296,   297,   298,    74,   313,   314,   315,    54,
      79,   822,   118,    86,   554,   233,   234,   177,    40,   554,
      54,   660,   207,   207,   242,     4,   186,    96,   166,   247,
      40,   207,   192,    41,    41,    94,    95,   106,   149,   170,
     207,   174,   260,     3,    40,   205,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,   276,   277,
     278,   279,   280,    17,   133,   207,   612,   126,     4,   112,
       4,    24,   612,    24,   143,    34,    35,    36,    37,    38,
      40,   299,   124,   612,    41,   144,   145,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,   167,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,   327,
     108,   657,    41,    41,     5,   661,    24,    40,    38,    41,
     660,    59,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,   365,     4,     4,
     166,   369,    47,   109,    40,    40,   127,    40,   704,    40,
       4,   707,   708,   657,    41,    82,   712,   661,   386,   142,
     388,   389,   712,    40,   392,     5,    97,     5,     5,   708,
       5,    40,    24,   712,    38,    24,   195,    10,    10,   207,
      54,   409,   410,    98,   412,   413,   414,   415,   416,   417,
     418,   419,    10,    41,    41,   423,   424,   425,   426,   427,
     428,   429,    41,   707,    24,   109,     4,     4,     4,   152,
     766,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,   202,   203,   204,   205,   464,     4,     8,     5,
     796,     5,     5,     5,     5,   207,   796,   768,   769,   770,
     771,   772,   773,   774,   775,   776,    41,   796,     5,    24,
       4,   163,   766,     5,    24,   194,    24,   534,   541,    23,
     453,   657,   707,   820,   502,   590,   646,   793,   506,   766,
     357,   650,   171,   711,   512,   579,   514,   498,   516,   688,
     330,   519,   101,   521,   325,   152,    18,   211,   482,   216,
     350,   458,    -1,   531,   241,   533,   446,     3,     4,     5,
       6,   539,     8,     9,    10,    -1,    12,    13,    14,    40,
      -1,    -1,    -1,    -1,    20,    46,   554,    -1,    49,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    32,    33,    34,    60,
      -1,    -1,    -1,    64,    40,    41,    -1,    -1,    -1,    45,
      71,   579,    -1,    74,    75,    76,    -1,    53,    79,    -1,
      56,    -1,    -1,    -1,    85,    -1,    87,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    96,    -1,    -1,    -1,    -1,
      -1,    77,    -1,    -1,    -1,   106,    -1,    -1,    -1,    -1,
      86,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   627,
      -1,   629,    -1,   631,    -1,    -1,    -1,   128,    -1,    -1,
      -1,   132,   133,    -1,   110,   136,    -1,   138,   139,    -1,
      -1,    -1,   143,    -1,    -1,   146,   147,    -1,    -1,    -1,
     151,   659,    -1,    -1,   662,    -1,    -1,    -1,   666,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,   167,    -1,    -1,   145,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   164,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,   709,    -1,     3,    -1,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   202,   203,   204,   205,
      -1,     3,    -1,    -1,    -1,   743,    -1,    -1,    -1,    -1,
      40,    -1,    -1,    -1,    -1,    -1,    18,    19,    20,    21,
      22,    23,    24,    25,    26,    27,    28,    29,    30,    31,
      32,    33,    34,    35,    36,    37,    38,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   782,    47,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   792,   793,    -1,    -1,    -1,    -1,
      -1,    -1,     3,     4,     5,     6,    -1,     8,     9,    10,
      -1,    12,    13,    14,    -1,    -1,    -1,    -1,    -1,    20,
      -1,    -1,    -1,    -1,   822,   823,    -1,    -1,    -1,    -1,
      -1,    32,    33,    34,    -1,     3,     4,     5,     6,    40,
       8,     9,    10,    -1,    12,    13,    14,    -1,    -1,    -1,
      -1,    -1,    20,   143,    -1,    56,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    32,    33,    -1,    -1,    -1,    -1,
      -1,    -1,    40,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    86,    -1,    -1,    56,    -1,
      -1,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,   202,   203,   204,   205,    -1,    -1,    86,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,   145,    -1,    -1,     3,    18,    19,
      20,    21,    22,    23,    24,    25,    26,    27,    28,    29,
      30,    31,    32,    33,    34,    35,    36,    37,    38,    -1,
      -1,    -1,    -1,    -1,    -1,   143,    -1,   145,    34,    -1,
     181,   182,   183,   184,   185,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,   202,   203,   204,   205,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,   181,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,   194,   195,   196,   197,
     198,   199,   200,   201,   202,   203,   204,   205,     3,     4,
       5,     6,     7,     8,     9,    10,    -1,    12,    13,    14,
      -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,    33,    -1,
      -1,     3,     4,     5,     6,    40,     8,     9,    10,    -1,
      12,    13,    14,    -1,    -1,    -1,    -1,    -1,    20,    -1,
     160,    56,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      32,    33,    -1,    -1,    -1,    -1,    -1,    -1,    40,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    86,    -1,    -1,    56,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   202,   203,   204,   205,
      -1,    -1,    -1,    -1,    86,    -1,    -1,    -1,    90,     3,
       4,     5,     6,    -1,     8,     9,    10,    -1,    12,    13,
      14,    -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,
     145,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,    33,
      -1,    -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    56,   145,    -1,    -1,   181,   182,   183,   184,
     185,   186,   187,   188,   189,   190,   191,   192,   193,   194,
     195,   196,   197,   198,   199,   200,   201,   202,   203,   204,
     205,    -1,    86,    -1,    -1,    -1,    90,    -1,    -1,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,    -1,    -1,    -1,    -1,    -1,     3,
       4,     5,     6,    -1,     8,     9,    10,    -1,    12,    13,
      14,    -1,    -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,
      -1,   145,    -1,    -1,    -1,    -1,    -1,    -1,    32,    33,
      -1,    -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    56,    -1,    -1,    -1,    -1,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     194,   195,   196,   197,   198,   199,   200,   201,   202,   203,
     204,   205,    86,    -1,    -1,    -1,    90,     3,     4,     5,
       6,    -1,     8,     9,    10,    -1,    12,    13,    14,    -1,
      -1,    -1,    -1,    -1,    20,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    32,    33,    -1,    -1,
       3,     4,     5,     6,    40,     8,     9,    10,    -1,    12,
      13,    14,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      56,   145,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    32,
      33,     3,    -1,    -1,    -1,    -1,    -1,    40,    -1,    -1,
      -1,    13,    14,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      86,    -1,    -1,    56,    -1,    -1,    -1,   181,   182,   183,
     184,   185,   186,   187,   188,   189,   190,   191,   192,   193,
     194,   195,   196,   197,   198,   199,   200,   201,   202,   203,
     204,   205,    -1,    86,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   145,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    94,    95,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    40,     3,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   145,    -1,    -1,   181,   182,   183,   184,   185,
     186,   187,   188,   189,   190,   191,   192,   193,   194,   195,
     196,   197,   198,   199,   200,   201,   202,   203,   204,   205,
      41,     3,   144,   145,    -1,    -1,    47,    -1,   181,   182,
     183,   184,   185,   186,   187,   188,   189,   190,   191,   192,
     193,   194,   195,   196,   197,   198,   199,   200,   201,   202,
     203,   204,   205,     3,    -1,    -1,    -1,    -1,    40,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,   143,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    47,    80,    -1,
      -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,    47,    -1,    -1,
      -1,    -1,    -1,    -1,     3,    -1,    -1,    -1,    -1,    -1,
     181,   182,   183,   184,   185,   186,   187,   188,   189,   190,
     191,   192,   193,   194,   195,   196,   197,   198,   199,   200,
     201,   202,   203,   204,   205,    34,     3,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,   181,
     182,   183,   184,   185,   186,   187,   188,   189,   190,   191,
     192,   193,   194,   195,   196,   197,   198,   199,   200,   201,
     202,   203,   204,   205,     3,    -1,    -1,    -1,    -1,    -1,
      -1,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,   202,   203,   204,   205,     3,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38,    -1,    -1,
      -1,   181,   182,   183,   184,   185,   186,   187,   188,   189,
     190,   191,   192,   193,   194,   195,   196,   197,   198,   199,
     200,   201,   202,   203,   204,   205,     3,    -1,    -1,    -1,
      -1,    -1,   129,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    90,
      -1,    -1,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   181,   182,   183,   184,   185,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   202,   203,   204,   205,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,   181,   182,   183,   184,   185,   186,   187,   188,
     189,   190,   191,   192,   193,   194,   195,   196,   197,   198,
     199,   200,   201,   202,   203,   204,   205,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   181,   182,   183,   184,   185,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   202,   203,   204,   205,    -1,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,    -1,    -1,    -1,   181,   182,   183,   184,   185,   186,
     187,   188,   189,   190,   191,   192,   193,   194,   195,   196,
     197,   198,   199,   200,   201,   202,   203,   204,   205,    50,
      51,    52,    -1,    -1,    -1,    -1,    57,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    65,    -1,    -1,    -1,    69,    70,
      -1,    72,    90,    -1,    -1,    -1,    -1,    78,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    88,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   105,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,   115,    -1,   117,    -1,   119,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     131,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,   148,    -1,    -1,
      -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     161,   162,   163,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
     171,   172,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    -1,    -1,    -1,    -1,    -1,    -1,    -1,
      -1,    47,    18,    19,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    90,    -1,    -1,    -1,    -1,    -1,
      -1,    -1,    -1,    48,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    90,    -1,    -1,    -1,    -1,    -1,
      75,    18,    19,    20,    21,    22,    23,    24,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    -1,    -1,    41,    18,    19,    20,    21,    22,
>>>>>>> refs/remotes/origin/master
      23,    24,    25,    26,    27,    28,    29,    30,    31,    32,
      33,    34,    35,    36,    37,    38,    -1,    -1,    41,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      -1,    -1,    41,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
<<<<<<< HEAD
      35,    36,    37,    38,    -1,    -1,    41,    18,    19,    20,
      21,    22,    23,    24,    25,    26,    27,    28,    29,    30,
      31,    32,    33,    34,    35,    36,    37,    38
=======
      35,    36,    37,    38,    20,    21,    22,    23,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38
>>>>>>> refs/remotes/origin/master
};

/* YYSTOS[STATE-NUM] -- The (internal number of the) accessing
   symbol of state STATE-NUM.  */
static const yytype_uint16 yystos[] =
{
<<<<<<< HEAD
       0,    40,    44,    53,    57,    58,    59,    69,    72,    83,
      87,    94,    97,    98,    99,   102,   104,   109,   111,   120,
     130,   140,   154,   158,   159,   162,   164,   165,   171,   174,
     175,   179,   195,   218,   249,   250,   251,   270,   271,   274,
     280,   282,   283,   291,   292,   293,   294,   296,   297,   298,
     299,   315,   317,   320,   321,   324,   326,   331,   332,   333,
     334,   335,   337,   368,   371,   372,   379,   384,   386,   388,
     391,   395,   396,   397,   398,   401,   406,   408,   411,   419,
     422,   423,   424,   430,   443,   444,   468,   469,   470,   332,
     333,     3,   221,   222,   223,   224,   225,   226,   227,   228,
     229,   230,   231,   232,   233,   234,   235,   236,   237,   238,
     239,   240,   241,   242,   243,   244,   245,   281,   441,   442,
       3,   281,   281,   281,   181,   186,   196,   204,   393,   393,
      43,   186,   196,   209,   114,     3,   364,   439,   442,   364,
      43,   186,   196,   209,   186,   409,   441,   244,   370,    68,
      69,    87,    97,   102,   104,   120,   130,   159,   171,   195,
     402,   403,   186,   409,   196,   402,   393,   118,   399,   123,
     340,     3,    13,    14,   118,   119,   152,   172,   173,   174,
     264,   412,   413,   438,   442,     3,    43,    86,    87,   115,
     118,   151,   169,   172,   186,   187,   209,   226,   232,   245,
     265,   375,   378,   440,   193,   340,   219,     0,   107,   246,
     131,   202,   325,    15,    16,    17,   148,   353,   354,   113,
     350,   154,    72,   447,    41,    41,    56,    40,    60,    61,
      62,    63,    64,   114,   295,   117,   174,   183,   184,   431,
     364,     4,   382,     3,   127,   300,   380,   381,   382,   300,
     364,     4,   377,   377,     3,   127,   127,   318,   382,   385,
     318,   318,   197,   420,   270,   271,   324,   331,   369,   161,
     404,   196,   149,   147,   247,   364,   114,   382,   389,   390,
     147,   407,   206,   207,   400,     3,    40,   125,   132,   133,
     156,   166,   167,   168,   209,   217,   341,   342,   343,    68,
     100,   359,    24,   194,   415,    40,   415,   438,    42,   113,
     387,   438,    42,    14,   414,   415,   247,   415,     3,    30,
     114,   186,    28,   201,   376,   236,   376,   147,   113,   374,
     387,   236,   137,   373,   245,   243,   205,    40,   203,   394,
     364,   364,   251,   364,     3,     4,     5,     6,     8,     9,
      10,    12,    13,    14,    20,    32,    33,    40,    79,   110,
     173,   231,   253,   254,   255,   257,   259,   264,   265,   266,
     267,   332,   438,   439,   442,   359,   359,   359,    77,   137,
     146,   339,   350,   195,   409,    44,    48,    49,    50,    58,
      79,   113,   127,   171,   174,   246,   270,   271,   280,   282,
     284,   285,   286,   287,   288,   289,   290,   298,   324,   331,
     336,   448,   449,   450,   454,   455,   456,   460,   464,   465,
     466,   467,   113,    41,   252,   257,     5,     5,    65,   432,
     433,   438,   144,   185,    90,    91,    66,    69,   102,   158,
     425,   426,   126,   139,   240,   392,    40,    20,   364,   247,
     126,   364,   201,   338,   110,   110,    40,   319,   332,   363,
     364,   365,   247,   322,   364,   319,    14,   264,   416,   421,
      34,   405,   439,   403,   270,   271,   324,   331,   410,   194,
     247,   405,   114,     5,    40,   344,    40,    40,    40,    40,
     124,   247,   343,    34,   257,   360,   361,   257,     5,    14,
     257,   415,   438,   382,    24,   415,   438,   415,   257,   413,
     257,   364,   364,   364,     4,   257,   376,   364,    89,   376,
       5,   376,    34,    41,    68,    76,   100,   135,   192,   252,
     257,   269,   351,    84,   174,    40,   316,    40,   327,   331,
     257,   257,   257,   252,   257,   332,   257,   260,   332,    42,
      18,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    31,    32,    33,    34,    35,    36,    37,
      38,    40,    42,   171,   332,   335,   337,   335,   335,   257,
     355,   356,     5,    12,   348,   348,   350,   421,   257,   270,
     271,   280,   282,   284,   285,   286,   287,   288,   289,   290,
     298,   324,   331,   336,   451,   452,   453,   455,   456,   460,
     464,   465,   466,   467,   202,   246,   281,   257,    14,   257,
     340,   412,   246,   246,   246,   246,   246,   246,   246,   246,
     246,   246,   246,   246,   246,   246,   246,   106,   450,   331,
     340,    41,   247,   194,     5,   247,    24,    24,    24,   144,
     144,    85,   427,   427,   427,   194,   427,   247,    77,    14,
      30,    41,    46,    47,   445,   446,   110,    40,   381,    77,
     147,     7,   257,   325,     3,   332,   363,   365,   247,    70,
     439,   115,   128,   132,   136,   163,   216,   366,    70,   439,
     382,   147,   247,    82,   434,   247,   194,   382,   389,   385,
       3,    41,   247,   211,   212,   213,   214,   215,   220,   345,
     346,   364,   116,   180,   182,   208,   347,   364,   342,    70,
     441,   114,   247,   339,   349,    41,    41,   257,   415,     4,
     383,   257,   415,    40,   417,   376,   376,   376,    40,   247,
      41,   114,   257,   114,   257,   114,   257,    41,    70,   114,
     257,    41,   177,   272,   273,   438,   328,   438,   328,   198,
     247,    41,   202,   261,   262,   438,   257,   257,    28,    29,
      30,   257,   257,   257,   257,   257,   257,   257,   257,    32,
      33,   255,   256,    40,   258,   332,     8,    10,    11,    20,
     257,   257,   257,   257,   257,   257,   257,   268,   270,   271,
     324,   331,    34,   438,   340,    71,    98,   357,   247,   146,
     247,   137,    73,    74,    75,    80,    88,    92,    93,    95,
     101,   112,   129,   141,   143,   145,   157,   176,   189,   190,
     191,   199,   200,   304,    52,   246,   246,   246,   246,   246,
     246,   246,   246,   246,   246,   246,   246,   246,   246,   246,
     106,   453,   257,    60,    61,    62,    63,    64,   131,   295,
     202,   461,   462,    30,   188,   361,   246,   340,   257,     5,
     433,    14,   254,     4,     4,    24,    24,   303,   438,   438,
     438,   364,   438,   158,   426,   383,   304,    14,    14,    14,
      41,   247,   155,   301,   302,   303,   383,   364,   257,    41,
     363,   439,   150,   367,   363,   216,   367,   216,   367,   132,
     439,   323,   364,   364,     4,   416,   385,    41,    41,   247,
     247,    41,   364,   441,   103,   362,   363,   360,   257,   257,
     254,   418,    41,     5,   257,   114,   257,   114,   257,   114,
     304,   257,    41,   247,   338,    24,    41,   247,    41,    40,
     329,   257,   257,   105,   262,   263,   257,   256,   258,   256,
     256,    19,    32,    33,    34,    35,    36,    37,    38,   252,
       8,    10,    11,    41,   359,   356,   348,   348,   348,    40,
     309,   309,    40,   305,   153,   307,    40,   306,   305,    40,
     308,   308,   309,   309,    54,    96,   246,   451,    49,   246,
     131,   131,   131,   131,     5,   421,    65,   257,   105,   462,
     463,    55,   256,   451,   131,   434,     4,     4,   304,   102,
     174,   429,    78,   160,   428,   194,    54,   304,   304,   304,
     446,   134,    41,   247,    40,   275,    70,   147,   363,   346,
     364,    41,   338,   247,   338,    41,   247,   257,   257,   257,
      41,   273,   325,   257,   438,   331,   257,   330,   247,   325,
      41,   188,   257,   106,    19,   256,   256,   256,   256,   256,
     256,   256,   256,    41,   361,     5,     5,     5,     5,   246,
     254,   106,   246,   421,   421,   421,   421,   131,     5,   188,
     451,   106,   256,   194,    45,   105,   106,   457,   458,   459,
     421,   170,   435,   310,    20,    96,    20,    96,   441,    54,
      54,    54,    40,    82,   223,   224,   225,   227,   229,   237,
     238,   239,   242,   312,   313,   302,   328,   210,   276,   439,
     257,   147,   247,   349,   363,   121,   352,   254,    41,   247,
      40,   257,   256,   114,    41,    41,   247,    41,    41,   246,
      48,   421,   131,   451,    79,   194,   256,   257,   451,   127,
     106,   458,   459,   106,   114,    24,   235,    10,    20,    96,
     155,   221,   311,    10,    10,   254,   328,    24,   314,   314,
     314,   314,   314,   314,   314,   314,   314,   314,   247,    41,
      40,   277,   278,   279,   313,   257,   364,    77,   122,   358,
     257,   330,     5,   246,   421,   246,   256,    49,   188,   246,
     127,   106,   127,   362,    76,   142,   178,    24,    10,   254,
     134,    41,     4,     4,   180,     4,     4,     5,     5,     5,
       5,     8,   313,   328,   247,    42,   252,   257,    41,    41,
      49,   451,   451,   246,   127,   246,   338,   222,   228,   230,
     241,   436,    41,   279,   364,   451,   106,   246,   350,    81,
     233,   437,   247,   106,    49,    24,    24,   364,    49,   246,
       5,     4,    42,   246,   234,   364,    24,    41,     5
=======
       0,    40,    46,    49,    60,    64,    71,    74,    75,    76,
      79,    85,    87,    96,   106,   128,   132,   133,   136,   138,
     139,   143,   146,   147,   151,   167,   209,   210,   211,   229,
     230,   233,   249,   252,   254,   259,   260,   261,   262,   263,
     264,   293,   296,   303,   308,   310,   312,   315,   319,   320,
     321,   322,   325,   330,   332,   335,   340,   343,   344,   345,
     351,   260,   261,   153,   158,   168,   176,   317,   317,   158,
     168,    90,     3,   181,   182,   183,   184,   185,   186,   187,
     188,   189,   190,   191,   192,   193,   194,   195,   196,   197,
     198,   199,   200,   201,   202,   203,   204,   205,   289,   360,
     363,   289,   158,   168,     3,   333,   362,   363,   204,   295,
      45,    46,    64,    74,    79,    96,   106,   133,   143,   167,
     326,   327,   333,   168,   326,   317,    94,   323,    99,   267,
       3,    13,    14,    94,    95,   126,   144,   145,   336,   337,
     359,   363,     3,    63,    64,    91,    94,   125,   141,   144,
     158,   159,   186,   192,   205,   224,   299,   302,   361,   165,
     267,     0,    83,   206,   107,   174,   253,    15,    16,    17,
     122,   278,   279,    89,   275,   128,    41,    41,    93,   146,
     155,   156,   352,   289,     4,   306,   103,   234,   304,   305,
     306,   289,     4,   301,   301,   103,   250,   306,   309,   169,
     341,   229,   230,   252,   259,   294,   135,   328,   168,   123,
     121,   207,    90,   306,   313,   314,   121,   331,   178,   179,
     324,    40,   101,   130,   140,   268,   269,   270,    45,    77,
     284,    24,   166,   338,   338,   359,    42,    89,   311,   359,
      42,   207,   338,    30,    90,   158,    28,   173,   300,   196,
     300,    89,   298,   311,   196,   112,   297,   205,   203,   177,
      40,   175,   318,   289,   211,   289,     3,     4,     5,     6,
       8,     9,    10,    12,    13,    14,    20,    32,    33,    40,
      56,    86,   145,   191,   213,   214,   215,   217,   219,   224,
     225,   226,   260,   359,   360,   363,   284,   284,   284,    54,
     112,   120,   266,   275,   167,   333,   353,   354,   359,   118,
     157,    67,    68,    43,    46,    79,   132,   346,   347,   102,
     114,   200,   316,    20,   289,   207,   102,   173,   265,    86,
      40,   251,   260,   288,   289,   290,   207,    14,   339,   342,
      34,   329,   360,   327,   229,   230,   252,   259,   334,   166,
     207,   329,    90,     5,   271,    40,   100,   207,   270,    34,
     217,   285,   286,   217,   217,   338,   359,   306,    24,   338,
     359,   337,   217,   289,   289,   289,     4,   217,   300,    66,
     300,     5,   300,    34,    41,    45,    53,    77,   110,   164,
     212,   217,   228,   276,    61,   146,    40,   255,   259,   217,
     217,   217,   212,   217,   260,   217,   220,   260,    42,    18,
      19,    20,    21,    22,    23,    24,    25,    26,    27,    28,
      29,    30,    31,    32,    33,    34,    35,    36,    37,    38,
      40,    42,   143,   260,   263,   264,   263,   263,   217,   280,
     281,     5,    12,   273,   273,   275,   207,    24,    24,    24,
     118,   118,    62,   348,   348,   348,   166,   348,   207,    54,
      86,    40,   305,    54,     7,   217,   253,   260,   288,   290,
     207,    47,   360,    91,   104,   108,   111,   137,   291,    47,
     360,   306,   207,   166,   306,   313,   309,    41,   207,    92,
     152,   154,   180,   272,   269,    47,   362,    90,   207,   266,
     274,   217,   338,     4,   307,   217,   338,   300,   300,    40,
     207,    41,    90,   217,    90,   217,    90,   217,    41,   207,
      47,    90,   217,    41,   149,   231,   232,   359,   256,   359,
     170,   207,    41,   174,   221,   222,   359,   217,   217,    28,
      29,    30,   217,   217,   217,   217,   217,   217,   217,   217,
      32,    33,   215,   216,    40,   218,   260,     8,    10,    11,
      20,   217,   217,   217,   217,   217,   217,   217,   227,   229,
     230,   252,   259,    34,   359,   267,    48,    75,   282,   207,
     120,   207,   112,   354,   214,     4,     4,    24,    24,   237,
     359,   359,   359,   289,   359,   132,   347,   307,   129,   235,
     236,   237,   307,   217,    41,   288,   360,   124,   292,   288,
     292,   292,   108,   360,   339,   309,    41,   362,    80,   287,
     288,   285,   217,   217,    41,     5,   217,    90,   217,    90,
     217,    90,   217,    50,    51,    52,    57,    65,    69,    70,
      72,    78,    88,   105,   115,   117,   119,   131,   148,   161,
     162,   163,   171,   172,   238,   217,    41,   207,   265,    24,
      41,   207,    40,   257,   217,   217,    81,   222,   223,   217,
     216,   218,   216,   216,    19,    32,    33,    34,    35,    36,
      37,    38,   212,   217,     8,    10,    11,    41,   284,   281,
     273,   273,   273,    59,   355,     4,     4,   238,    79,   146,
     350,    55,   134,   349,   166,   109,    41,   207,    47,   121,
     288,   265,   207,   265,   217,   217,   217,    40,   243,   243,
      40,   239,   127,   241,    40,   240,   239,    40,   242,   242,
     243,   243,    41,   232,   253,   217,   259,   359,   217,   258,
     207,   253,    41,   160,   217,    82,    19,   216,   216,   216,
     216,   216,   216,   216,   216,    41,   286,     4,   142,   356,
     244,    20,    73,    20,    73,   362,    40,    59,   183,   184,
     185,   187,   189,   197,   198,   199,   202,   246,   247,   236,
     360,   217,   121,   274,   288,    97,   277,     5,     5,     5,
       5,    41,   207,    40,   217,   216,    90,    24,   195,    10,
      20,    73,   129,   181,   245,    10,    10,   214,   256,    24,
     248,   248,   248,   248,   248,   248,   248,   248,   248,   248,
     207,   217,    54,    98,   283,    41,    41,   207,    41,    41,
     217,   258,    53,   116,   150,    24,    10,   214,   109,    41,
       4,     4,   152,     4,     4,     5,     5,     5,     5,     8,
     247,   212,   217,     5,    41,   182,   188,   190,   201,   357,
      41,    58,   193,   358,    24,    24,     5,     4,   194,    24,
       5
>>>>>>> refs/remotes/origin/master
};

#define yyerrok		(yyerrstatus = 0)
#define yyclearin	(yychar = YYEMPTY)
#define YYEMPTY		(-2)
#define YYEOF		0

#define YYACCEPT	goto yyacceptlab
#define YYABORT		goto yyabortlab
#define YYERROR		goto yyerrorlab


/* Like YYERROR except do call yyerror.  This remains here temporarily
   to ease the transition to the new meaning of YYERROR, for GCC.
<<<<<<< HEAD
   Once GCC version 2 has supplanted version 1, this can go.  */

#define YYFAIL		goto yyerrlab
=======
   Once GCC version 2 has supplanted version 1, this can go.  However,
   YYFAIL appears to be in use.  Nevertheless, it is formally deprecated
   in Bison 2.4.2's NEWS entry, where a plan to phase it out is
   discussed.  */

#define YYFAIL		goto yyerrlab
#if defined YYFAIL
  /* This is here to suppress warnings from the GCC cpp's
     -Wunused-macros.  Normally we don't worry about that warning, but
     some users do, and we want to make it easy for users to remove
     YYFAIL uses, which will produce warnings from Bison 2.5.  */
#endif
>>>>>>> refs/remotes/origin/master

#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)					\
do								\
  if (yychar == YYEMPTY && yylen == 1)				\
    {								\
      yychar = (Token);						\
      yylval = (Value);						\
<<<<<<< HEAD
      yytoken = YYTRANSLATE (yychar);				\
=======
>>>>>>> refs/remotes/origin/master
      YYPOPSTACK (1);						\
      goto yybackup;						\
    }								\
  else								\
    {								\
      yyerror (&yylloc, result, YY_("syntax error: cannot back up")); \
      YYERROR;							\
    }								\
while (YYID (0))


#define YYTERROR	1
#define YYERRCODE	256


/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#define YYRHSLOC(Rhs, K) ((Rhs)[K])
#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)				\
    do									\
      if (YYID (N))                                                    \
	{								\
	  (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;	\
	  (Current).first_column = YYRHSLOC (Rhs, 1).first_column;	\
	  (Current).last_line    = YYRHSLOC (Rhs, N).last_line;		\
	  (Current).last_column  = YYRHSLOC (Rhs, N).last_column;	\
	}								\
      else								\
	{								\
	  (Current).first_line   = (Current).last_line   =		\
	    YYRHSLOC (Rhs, 0).last_line;				\
	  (Current).first_column = (Current).last_column =		\
	    YYRHSLOC (Rhs, 0).last_column;				\
	}								\
    while (YYID (0))
#endif


/* YY_LOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

#ifndef YY_LOCATION_PRINT
<<<<<<< HEAD
# if YYLTYPE_IS_TRIVIAL
=======
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
>>>>>>> refs/remotes/origin/master
#  define YY_LOCATION_PRINT(File, Loc)			\
     fprintf (File, "%d.%d-%d.%d",			\
	      (Loc).first_line, (Loc).first_column,	\
	      (Loc).last_line,  (Loc).last_column)
# else
#  define YY_LOCATION_PRINT(File, Loc) ((void) 0)
# endif
#endif


/* YYLEX -- calling `yylex' with the right arguments.  */

#ifdef YYLEX_PARAM
# define YYLEX yylex (&yylval, &yylloc, YYLEX_PARAM)
#else
# define YYLEX yylex (&yylval, &yylloc)
#endif

/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)			\
do {						\
  if (yydebug)					\
    YYFPRINTF Args;				\
} while (YYID (0))

# define YY_SYMBOL_PRINT(Title, Type, Value, Location)			  \
do {									  \
  if (yydebug)								  \
    {									  \
      YYFPRINTF (stderr, "%s ", Title);					  \
      yy_symbol_print (stderr,						  \
		  Type, Value, Location, result); \
      YYFPRINTF (stderr, "\n");						  \
    }									  \
} while (YYID (0))


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_value_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (!yyvaluep)
    return;
  YYUSE (yylocationp);
  YYUSE (result);
# ifdef YYPRINT
  if (yytype < YYNTOKENS)
    YYPRINT (yyoutput, yytoknum[yytype], *yyvaluep);
# else
  YYUSE (yyoutput);
# endif
  switch (yytype)
    {
      default:
	break;
    }
}


/*--------------------------------.
| Print this symbol on YYOUTPUT.  |
`--------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_symbol_print (FILE *yyoutput, int yytype, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, ParseResult* result)
#else
static void
yy_symbol_print (yyoutput, yytype, yyvaluep, yylocationp, result)
    FILE *yyoutput;
    int yytype;
    YYSTYPE const * const yyvaluep;
    YYLTYPE const * const yylocationp;
    ParseResult* result;
#endif
{
  if (yytype < YYNTOKENS)
    YYFPRINTF (yyoutput, "token %s (", yytname[yytype]);
  else
    YYFPRINTF (yyoutput, "nterm %s (", yytname[yytype]);

  YY_LOCATION_PRINT (yyoutput, *yylocationp);
  YYFPRINTF (yyoutput, ": ");
  yy_symbol_value_print (yyoutput, yytype, yyvaluep, yylocationp, result);
  YYFPRINTF (yyoutput, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_stack_print (yytype_int16 *yybottom, yytype_int16 *yytop)
#else
static void
yy_stack_print (yybottom, yytop)
    yytype_int16 *yybottom;
    yytype_int16 *yytop;
#endif
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)				\
do {								\
  if (yydebug)							\
    yy_stack_print ((Bottom), (Top));				\
} while (YYID (0))


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yy_reduce_print (YYSTYPE *yyvsp, YYLTYPE *yylsp, int yyrule, ParseResult* result)
#else
static void
yy_reduce_print (yyvsp, yylsp, yyrule, result)
    YYSTYPE *yyvsp;
    YYLTYPE *yylsp;
    int yyrule;
    ParseResult* result;
#endif
{
  int yynrhs = yyr2[yyrule];
  int yyi;
  unsigned long int yylno = yyrline[yyrule];
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %lu):\n",
	     yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr, yyrhs[yyprhs[yyrule] + yyi],
		       &(yyvsp[(yyi + 1) - (yynrhs)])
		       , &(yylsp[(yyi + 1) - (yynrhs)])		       , result);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)		\
do {					\
  if (yydebug)				\
    yy_reduce_print (yyvsp, yylsp, Rule, result); \
} while (YYID (0))

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args)
# define YY_SYMBOL_PRINT(Title, Type, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef	YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif

<<<<<<< HEAD

=======
>>>>>>> refs/remotes/origin/master

#if YYERROR_VERBOSE

# ifndef yystrlen
#  if defined __GLIBC__ && defined _STRING_H
#   define yystrlen strlen
#  else
/* Return the length of YYSTR.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static YYSIZE_T
yystrlen (const char *yystr)
#else
static YYSIZE_T
yystrlen (yystr)
    const char *yystr;
#endif
{
  YYSIZE_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
#  endif
# endif

# ifndef yystpcpy
#  if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#   define yystpcpy stpcpy
#  else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static char *
yystpcpy (char *yydest, const char *yysrc)
#else
static char *
yystpcpy (yydest, yysrc)
    char *yydest;
    const char *yysrc;
#endif
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
#  endif
# endif

# ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYSIZE_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYSIZE_T yyn = 0;
      char const *yyp = yystr;

      for (;;)
	switch (*++yyp)
	  {
	  case '\'':
	  case ',':
	    goto do_not_strip_quotes;

	  case '\\':
	    if (*++yyp != '\\')
	      goto do_not_strip_quotes;
	    /* Fall through.  */
	  default:
	    if (yyres)
	      yyres[yyn] = *yyp;
	    yyn++;
	    break;

	  case '"':
	    if (yyres)
	      yyres[yyn] = '\0';
	    return yyn;
	  }
    do_not_strip_quotes: ;
    }

  if (! yyres)
    return yystrlen (yystr);

  return yystpcpy (yyres, yystr) - yyres;
}
# endif

<<<<<<< HEAD
/* Copy into YYRESULT an error message about the unexpected token
   YYCHAR while in state YYSTATE.  Return the number of bytes copied,
   including the terminating null byte.  If YYRESULT is null, do not
   copy anything; just return the number of bytes that would be
   copied.  As a special case, return 0 if an ordinary "syntax error"
   message will do.  Return YYSIZE_MAXIMUM if overflow occurs during
   size calculation.  */
static YYSIZE_T
yysyntax_error (char *yyresult, int yystate, int yychar)
{
  int yyn = yypact[yystate];

  if (! (YYPACT_NINF < yyn && yyn <= YYLAST))
    return 0;
  else
    {
      int yytype = YYTRANSLATE (yychar);
      YYSIZE_T yysize0 = yytnamerr (0, yytname[yytype]);
      YYSIZE_T yysize = yysize0;
      YYSIZE_T yysize1;
      int yysize_overflow = 0;
      enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
      char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
      int yyx;

# if 0
      /* This is so xgettext sees the translatable formats that are
	 constructed on the fly.  */
      YY_("syntax error, unexpected %s");
      YY_("syntax error, unexpected %s, expecting %s");
      YY_("syntax error, unexpected %s, expecting %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s");
      YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s");
# endif
      char *yyfmt;
      char const *yyf;
      static char const yyunexpected[] = "syntax error, unexpected %s";
      static char const yyexpecting[] = ", expecting %s";
      static char const yyor[] = " or %s";
      char yyformat[sizeof yyunexpected
		    + sizeof yyexpecting - 1
		    + ((YYERROR_VERBOSE_ARGS_MAXIMUM - 2)
		       * (sizeof yyor - 1))];
      char const *yyprefix = yyexpecting;

      /* Start YYX at -YYN if negative to avoid negative indexes in
	 YYCHECK.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;

      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yycount = 1;

      yyarg[0] = yytname[yytype];
      yyfmt = yystpcpy (yyformat, yyunexpected);

      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
	if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR)
	  {
	    if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
	      {
		yycount = 1;
		yysize = yysize0;
		yyformat[sizeof yyunexpected - 1] = '\0';
		break;
	      }
	    yyarg[yycount++] = yytname[yyx];
	    yysize1 = yysize + yytnamerr (0, yytname[yyx]);
	    yysize_overflow |= (yysize1 < yysize);
	    yysize = yysize1;
	    yyfmt = yystpcpy (yyfmt, yyprefix);
	    yyprefix = yyor;
	  }

      yyf = YY_(yyformat);
      yysize1 = yysize + yystrlen (yyf);
      yysize_overflow |= (yysize1 < yysize);
      yysize = yysize1;

      if (yysize_overflow)
	return YYSIZE_MAXIMUM;

      if (yyresult)
	{
	  /* Avoid sprintf, as that infringes on the user's name space.
	     Don't have undefined behavior even if the translation
	     produced a string with the wrong number of "%s"s.  */
	  char *yyp = yyresult;
	  int yyi = 0;
	  while ((*yyp = *yyf) != '\0')
	    {
	      if (*yyp == '%' && yyf[1] == 's' && yyi < yycount)
		{
		  yyp += yytnamerr (yyp, yyarg[yyi++]);
		  yyf += 2;
		}
	      else
		{
		  yyp++;
		  yyf++;
		}
	    }
	}
      return yysize;
    }
}
#endif /* YYERROR_VERBOSE */

=======
/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return 1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return 2 if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYSIZE_T *yymsg_alloc, char **yymsg,
                yytype_int16 *yyssp, int yytoken)
{
  YYSIZE_T yysize0 = yytnamerr (0, yytname[yytoken]);
  YYSIZE_T yysize = yysize0;
  YYSIZE_T yysize1;
  enum { YYERROR_VERBOSE_ARGS_MAXIMUM = 5 };
  /* Internationalized format string. */
  const char *yyformat = 0;
  /* Arguments of yyformat. */
  char const *yyarg[YYERROR_VERBOSE_ARGS_MAXIMUM];
  /* Number of reported tokens (one for the "unexpected", one per
     "expected"). */
  int yycount = 0;

  /* There are many possibilities here to consider:
     - Assume YYFAIL is not used.  It's too flawed to consider.  See
       <http://lists.gnu.org/archive/html/bison-patches/2009-12/msg00024.html>
       for details.  YYERROR is fine as it does not invoke this
       function.
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yytoken != YYEMPTY)
    {
      int yyn = yypact[*yyssp];
      yyarg[yycount++] = yytname[yytoken];
      if (!yypact_value_is_default (yyn))
        {
          /* Start YYX at -YYN if negative to avoid negative indexes in
             YYCHECK.  In other words, skip the first -YYN actions for
             this state because they are default actions.  */
          int yyxbegin = yyn < 0 ? -yyn : 0;
          /* Stay within bounds of both yycheck and yytname.  */
          int yychecklim = YYLAST - yyn + 1;
          int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
          int yyx;

          for (yyx = yyxbegin; yyx < yyxend; ++yyx)
            if (yycheck[yyx + yyn] == yyx && yyx != YYTERROR
                && !yytable_value_is_error (yytable[yyx + yyn]))
              {
                if (yycount == YYERROR_VERBOSE_ARGS_MAXIMUM)
                  {
                    yycount = 1;
                    yysize = yysize0;
                    break;
                  }
                yyarg[yycount++] = yytname[yyx];
                yysize1 = yysize + yytnamerr (0, yytname[yyx]);
                if (! (yysize <= yysize1
                       && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
                  return 2;
                yysize = yysize1;
              }
        }
    }

  switch (yycount)
    {
# define YYCASE_(N, S)                      \
      case N:                               \
        yyformat = S;                       \
      break
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
# undef YYCASE_
    }

  yysize1 = yysize + yystrlen (yyformat);
  if (! (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM))
    return 2;
  yysize = yysize1;

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return 1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yyarg[yyi++]);
          yyformat += 2;
        }
      else
        {
          yyp++;
          yyformat++;
        }
  }
  return 0;
}
#endif /* YYERROR_VERBOSE */
>>>>>>> refs/remotes/origin/master

/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

/*ARGSUSED*/
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
static void
yydestruct (const char *yymsg, int yytype, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, ParseResult* result)
#else
static void
yydestruct (yymsg, yytype, yyvaluep, yylocationp, result)
    const char *yymsg;
    int yytype;
    YYSTYPE *yyvaluep;
    YYLTYPE *yylocationp;
    ParseResult* result;
#endif
{
  YYUSE (yyvaluep);
  YYUSE (yylocationp);
  YYUSE (result);

  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yytype, yyvaluep, yylocationp);

  switch (yytype)
    {
      case 3: /* "NAME" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 4: /* "STRING" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 5: /* "INTNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 6: /* "DATE_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 7: /* "HINT_VALUE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 8: /* "BOOL" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 9: /* "APPROXNUM" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 10: /* "NULLX" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 11: /* "UNKNOWN" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 12: /* "QUESTIONMARK" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 13: /* "SYSTEM_VARIABLE" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 14: /* "TEMP_VARIABLE" */

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 249: /* "sql_stmt" */
=======
      case 209: /* "sql_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 250: /* "stmt_list" */
=======
      case 210: /* "stmt_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 251: /* "stmt" */
=======
      case 211: /* "stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 252: /* "expr_list" */
=======
      case 212: /* "expr_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 253: /* "column_ref" */
=======
      case 213: /* "column_ref" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 254: /* "expr_const" */
=======
      case 214: /* "expr_const" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 255: /* "simple_expr" */
=======
      case 215: /* "simple_expr" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 256: /* "arith_expr" */
=======
      case 216: /* "arith_expr" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 257: /* "expr" */
=======
      case 217: /* "expr" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 258: /* "in_expr" */
=======
      case 218: /* "in_expr" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 259: /* "case_expr" */
=======
      case 219: /* "case_expr" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 260: /* "case_arg" */
=======
      case 220: /* "case_arg" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 261: /* "when_clause_list" */
=======
      case 221: /* "when_clause_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 262: /* "when_clause" */
=======
      case 222: /* "when_clause" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 263: /* "case_default" */
=======
      case 223: /* "case_default" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 264: /* "array_expr" */
=======
      case 224: /* "func_expr" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 265: /* "func_expr" */
=======
      case 225: /* "when_func" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 266: /* "when_func" */
=======
      case 226: /* "when_func_name" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 267: /* "when_func_name" */
=======
      case 227: /* "when_func_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 268: /* "when_func_stmt" */
=======
      case 228: /* "distinct_or_all" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 269: /* "distinct_or_all" */
=======
      case 229: /* "delete_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 270: /* "delete_stmt" */
=======
      case 230: /* "update_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 271: /* "update_stmt" */
=======
      case 231: /* "update_asgn_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 272: /* "update_asgn_list" */
=======
      case 232: /* "update_asgn_factor" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 273: /* "update_asgn_factor" */
=======
      case 233: /* "create_table_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 274: /* "create_index_stmt" */
=======
      case 234: /* "opt_if_not_exists" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 275: /* "opt_index_columns" */
=======
      case 235: /* "table_element_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 276: /* "opt_storing" */
=======
      case 236: /* "table_element" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 277: /* "opt_storing_columns" */
=======
      case 237: /* "column_definition" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 278: /* "opt_index_option_list" */
=======
      case 238: /* "data_type" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 279: /* "index_option" */
=======
      case 239: /* "opt_decimal" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 280: /* "cursor_declare_stmt" */
=======
      case 240: /* "opt_float" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 281: /* "cursor_name" */
=======
      case 241: /* "opt_precision" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 282: /* "cursor_open_stmt" */
=======
      case 242: /* "opt_time_precision" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 283: /* "cursor_fetch_stmt" */
=======
      case 243: /* "opt_char_length" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 284: /* "cursor_fetch_into_stmt" */
=======
      case 244: /* "opt_column_attribute_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 285: /* "cursor_fetch_next_into_stmt" */
=======
      case 245: /* "column_attribute" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 286: /* "cursor_fetch_first_into_stmt" */
=======
      case 246: /* "opt_table_option_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 287: /* "cursor_fetch_prior_into_stmt" */
=======
      case 247: /* "table_option" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 288: /* "cursor_fetch_last_into_stmt" */
=======
      case 248: /* "opt_equal_mark" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 289: /* "cursor_fetch_absolute_into_stmt" */
=======
      case 249: /* "drop_table_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 290: /* "cursor_fetch_relative_into_stmt" */
=======
      case 250: /* "opt_if_exists" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 291: /* "fetch_prior_stmt" */
=======
      case 251: /* "table_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 292: /* "fetch_first_stmt" */
=======
      case 252: /* "insert_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 293: /* "fetch_last_stmt" */
=======
      case 253: /* "opt_when" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 294: /* "fetch_relative_stmt" */
=======
      case 254: /* "replace_or_insert" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 295: /* "next_or_prior" */
=======
      case 255: /* "opt_insert_columns" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 296: /* "fetch_absolute_stmt" */
=======
      case 256: /* "column_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 297: /* "fetch_fromto_stmt" */
=======
      case 257: /* "insert_vals_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 298: /* "cursor_close_stmt" */
=======
      case 258: /* "insert_vals" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 299: /* "create_table_stmt" */
=======
      case 259: /* "select_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 300: /* "opt_if_not_exists" */
=======
      case 260: /* "select_with_parens" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 301: /* "table_element_list" */
=======
      case 261: /* "select_no_parens" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 302: /* "table_element" */
=======
      case 262: /* "no_table_select" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 303: /* "column_definition" */
=======
      case 263: /* "select_clause" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 304: /* "data_type" */
=======
      case 264: /* "simple_select" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 305: /* "opt_decimal" */
=======
      case 265: /* "opt_where" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 306: /* "opt_float" */
=======
      case 266: /* "select_limit" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 307: /* "opt_precision" */
=======
      case 267: /* "opt_hint" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 308: /* "opt_time_precision" */
=======
      case 268: /* "opt_hint_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 309: /* "opt_char_length" */
=======
      case 269: /* "hint_options" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 310: /* "opt_column_attribute_list" */
=======
      case 270: /* "hint_option" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 311: /* "column_attribute" */
=======
      case 271: /* "opt_comma_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 312: /* "opt_table_option_list" */
=======
      case 273: /* "limit_expr" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 313: /* "table_option" */
=======
      case 274: /* "opt_select_limit" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 314: /* "opt_equal_mark" */
=======
      case 275: /* "opt_for_update" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 315: /* "gather_statistics_stmt" */
=======
      case 276: /* "parameterized_trim" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 316: /* "opt_gather_columns" */
=======
      case 277: /* "opt_groupby" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 317: /* "drop_table_stmt" */
=======
      case 278: /* "opt_order_by" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 318: /* "opt_if_exists" */
=======
      case 279: /* "order_by" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 319: /* "table_list" */
=======
      case 280: /* "sort_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 320: /* "truncate_table_stmt" */
=======
      case 281: /* "sort_key" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 321: /* "drop_index_stmt" */
=======
      case 282: /* "opt_asc_desc" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 322: /* "index_list" */
=======
      case 283: /* "opt_having" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 323: /* "table_name" */
=======
      case 284: /* "opt_distinct" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 324: /* "insert_stmt" */
=======
      case 285: /* "projection" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 325: /* "opt_when" */
=======
      case 286: /* "select_expr_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 326: /* "replace_or_insert" */
=======
      case 287: /* "from_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 327: /* "opt_insert_columns" */
=======
      case 288: /* "table_factor" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 328: /* "column_list" */
=======
      case 289: /* "relation_factor" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 329: /* "insert_vals_list" */
=======
      case 290: /* "joined_table" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 330: /* "insert_vals" */
=======
      case 291: /* "join_type" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 331: /* "select_stmt" */
=======
      case 292: /* "join_outer" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 332: /* "select_with_parens" */
=======
      case 293: /* "explain_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 333: /* "select_no_parens" */
=======
      case 294: /* "explainable_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 334: /* "no_table_select" */
=======
      case 295: /* "opt_verbose" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 335: /* "select_clause" */
=======
      case 296: /* "show_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 336: /* "select_into_clause" */
=======
      case 297: /* "opt_limit" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 337: /* "simple_select" */
=======
      case 298: /* "opt_for_grant_user" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 338: /* "opt_where" */
=======
      case 300: /* "opt_show_condition" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 339: /* "select_limit" */
=======
      case 301: /* "opt_like_condition" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 340: /* "opt_hint" */
=======
      case 303: /* "create_user_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 341: /* "opt_hint_list" */
=======
      case 304: /* "user_specification_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 342: /* "hint_options" */
=======
      case 305: /* "user_specification" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 343: /* "hint_option" */
=======
      case 306: /* "user" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 344: /* "opt_comma_list" */
=======
      case 307: /* "password" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 345: /* "join_op_type_list" */
=======
      case 308: /* "drop_user_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 346: /* "join_op_type" */
=======
      case 309: /* "user_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 348: /* "limit_expr" */
=======
      case 310: /* "set_password_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 349: /* "opt_select_limit" */
=======
      case 311: /* "opt_for_user" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 350: /* "opt_for_update" */
=======
      case 312: /* "rename_user_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 351: /* "parameterized_trim" */
=======
      case 313: /* "rename_info" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 352: /* "opt_groupby" */
=======
      case 314: /* "rename_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 353: /* "opt_order_by" */
=======
      case 315: /* "lock_user_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 354: /* "order_by" */
=======
      case 316: /* "lock_spec" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 355: /* "sort_list" */
=======
      case 317: /* "opt_work" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 356: /* "sort_key" */
=======
      case 319: /* "begin_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 357: /* "opt_asc_desc" */
=======
      case 320: /* "commit_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 358: /* "opt_having" */
=======
      case 321: /* "rollback_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 359: /* "opt_distinct" */
=======
      case 322: /* "kill_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 360: /* "projection" */
=======
      case 323: /* "opt_is_global" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 361: /* "select_expr_list" */
=======
      case 324: /* "opt_flag" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 362: /* "from_list" */
=======
      case 325: /* "grant_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 363: /* "table_factor" */
=======
      case 326: /* "priv_type_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 364: /* "relation_factor" */
=======
      case 327: /* "priv_type" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 365: /* "joined_table" */
=======
      case 328: /* "opt_privilege" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 366: /* "join_type" */
=======
      case 329: /* "priv_level" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 367: /* "join_outer" */
=======
      case 330: /* "revoke_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 368: /* "explain_stmt" */
=======
      case 331: /* "opt_on_priv_level" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 369: /* "explainable_stmt" */
=======
      case 332: /* "prepare_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 370: /* "opt_verbose" */
=======
      case 333: /* "stmt_name" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 371: /* "show_stmt" */
=======
      case 334: /* "preparable_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 372: /* "lock_table_stmt" */
=======
      case 335: /* "variable_set_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 373: /* "opt_limit" */
=======
      case 336: /* "var_and_val_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 374: /* "opt_for_grant_user" */
=======
      case 337: /* "var_and_val" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 376: /* "opt_show_condition" */
=======
      case 338: /* "to_or_eq" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 377: /* "opt_like_condition" */
=======
      case 339: /* "argument" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 379: /* "create_user_stmt" */
=======
      case 340: /* "execute_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 380: /* "user_specification_list" */
=======
      case 341: /* "opt_using_args" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 381: /* "user_specification" */
=======
      case 342: /* "argument_list" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 382: /* "user" */
=======
      case 343: /* "deallocate_prepare_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 383: /* "password" */
=======
      case 344: /* "deallocate_or_drop" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 384: /* "drop_user_stmt" */
=======
      case 345: /* "alter_table_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 385: /* "user_list" */
=======
      case 346: /* "alter_column_actions" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 386: /* "set_password_stmt" */
=======
      case 347: /* "alter_column_action" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 387: /* "opt_for_user" */
=======
      case 348: /* "opt_column" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 388: /* "rename_user_stmt" */
=======
      case 350: /* "alter_column_behavior" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 389: /* "rename_info" */
=======
      case 351: /* "alter_system_stmt" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 390: /* "rename_list" */
=======
      case 352: /* "opt_force" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 391: /* "lock_user_stmt" */
=======
      case 353: /* "alter_system_actions" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 392: /* "lock_spec" */
=======
      case 354: /* "alter_system_action" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 393: /* "opt_work" */
=======
      case 355: /* "opt_comment" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 395: /* "begin_stmt" */
=======
      case 357: /* "server_type" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 396: /* "commit_stmt" */
=======
      case 358: /* "opt_cluster_or_address" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 397: /* "rollback_stmt" */
=======
      case 359: /* "column_name" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 398: /* "kill_stmt" */
=======
      case 360: /* "relation_name" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 399: /* "opt_is_global" */
=======
      case 361: /* "function_name" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 400: /* "opt_flag" */
=======
      case 362: /* "column_label" */
>>>>>>> refs/remotes/origin/master

	{destroy_tree((yyvaluep->node));};

	break;
<<<<<<< HEAD
      case 401: /* "grant_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 402: /* "priv_type_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 403: /* "priv_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 404: /* "opt_privilege" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 405: /* "priv_level" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 406: /* "revoke_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 407: /* "opt_on_priv_level" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 408: /* "prepare_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 409: /* "stmt_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 410: /* "preparable_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 411: /* "variable_set_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 412: /* "var_and_val_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 413: /* "var_and_val" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 414: /* "var_and_array_val" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 415: /* "to_or_eq" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 416: /* "argument" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 417: /* "array_vals_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 418: /* "array_val_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 419: /* "execute_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 420: /* "opt_using_args" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 421: /* "argument_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 422: /* "deallocate_prepare_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 423: /* "deallocate_or_drop" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 424: /* "alter_table_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 425: /* "alter_column_actions" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 426: /* "alter_column_action" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 427: /* "opt_column" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 429: /* "alter_column_behavior" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 430: /* "alter_system_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 431: /* "opt_force" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 432: /* "alter_system_actions" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 433: /* "alter_system_action" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 434: /* "opt_comment" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 436: /* "server_type" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 437: /* "opt_cluster_or_address" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 438: /* "column_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 439: /* "relation_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 440: /* "function_name" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 441: /* "column_label" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 443: /* "create_procedure_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 444: /* "proc_decl" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 445: /* "proc_parameter_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 446: /* "proc_parameter" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 447: /* "proc_block" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 448: /* "proc_sect" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 449: /* "proc_stmts" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 450: /* "proc_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 451: /* "control_sect" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 452: /* "control_stmts" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 453: /* "control_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 454: /* "stmt_declare" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 455: /* "stmt_assign" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 456: /* "stmt_if" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 457: /* "stmt_elsifs" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 458: /* "stmt_elsif" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 459: /* "stmt_else" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 460: /* "stmt_case" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 461: /* "case_when_list" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 462: /* "case_when" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 463: /* "case_else" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 464: /* "stmt_loop" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 465: /* "stmt_while" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 466: /* "stmt_null" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 467: /* "stmt_exit" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 468: /* "exec_procedure_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 469: /* "drop_procedure_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;
      case 470: /* "show_procedure_stmt" */

	{destroy_tree((yyvaluep->node));};

	break;

      default:
	break;
    }
}

/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (ParseResult* result);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */





/*-------------------------.
| yyparse or yypush_parse.  |
`-------------------------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (ParseResult* result)
#else
int
yyparse (result)
    ParseResult* result;
#endif
#endif
{
/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Location data for the lookahead symbol.  */
YYLTYPE yylloc;

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.
       `yyls': related to locations.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[2];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;
  yylsp = yyls;

#if YYLTYPE_IS_TRIVIAL
  /* Initialize the default location before parsing starts.  */
  yylloc.first_line   = yylloc.last_line   = 1;
  yylloc.first_column = yylloc.last_column = 1;
#endif

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;
	YYLTYPE *yyls1 = yyls;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yyls1, yysize * sizeof (*yylsp),
		    &yystacksize);

	yyls = yyls1;
	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss_alloc, yyss);
	YYSTACK_RELOCATE (yyvs_alloc, yyvs);
	YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yyn == YYPACT_NINF)
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yyn == 0 || yyn == YYTABLE_NINF)
	goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;
  *++yylsp = yylloc;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_STMT_LIST, (yyvsp[(1) - (2)].node));
      result->result_tree_ = (yyval.node);
      YYACCEPT;
    ;}
    break;

  case 3:

    {
      if ((yyvsp[(3) - (3)].node) != NULL)
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      else
        (yyval.node) = (yyvsp[(1) - (3)].node);
    ;}
    break;

  case 4:

    {
      (yyval.node) = ((yyvsp[(1) - (1)].node) != NULL) ? (yyvsp[(1) - (1)].node) : NULL;
    ;}
    break;

  case 5:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 6:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 7:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 8:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 9:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 10:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 11:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 12:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 13:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 14:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 15:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 16:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 17:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 18:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 19:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 20:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 21:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 22:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 23:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 24:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 25:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 26:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 27:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 28:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 29:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 30:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 31:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 32:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 33:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 34:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 35:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 36:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 37:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 38:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 39:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 40:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 41:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 42:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 43:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 44:

    {(yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 45:

    {(yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 46:

    {(yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 47:

    {(yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 48:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 49:

    { (yyval.node) = NULL; ;}
    break;

  case 50:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 51:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 52:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 53:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
    ;}
    break;

  case 54:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), node);
    ;}
    break;

  case 55:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 56:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 57:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 58:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 59:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 60:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 61:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 62:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 63:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 64:

    { (yyvsp[(3) - (3)].node)->type_ = T_SYSTEM_VARIABLE; (yyval.node) = (yyvsp[(3) - (3)].node); ;}
    break;

  case 65:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 66:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 67:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 68:

    {
      ParseNode *node = NULL;
      malloc_non_terminal_node(node, result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node));
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, node);
    ;}
    break;

  case 69:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      /*
      yyerror(&@1, result, "CASE expression is not supported yet!");
      YYABORT;
      */
    ;}
    break;

  case 70:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 71:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 72:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 73:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EXISTS, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 74:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 75:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 76:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 77:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 78:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 79:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 80:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 81:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 82:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 83:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 84:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 85:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 86:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 87:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 88:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 89:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 90:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 91:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 92:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 93:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 94:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 95:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 96:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 97:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EQ, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 98:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 99:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 100:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 101:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 102:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_LIKE, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); ;}
    break;

  case 103:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_AND, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 104:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_OR, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 105:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 106:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 107:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 108:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 109:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 110:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 111:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 112:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_BTW, 3, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 113:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_BTW, 3, (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 114:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 115:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_IN, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 116:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_CNN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 117:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 118:

    { merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(2) - (3)].node)); ;}
    break;

  case 119:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_WHEN_LIST, (yyvsp[(3) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CASE, 3, (yyvsp[(2) - (5)].node), (yyval.node), (yyvsp[(4) - (5)].node));
    ;}
    break;

  case 120:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 121:

    { (yyval.node) = NULL; ;}
    break;

  case 122:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 123:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); ;}
    break;

  case 124:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHEN, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 125:

    { (yyval.node) = (yyvsp[(2) - (2)].node); ;}
    break;

  case 126:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL); ;}
    break;

  case 127:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ARRAY, 2, (yyvsp[(1) - (4)].node), (yyvsp[(3) - (4)].node));
    ;}
    break;

  case 128:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ARRAY, 2, (yyvsp[(1) - (4)].node), (yyvsp[(3) - (4)].node));
    ;}
    break;

  case 129:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") != 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "Only COUNT function can be with '*' parameter!");
        YYABORT;
      }
      else
      {
        ParseNode* node = NULL;
        malloc_terminal_node(node, result->malloc_pool_, T_STAR);
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 1, node);
      }
    ;}
    break;

  case 130:

    {
      if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "count") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "sum") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "max") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "min") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "avg") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else
      {
        yyerror(&(yylsp[(1) - (5)]), result, "Wrong system function with 'DISTINCT/ALL'!");
        YYABORT;
      }
    ;}
    break;

  case 131:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "COUNT function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "sum") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "SUM function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "max") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MAX function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "min") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MIN function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "avg") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "AVG function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "trim") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "TRIM function syntax error! TRIM don't take %d params", (yyvsp[(3) - (4)].node)->num_child_);
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
          malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (4)].node));
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
        }
      }
      else  /* system function */
      {
        ParseNode *params = NULL;
        merge_nodes(params, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
      }
    ;}
    break;

  case 132:

    {
      if (strcasecmp((yyvsp[(1) - (6)].node)->str_value_, "cast") == 0)
      {
        /*modify fanqiushi DECIMAL OceanBase_BankCommV0.2 2014_6_16:b*/
        /*$5->value_ = $5->type_;
        $5->type_ = T_INT;*/
        if((yyvsp[(5) - (6)].node)->type_!=T_TYPE_DECIMAL)
        {
            (yyvsp[(5) - (6)].node)->value_ = (yyvsp[(5) - (6)].node)->type_;
            (yyvsp[(5) - (6)].node)->type_ = T_INT;
        }
        /*modify:e*/
        ParseNode *params = NULL;
        malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, (yyvsp[(3) - (6)].node), (yyvsp[(5) - (6)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (6)].node), params);
      }
      else
      {
        yyerror(&(yylsp[(1) - (6)]), result, "AS support cast function only!");
        YYABORT;
      }
    ;}
    break;

  case 133:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), (yyvsp[(3) - (4)].node));
    ;}
    break;

  case 134:

    {
      if (strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "now") == 0 ||
          strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "current_time") == 0 ||
          strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "current_timestamp") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME, 1, (yyvsp[(1) - (3)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "strict_current_timestamp") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME_UPS, 1, (yyvsp[(1) - (3)].node));
      }
      else
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 1, (yyvsp[(1) - (3)].node));
      }
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    ;}
    break;

  case 135:

    {
      (yyval.node) = (yyvsp[(1) - (4)].node);
      (yyval.node)->children_[0] = (yyvsp[(3) - (4)].node);
    ;}
    break;

  case 136:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ROW_COUNT, 1, NULL);
    ;}
    break;

  case 141:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    ;}
    break;

  case 142:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    ;}
    break;

  case 143:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DELETE, 3, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 144:

    {
      ParseNode* assign_list = NULL;
      merge_nodes(assign_list, result->malloc_pool_, T_ASSIGN_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_UPDATE, 5, (yyvsp[(3) - (7)].node), assign_list, (yyvsp[(6) - (7)].node), (yyvsp[(7) - (7)].node), (yyvsp[(2) - (7)].node));
    ;}
    break;

  case 145:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 146:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 147:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ASSIGN_ITEM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 148:

    {
		ParseNode *index_options = NULL;
		merge_nodes(index_options, result->malloc_pool_, T_INDEX_OPTION_LIST, (yyvsp[(9) - (9)].node));
		malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_INDEX, 6,
								  (yyvsp[(3) - (9)].node), /* if not exists */
								  (yyvsp[(6) - (9)].node), /* table name */
								  (yyvsp[(4) - (9)].node), /* index name */ 
		                          (yyvsp[(7) - (9)].node), /* index columns */
		                          (yyvsp[(8) - (9)].node), /* storing list */
		                          index_options /* option list */
		                        );
	;}
    break;

  case 149:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 150:

    {
  		(yyval.node)=(yyvsp[(2) - (2)].node);
  	;}
    break;

  case 151:

    {
  		(yyval.node)=NULL;
  	;}
    break;

  case 152:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 153:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 154:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 155:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 156:

    {
		(yyval.node) = (yyvsp[(1) - (1)].node);
	;}
    break;

  case 157:

    {					 
       malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_DECLARE, 2,
                                        (yyvsp[(2) - (5)].node),		/*cursor name*/
                                        (yyvsp[(5) - (5)].node)		/*result from select_stmt*/
					             ); 
	 ;}
    break;

  case 158:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 159:

    {
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_OPEN, 1,
            (yyvsp[(2) - (2)].node)     /* cursor_name*/
                                    );
            ;}
    break;

  case 160:

    {
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH, 1,
                   (yyvsp[(2) - (2)].node)     /* cursor_name*/
                                       );
            ;}
    break;

  case 161:

    {
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH, 1,
                   (yyvsp[(2) - (3)].node)     /* cursor_name*/
                                       );
            ;}
    break;

  case 162:

    {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH, 1, (yyvsp[(2) - (4)].node));
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(4) - (4)].node));
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_INTO, 2, fetch, args);
            ;}
    break;

  case 163:

    {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_NEXT, 1, (yyvsp[(2) - (5)].node));
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(5) - (5)].node));
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_NEXT_INTO, 2, fetch, args);
            ;}
    break;

  case 164:

    {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_FIRST, 1, (yyvsp[(2) - (5)].node));
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(5) - (5)].node));
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_FIRST_INTO, 2, fetch, args);
            ;}
    break;

  case 165:

    {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_PRIOR, 1, (yyvsp[(2) - (5)].node));
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(5) - (5)].node));
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_PRIOR_INTO, 2, fetch, args);
            ;}
    break;

  case 166:

    {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_LAST, 1, (yyvsp[(2) - (5)].node));
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(5) - (5)].node));
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_LAST_INTO, 2, fetch, args);
            ;}
    break;

  case 167:

    {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_ABSOLUTE, 2, (yyvsp[(2) - (6)].node), (yyvsp[(4) - (6)].node));
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(6) - (6)].node));
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_ABS_INTO, 2, fetch, args);
            ;}
    break;

  case 168:

    {
           	  ParseNode* args = NULL;
           	  ParseNode* fetch = NULL;
           	  malloc_non_terminal_node(fetch, result->malloc_pool_, T_CURSOR_FETCH_RELATIVE, 3, (yyvsp[(2) - (7)].node), (yyvsp[(3) - (7)].node), (yyvsp[(5) - (7)].node));
			  merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(7) - (7)].node));
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_RELATIVE_INTO, 2, fetch, args);
            ;}
    break;

  case 169:

    {
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_PRIOR, 1,
                   (yyvsp[(2) - (3)].node)     /* cursor_name*/
                    
                    
                    
                                       );
            ;}
    break;

  case 170:

    {
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_FIRST, 1,
                   (yyvsp[(2) - (3)].node)     /* cursor_name*/
                                       );
            ;}
    break;

  case 171:

    {
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_LAST, 1,
                   (yyvsp[(2) - (3)].node)     /* cursor_name*/
                                       );
            ;}
    break;

  case 172:

    {
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_RELATIVE, 3,
                   (yyvsp[(2) - (5)].node),     /* cursor_name*/
                   (yyvsp[(3) - (5)].node),                  
                   (yyvsp[(5) - (5)].node)                  
                                       );
            ;}
    break;

  case 173:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 174:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 175:

    {
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_ABSOLUTE, 2,
                   (yyvsp[(2) - (4)].node),     /* cursor_name*/
                   (yyvsp[(4) - (4)].node)
                                       );
            ;}
    break;

  case 176:

    {
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_FETCH_FROMTO, 3,
                   (yyvsp[(2) - (6)].node),     /* cursor_name*/
                   (yyvsp[(4) - (6)].node),
                   (yyvsp[(6) - (6)].node)
                                       );
            ;}
    break;

  case 177:

    {
             malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CURSOR_CLOSE, 1,
                   (yyvsp[(2) - (2)].node)     /* cursor_name*/
                                      );
             ;}
    break;

  case 178:

    {
      ParseNode *table_elements = NULL;
      ParseNode *table_options = NULL;
      merge_nodes(table_elements, result->malloc_pool_, T_TABLE_ELEMENT_LIST, (yyvsp[(6) - (8)].node));
      merge_nodes(table_options, result->malloc_pool_, T_TABLE_OPTION_LIST, (yyvsp[(8) - (8)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_TABLE, 4,
              (yyvsp[(3) - (8)].node),                   /* if not exists */
              (yyvsp[(4) - (8)].node),                   /* table name */
              table_elements,       /* columns or primary key */
              table_options         /* table option(s) */
              );
    ;}
    break;

  case 179:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_NOT_EXISTS); ;}
    break;

  case 180:

    { (yyval.node) = NULL; ;}
    break;

  case 181:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 182:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 183:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 184:

    {
      ParseNode* col_list= NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIMARY_KEY, 1, col_list);
    ;}
    break;

  case 185:

    {
      ParseNode *attributes = NULL;
      merge_nodes(attributes, result->malloc_pool_, T_COLUMN_ATTRIBUTES, (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DEFINITION, 3, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node), attributes);
    ;}
    break;

  case 186:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER ); ;}
    break;

  case 187:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); ;}
    break;

  case 188:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); ;}
    break;

  case 189:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); ;}
    break;

  case 190:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); ;}
    break;

  case 191:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      /* modify xsl ECNU_DECIMAL 2017_2
      yyerror(&@1, result, "DECIMAL type is not supported");
      YYABORT;
      */
      //modify e
    ;}
    break;

  case 192:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "NUMERIC type is not supported");
      YYABORT;
    ;}
    break;

  case 193:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_BOOLEAN ); ;}
    break;

  case 194:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_FLOAT, 1, (yyvsp[(2) - (2)].node)); ;}
    break;

  case 195:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE); ;}
    break;

  case 196:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE);
    ;}
    break;

  case 197:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 198:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP); ;}
    break;

  case 199:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 200:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 201:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 202:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 203:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CREATETIME); ;}
    break;

  case 204:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_MODIFYTIME); ;}
    break;

  case 205:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DATE);
      yyerror(&(yylsp[(1) - (1)]), result, "DATE type is not supported");
      YYABORT;
    ;}
    break;

  case 206:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME, 1, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "TIME type is not supported");
      YYABORT;
    ;}
    break;

  case 207:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node)); ;}
    break;

  case 208:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 209:

    { (yyval.node) = NULL; ;}
    break;

  case 210:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 211:

    { (yyval.node) = NULL; ;}
    break;

  case 212:

    { (yyval.node) = NULL; ;}
    break;

  case 213:

    { (yyval.node) = NULL; ;}
    break;

  case 214:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 215:

    { (yyval.node) = NULL; ;}
    break;

  case 216:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 217:

    { (yyval.node) = NULL; ;}
    break;

  case 218:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); ;}
    break;

  case 219:

    { (yyval.node) = NULL; ;}
    break;

  case 220:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    ;}
    break;

  case 221:

    {
      (void)((yyvsp[(1) - (1)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    ;}
    break;

  case 222:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(2) - (2)].node)); ;}
    break;

  case 223:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_AUTO_INCREMENT); ;}
    break;

  case 224:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_PRIMARY_KEY); ;}
    break;

  case 225:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 226:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 227:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 228:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_INFO, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 229:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPIRE_INFO, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 230:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_MAX_SIZE, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 231:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_BLOCK_SIZE, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 232:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_ID, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 233:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REPLICA_NUM, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 234:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COMPRESS_METHOD, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 235:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_USE_BLOOM_FILTER, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 236:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSISTENT_MODE);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 237:

    {
      (void)((yyvsp[(2) - (3)].node)); /*  make bison mute*/
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COMMENT, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 238:

    { (yyval.node) = NULL; ;}
    break;

  case 239:

    { (yyval.node) = NULL; ;}
    break;

  case 240:

    {
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_GATHER_STATISTICS,2,
                              (yyvsp[(3) - (4)].node), /*table name*/
                              (yyvsp[(4) - (4)].node)  /*column name*/
                            );
  ;}
    break;

  case 241:

    {
    merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
  ;}
    break;

  case 242:

    {
      ParseNode *tables = NULL;
      merge_nodes(tables, result->malloc_pool_, T_TABLE_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DROP_TABLE, 2, (yyvsp[(3) - (4)].node), tables);
    ;}
    break;

  case 243:

    { (yyval.node) = NULL; ;}
    break;

  case 244:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_EXISTS); ;}
    break;

  case 245:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 246:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 247:

    {
          ParseNode *tables = NULL;
          merge_nodes(tables, result->malloc_pool_, T_TABLE_LIST, (yyvsp[(4) - (5)].node));
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TRUNCATE_TABLE, 3, (yyvsp[(3) - (5)].node), tables, (yyvsp[(5) - (5)].node));
        ;}
    break;

  case 248:

    {
      ParseNode *indexs = NULL;
      merge_nodes(indexs, result->malloc_pool_, T_INDEX_LIST, (yyvsp[(4) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DROP_INDEX, 3, (yyvsp[(3) - (6)].node), indexs, (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 249:

    { (yyval.node) = NULL; ;}
    break;

  case 250:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 251:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 252:

    {
	  (yyval.node) = (yyvsp[(1) - (1)].node);
	;}
    break;

  case 253:

    {
      ParseNode* val_list = NULL;
      merge_nodes(val_list, result->malloc_pool_, T_VALUE_LIST, (yyvsp[(6) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              (yyvsp[(4) - (7)].node),           /* column list */
                              val_list,     /* value list */
                              NULL,         /* value from sub-query */
                              (yyvsp[(1) - (7)].node),           /* is replacement */
                              (yyvsp[(7) - (7)].node)            /* when expression */
                              );
    ;}
    break;

  case 254:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (4)].node),           /* target relation */
                              NULL,         /* column list */
                              NULL,         /* value list */
                              (yyvsp[(4) - (4)].node),           /* value from sub-query */
                              (yyvsp[(1) - (4)].node),           /* is replacement */
                              NULL          /* when expression */
                              );
    ;}
    break;

  case 255:

    {
      /* if opt_when is really needed, use select_with_parens instead */
      ParseNode* col_list = NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              col_list,     /* column list */
                              NULL,         /* value list */
                              (yyvsp[(7) - (7)].node),           /* value from sub-query */
                              (yyvsp[(1) - (7)].node),           /* is replacement */
                              NULL          /* when expression */
                              );
    ;}
    break;

  case 256:

    { (yyval.node) = NULL; ;}
    break;

  case 257:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 258:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 259:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 260:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 261:

    { (yyval.node) = NULL; ;}
    break;

  case 262:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 263:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 264:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 265:

    {
    merge_nodes((yyvsp[(4) - (5)].node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(4) - (5)].node));
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (5)].node), (yyvsp[(4) - (5)].node));
  ;}
    break;

  case 266:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 267:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 268:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
      (yyval.node)->children_[14] = (yyvsp[(2) - (2)].node);
      if ((yyval.node)->children_[12] == NULL && (yyvsp[(2) - (2)].node) != NULL)
      {
        malloc_terminal_node((yyval.node)->children_[12], result->malloc_pool_, T_BOOL);
        (yyval.node)->children_[12]->value_ = 1;
      }
    ;}
    break;

  case 269:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 270:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 271:

    { (yyval.node) = (yyvsp[(2) - (3)].node); ;}
    break;

  case 272:

    {
      (yyval.node)= (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 273:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
      (yyval.node)->children_[12] = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 274:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (3)].node);
      if (select->children_[10])
        destroy_tree(select->children_[10]);
      select->children_[10] = (yyvsp[(2) - (3)].node);
      select->children_[12] = (yyvsp[(3) - (3)].node);
      (yyval.node) = select;
    ;}
    break;

  case 275:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (4)].node);
      if ((yyvsp[(2) - (4)].node))
      {
        if (select->children_[10])
          destroy_tree(select->children_[10]);
        select->children_[10] = (yyvsp[(2) - (4)].node);
      }

      /* set limit value */
      if (select->children_[11])
        destroy_tree(select->children_[11]);
      select->children_[11] = (yyvsp[(3) - (4)].node);
      select->children_[12] = (yyvsp[(4) - (4)].node);
      (yyval.node) = select;
    ;}
    break;

  case 276:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              (yyvsp[(3) - (5)].node),             /* 1. distinct */
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
                              (yyvsp[(5) - (5)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (5)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    ;}
    break;

  case 277:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (8)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              (yyvsp[(3) - (8)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              (yyvsp[(7) - (8)].node),             /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              (yyvsp[(8) - (8)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (8)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    ;}
    break;

  case 278:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 279:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 280:

    {
						  ParseNode* project_list = NULL;
						  ParseNode* from_list = NULL;
						  ParseNode* select = NULL;
						  ParseNode* args = NULL;
              merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(3) - (9)].node));
              merge_nodes(from_list, result->malloc_pool_, T_FROM_LIST, (yyvsp[(7) - (9)].node));
						  malloc_non_terminal_node(select, result->malloc_pool_, T_SELECT, 16,
                              NULL,           /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              (yyvsp[(8) - (9)].node),             /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,             /* 12. limit */
                              (yyvsp[(9) - (9)].node),           /* 13. for update */
                              (yyvsp[(2) - (9)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              ,NULL
                              );
              merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(5) - (9)].node));
						  malloc_non_terminal_node((yyval.node),result->malloc_pool_, T_SELECT_INTO, 2, args, select);
						;}
    break;

  case 281:

    {
      ParseNode* project_list = NULL;
      ParseNode* from_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (9)].node));
      merge_nodes(from_list, result->malloc_pool_, T_FROM_LIST, (yyvsp[(6) - (9)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              (yyvsp[(3) - (9)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              (yyvsp[(7) - (9)].node),             /* 4. where */
                              (yyvsp[(8) - (9)].node),             /* 5. group by */
                              (yyvsp[(9) - (9)].node),             /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (9)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    ;}
    break;

  case 282:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_UNION);
	    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    ;}
    break;

  case 283:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_INTERSECT);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    ;}
    break;

  case 284:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_EXCEPT);
	    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    ;}
    break;

  case 285:

    {(yyval.node) = NULL;;}
    break;

  case 286:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 287:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 2, (yyvsp[(3) - (3)].node), (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 288:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 289:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    ;}
    break;

  case 290:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (2)].node), NULL);
    ;}
    break;

  case 291:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, NULL, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 292:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    ;}
    break;

  case 293:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 294:

    {
      if ((yyvsp[(2) - (3)].node))
      {
        merge_nodes((yyval.node), result->malloc_pool_, T_HINT_OPTION_LIST, (yyvsp[(2) - (3)].node));
      }
      else
      {
        (yyval.node) = NULL;
      }
    ;}
    break;

  case 295:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 296:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 297:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 298:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 299:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 300:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_STATIC);
    ;}
    break;

  case 301:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_HOTSPOT);
    ;}
    break;

  case 302:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SEMI_JOIN, 6, (yyvsp[(3) - (14)].node), (yyvsp[(5) - (14)].node), (yyvsp[(7) - (14)].node), (yyvsp[(9) - (14)].node), (yyvsp[(11) - (14)].node), (yyvsp[(13) - (14)].node));
    ;}
    break;

  case 303:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_CONSISTENCY);
      (yyval.node)->value_ = (yyvsp[(3) - (4)].ival);
    ;}
    break;

  case 304:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_USE_INDEX, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
    ;}
    break;

  case 305:

    {
	  (void)((yyvsp[(1) - (4)].node));
	  (void)((yyvsp[(3) - (4)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_UNKOWN_HINT);
    ;}
    break;

  case 306:

    {
      (yyval.node) = (yyvsp[(2) - (3)].node);
    ;}
    break;

  case 307:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_JOIN_OP_TYPE_LIST, (yyvsp[(3) - (4)].node));
    ;}
    break;

  case 308:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_NO_GROUP);
    ;}
    break;

  case 309:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_LONG_TRANS);
    ;}
    break;

  case 310:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_NO_QUERY_OPT);
    ;}
    break;

  case 311:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
    ;}
    break;

  case 312:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 313:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 314:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 315:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BLOOMFILTER_JOIN);
    ;}
    break;

  case 316:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_MERGE_JOIN);
    ;}
    break;

  case 317:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEMI_JOIN);
    ;}
    break;

  case 318:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEMI_BTW_JOIN);
    ;}
    break;

  case 319:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SEMI_MULTI_JOIN);
    ;}
    break;

  case 320:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_HASH_JOIN_SINGLE);
    ;}
    break;

  case 321:

    {
    (yyval.ival) = 3;
  ;}
    break;

  case 322:

    {
    (yyval.ival) = 4;
  ;}
    break;

  case 323:

    {
    (yyval.ival) = 1;
  ;}
    break;

  case 324:

    {
    (yyval.ival) = 2;
  ;}
    break;

  case 325:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 326:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 327:

    { (yyval.node) = NULL; ;}
    break;

  case 328:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 329:

    { (yyval.node) = NULL; ;}
    break;

  case 330:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 331:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 332:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 333:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 334:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 335:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 336:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 337:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 338:

    { (yyval.node) = NULL; ;}
    break;

  case 339:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 340:

    { (yyval.node) = (yyvsp[(1) - (1)].node);;}
    break;

  case 341:

    { (yyval.node) = NULL; ;}
    break;

  case 342:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SORT_LIST, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 343:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 344:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 345:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SORT_KEY, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 346:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); ;}
    break;

  case 347:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); ;}
    break;

  case 348:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_DESC); ;}
    break;

  case 349:

    { (yyval.node) = 0; ;}
    break;

  case 350:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 351:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 352:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    ;}
    break;

  case 353:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    ;}
    break;

  case 354:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, (yyvsp[(1) - (1)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    ;}
    break;

  case 355:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(2) - (2)]).first_column, (yylsp[(2) - (2)]).last_column);
    ;}
    break;

  case 356:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (3)]).first_column, (yylsp[(1) - (3)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
    ;}
    break;

  case 357:

    {
      ParseNode* star_node = NULL;
      malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    ;}
    break;

  case 358:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 359:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 360:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 361:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 362:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 363:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 364:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 365:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 366:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 367:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 368:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(2) - (5)].node), (yyvsp[(5) - (5)].node));
    	yyerror(&(yylsp[(1) - (5)]), result, "qualied joined table can not be aliased!");
      YYABORT;
    ;}
    break;

  case 369:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 370:

    {
    	(yyval.node) = (yyvsp[(2) - (3)].node);
    ;}
    break;

  case 371:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, (yyvsp[(2) - (6)].node), (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 372:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_JOIN_INNER);
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, node, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 373:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_FULL);
    ;}
    break;

  case 374:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_LEFT);
    ;}
    break;

  case 375:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_RIGHT);
    ;}
    break;

  case 376:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_INNER);
    ;}
    break;

  case 377:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_SEMI);
    ;}
    break;

  case 378:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_SEMI_LEFT);
    ;}
    break;

  case 379:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_SEMI_RIGHT);
    ;}
    break;

  case 380:

    { (yyval.node) = NULL; ;}
    break;

  case 381:

    { (yyval.node) = NULL; ;}
    break;

  case 382:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPLAIN, 1, (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = ((yyvsp[(2) - (3)].node) ? 1 : 0); /* positive: verbose */
    ;}
    break;

  case 383:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 384:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 385:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 386:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 387:

    { (yyval.node) = (ParseNode*)1; ;}
    break;

  case 388:

    { (yyval.node) = NULL; ;}
    break;

  case 389:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLES, 1, (yyvsp[(3) - (3)].node)); ;}
    break;

  case 390:

    {  malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_INDEX, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); ;}
    break;

  case 391:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); ;}
    break;

  case 392:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); ;}
    break;

  case 393:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLE_STATUS, 1, (yyvsp[(4) - (4)].node)); ;}
    break;

  case 394:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SERVER_STATUS, 1, (yyvsp[(4) - (4)].node)); ;}
    break;

  case 395:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_VARIABLES, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(2) - (4)].ival);
    ;}
    break;

  case 396:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SCHEMA); ;}
    break;

  case 397:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, (yyvsp[(4) - (4)].node)); ;}
    break;

  case 398:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 399:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); ;}
    break;

  case 400:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 401:

    {
      if ((yyvsp[(2) - (3)].node)->type_ != T_FUN_COUNT)
      {
        yyerror(&(yylsp[(1) - (3)]), result, "Only COUNT(*) function is supported in SHOW WARNINGS statement!");
        YYABORT;
      }
      else
      {
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS);
        (yyval.node)->value_ = 1;
      }
    ;}
    break;

  case 402:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_GRANTS, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 403:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_PARAMETERS, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 404:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_PROCESSLIST);
      (yyval.node)->value_ = (yyvsp[(2) - (3)].ival);
    ;}
    break;

  case 405:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LOCK_TABLE, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 406:

    {
      if ((yyvsp[(2) - (4)].node)->value_ < 0 || (yyvsp[(4) - (4)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "OFFSET/COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 407:

    {
      if ((yyvsp[(2) - (2)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (2)]), result, "COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, NULL, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 408:

    { (yyval.node) = NULL; ;}
    break;

  case 409:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 410:

    { (yyval.node) = NULL; ;}
    break;

  case 411:

    { (yyval.node) = NULL; ;}
    break;

  case 412:

    { (yyval.ival) = 1; ;}
    break;

  case 413:

    { (yyval.ival) = 0; ;}
    break;

  case 414:

    { (yyval.ival) = 0; ;}
    break;

  case 415:

    { (yyval.node) = NULL; ;}
    break;

  case 416:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(2) - (2)].node)); ;}
    break;

  case 417:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 1, (yyvsp[(2) - (2)].node)); ;}
    break;

  case 418:

    { (yyval.node) = NULL; ;}
    break;

  case 419:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(1) - (1)].node)); ;}
    break;

  case 420:

    { (yyval.ival) = 0; ;}
    break;

  case 421:

    { (yyval.ival) = 1; ;}
    break;

  case 422:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_CREATE_USER, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 423:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 424:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 425:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_USER_SPEC, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 426:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 427:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 428:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_DROP_USER, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 429:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 430:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 431:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 432:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 433:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 434:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 435:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_RENAME_USER, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 436:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RENAME_INFO, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 437:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 438:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 439:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LOCK_USER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 440:
=======

      default:
	break;
    }
}


/* Prevent warnings from -Wmissing-prototypes.  */
#ifdef YYPARSE_PARAM
#if defined __STDC__ || defined __cplusplus
int yyparse (void *YYPARSE_PARAM);
#else
int yyparse ();
#endif
#else /* ! YYPARSE_PARAM */
#if defined __STDC__ || defined __cplusplus
int yyparse (ParseResult* result);
#else
int yyparse ();
#endif
#endif /* ! YYPARSE_PARAM */


/*----------.
| yyparse.  |
`----------*/

#ifdef YYPARSE_PARAM
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (void *YYPARSE_PARAM)
#else
int
yyparse (YYPARSE_PARAM)
    void *YYPARSE_PARAM;
#endif
#else /* ! YYPARSE_PARAM */
#if (defined __STDC__ || defined __C99__FUNC__ \
     || defined __cplusplus || defined _MSC_VER)
int
yyparse (ParseResult* result)
#else
int
yyparse (result)
    ParseResult* result;
#endif
#endif
{
/* The lookahead symbol.  */
int yychar;

/* The semantic value of the lookahead symbol.  */
YYSTYPE yylval;

/* Location data for the lookahead symbol.  */
YYLTYPE yylloc;

    /* Number of syntax errors so far.  */
    int yynerrs;

    int yystate;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus;

    /* The stacks and their tools:
       `yyss': related to states.
       `yyvs': related to semantic values.
       `yyls': related to locations.

       Refer to the stacks thru separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* The state stack.  */
    yytype_int16 yyssa[YYINITDEPTH];
    yytype_int16 *yyss;
    yytype_int16 *yyssp;

    /* The semantic value stack.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs;
    YYSTYPE *yyvsp;

    /* The location stack.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls;
    YYLTYPE *yylsp;

    /* The locations where the error started and ended.  */
    YYLTYPE yyerror_range[3];

    YYSIZE_T yystacksize;

  int yyn;
  int yyresult;
  /* Lookahead token as an internal (translated) token number.  */
  int yytoken;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

#if YYERROR_VERBOSE
  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYSIZE_T yymsg_alloc = sizeof yymsgbuf;
#endif

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  yytoken = 0;
  yyss = yyssa;
  yyvs = yyvsa;
  yyls = yylsa;
  yystacksize = YYINITDEPTH;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yystate = 0;
  yyerrstatus = 0;
  yynerrs = 0;
  yychar = YYEMPTY; /* Cause a token to be read.  */

  /* Initialize stack pointers.
     Waste one element of value and location stack
     so that they stay on the same level as the state stack.
     The wasted elements are never initialized.  */
  yyssp = yyss;
  yyvsp = yyvs;
  yylsp = yyls;

#if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  /* Initialize the default location before parsing starts.  */
  yylloc.first_line   = yylloc.last_line   = 1;
  yylloc.first_column = yylloc.last_column = 1;
#endif

  goto yysetstate;

/*------------------------------------------------------------.
| yynewstate -- Push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
 yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;

 yysetstate:
  *yyssp = yystate;

  if (yyss + yystacksize - 1 <= yyssp)
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYSIZE_T yysize = yyssp - yyss + 1;

#ifdef yyoverflow
      {
	/* Give user a chance to reallocate the stack.  Use copies of
	   these so that the &'s don't force the real ones into
	   memory.  */
	YYSTYPE *yyvs1 = yyvs;
	yytype_int16 *yyss1 = yyss;
	YYLTYPE *yyls1 = yyls;

	/* Each stack pointer address is followed by the size of the
	   data in use in that stack, in bytes.  This used to be a
	   conditional around just the two extra args, but that might
	   be undefined if yyoverflow is a macro.  */
	yyoverflow (YY_("memory exhausted"),
		    &yyss1, yysize * sizeof (*yyssp),
		    &yyvs1, yysize * sizeof (*yyvsp),
		    &yyls1, yysize * sizeof (*yylsp),
		    &yystacksize);

	yyls = yyls1;
	yyss = yyss1;
	yyvs = yyvs1;
      }
#else /* no yyoverflow */
# ifndef YYSTACK_RELOCATE
      goto yyexhaustedlab;
# else
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
	goto yyexhaustedlab;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
	yystacksize = YYMAXDEPTH;

      {
	yytype_int16 *yyss1 = yyss;
	union yyalloc *yyptr =
	  (union yyalloc *) YYSTACK_ALLOC (YYSTACK_BYTES (yystacksize));
	if (! yyptr)
	  goto yyexhaustedlab;
	YYSTACK_RELOCATE (yyss_alloc, yyss);
	YYSTACK_RELOCATE (yyvs_alloc, yyvs);
	YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
	if (yyss1 != yyssa)
	  YYSTACK_FREE (yyss1);
      }
# endif
#endif /* no yyoverflow */

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YYDPRINTF ((stderr, "Stack size increased to %lu\n",
		  (unsigned long int) yystacksize));

      if (yyss + yystacksize - 1 <= yyssp)
	YYABORT;
    }

  YYDPRINTF ((stderr, "Entering state %d\n", yystate));

  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;

/*-----------.
| yybackup.  |
`-----------*/
yybackup:

  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either YYEMPTY or YYEOF or a valid lookahead symbol.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token: "));
      yychar = YYLEX;
    }

  if (yychar <= YYEOF)
    {
      yychar = yytoken = YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);

  /* Discard the shifted token.  */
  yychar = YYEMPTY;

  yystate = yyn;
  *++yyvsp = yylval;
  *++yylsp = yylloc;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- Do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     `$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location.  */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
        case 2:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_STMT_LIST, (yyvsp[(1) - (2)].node));
      result->result_tree_ = (yyval.node);
      YYACCEPT;
    }
    break;

  case 3:

    {
      if ((yyvsp[(3) - (3)].node) != NULL)
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      else
        (yyval.node) = (yyvsp[(1) - (3)].node);
    }
    break;

  case 4:

    {
      (yyval.node) = ((yyvsp[(1) - (1)].node) != NULL) ? (yyvsp[(1) - (1)].node) : NULL;
    }
    break;

  case 5:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 6:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 7:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 8:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 9:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 10:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 11:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 12:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 13:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 14:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 15:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 16:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 17:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 18:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 19:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 20:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 21:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 22:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 23:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 24:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 25:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 26:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 27:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 28:

    {(yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 29:

    {(yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 30:

    { (yyval.node) = NULL; }
    break;

  case 31:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 32:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 33:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 34:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
    }
    break;

  case 35:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NAME_FIELD, 2, (yyvsp[(1) - (3)].node), node);
    }
    break;

  case 36:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 37:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 38:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 39:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 40:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 41:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 42:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 43:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 44:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 45:

    { (yyvsp[(3) - (3)].node)->type_ = T_SYSTEM_VARIABLE; (yyval.node) = (yyvsp[(3) - (3)].node); }
    break;

  case 46:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 47:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 48:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 49:

    {
      ParseNode *node = NULL;
      malloc_non_terminal_node(node, result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node));
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, node);
    }
    break;

  case 50:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
      /*
      yyerror(&@1, result, "CASE expression is not supported yet!");
      YYABORT;
      */
    }
    break;

  case 51:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 52:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 53:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 54:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EXISTS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 55:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 56:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 57:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 58:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 59:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 60:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 61:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 62:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 63:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 64:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 65:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 66:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POS, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 67:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NEG, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 68:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_ADD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 69:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MINUS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 70:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MUL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 71:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_DIV, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 72:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_REM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 73:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_POW, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 74:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_MOD, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 75:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 76:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 77:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_EQ, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 78:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 79:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_GT, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 80:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 81:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 82:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_LIKE, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node)); }
    break;

  case 83:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_AND, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 84:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_OR, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 85:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 86:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 87:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 88:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 89:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 90:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 91:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IS_NOT, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 92:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_BTW, 3, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 93:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_BTW, 3, (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 94:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_IN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 95:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_NOT_IN, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 96:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_CNN, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 97:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 98:

    { merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(2) - (3)].node)); }
    break;

  case 99:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_WHEN_LIST, (yyvsp[(3) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CASE, 3, (yyvsp[(2) - (5)].node), (yyval.node), (yyvsp[(4) - (5)].node));
    }
    break;

  case 100:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 101:

    { (yyval.node) = NULL; }
    break;

  case 102:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 103:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); }
    break;

  case 104:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHEN, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 105:

    { (yyval.node) = (yyvsp[(2) - (2)].node); }
    break;

  case 106:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL); }
    break;

  case 107:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") != 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "Only COUNT function can be with '*' parameter!");
        YYABORT;
      }
      else
      {
        ParseNode* node = NULL;
        malloc_terminal_node(node, result->malloc_pool_, T_STAR);
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 1, node);
      }
    }
    break;

  case 108:

    {
      if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "count") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "sum") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "max") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "min") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (5)].node)->str_value_, "avg") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node));
      }
      else
      {
        yyerror(&(yylsp[(1) - (5)]), result, "Wrong system function with 'DISTINCT/ALL'!");
        YYABORT;
      }
    }
    break;

  case 109:

    {
      if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "count") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "COUNT function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_COUNT, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "sum") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "SUM function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SUM, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "max") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MAX function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MAX, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "min") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "MIN function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_MIN, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "avg") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "AVG function only support 1 parameter!");
          YYABORT;
        }
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_AVG, 2, NULL, (yyvsp[(3) - (4)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (4)].node)->str_value_, "trim") == 0)
      {
        if ((yyvsp[(3) - (4)].node)->type_ == T_LINK_NODE)
        {
          yyerror(&(yylsp[(1) - (4)]), result, "TRIM function syntax error! TRIM don't take %d params", (yyvsp[(3) - (4)].node)->num_child_);
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
          malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (4)].node));
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
        }
      }
      else  /* system function */
      {
        ParseNode *params = NULL;
        merge_nodes(params, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (4)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), params);
      }
    }
    break;

  case 110:

    {
      if (strcasecmp((yyvsp[(1) - (6)].node)->str_value_, "cast") == 0)
      {
        (yyvsp[(5) - (6)].node)->value_ = (yyvsp[(5) - (6)].node)->type_;
        (yyvsp[(5) - (6)].node)->type_ = T_INT;
        ParseNode *params = NULL;
        malloc_non_terminal_node(params, result->malloc_pool_, T_EXPR_LIST, 2, (yyvsp[(3) - (6)].node), (yyvsp[(5) - (6)].node));
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (6)].node), params);
      }
      else
      {
        yyerror(&(yylsp[(1) - (6)]), result, "AS support cast function only!");
        YYABORT;
      }
    }
    break;

  case 111:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 2, (yyvsp[(1) - (4)].node), (yyvsp[(3) - (4)].node));
    }
    break;

  case 112:

    {
      if (strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "now") == 0 ||
          strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "current_time") == 0 ||
          strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "current_timestamp") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME, 1, (yyvsp[(1) - (3)].node));
      }
      else if (strcasecmp((yyvsp[(1) - (3)].node)->str_value_, "strict_current_timestamp") == 0)
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CUR_TIME_UPS, 1, (yyvsp[(1) - (3)].node));
      }
      else
      {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_FUN_SYS, 1, (yyvsp[(1) - (3)].node));
      }
      //yyerror(&@1, result, "system/user-define function is not supported yet!");
      //YYABORT;
    }
    break;

  case 113:

    {
      (yyval.node) = (yyvsp[(1) - (4)].node);
      (yyval.node)->children_[0] = (yyvsp[(3) - (4)].node);
    }
    break;

  case 114:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ROW_COUNT, 1, NULL);
    }
    break;

  case 119:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    }
    break;

  case 120:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    }
    break;

  case 121:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DELETE, 3, (yyvsp[(3) - (5)].node), (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 122:

    {
      ParseNode* assign_list = NULL;
      merge_nodes(assign_list, result->malloc_pool_, T_ASSIGN_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_UPDATE, 5, (yyvsp[(3) - (7)].node), assign_list, (yyvsp[(6) - (7)].node), (yyvsp[(7) - (7)].node), (yyvsp[(2) - (7)].node));
    }
    break;

  case 123:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 124:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 125:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ASSIGN_ITEM, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 126:

    {
      ParseNode *table_elements = NULL;
      ParseNode *table_options = NULL;
      merge_nodes(table_elements, result->malloc_pool_, T_TABLE_ELEMENT_LIST, (yyvsp[(6) - (8)].node));
      merge_nodes(table_options, result->malloc_pool_, T_TABLE_OPTION_LIST, (yyvsp[(8) - (8)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_TABLE, 4,
              (yyvsp[(3) - (8)].node),                   /* if not exists */
              (yyvsp[(4) - (8)].node),                   /* table name */
              table_elements,       /* columns or primary key */
              table_options         /* table option(s) */
              );
    }
    break;

  case 127:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_NOT_EXISTS); }
    break;

  case 128:

    { (yyval.node) = NULL; }
    break;

  case 129:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 130:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 131:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 132:

    {
      ParseNode* col_list= NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIMARY_KEY, 1, col_list);
    }
    break;

  case 133:

    {
      ParseNode *attributes = NULL;
      merge_nodes(attributes, result->malloc_pool_, T_COLUMN_ATTRIBUTES, (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DEFINITION, 3, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node), attributes);
    }
    break;

  case 134:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER ); }
    break;

  case 135:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 136:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 137:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 138:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_INTEGER); }
    break;

  case 139:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "DECIMAL type is not supported");
      YYABORT;
    }
    break;

  case 140:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL);
      else
        merge_nodes((yyval.node), result->malloc_pool_, T_TYPE_DECIMAL, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "NUMERIC type is not supported");
      YYABORT;
    }
    break;

  case 141:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_BOOLEAN ); }
    break;

  case 142:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_FLOAT, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 143:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE); }
    break;

  case 144:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DOUBLE);
    }
    break;

  case 145:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 146:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIMESTAMP); }
    break;

  case 147:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 148:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CHARACTER, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 149:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 150:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_VARCHAR, 1, (yyvsp[(2) - (2)].node));
    }
    break;

  case 151:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_CREATETIME); }
    break;

  case 152:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_MODIFYTIME); }
    break;

  case 153:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_DATE);
      yyerror(&(yylsp[(1) - (1)]), result, "DATE type is not supported");
      YYABORT;
    }
    break;

  case 154:

    {
      if ((yyvsp[(2) - (2)].node) == NULL)
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME);
      else
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TYPE_TIME, 1, (yyvsp[(2) - (2)].node));
      yyerror(&(yylsp[(1) - (2)]), result, "TIME type is not supported");
      YYABORT;
    }
    break;

  case 155:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(2) - (5)].node), (yyvsp[(4) - (5)].node)); }
    break;

  case 156:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 157:

    { (yyval.node) = NULL; }
    break;

  case 158:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 159:

    { (yyval.node) = NULL; }
    break;

  case 160:

    { (yyval.node) = NULL; }
    break;

  case 161:

    { (yyval.node) = NULL; }
    break;

  case 162:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 163:

    { (yyval.node) = NULL; }
    break;

  case 164:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 165:

    { (yyval.node) = NULL; }
    break;

  case 166:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node)); }
    break;

  case 167:

    { (yyval.node) = NULL; }
    break;

  case 168:

    {
      (void)((yyvsp[(2) - (2)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    }
    break;

  case 169:

    {
      (void)((yyvsp[(1) - (1)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    }
    break;

  case 170:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 171:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_AUTO_INCREMENT); }
    break;

  case 172:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_PRIMARY_KEY); }
    break;

  case 173:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 174:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 175:

    {
      (yyval.node) = NULL;
    }
    break;

  case 176:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_INFO, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 177:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPIRE_INFO, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 178:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_MAX_SIZE, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 179:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_BLOCK_SIZE, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 180:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLET_ID, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 181:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REPLICA_NUM, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 182:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COMPRESS_METHOD, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 183:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_USE_BLOOM_FILTER, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 184:

    {
      (void)((yyvsp[(2) - (3)].node)) ; /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSISTENT_MODE);
      (yyval.node)->value_ = 1;
    }
    break;

  case 185:

    {
      (void)((yyvsp[(2) - (3)].node)); /*  make bison mute*/
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COMMENT, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 186:

    { (yyval.node) = NULL; }
    break;

  case 187:

    { (yyval.node) = NULL; }
    break;

  case 188:

    {
      ParseNode *tables = NULL;
      merge_nodes(tables, result->malloc_pool_, T_TABLE_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DROP_TABLE, 2, (yyvsp[(3) - (4)].node), tables);
    }
    break;

  case 189:

    { (yyval.node) = NULL; }
    break;

  case 190:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_IF_EXISTS); }
    break;

  case 191:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 192:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 193:

    {
      ParseNode* val_list = NULL;
      merge_nodes(val_list, result->malloc_pool_, T_VALUE_LIST, (yyvsp[(6) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              (yyvsp[(4) - (7)].node),           /* column list */
                              val_list,     /* value list */
                              NULL,         /* value from sub-query */
                              (yyvsp[(1) - (7)].node),           /* is replacement */
                              (yyvsp[(7) - (7)].node)            /* when expression */
                              );
    }
    break;

  case 194:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (4)].node),           /* target relation */
                              NULL,         /* column list */
                              NULL,         /* value list */
                              (yyvsp[(4) - (4)].node),           /* value from sub-query */
                              (yyvsp[(1) - (4)].node),           /* is replacement */
                              NULL          /* when expression */
                              );
    }
    break;

  case 195:

    {
      /* if opt_when is really needed, use select_with_parens instead */
      ParseNode* col_list = NULL;
      merge_nodes(col_list, result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(5) - (7)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INSERT, 6,
                              (yyvsp[(3) - (7)].node),           /* target relation */
                              col_list,     /* column list */
                              NULL,         /* value list */
                              (yyvsp[(7) - (7)].node),           /* value from sub-query */
                              (yyvsp[(1) - (7)].node),           /* is replacement */
                              NULL          /* when expression */
                              );
    }
    break;

  case 196:

    { (yyval.node) = NULL; }
    break;

  case 197:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 198:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 199:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 200:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_COLUMN_LIST, (yyvsp[(2) - (3)].node));
    }
    break;

  case 201:

    { (yyval.node) = NULL; }
    break;

  case 202:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 203:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 204:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(2) - (3)].node));
    }
    break;

  case 205:

    {
    merge_nodes((yyvsp[(4) - (5)].node), result->malloc_pool_, T_VALUE_VECTOR, (yyvsp[(4) - (5)].node));
    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (5)].node), (yyvsp[(4) - (5)].node));
  }
    break;

  case 206:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 207:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 208:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
      (yyval.node)->children_[14] = (yyvsp[(2) - (2)].node);
      if ((yyval.node)->children_[12] == NULL && (yyvsp[(2) - (2)].node) != NULL)
      {
        malloc_terminal_node((yyval.node)->children_[12], result->malloc_pool_, T_BOOL);
        (yyval.node)->children_[12]->value_ = 1;
      }
    }
    break;

  case 209:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 210:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 211:

    { (yyval.node) = (yyvsp[(2) - (3)].node); }
    break;

  case 212:

    {
      (yyval.node)= (yyvsp[(1) - (1)].node);
    }
    break;

  case 213:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
      (yyval.node)->children_[12] = (yyvsp[(2) - (2)].node);
    }
    break;

  case 214:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (3)].node);
      if (select->children_[10])
        destroy_tree(select->children_[10]);
      select->children_[10] = (yyvsp[(2) - (3)].node);
      select->children_[12] = (yyvsp[(3) - (3)].node);
      (yyval.node) = select;
    }
    break;

  case 215:

    {
      /* use the new order by to replace old one */
      ParseNode* select = (ParseNode*)(yyvsp[(1) - (4)].node);
      if ((yyvsp[(2) - (4)].node))
      {
        if (select->children_[10])
          destroy_tree(select->children_[10]);
        select->children_[10] = (yyvsp[(2) - (4)].node);
      }

      /* set limit value */
      if (select->children_[11])
        destroy_tree(select->children_[11]);
      select->children_[11] = (yyvsp[(3) - (4)].node);
      select->children_[12] = (yyvsp[(4) - (4)].node);
      (yyval.node) = select;
    }
    break;

  case 216:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              (yyvsp[(3) - (5)].node),             /* 1. distinct */
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
                              (yyvsp[(5) - (5)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (5)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 217:

    {
      ParseNode* project_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (8)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              (yyvsp[(3) - (8)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              NULL,           /* 3. from clause */
                              (yyvsp[(7) - (8)].node),             /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              (yyvsp[(8) - (8)].node),             /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (8)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 218:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 219:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 220:

    {
      ParseNode* project_list = NULL;
      ParseNode* from_list = NULL;
      merge_nodes(project_list, result->malloc_pool_, T_PROJECT_LIST, (yyvsp[(4) - (9)].node));
      merge_nodes(from_list, result->malloc_pool_, T_FROM_LIST, (yyvsp[(6) - (9)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              (yyvsp[(3) - (9)].node),             /* 1. distinct */
                              project_list,   /* 2. select clause */
                              from_list,      /* 3. from clause */
                              (yyvsp[(7) - (9)].node),             /* 4. where */
                              (yyvsp[(8) - (9)].node),             /* 5. group by */
                              (yyvsp[(9) - (9)].node),             /* 6. having */
                              NULL,           /* 7. set operation */
                              NULL,           /* 8. all specified? */
                              NULL,           /* 9. former select stmt */
                              NULL,           /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              (yyvsp[(2) - (9)].node),             /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 221:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_UNION);
	    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 222:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_INTERSECT);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 223:

    {
      ParseNode* set_op = NULL;
      malloc_terminal_node(set_op, result->malloc_pool_, T_SET_EXCEPT);
	    malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SELECT, 15,
                              NULL,           /* 1. distinct */
                              NULL,           /* 2. select clause */
                              NULL,           /* 3. from clause */
                              NULL,           /* 4. where */
                              NULL,           /* 5. group by */
                              NULL,           /* 6. having */
                              set_op,   /* 7. set operation */
                              (yyvsp[(3) - (4)].node),             /* 8. all specified? */
                              (yyvsp[(1) - (4)].node),             /* 9. former select stmt */
                              (yyvsp[(4) - (4)].node),             /* 10. later select stmt */
                              NULL,           /* 11. order by */
                              NULL,           /* 12. limit */
                              NULL,           /* 13. for update */
                              NULL,           /* 14 hints */
                              NULL            /* 15 when clause */
                              );
    }
    break;

  case 224:

    {(yyval.node) = NULL;}
    break;

  case 225:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 226:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 2, (yyvsp[(3) - (3)].node), (yyvsp[(2) - (3)].node));
    }
    break;

  case 227:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 228:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    }
    break;

  case 229:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(2) - (2)].node), NULL);
    }
    break;

  case 230:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, NULL, (yyvsp[(2) - (2)].node));
    }
    break;

  case 231:

    {
      if ((yyvsp[(2) - (4)].node)->type_ == T_QUESTIONMARK && (yyvsp[(4) - (4)].node)->type_ == T_QUESTIONMARK)
      {
        (yyvsp[(4) - (4)].node)->value_++;
      }
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LIMIT_CLAUSE, 2, (yyvsp[(4) - (4)].node), (yyvsp[(2) - (4)].node));
    }
    break;

  case 232:

    {
      (yyval.node) = NULL;
    }
    break;

  case 233:

    {
      if ((yyvsp[(2) - (3)].node))
      {
        merge_nodes((yyval.node), result->malloc_pool_, T_HINT_OPTION_LIST, (yyvsp[(2) - (3)].node));
      }
      else
      {
        (yyval.node) = NULL;
      }
    }
    break;

  case 234:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 235:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 236:

    {
      (yyval.node) = NULL;
    }
    break;

  case 237:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 238:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 239:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_STATIC);
    }
    break;

  case 240:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_HOTSPOT);
    }
    break;

  case 241:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_READ_CONSISTENCY);
      (yyval.node)->value_ = (yyvsp[(3) - (4)].ival);
    }
    break;

  case 242:

    {
      (yyval.node) = (yyvsp[(2) - (3)].node);
    }
    break;

  case 243:

    {
      (yyval.node) = (yyvsp[(1) - (2)].node);
    }
    break;

  case 244:

    {
      (yyval.node) = NULL;
    }
    break;

  case 245:

    {
    (yyval.ival) = 3;
  }
    break;

  case 246:

    {
    (yyval.ival) = 4;
  }
    break;

  case 247:

    {
    (yyval.ival) = 1;
  }
    break;

  case 248:

    {
    (yyval.ival) = 2;
  }
    break;

  case 249:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 250:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 251:

    { (yyval.node) = NULL; }
    break;

  case 252:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 253:

    { (yyval.node) = NULL; }
    break;

  case 254:
>>>>>>> refs/remotes/origin/master

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
<<<<<<< HEAD
    ;}
    break;

  case 441:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 442:

    {
      (void)(yyval.node);
    ;}
    break;

  case 443:

    {
      (void)(yyval.node);
    ;}
    break;

  case 444:

    {
      (yyval.ival) = 1;
    ;}
    break;

  case 445:

    {
      (yyval.ival) = 0;
    ;}
    break;

  case 446:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 447:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = (yyvsp[(3) - (3)].ival);
    ;}
    break;

  case 448:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_COMMIT);
    ;}
    break;

  case 449:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ROLLBACK);
    ;}
    break;

  case 450:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_KILL, 3, (yyvsp[(2) - (4)].node), (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 451:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 452:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 453:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 454:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 455:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    ;}
    break;

  case 456:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *users_node = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (6)].node));
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_GRANT,
                                 3, privileges_node, (yyvsp[(4) - (6)].node), users_node);
    ;}
    break;

  case 457:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 458:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 459:

    {
      (void)(yyvsp[(2) - (2)].node);                 /* useless */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALL;
    ;}
    break;

  case 460:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALTER;
    ;}
    break;

  case 461:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE;
    ;}
    break;

  case 462:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE_USER;
    ;}
    break;

  case 463:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DELETE;
    ;}
    break;

  case 464:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DROP;
    ;}
    break;

  case 465:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DROP;
    ;}
    break;

  case 466:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_GRANT_OPTION;
    ;}
    break;

  case 467:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_INSERT;
    ;}
    break;

  case 468:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_UPDATE;
    ;}
    break;

  case 469:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_SELECT;
    ;}
    break;

  case 470:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_REPLACE;
    ;}
    break;

  case 471:

    {
      (void)(yyval.node);
    ;}
    break;

  case 472:

    {
      (void)(yyval.node);
    ;}
    break;

  case 473:

    {
      /* means global priv_level */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL);
    ;}
    break;

  case 474:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL, 1, (yyvsp[(1) - (1)].node));
    ;}
    break;

  case 475:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *priv_level = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (5)].node));
      if ((yyvsp[(3) - (5)].node) == NULL)
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
          yyerror(&(yylsp[(1) - (5)]), result, "support only ALL PRIVILEGES, GRANT OPTION");
          YYABORT;
        }
      }
      else
      {
        priv_level = (yyvsp[(3) - (5)].node);
      }
      ParseNode *users_node = NULL;
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(5) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REVOKE,
                                 3, privileges_node, priv_level, users_node);
    ;}
    break;

  case 476:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    ;}
    break;

  case 477:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 478:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PREPARE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 479:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 480:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 481:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 482:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 483:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 484:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VARIABLE_SET, (yyvsp[(2) - (2)].node));;
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 485:

    {
      (yyval.node) = (yyvsp[(3) - (3)].node);
    ;}
    break;

  case 486:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 487:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 488:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 489:

    {
      (void)((yyvsp[(2) - (3)].node));
      (yyvsp[(1) - (3)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 490:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 491:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 492:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 493:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 494:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 495:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 496:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_ARRAY_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 497:

    { (yyval.node) = NULL; ;}
    break;

  case 498:

    { (yyval.node) = NULL; ;}
    break;

  case 499:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 500:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 501:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_ARRAY_VAL, (yyvsp[(2) - (3)].node));
    ;}
    break;

  case 502:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 503:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 504:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXECUTE, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 505:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(2) - (2)].node));
    ;}
    break;

  case 506:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 507:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 508:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 509:

    {
      (void)((yyvsp[(1) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DEALLOCATE, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 510:

    { (yyval.node) = NULL; ;}
    break;

  case 511:

    { (yyval.node) = NULL; ;}
    break;

  case 512:

    {
      ParseNode *alter_actions = NULL;
      merge_nodes(alter_actions, result->malloc_pool_, T_ALTER_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (4)].node), alter_actions);
    ;}
    break;

  case 513:

    {
      yyerror(&(yylsp[(1) - (6)]), result, "Table rename is not supported now");
      YYABORT;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLE_RENAME, 1, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_ACTION_LIST, 1, (yyval.node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (6)].node), (yyval.node));
    ;}
    break;

  case 514:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 515:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 516:

    {
      (void)((yyvsp[(2) - (3)].node)); /* make bison mute */
      (yyval.node) = (yyvsp[(3) - (3)].node); /* T_COLUMN_DEFINITION */
    ;}
    break;

  case 517:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DROP, 1, (yyvsp[(3) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(4) - (4)].ival);
    ;}
    break;

  case 518:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_ALTER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    ;}
    break;

  case 519:

    {
      (void)((yyvsp[(2) - (5)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_RENAME, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    ;}
    break;

  case 520:

    { (yyval.node) = NULL; ;}
    break;

  case 521:

    { (yyval.node) = NULL; ;}
    break;

  case 522:

    { (yyval.ival) = 2; ;}
    break;

  case 523:

    { (yyval.ival) = 1; ;}
    break;

  case 524:

    { (yyval.ival) = 0; ;}
    break;

  case 525:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    ;}
    break;

  case 526:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    ;}
    break;

  case 527:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 528:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyval.node));
    ;}
    break;

  case 529:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SYTEM_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_SYSTEM, 1, (yyval.node));
    ;}
    break;

  case 530:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 3, node, (yyvsp[(7) - (7)].node), (yyvsp[(3) - (7)].node));
    ;}
    break;

  case 531:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 3, node, (yyvsp[(7) - (7)].node), (yyvsp[(3) - (7)].node));
    ;}
    break;

  case 532:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 2, node, (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 533:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 2, node, (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 534:

    {
      (yyval.node) = NULL;
    ;}
    break;

  case 535:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_FORCE);
    ;}
    break;

  case 536:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    ;}
    break;

  case 537:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 538:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SYSTEM_ACTION, 5,
                               (yyvsp[(1) - (9)].node),    /* param_name */
                               (yyvsp[(3) - (9)].node),    /* param_value */
                               (yyvsp[(4) - (9)].node),    /* comment */
                               (yyvsp[(8) - (9)].node),    /* server type */
                               (yyvsp[(9) - (9)].node)     /* cluster or IP/port */
                               );
      (yyval.node)->value_ = (yyvsp[(5) - (9)].ival);                /* scope */
    ;}
    break;

  case 539:

    { (yyval.node) = (yyvsp[(2) - (2)].node); ;}
    break;

  case 540:

    { (yyval.node) = NULL; ;}
    break;

  case 541:

    { (yyval.ival) = 0; ;}
    break;

  case 542:

    { (yyval.ival) = 1; ;}
    break;

  case 543:

    { (yyval.ival) = 2; ;}
    break;

  case 544:

    { (yyval.ival) = 2; ;}
    break;

  case 545:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 1;
    ;}
    break;

  case 546:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 4;
    ;}
    break;

  case 547:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 2;
    ;}
    break;

  case 548:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 3;
    ;}
    break;

  case 549:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CLUSTER, 1, (yyvsp[(3) - (3)].node));
    ;}
    break;

  case 550:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SERVER_ADDRESS, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    ;}
    break;

  case 551:

    { (yyval.node) = NULL; ;}
    break;

  case 552:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 553:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    ;}
    break;

  case 554:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 555:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    ;}
    break;

  case 557:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 558:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
    ;}
    break;

  case 584:

    {
								malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_CREATE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
							;}
    break;

  case 585:

    {
					ParseNode *params = NULL;
        			merge_nodes(params, result->malloc_pool_, T_PARAM_LIST, (yyvsp[(5) - (6)].node));
					malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_DECL, 2, (yyvsp[(3) - (6)].node), params);
				;}
    break;

  case 586:

    {
					ParseNode *params = NULL;
					malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_DECL, 2, (yyvsp[(3) - (5)].node), params);
				;}
    break;

  case 587:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
					;}
    break;

  case 588:

    {
						(yyval.node) = (yyvsp[(1) - (1)].node);
					;}
    break;

  case 589:

    {
                malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PARAM_DEFINITION, 3, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node), NULL);
					;}
    break;

  case 590:

    {
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_IN_PARAM_DEFINITION, 3, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node), NULL);
					;}
    break;

  case 591:

    {
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OUT_PARAM_DEFINITION, 3, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node), NULL);
					;}
    break;

  case 592:

    {
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INOUT_PARAM_DEFINITION, 3, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node), NULL);
          ;}
    break;

  case 593:

    {
          ParseNode *array_flag = NULL;
          malloc_terminal_node(array_flag, result->malloc_pool_, T_BOOL);
          array_flag->value_ = 1;
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PARAM_DEFINITION, 3, (yyvsp[(1) - (3)].node), (yyvsp[(2) - (3)].node), array_flag);
        ;}
    break;

  case 594:

    {
          ParseNode *array_flag = NULL;
          malloc_terminal_node(array_flag, result->malloc_pool_, T_BOOL);
          array_flag->value_ = 1;
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_IN_PARAM_DEFINITION, 3, (yyvsp[(2) - (4)].node), (yyvsp[(3) - (4)].node), array_flag);
        ;}
    break;

  case 595:

    {
          ParseNode *array_flag = NULL;
          malloc_terminal_node(array_flag, result->malloc_pool_, T_BOOL);
          array_flag->value_ = 1;
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OUT_PARAM_DEFINITION, 3, (yyvsp[(2) - (4)].node), (yyvsp[(3) - (4)].node), array_flag);
        ;}
    break;

  case 596:

    {
          ParseNode *array_flag = NULL;
          malloc_terminal_node(array_flag, result->malloc_pool_, T_BOOL);
          array_flag->value_ = 1;
          malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_INOUT_PARAM_DEFINITION, 3, (yyvsp[(2) - (4)].node), (yyvsp[(3) - (4)].node), array_flag);
        ;}
    break;

  case 597:

    { 
					//malloc_non_terminal_node($$, result->malloc_pool_, T_PROCEDURE_BLOCK, 1, $2);
					(yyval.node)=(yyvsp[(2) - (3)].node);
				;}
    break;

  case 598:

    { 
					(yyval.node)=NULL;
				;}
    break;

  case 599:

    { 
        			merge_nodes((yyval.node), result->malloc_pool_, T_PROCEDURE_STMTS, (yyvsp[(1) - (1)].node));
				;}
    break;

  case 600:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
				;}
    break;

  case 601:

    {
					(yyval.node)=(yyvsp[(1) - (1)].node);
				;}
    break;

  case 602:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 603:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 604:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 605:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 606:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 607:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 608:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 609:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 610:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 611:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 612:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 613:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 614:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 615:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 616:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 617:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 618:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 619:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 620:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 621:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 622:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 623:

    { (yyval.node) =(yyvsp[(1) - (1)].node); ;}
    break;

  case 624:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 625:

    { 
					(yyval.node)=NULL;
				;}
    break;

  case 626:

    { 
        			merge_nodes((yyval.node), result->malloc_pool_, T_PROCEDURE_STMTS, (yyvsp[(1) - (1)].node));
				;}
    break;

  case 627:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
				;}
    break;

  case 628:

    {
					(yyval.node)=(yyvsp[(1) - (1)].node);
				;}
    break;

  case 629:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 630:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 631:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 632:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 633:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 634:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 635:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 636:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 637:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 638:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 639:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 640:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 641:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 642:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 643:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 644:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 645:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 646:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 647:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 648:

    { (yyval.node) = (yyvsp[(1) - (2)].node); ;}
    break;

  case 649:

    { (yyval.node) =(yyvsp[(1) - (1)].node); ;}
    break;

  case 650:

    { (yyval.node) = (yyvsp[(1) - (1)].node); ;}
    break;

  case 651:

    {
						ParseNode *args = NULL;
						merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(2) - (4)].node));
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_DECLARE, 4, args, (yyvsp[(3) - (4)].node), NULL, NULL);
					;}
    break;

  case 652:

    {
						ParseNode *args = NULL;
						merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(2) - (6)].node));
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_DECLARE, 4, args, (yyvsp[(3) - (6)].node), (yyvsp[(5) - (6)].node), NULL);
          ;}
    break;

  case 653:

    {
            ParseNode *args = NULL;
            ParseNode *arr = NULL;
            malloc_terminal_node(arr, result->malloc_pool_, T_BOOL);
            arr->value_ = 1;
            merge_nodes(args, result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(2) - (5)].node));
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_DECLARE, 4,
                                     args,  //arguments
                                     (yyvsp[(3) - (5)].node),    //data type
                                     NULL, 	//default value
                                     arr 		//is array
                                     );
          ;}
    break;

  case 654:

    {
						ParseNode *var_list = NULL;
						merge_nodes(var_list, result->malloc_pool_, T_VAR_VAL_LIST, (yyvsp[(2) - (3)].node));
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_ASSGIN, 1,var_list);
					;}
    break;

  case 655:

    {
						ParseNode *elsifs = NULL;
						merge_nodes(elsifs, result->malloc_pool_, T_PROCEDURE_ELSEIFS, (yyvsp[(5) - (9)].node));
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_IF, 4, (yyvsp[(2) - (9)].node), (yyvsp[(4) - (9)].node), elsifs ,(yyvsp[(6) - (9)].node));
					;}
    break;

  case 656:

    {
						ParseNode *elsifs = NULL;
						merge_nodes(elsifs, result->malloc_pool_, T_PROCEDURE_ELSEIFS, (yyvsp[(5) - (8)].node));
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_IF, 4, (yyvsp[(2) - (8)].node), (yyvsp[(4) - (8)].node), elsifs ,NULL);
					;}
    break;

  case 657:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_IF, 4, (yyvsp[(2) - (8)].node), (yyvsp[(4) - (8)].node), NULL ,(yyvsp[(5) - (8)].node));
					;}
    break;

  case 658:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_IF, 4, (yyvsp[(2) - (7)].node), (yyvsp[(4) - (7)].node), NULL ,NULL);
					;}
    break;

  case 659:

    {
						(yyval.node)=(yyvsp[(1) - (1)].node);
					;}
    break;

  case 660:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
					;}
    break;

  case 661:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_ELSEIF, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
					;}
    break;

  case 662:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_ELSE, 1, (yyvsp[(2) - (2)].node));
						//$$ = $2;
					;}
    break;

  case 663:

    {
						ParseNode *casewhenlist = NULL;
						merge_nodes(casewhenlist, result->malloc_pool_, T_PROCEDURE_CASE_WHEN_LIST, (yyvsp[(3) - (7)].node));
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_CASE, 3, (yyvsp[(2) - (7)].node), casewhenlist, (yyvsp[(4) - (7)].node));
					;}
    break;

  case 664:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
					;}
    break;

  case 665:

    {
						(yyval.node)=(yyvsp[(1) - (1)].node);
					;}
    break;

  case 666:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_CASE_WHEN, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
					;}
    break;

  case 667:

    {
						(yyval.node) = NULL;
					;}
    break;

  case 668:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_ELSE, 1, (yyvsp[(2) - (2)].node));
						//$$ = $2;
					;}
    break;

  case 669:

    {
            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_LOOP, 5,
                                     NULL,
                                     NULL,
                                     NULL,
                                     NULL,
                                     (yyvsp[(2) - (5)].node));
          ;}
    break;

  case 670:

    {
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_LOOP, 5,
                                       (yyvsp[(2) - (11)].node),
                                       NULL,
                                       (yyvsp[(4) - (11)].node),
                                       (yyvsp[(6) - (11)].node),
                                       (yyvsp[(8) - (11)].node));
            ;}
    break;

  case 671:

    {
              ParseNode *rev_flag = NULL;
              malloc_terminal_node(rev_flag, result->malloc_pool_, T_BOOL);
              rev_flag->value_ = 1;
              malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_LOOP, 5,
                                       (yyvsp[(2) - (12)].node), 				//loop counter
                                       rev_flag,  //reverse loop
                                       (yyvsp[(5) - (12)].node),        //lowest_number
                                       (yyvsp[(7) - (12)].node),        //highest number
                                       (yyvsp[(9) - (12)].node));       //loop body
            ;}
    break;

  case 672:

    {
						malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_WHILE, 2, (yyvsp[(2) - (7)].node), (yyvsp[(4) - (7)].node));
					;}
    break;

  case 673:

    {
						/* We do not bother building a node for NULL */
						(yyval.node) = NULL;
					;}
    break;

  case 674:

    {
                        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_EXIT, 1, NULL);
						(yyval.node)->value_=1;
					;}
    break;

  case 675:

    {
                        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_EXIT, 1, (yyvsp[(3) - (4)].node));//when_expr
                    ;}
    break;

  case 676:

    {
        					ParseNode *param_list = NULL;
            				merge_nodes(param_list, result->malloc_pool_, T_EXPR_LIST, (yyvsp[(4) - (6)].node));
        					
                            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_EXEC, 3, (yyvsp[(2) - (6)].node), param_list, (yyvsp[(6) - (6)].node));
						;}
    break;

  case 677:

    {
							ParseNode *params = NULL;
                            malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_EXEC, 3, (yyvsp[(2) - (5)].node), params, (yyvsp[(5) - (5)].node));
                        ;}
    break;

  case 678:

    {
							malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_DROP, 2, (yyvsp[(5) - (5)].node),(yyvsp[(5) - (5)].node));
						;}
    break;

  case 679:

    {
							malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_DROP, 1, (yyvsp[(3) - (3)].node));
						;}
    break;

  case 680:

    {
							malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROCEDURE_SHOW, 1, (yyvsp[(3) - (3)].node));
						;}
=======
    }
    break;

  case 255:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 256:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 257:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 258:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 259:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 0;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    }
    break;

  case 260:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 1;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    }
    break;

  case 261:

    {
      ParseNode *default_type = NULL;
      malloc_terminal_node(default_type, result->malloc_pool_, T_INT);
      default_type->value_ = 2;
      ParseNode *default_operand = NULL;
      malloc_terminal_node(default_operand, result->malloc_pool_, T_STRING);
      default_operand->str_value_ = " "; /* blank for default */
      default_operand->value_ = strlen(default_operand->str_value_);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPR_LIST, 3, default_type, default_operand, (yyvsp[(3) - (3)].node));
    }
    break;

  case 262:

    { (yyval.node) = NULL; }
    break;

  case 263:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_EXPR_LIST, (yyvsp[(3) - (3)].node));
    }
    break;

  case 264:

    { (yyval.node) = (yyvsp[(1) - (1)].node);}
    break;

  case 265:

    { (yyval.node) = NULL; }
    break;

  case 266:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SORT_LIST, (yyvsp[(3) - (3)].node));
    }
    break;

  case 267:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 268:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 269:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SORT_KEY, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 270:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); }
    break;

  case 271:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_ASC); }
    break;

  case 272:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SORT_DESC); }
    break;

  case 273:

    { (yyval.node) = 0; }
    break;

  case 274:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 275:

    {
      (yyval.node) = NULL;
    }
    break;

  case 276:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ALL);
    }
    break;

  case 277:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_DISTINCT);
    }
    break;

  case 278:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, (yyvsp[(1) - (1)].node));
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    }
    break;

  case 279:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (2)]).first_column, (yylsp[(1) - (2)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(2) - (2)]).first_column, (yylsp[(2) - (2)]).last_column);
    }
    break;

  case 280:

    {
      ParseNode* alias_node = NULL;
      malloc_non_terminal_node(alias_node, result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, alias_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (3)]).first_column, (yylsp[(1) - (3)]).last_column);
      dup_expr_string(alias_node->str_value_, result, (yylsp[(3) - (3)]).first_column, (yylsp[(3) - (3)]).last_column);
    }
    break;

  case 281:

    {
      ParseNode* star_node = NULL;
      malloc_terminal_node(star_node, result->malloc_pool_, T_STAR);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PROJECT_STRING, 1, star_node);
      dup_expr_string((yyval.node)->str_value_, result, (yylsp[(1) - (1)]).first_column, (yylsp[(1) - (1)]).last_column);
    }
    break;

  case 282:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 283:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 284:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 285:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 286:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 287:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 288:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 289:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 290:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(1) - (2)].node), (yyvsp[(2) - (2)].node));
    }
    break;

  case 291:

    {
    	(yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 292:

    {
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALIAS, 2, (yyvsp[(2) - (5)].node), (yyvsp[(5) - (5)].node));
    	yyerror(&(yylsp[(1) - (5)]), result, "qualied joined table can not be aliased!");
      YYABORT;
    }
    break;

  case 293:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 294:

    {
    	(yyval.node) = (yyvsp[(2) - (3)].node);
    }
    break;

  case 295:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, (yyvsp[(2) - (6)].node), (yyvsp[(1) - (6)].node), (yyvsp[(4) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 296:

    {
      ParseNode* node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_JOIN_INNER);
    	malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_JOINED_TABLE, 4, node, (yyvsp[(1) - (5)].node), (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 297:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_FULL);
    }
    break;

  case 298:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_LEFT);
    }
    break;

  case 299:

    {
      /* make bison mute */
      (void)((yyvsp[(2) - (2)].node));
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_RIGHT);
    }
    break;

  case 300:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_JOIN_INNER);
    }
    break;

  case 301:

    { (yyval.node) = NULL; }
    break;

  case 302:

    { (yyval.node) = NULL; }
    break;

  case 303:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXPLAIN, 1, (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = ((yyvsp[(2) - (3)].node) ? 1 : 0); /* positive: verbose */
    }
    break;

  case 304:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 305:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 306:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 307:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 308:

    { (yyval.node) = (ParseNode*)1; }
    break;

  case 309:

    { (yyval.node) = NULL; }
    break;

  case 310:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLES, 1, (yyvsp[(3) - (3)].node)); }
    break;

  case 311:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); }
    break;

  case 312:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(4) - (5)].node), (yyvsp[(5) - (5)].node)); }
    break;

  case 313:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_TABLE_STATUS, 1, (yyvsp[(4) - (4)].node)); }
    break;

  case 314:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SERVER_STATUS, 1, (yyvsp[(4) - (4)].node)); }
    break;

  case 315:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_VARIABLES, 1, (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(2) - (4)].ival);
    }
    break;

  case 316:

    { malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_SCHEMA); }
    break;

  case 317:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_CREATE_TABLE, 1, (yyvsp[(4) - (4)].node)); }
    break;

  case 318:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 319:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_COLUMNS, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node)); }
    break;

  case 320:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 321:

    {
      if ((yyvsp[(2) - (3)].node)->type_ != T_FUN_COUNT)
      {
        yyerror(&(yylsp[(1) - (3)]), result, "Only COUNT(*) function is supported in SHOW WARNINGS statement!");
        YYABORT;
      }
      else
      {
        malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_WARNINGS);
        (yyval.node)->value_ = 1;
      }
    }
    break;

  case 322:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_GRANTS, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 323:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_PARAMETERS, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 324:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_PROCESSLIST);
      (yyval.node)->value_ = (yyvsp[(2) - (3)].ival);
    }
    break;

  case 325:

    {
      if ((yyvsp[(2) - (4)].node)->value_ < 0 || (yyvsp[(4) - (4)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (4)]), result, "OFFSET/COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 326:

    {
      if ((yyvsp[(2) - (2)].node)->value_ < 0)
      {
        yyerror(&(yylsp[(1) - (2)]), result, "COUNT must not be less than 0!");
        YYABORT;
      }
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SHOW_LIMIT, 2, NULL, (yyvsp[(2) - (2)].node));
    }
    break;

  case 327:

    { (yyval.node) = NULL; }
    break;

  case 328:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 329:

    { (yyval.node) = NULL; }
    break;

  case 330:

    { (yyval.node) = NULL; }
    break;

  case 331:

    { (yyval.ival) = 1; }
    break;

  case 332:

    { (yyval.ival) = 0; }
    break;

  case 333:

    { (yyval.ival) = 0; }
    break;

  case 334:

    { (yyval.node) = NULL; }
    break;

  case 335:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 336:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_WHERE_CLAUSE, 1, (yyvsp[(2) - (2)].node)); }
    break;

  case 337:

    { (yyval.node) = NULL; }
    break;

  case 338:

    { malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_OP_LIKE, 1, (yyvsp[(1) - (1)].node)); }
    break;

  case 339:

    { (yyval.ival) = 0; }
    break;

  case 340:

    { (yyval.ival) = 1; }
    break;

  case 341:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_CREATE_USER, (yyvsp[(3) - (3)].node));
    }
    break;

  case 342:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 343:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 344:

    {
        malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CREATE_USER_SPEC, 2, (yyvsp[(1) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 345:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 346:

    {
        (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 347:

    {
        merge_nodes((yyval.node), result->malloc_pool_, T_DROP_USER, (yyvsp[(3) - (3)].node));
    }
    break;

  case 348:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 349:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 350:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 351:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SET_PASSWORD, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 352:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 353:

    {
      (yyval.node) = NULL;
    }
    break;

  case 354:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_RENAME_USER, (yyvsp[(3) - (3)].node));
    }
    break;

  case 355:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_RENAME_INFO, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 356:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 357:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 358:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LOCK_USER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 359:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 360:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 361:

    {
      (void)(yyval.node);
    }
    break;

  case 362:

    {
      (void)(yyval.node);
    }
    break;

  case 363:

    {
      (yyval.ival) = 1;
    }
    break;

  case 364:

    {
      (yyval.ival) = 0;
    }
    break;

  case 365:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = 0;
    }
    break;

  case 366:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BEGIN);
      (yyval.node)->value_ = (yyvsp[(3) - (3)].ival);
    }
    break;

  case 367:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_COMMIT);
    }
    break;

  case 368:

    {
      (void)(yyvsp[(2) - (2)].node);
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_ROLLBACK);
    }
    break;

  case 369:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_KILL, 3, (yyvsp[(2) - (4)].node), (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 370:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 371:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 372:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 373:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 1;
    }
    break;

  case 374:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_BOOL);
      (yyval.node)->value_ = 0;
    }
    break;

  case 375:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *users_node = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (6)].node));
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_GRANT,
                                 3, privileges_node, (yyvsp[(4) - (6)].node), users_node);
    }
    break;

  case 376:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 377:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 378:

    {
      (void)(yyvsp[(2) - (2)].node);                 /* useless */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALL;
    }
    break;

  case 379:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_ALTER;
    }
    break;

  case 380:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE;
    }
    break;

  case 381:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_CREATE_USER;
    }
    break;

  case 382:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DELETE;
    }
    break;

  case 383:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_DROP;
    }
    break;

  case 384:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_GRANT_OPTION;
    }
    break;

  case 385:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_INSERT;
    }
    break;

  case 386:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_UPDATE;
    }
    break;

  case 387:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_SELECT;
    }
    break;

  case 388:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_TYPE);
      (yyval.node)->value_ = OB_PRIV_REPLACE;
    }
    break;

  case 389:

    {
      (void)(yyval.node);
    }
    break;

  case 390:

    {
      (void)(yyval.node);
    }
    break;

  case 391:

    {
      /* means global priv_level */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL);
    }
    break;

  case 392:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PRIV_LEVEL, 1, (yyvsp[(1) - (1)].node));
    }
    break;

  case 393:

    {
      ParseNode *privileges_node = NULL;
      ParseNode *priv_level = NULL;
      merge_nodes(privileges_node, result->malloc_pool_, T_PRIVILEGES, (yyvsp[(2) - (5)].node));
      if ((yyvsp[(3) - (5)].node) == NULL)
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
          yyerror(&(yylsp[(1) - (5)]), result, "support only ALL PRIVILEGES, GRANT OPTION");
          YYABORT;
        }
      }
      else
      {
        priv_level = (yyvsp[(3) - (5)].node);
      }
      ParseNode *users_node = NULL;
      merge_nodes(users_node, result->malloc_pool_, T_USERS, (yyvsp[(5) - (5)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_REVOKE,
                                 3, privileges_node, priv_level, users_node);
    }
    break;

  case 394:

    {
      (yyval.node) = (yyvsp[(2) - (2)].node);
    }
    break;

  case 395:

    {
      (yyval.node) = NULL;
    }
    break;

  case 396:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_PREPARE, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 397:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 398:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 399:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 400:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 401:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 402:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_VARIABLE_SET, (yyvsp[(2) - (2)].node));;
      (yyval.node)->value_ = 2;
    }
    break;

  case 403:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 404:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 405:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 406:

    {
      (void)((yyvsp[(2) - (3)].node));
      (yyvsp[(1) - (3)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 407:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 1;
    }
    break;

  case 408:

    {
      (void)((yyvsp[(3) - (4)].node));
      (yyvsp[(2) - (4)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(2) - (4)].node), (yyvsp[(4) - (4)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 409:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 1;
    }
    break;

  case 410:

    {
      (void)((yyvsp[(4) - (5)].node));
      (yyvsp[(3) - (5)].node)->type_ = T_SYSTEM_VARIABLE;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 411:

    {
      (void)((yyvsp[(2) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_VAR_VAL, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
      (yyval.node)->value_ = 2;
    }
    break;

  case 412:

    { (yyval.node) = NULL; }
    break;

  case 413:

    { (yyval.node) = NULL; }
    break;

  case 414:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 415:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_EXECUTE, 2, (yyvsp[(2) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 416:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_ARGUMENT_LIST, (yyvsp[(2) - (2)].node));
    }
    break;

  case 417:

    {
      (yyval.node) = NULL;
    }
    break;

  case 418:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 419:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 420:

    {
      (void)((yyvsp[(1) - (3)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_DEALLOCATE, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 421:

    { (yyval.node) = NULL; }
    break;

  case 422:

    { (yyval.node) = NULL; }
    break;

  case 423:

    {
      ParseNode *alter_actions = NULL;
      merge_nodes(alter_actions, result->malloc_pool_, T_ALTER_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (4)].node), alter_actions);
    }
    break;

  case 424:

    {
      yyerror(&(yylsp[(1) - (6)]), result, "Table rename is not supported now");
      YYABORT;
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_TABLE_RENAME, 1, (yyvsp[(6) - (6)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_ACTION_LIST, 1, (yyval.node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_TABLE, 2, (yyvsp[(3) - (6)].node), (yyval.node));
    }
    break;

  case 425:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 426:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 427:

    {
      (void)((yyvsp[(2) - (3)].node)); /* make bison mute */
      (yyval.node) = (yyvsp[(3) - (3)].node); /* T_COLUMN_DEFINITION */
    }
    break;

  case 428:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_DROP, 1, (yyvsp[(3) - (4)].node));
      (yyval.node)->value_ = (yyvsp[(4) - (4)].ival);
    }
    break;

  case 429:

    {
      (void)((yyvsp[(2) - (4)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_ALTER, 2, (yyvsp[(3) - (4)].node), (yyvsp[(4) - (4)].node));
    }
    break;

  case 430:

    {
      (void)((yyvsp[(2) - (5)].node)); /* make bison mute */
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_COLUMN_RENAME, 2, (yyvsp[(3) - (5)].node), (yyvsp[(5) - (5)].node));
    }
    break;

  case 431:

    { (yyval.node) = NULL; }
    break;

  case 432:

    { (yyval.node) = NULL; }
    break;

  case 433:

    { (yyval.ival) = 2; }
    break;

  case 434:

    { (yyval.ival) = 1; }
    break;

  case 435:

    { (yyval.ival) = 0; }
    break;

  case 436:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NOT_NULL);
    }
    break;

  case 437:

    {
      (void)((yyvsp[(3) - (3)].node)); /* make bison mute */
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_NULL);
    }
    break;

  case 438:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 439:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_NULL);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CONSTR_DEFAULT, 1, (yyval.node));
    }
    break;

  case 440:

    {
      merge_nodes((yyval.node), result->malloc_pool_, T_SYTEM_ACTION_LIST, (yyvsp[(4) - (4)].node));
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_ALTER_SYSTEM, 1, (yyval.node));
    }
    break;

  case 441:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 3, node, (yyvsp[(7) - (7)].node), (yyvsp[(3) - (7)].node));
    }
    break;

  case 442:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 3, node, (yyvsp[(7) - (7)].node), (yyvsp[(3) - (7)].node));
    }
    break;

  case 443:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_MASTER);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 2, node, (yyvsp[(6) - (6)].node));
    }
    break;

  case 444:

    {
      ParseNode *node = NULL;
      malloc_terminal_node(node, result->malloc_pool_, T_SET_SLAVE);
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CHANGE_OBI, 2, node, (yyvsp[(6) - (6)].node));
    }
    break;

  case 445:

    {
      (yyval.node) = NULL;
    }
    break;

  case 446:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_FORCE);
    }
    break;

  case 447:

    {
      (yyval.node) = (yyvsp[(1) - (1)].node);
    }
    break;

  case 448:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_LINK_NODE, 2, (yyvsp[(1) - (3)].node), (yyvsp[(3) - (3)].node));
    }
    break;

  case 449:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SYSTEM_ACTION, 5,
                               (yyvsp[(1) - (9)].node),    /* param_name */
                               (yyvsp[(3) - (9)].node),    /* param_value */
                               (yyvsp[(4) - (9)].node),    /* comment */
                               (yyvsp[(8) - (9)].node),    /* server type */
                               (yyvsp[(9) - (9)].node)     /* cluster or IP/port */
                               );
      (yyval.node)->value_ = (yyvsp[(5) - (9)].ival);                /* scope */
    }
    break;

  case 450:

    { (yyval.node) = (yyvsp[(2) - (2)].node); }
    break;

  case 451:

    { (yyval.node) = NULL; }
    break;

  case 452:

    { (yyval.ival) = 0; }
    break;

  case 453:

    { (yyval.ival) = 1; }
    break;

  case 454:

    { (yyval.ival) = 2; }
    break;

  case 455:

    { (yyval.ival) = 2; }
    break;

  case 456:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 1;
    }
    break;

  case 457:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 4;
    }
    break;

  case 458:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 2;
    }
    break;

  case 459:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_INT);
      (yyval.node)->value_ = 3;
    }
    break;

  case 460:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_CLUSTER, 1, (yyvsp[(3) - (3)].node));
    }
    break;

  case 461:

    {
      malloc_non_terminal_node((yyval.node), result->malloc_pool_, T_SERVER_ADDRESS, 2, (yyvsp[(3) - (6)].node), (yyvsp[(6) - (6)].node));
    }
    break;

  case 462:

    { (yyval.node) = NULL; }
    break;

  case 463:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 464:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    }
    break;

  case 465:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 466:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
      else
      {
        (yyval.node)->value_ = strlen((yyval.node)->str_value_);
      }
    }
    break;

  case 468:

    { (yyval.node) = (yyvsp[(1) - (1)].node); }
    break;

  case 469:

    {
      malloc_terminal_node((yyval.node), result->malloc_pool_, T_IDENT);
      (yyval.node)->str_value_ = parse_strdup((yyvsp[(1) - (1)].non_reserved_keyword)->keyword_name, result->malloc_pool_);
      if ((yyval.node)->str_value_ == NULL)
      {
        yyerror(NULL, result, "No more space for string duplicate");
        YYABORT;
      }
    }
>>>>>>> refs/remotes/origin/master
    break;



      default: break;
    }
<<<<<<< HEAD
=======
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
>>>>>>> refs/remotes/origin/master
  YY_SYMBOL_PRINT ("-> $$ =", yyr1[yyn], &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now `shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */

  yyn = yyr1[yyn];

  yystate = yypgoto[yyn - YYNTOKENS] + *yyssp;
  if (0 <= yystate && yystate <= YYLAST && yycheck[yystate] == *yyssp)
    yystate = yytable[yystate];
  else
    yystate = yydefgoto[yyn - YYNTOKENS];

  goto yynewstate;


/*------------------------------------.
| yyerrlab -- here on detecting error |
`------------------------------------*/
yyerrlab:
<<<<<<< HEAD
=======
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYEMPTY : YYTRANSLATE (yychar);

>>>>>>> refs/remotes/origin/master
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
#if ! YYERROR_VERBOSE
      yyerror (&yylloc, result, YY_("syntax error"));
#else
<<<<<<< HEAD
      {
	YYSIZE_T yysize = yysyntax_error (0, yystate, yychar);
	if (yymsg_alloc < yysize && yymsg_alloc < YYSTACK_ALLOC_MAXIMUM)
	  {
	    YYSIZE_T yyalloc = 2 * yysize;
	    if (! (yysize <= yyalloc && yyalloc <= YYSTACK_ALLOC_MAXIMUM))
	      yyalloc = YYSTACK_ALLOC_MAXIMUM;
	    if (yymsg != yymsgbuf)
	      YYSTACK_FREE (yymsg);
	    yymsg = (char *) YYSTACK_ALLOC (yyalloc);
	    if (yymsg)
	      yymsg_alloc = yyalloc;
	    else
	      {
		yymsg = yymsgbuf;
		yymsg_alloc = sizeof yymsgbuf;
	      }
	  }

	if (0 < yysize && yysize <= yymsg_alloc)
	  {
	    (void) yysyntax_error (yymsg, yystate, yychar);
	    yyerror (&yylloc, result, yymsg);
	  }
	else
	  {
	    yyerror (&yylloc, result, YY_("syntax error"));
	    if (yysize != 0)
	      goto yyexhaustedlab;
	  }
      }
#endif
    }

  yyerror_range[0] = yylloc;
=======
# define YYSYNTAX_ERROR yysyntax_error (&yymsg_alloc, &yymsg, \
                                        yyssp, yytoken)
      {
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = YYSYNTAX_ERROR;
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == 1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = (char *) YYSTACK_ALLOC (yymsg_alloc);
            if (!yymsg)
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = 2;
              }
            else
              {
                yysyntax_error_status = YYSYNTAX_ERROR;
                yymsgp = yymsg;
              }
          }
        yyerror (&yylloc, result, yymsgp);
        if (yysyntax_error_status == 2)
          goto yyexhaustedlab;
      }
# undef YYSYNTAX_ERROR
#endif
    }

  yyerror_range[1] = yylloc;
>>>>>>> refs/remotes/origin/master

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
	 error, discard it.  */

      if (yychar <= YYEOF)
	{
	  /* Return failure if at end of input.  */
	  if (yychar == YYEOF)
	    YYABORT;
	}
      else
	{
	  yydestruct ("Error: discarding",
		      yytoken, &yylval, &yylloc, result);
	  yychar = YYEMPTY;
	}
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:

  /* Pacify compilers like GCC when the user code never invokes
     YYERROR and the label yyerrorlab therefore never appears in user
     code.  */
  if (/*CONSTCOND*/ 0)
     goto yyerrorlab;

<<<<<<< HEAD
  yyerror_range[0] = yylsp[1-yylen];
=======
  yyerror_range[1] = yylsp[1-yylen];
>>>>>>> refs/remotes/origin/master
  /* Do not reclaim the symbols of the rule which action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;	/* Each real token shifted decrements this.  */

  for (;;)
    {
      yyn = yypact[yystate];
<<<<<<< HEAD
      if (yyn != YYPACT_NINF)
=======
      if (!yypact_value_is_default (yyn))
>>>>>>> refs/remotes/origin/master
	{
	  yyn += YYTERROR;
	  if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYTERROR)
	    {
	      yyn = yytable[yyn];
	      if (0 < yyn)
		break;
	    }
	}

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
	YYABORT;

<<<<<<< HEAD
      yyerror_range[0] = *yylsp;
=======
      yyerror_range[1] = *yylsp;
>>>>>>> refs/remotes/origin/master
      yydestruct ("Error: popping",
		  yystos[yystate], yyvsp, yylsp, result);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  *++yyvsp = yylval;

<<<<<<< HEAD
  yyerror_range[1] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, (yyerror_range - 1), 2);
=======
  yyerror_range[2] = yylloc;
  /* Using YYLLOC is tempting, but would change the location of
     the lookahead.  YYLOC is available though.  */
  YYLLOC_DEFAULT (yyloc, yyerror_range, 2);
>>>>>>> refs/remotes/origin/master
  *++yylsp = yyloc;

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", yystos[yyn], yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturn;

/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturn;

#if !defined(yyoverflow) || YYERROR_VERBOSE
/*-------------------------------------------------.
| yyexhaustedlab -- memory exhaustion comes here.  |
`-------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, result, YY_("memory exhausted"));
  yyresult = 2;
  /* Fall through.  */
#endif

yyreturn:
  if (yychar != YYEMPTY)
<<<<<<< HEAD
     yydestruct ("Cleanup: discarding lookahead",
		 yytoken, &yylval, &yylloc, result);
=======
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, result);
    }
>>>>>>> refs/remotes/origin/master
  /* Do not reclaim the symbols of the rule which action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
		  yystos[*yyssp], yyvsp, yylsp, result);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
#if YYERROR_VERBOSE
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
#endif
  /* Make sure YYID is used.  */
  return YYID (yyresult);
}





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

