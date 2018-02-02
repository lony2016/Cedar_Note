AM_CPPFLAGS= -I${TBLIB_ROOT}/include/tbsys \
<<<<<<< HEAD
             -I${ONEV_ROOT}/include/onev \
=======
             -I${EASY_ROOT}/include/easy \
>>>>>>> refs/remotes/origin/master
	     -I${top_srcdir}/include \
	     -I${top_srcdir}/src \
	     -fPIC

<<<<<<< HEAD
LDADD = ${ONEV_LIB_PATH}/libonev.a \
=======
LDADD = ${EASY_LIB_PATH}/libeasy.a \
>>>>>>> refs/remotes/origin/master
	${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS=-lpthread -lc -lm  -lrt -lcrypt -lssl #-pg
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libcompactsstable.a

libcompactsstable_a_SOURCES = ob_block_membuf.cpp \
			      ob_compactsstable_mem.cpp \
                              ob_compact_row.cpp




EXTRA_DIST =  ob_block_membuf.h \
              ob_compact_obj.h \
              ob_compact_row.h \
              ob_compactsstable_mem.h

clean-local:
	-rm -f *.gcov *.gcno *.gcda
