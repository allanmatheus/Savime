AC_INIT([savime], [1.0], [hermano@lncc.br])
AC_CONFIG_MACRO_DIRS([m4])
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects])
m4_ifdef([AM_PROG_AR], [AM_PROG_AR])
LT_INIT
AC_PROG_CXX
AC_PROG_CC
AC_CONFIG_HEADERS([config.h] )
CXXFLAGS='-g -DTIME  -rdynamic -m64 -fpic -w -Wall -Wextra -fopenmp -std=gnu++0x'
AC_CONFIG_FILES([
 Makefile
 core/Makefile
 client/Makefile
 rdmap/Makefile
 engine/Makefile
])
AC_ARG_ENABLE(catalyst,
[  --enable-catalyst    Turn on catalyst support.],
[case "${enableval}" in
  yes) catalyst=true ;;
  no)  catalyst=false ;;
  *) AC_MSG_ERROR(bad value ${enableval} for --enable-catalyst) ;; esac],
[catalyst=false]
)
AM_CONDITIONAL(CATALYST, test x$catalyst = xtrue)
AC_SUBST([abs_top_builddir])
$(pwd)/parser/build_grammar.sh
AC_OUTPUT
