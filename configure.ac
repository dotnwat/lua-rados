AC_PREREQ(2.59)

AC_INIT([lua-rados], [0.0.1], [noahwatkins@gmail.com])

AC_CONFIG_MACRO_DIR([m4])

AM_INIT_AUTOMAKE([foreign])

m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])

AX_CXX_COMPILE_STDCXX_11([],[mandatory])

AC_PROG_LIBTOOL
AC_PROG_CXX

AX_PROG_LUA(5.1, 5.2)
AX_LUA_HEADERS

AC_CHECK_LIB([rados], [rados_version],, AC_MSG_FAILURE([librados not found]))
AC_CHECK_HEADER([rados/librados.h],, AC_MSG_FAILURE([librados.h not found]))

AC_CONFIG_FILES([
    Makefile
    lua-rados.rockspec
])

AC_OUTPUT
