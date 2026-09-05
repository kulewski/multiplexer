// Run a statement at static initialization time, in every translation unit
// that says so: MX_TRIGGER_STATIC_INITIALIZATION(stmt, condition). Used to
// register mxcontrol subcommands and to check that modules initialize in the
// right order. See the note below on why the generated struct must be in an
// anonymous namespace.
#ifndef MX_LIB_INITIALIZATION_H_
#define MX_LIB_INITIALIZATION_H_

#include "lib/preproc/common.h"

#/*
#  * MX_TRIGGER_STATIC_INITIALIZATION(stmt, condition)
#  *	statically:
#  *	    if condition:
#  *		stmt
#  *
#  * Note:
#  *	stmt must be a valid statement (if it's function call, it must have '(' ')').
#  *	This allows to call initializers taking arguments;
#  */
#define MX_TRIGGER_STATIC_INITIALIZATION(stmt, condition) MX_TRIGGER_STATIC_INITIALIZATION_CODE((stmt), condition)

#/*
#  * MX_TRIGGER_STATIC_INITIALIZATION_CODE
#  *	Like MX_TRIGGER_STATIC_INITIALIZATION but code can be any C++ code
#  *	in braces.
#  */
#define MX_TRIGGER_STATIC_INITIALIZATION_CODE(code, condition)                                                         \
  MX_TRIGGER_STATIC_INITIALIZATION_CODE_NAME(code, _, condition)

#/*
#  * MX_TRIGGER_STATIC_INITIALIZATION_CODE_NAME
#  *	Like MX_TRIGGER_STATIC_INITIALIZATION_CODE. `name' is used as a static initializer
#  *	name seed (use when auto generation of name fails.
#  */
#define MX_TRIGGER_STATIC_INITIALIZATION_CODE_NAME(code, name, condition)                                              \
  __MX_TRIGGER_STATIC_INITIALIZATION_CODE(MX_UNIQUE_NAME(BOOST_PP_CAT(name, _trigger_static_initialization)), code,    \
                                          condition)

#/*
#  * The struct lives in an anonymous namespace on purpose: its name is built
#  * from __COUNTER__, and two translation units with the same include list
#  * reach the same counter value. With external linkage the two identically
#  * named classes were merged by the linker, so one constructor ran twice and
#  * the other never ran (mxcontrol registered "receivelogs" twice and lost
#  * "streamlogs").
#  */
#define __MX_TRIGGER_STATIC_INITIALIZATION_CODE(name, code, condition)                                                 \
  namespace {                                                                                                          \
  struct BOOST_PP_CAT(name, _struct) {                                                                                 \
    BOOST_PP_CAT(name, _struct)() {                                                                                    \
      if (condition) {                                                                                                 \
        MX_PP_REMOVE_BRACES code;                                                                                      \
      }                                                                                                                \
    }                                                                                                                  \
  } BOOST_PP_CAT(name, _instance);                                                                                     \
  }

#endif // MX_LIB_INITIALIZATION_H_
