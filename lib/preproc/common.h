// Small preprocessor helpers shared by the macro-heavy headers: unique
// names, parenthesis removal, always_inline.
#ifndef MX_LIB_PREPROC_COMMON_H_
#define MX_LIB_PREPROC_COMMON_H_

#include <boost/preprocessor/seq/cat.hpp>

#/*
#  * MX_UNIQUE_INT
#  *	Expand to some int.
#  */
#ifdef __COUNTER__
#define MX_UNIQUE_INT() __COUNTER__
#else
#define MX_UNIQUE_INT() __LINE__
#endif

#/*
#  * MX_REMOVE_PARENS
#  *	Remove parenthesis.
#  *
#  *	Example:
#  *	    #define foo(sth_in_parenthesis) MX_REMOVE_PARENS sth_in_parenthesis
#  */
#define MX_REMOVE_PARENS(arg...) arg

#/*
#  * MX_UNIQUE_NAME(name)
#  *	Generate unique name starting with name as a base.
#  */
#define MX_UNIQUE_NAME(name) BOOST_PP_SEQ_CAT((__uniqname__)(name)(_)(MX_UNIQUE_INT())(_))

#/*
#  * MX_ATTRIBUTE_ALWAYS_INLINE
#  *	Declare __attribute__(always_inline) where it's possible.
#  */
#define MX_ATTRIBUTE_ALWAYS_INLINE __attribute__((always_inline))

#/*
#  * MX_PP_REMOVE_BRACES
#  */
#define MX_PP_REMOVE_BRACES(args...) args

#endif // MX_LIB_PREPROC_COMMON_H_
