// Small preprocessor helpers shared by the macro-heavy headers: token
// pasting and stringizing with their arguments expanded, unique names,
// parenthesis removal, always_inline.
#ifndef MX_LIB_PREPROC_COMMON_H_
#define MX_LIB_PREPROC_COMMON_H_

#/*
#  * MX_PP_CAT(a, b), MX_PP_STRINGIZE(x)
#  *	Paste or stringize after expanding the arguments.
#  */
#define MX_PP_CAT(a, b) MX_PP_CAT_I(a, b)
#define MX_PP_CAT_I(a, b) a##b
#define MX_PP_STRINGIZE(x) MX_PP_STRINGIZE_I(x)
#define MX_PP_STRINGIZE_I(x) #x

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
#define MX_UNIQUE_NAME(name) MX_PP_CAT(MX_PP_CAT(__uniqname__, name), MX_PP_CAT(_, MX_UNIQUE_INT()))

#/*
#  * MX_ATTRIBUTE_ALWAYS_INLINE
#  *	Declare __attribute__(always_inline) where it's possible.
#  */
#define MX_ATTRIBUTE_ALWAYS_INLINE __attribute__((always_inline))

#/*
#  * MX_PP_REMOVE_BRACES
#  */
#define MX_PP_REMOVE_BRACES(args...) args

#endif  // MX_LIB_PREPROC_COMMON_H_
