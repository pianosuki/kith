#ifndef KITH_API_H
#define KITH_API_H

/**
 * Symbol visibility and ABI-lifecycle markers for the public C surface.
 *
 * The build defaults to hidden visibility, so only symbols marked KITH_API
 * export from each shared library. KITH_LOCAL keeps header-declared symbols
 * internal to a library even though they appear in a header. KITH_DEPRECATED
 * marks public symbols slated for removal and warns callers on use.
 *
 * The macros expand to nothing on toolchains without visibility attributes.
 */

/**
 * @defgroup kith_api Public API markers
 * @{
 */

#if defined(__GNUC__) || defined(__clang__)
/** Marks a public symbol: exported from the shared library. */
#define KITH_API __attribute__((visibility("default")))
/** Marks a library-internal symbol declared in a public header. */
#define KITH_LOCAL __attribute__((visibility("hidden")))
/** Marks a public symbol slated for removal; use warns at compile time. */
#define KITH_DEPRECATED __attribute__((deprecated))
#else
#define KITH_API
#define KITH_LOCAL
#define KITH_DEPRECATED
#endif

/** @} */

#endif /* KITH_API_H */
