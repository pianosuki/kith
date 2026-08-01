#ifndef KITH_VERSION_H
#define KITH_VERSION_H

/**
 * Framework release version and size-versioned struct ABI generation.
 *
 * KITH_VERSION_MAJOR, KITH_VERSION_MINOR, and KITH_VERSION_PATCH identify
 * the release; KITH_VERSION_STRING is the dotted form. KITH_ABI_VERSION is
 * the current generation of the size-versioned public structs: callers store
 * it in the abi_version field of a config struct, and the framework rejects
 * structs built against an incompatible generation.
 */

/**
 * @defgroup kith_version Version and ABI generation
 * @{
 */

/** Major component of the framework release version. */
#define KITH_VERSION_MAJOR 1
/** Minor component of the framework release version. */
#define KITH_VERSION_MINOR 0
/** Patch component of the framework release version. */
#define KITH_VERSION_PATCH 0
/** The dotted release version string. */
#define KITH_VERSION_STRING "1.0.0"
/** Current generation of the size-versioned public structs. */
#define KITH_ABI_VERSION 1u

/** @} */

#endif /* KITH_VERSION_H */
