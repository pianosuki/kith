# Fixture corpus for check_comments.py.
#
# Each case is a label comment (containing no forbidden tokens) followed
# by the test comment. The pre-commit hook excludes tools/fixtures/, so
# this file is never checked by the hook. A test runs the checker on
# this file and verifies that the expected lines trigger violations.
#
# Label format:
#   case: violation  -- the next comment must trigger a violation
#   case: clean      -- the next comment must not trigger

# case: violation
# TODO: implement this

# case: violation
# FIXME: broken

# case: violation
# HACK: workaround

# case: violation
# XXX: suspicious

# case: violation
# TEMP: temporary code

# case: violation
# PLACEHOLDER: stub

# case: violation
# WIP: work in progress

# case: violation
# someday this will be done

# case: violation
# eventually fix this

# case: violation
# in the future add logging

# case: violation
# fix this later

# case: violation
# for now just return zero

# case: violation
# I think this is correct

# case: violation
# we decided to use this approach

# case: violation
# you must call init first

# case: violation
# let's wrap this in a struct

# case: violation
# let me explain the algorithm

# case: violation
# this should be called after init

# case: violation
# this would fail on empty input

# case: violation
# could return NULL on OOM

# case: violation
# @author jdoe

# case: violation
# @date 2024-01-01

# case: violation
# @since v1.0

# case: violation
# Written by Jane Doe

# case: violation
# JIRA-123 blocking

# case: violation
# GH-456 merged

# case: violation
# see #789 for context

# case: violation
# ref 42 is related
# ticket 42 is related

# case: clean
"""Initialize the reactor.

The caller should ensure the config is valid. The function would
return an error if the config is NULL. This could block briefly.

Args:
    cfg: configuration struct.
Returns:
    0 on success, negative on failure.
"""

# case: clean
x = 0  # NOLINT
# NOLINTNEXTLINE
y = 0

# case: clean
# performs I/O on the socket

# case: clean
# the connection is closed when the refcount reaches zero

# case: clean
# the hash uses open addressing with linear probing

# case: clean
# uses memset_explicit to clear the token without elision
