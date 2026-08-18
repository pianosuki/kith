"""Version attribute tests for the kith package.

``kith.__version__`` reports the installed distribution's version when the
package was installed and the generated constant on a from-source checkout.
Both derive from the same CMake project version — the metadata through the
packaging backend's regex over it, the constant through the header the
bindings were generated from — so the two paths agree in any consistent
tree. A red result here means the installed metadata is stale relative to
the header (re-run ``uv sync``), not that the assertion is wrong.
"""

from __future__ import annotations

import kith
from kith._generated import version as gen_version


def test_version_matches_the_generated_string() -> None:
    """The attribute and the generated constant carry the same version."""
    assert kith.__version__ == gen_version.KITH_VERSION_STRING
