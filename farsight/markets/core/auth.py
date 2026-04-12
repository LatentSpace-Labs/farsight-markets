"""
Pluggable authentication for Farsight Markets.

Provides a default no-op auth dependency for standalone use.
When integrated with a parent application (e.g., Farsight), the auth
dependency can be swapped via `set_auth_dependency()`.

Standalone usage (no auth):
    Routes use `get_current_user` which returns a default anonymous user.

Integrated usage (with Farsight or custom auth):
    from farsight.markets.core.auth import set_auth_dependency
    from my_app.auth import get_current_user as my_auth
    set_auth_dependency(my_auth)
"""

import logging
from dataclasses import dataclass
from typing import Callable, Optional
from uuid import UUID

logger = logging.getLogger(__name__)


@dataclass
class AuthenticatedUser:
    """Minimal user identity returned by auth dependencies."""
    id: UUID
    external_id: str
    email: str


# Default anonymous user for standalone mode
_ANONYMOUS_USER = AuthenticatedUser(
    id=UUID("00000000-0000-0000-0000-000000000000"),
    external_id="anonymous",
    email="anonymous@localhost",
)

# Pluggable auth dependency — starts as None (uses default)
_auth_dependency: Optional[Callable] = None


def set_auth_dependency(dependency: Callable) -> None:
    """Override the default auth dependency.

    Args:
        dependency: A FastAPI-compatible dependency that returns an
                    AuthenticatedUser (or compatible object with .id).
    """
    global _auth_dependency
    _auth_dependency = dependency
    logger.info("Auth dependency overridden: %s", dependency.__qualname__)


async def get_current_user() -> AuthenticatedUser:
    """Default auth dependency — returns anonymous user.

    When a custom auth dependency is set via set_auth_dependency(),
    routes will use that instead (see route files).
    """
    return _ANONYMOUS_USER


def get_auth_dependency() -> Callable:
    """Return the active auth dependency (custom or default)."""
    return _auth_dependency or get_current_user
