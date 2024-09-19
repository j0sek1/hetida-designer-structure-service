from __future__ import annotations  # for type hinting the Session from sessionmaker

import json
import logging
from functools import cache
from typing import Any
from uuid import UUID

from pydantic import SecretStr
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemySession
from sqlalchemy.pool import NullPool, StaticPool

from hetdesrun.webservice.config import get_config

logger = logging.getLogger(__name__)


def _default(val: Any) -> str:
    if isinstance(val, UUID):
        return str(val)
    raise TypeError()


def dumps(d: Any) -> str:
    return json.dumps(d, default=_default)


@cache
def get_db_engine(override_db_url: SecretStr | str | URL | None = None) -> AsyncEngine:
    if get_config().sqlalchemy_connection_string is None:
        raise TypeError("No sqlalchemy connection string configured/inferred!")

    db_url_to_use: SecretStr | str | URL
    if override_db_url is None:
        db_url_to_use = get_config().sqlalchemy_connection_string  # type: ignore
    else:
        db_url_to_use = override_db_url

    if isinstance(db_url_to_use, SecretStr):
        db_url_to_use = db_url_to_use.get_secret_value()

    db_url_str = str(db_url_to_use) if isinstance(db_url_to_use, URL) else db_url_to_use

    # Parse the DB URL into a URL object to inspect its components
    url_object = make_url(db_url_str)

    # General engine arguments
    engine_kwargs = {
        "future": True,
        "json_serializer": dumps,
    }

    # Adjust engine arguments based on the database dialect
    if url_object.drivername.startswith("sqlite"):
        # SQLite-specific configuration
        connect_args = {
            "check_same_thread": False
        }  # Required for async usage with SQLite to allow usage from multiple threads

        if url_object.database in (":memory:", ""):
            # In-memory SQLite database
            # Use StaticPool to maintain a single connection for in-memory database
            engine_kwargs["poolclass"] = StaticPool
            engine_kwargs["connect_args"] = connect_args
        else:
            # File-based SQLite database
            # Use NullPool to disable pooling (each connection is a fresh connection)
            engine_kwargs["poolclass"] = NullPool
            engine_kwargs["connect_args"] = connect_args
    else:
        # Configuration for other databases (e.g., Postgres)
        # Set pool parameters to control ceonnection pooling behavior
        engine_kwargs["pool_size"] = (
            get_config().sqlalchemy_pool_size or 100
        )  # Set pool_size, default to 100 if not configured
        engine_kwargs["max_overflow"] = (
            get_config().sqlalchemy_max_overflow or 10
        )  # Set max_overflow, default to 10
        engine_kwargs["pool_timeout"] = (
            get_config().sqlalchemy_pool_timeout or 30
        )  # Set pool_timeout, default to 30 seconds
        engine_kwargs["pool_recycle"] = (
            get_config().sqlalchemy_pool_recycle or 1800
        )  # Set pool_recycle, default to 30 minutes
        engine_kwargs["pool_pre_ping"] = (
            True  # Enable pool_pre_ping to check connections before use
        )

    engine = create_async_engine(
        db_url_str,
        **engine_kwargs,  # Pass the adjusted engine arguments
    )

    logger.debug("Created DB Engine with url: %s", repr(engine.url))

    return engine


Session = async_sessionmaker(bind=get_db_engine(), future=True)


def get_session() -> async_sessionmaker[SQLAlchemySession]:
    return Session
