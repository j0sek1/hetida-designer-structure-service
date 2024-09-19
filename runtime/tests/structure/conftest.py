from typing import Any
from unittest import mock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.future.engine import Engine

from hetdesrun.persistence.db_engine_and_session import async_sessionmaker, get_db_engine
from hetdesrun.persistence.structure_service_dbmodels import Base
from hetdesrun.structure.db.orm_service import (
    update_structure_from_file,
)


def pytest_configure(config):
    """
    Configure pytest-asyncio mode.
    """
    # Sets the asyncio_mode to 'auto', allowing pytest_asyncio to automatically
    # detect asynchronous fixtures and tests.
    import pytest_asyncio

    pytest_asyncio.plugin.asyncio_mode = "auto"


# Fixture to provide the test database engine
@pytest.fixture(scope="session")
def test_db_engine(use_in_memory_db: bool) -> Engine:
    if use_in_memory_db:
        in_memory_database_url = "sqlite+aiosqlite:///:memory:"
        engine = get_db_engine(override_db_url=in_memory_database_url)
    else:
        engine = get_db_engine()
    return engine


# Fixture to clean the database and set up the schema
@pytest_asyncio.fixture()
async def clean_test_db_engine(test_db_engine: AsyncEngine) -> AsyncEngine:
    """
    Fixture for cleaning up the test database before each test.
    """
    async with test_db_engine.begin() as conn:
        # Using run_sync to execute drop_all and create_all within an asynchronous context
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield test_db_engine


# Fixture to provide a mocked session bound to the clean test database engine
@pytest.fixture()
def mocked_clean_test_db_session(clean_test_db_engine):
    with mock.patch(
        "hetdesrun.persistence.db_engine_and_session.Session",
        async_sessionmaker(clean_test_db_engine, future=True),
    ) as _fixture:
        yield _fixture


# Fixture to determine whether to use an in-memory database, based on pytest options
@pytest.fixture(scope="session")
def use_in_memory_db(pytestconfig: pytest.Config) -> Any:
    return pytestconfig.getoption("use_in_memory_db")


# Fixture to load an empty database structure from a JSON file
@pytest_asyncio.fixture()
async def _db_empty_database(mocked_clean_test_db_session):
    file_path = "tests/structure/data/db_empty_structure.json"
    await update_structure_from_file(file_path)


# Fixture to load a basic test structure into the database from a JSON file
@pytest_asyncio.fixture()
async def _db_test_structure(mocked_clean_test_db_session):
    file_path = "tests/structure/data/db_test_structure.json"
    await update_structure_from_file(file_path)


# Fixture to provide the file path of the test structure JSON
@pytest.fixture()
def db_test_structure_file_path():
    return "tests/structure/data/db_test_structure.json"


# Fixture to load an unordered test structure into the database from a JSON file
@pytest_asyncio.fixture()
async def _db_test_unordered_structure(mocked_clean_test_db_session):
    file_path = "tests/structure/data/db_test_unordered_structure.json"
    await update_structure_from_file(file_path)
