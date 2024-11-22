from typing import Any
from uuid import UUID

from sqlalchemy import Table, delete
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession
from hetdesrun.persistence.structure_service_dbmodels import (
    StructureServiceElementTypeDBModel,
    StructureServiceSinkDBModel,
    StructureServiceSourceDBModel,
    StructureServiceThingNodeDBModel,
)

DBModelType = (
    StructureServiceElementTypeDBModel
    | StructureServiceThingNodeDBModel
    | StructureServiceSourceDBModel
    | StructureServiceSinkDBModel
)


def get_insert_statement(session: SQLAlchemySession, model: type[DBModelType] | Table) -> Any:
    """Get the appropriate insert statement based on the database dialect."""
    if session.bind is None or session.bind.dialect is None:
        raise ValueError("Session's bind is None; cannot determine database dialect.")
    dialect_name = session.bind.dialect.name
    if dialect_name == "postgresql":
        return pg_insert(model)
    return sqlite_insert(model)
