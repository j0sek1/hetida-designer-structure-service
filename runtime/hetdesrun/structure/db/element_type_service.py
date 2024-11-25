import logging
from itertools import batched
from math import ceil

from sqlalchemy import Connection, Engine, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.postgresql.dml import Insert as pg_insert_typing
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.sqlite.dml import Insert as sqlite_insert_typing
from sqlalchemy.exc import IntegrityError

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession
from hetdesrun.persistence.structure_service_dbmodels import StructureServiceElementTypeDBModel
from hetdesrun.structure.db.exceptions import DBError, DBIntegrityError, DBUpdateError
from hetdesrun.structure.models import StructureServiceElementType
from hetdesrun.structure.utils import is_postgresql, is_sqlite

logger = logging.getLogger(__name__)


def fetch_element_types(
    session: SQLAlchemySession, keys: set[tuple[str, str]], batch_size: int = 500
) -> dict[tuple[str, str], StructureServiceElementTypeDBModel]:
    """Fetch element types by stakeholder_key and external_id.

    Retrieves records matching stakeholder_key and external_id, returns as a dictionary.
    """
    existing_ets_mapping: dict[tuple[str, str], StructureServiceElementTypeDBModel] = {}
    if not keys:
        return existing_ets_mapping
    try:
        # Loop through keys in batches of size <batch_size> or less
        for key_batch in batched(keys, ceil(len(keys) / batch_size)):
            batch_query = session.query(StructureServiceElementTypeDBModel).filter(
                tuple_(
                    StructureServiceElementTypeDBModel.stakeholder_key,
                    StructureServiceElementTypeDBModel.external_id,
                ).in_(key_batch)
            )
            batch_results = batch_query.all()
            for et in batch_results:
                key = (et.stakeholder_key, et.external_id)
                existing_ets_mapping[key] = et
        logger.debug(
            "Fetched %d StructureServiceElementTypeDBModel items from the database for %d keys.",
            len(existing_ets_mapping),
            len(keys),
        )
        return existing_ets_mapping
    except IntegrityError as e:
        logger.error("Integrity Error while fetching StructureServiceElementTypes: %s", e)
        raise DBIntegrityError("Integrity Error while fetching StructureServiceElementTypes") from e
    except Exception as e:
        logger.error("Unexpected error while fetching StructureServiceElementTypes: %s", e)
        raise DBError("Unexpected error while fetching StructureServiceElementTypes") from e


def search_element_types_by_name(
    session: SQLAlchemySession, name_query: str
) -> list[StructureServiceElementTypeDBModel]:
    """Search element types by name.

    Finds element types using case-insensitive substring matching.
    """
    try:
        element_types = (
            session.query(StructureServiceElementTypeDBModel)
            .filter(StructureServiceElementTypeDBModel.name.ilike(f"%{name_query}%"))
            .all()
        )
        logger.debug(
            "Found %d StructureServiceElementTypeDBModel items matching "
            "name query '%s' from %d total records.",
            len(element_types),
            name_query,
            session.query(StructureServiceElementTypeDBModel).count(),
        )
        return element_types
    except IntegrityError as e:
        logger.error(
            "Integrity Error while searching StructureServiceElementTypeDBModel by name: %s", e
        )
        raise DBIntegrityError(
            "Integrity Error while searching StructureServiceElementTypeDBModel by name"
        ) from e
    except Exception as e:
        logger.error(
            "Unexpected error while searching StructureServiceElementTypeDBModel by name: %s", e
        )
        raise DBError(
            "Unexpected error while searching StructureServiceElementTypeDBModel by name"
        ) from e


def upsert_element_types(
    session: SQLAlchemySession,
    elements: list[StructureServiceElementType],
) -> dict[tuple[str, str], StructureServiceElementTypeDBModel]:
    """Insert or update element type records in the database.

    For each StructureServiceElementType, updates existing records if they are found;
    otherwise, creates new records. Returns the element type as a dictionary indexed
    by (stakeholder_key, external_id).
    """
    if not elements:
        return {}

    # Exclude non-column fields and ensure scalar types
    element_dicts = [
        {
            key: value
            for key, value in el.dict().items()
            if key != "thing_nodes" and not isinstance(value, list)
        }
        for el in elements
    ]

    try:
        engine: Engine | Connection = session.get_bind()
        if isinstance(engine, Connection):
            raise ValueError("The session in use has to be bound to an Engine, not a Connection.")

        upsert_stmt: sqlite_insert_typing | pg_insert_typing

        if is_postgresql(engine):
            upsert_stmt = pg_insert(StructureServiceElementTypeDBModel).values(element_dicts)
        elif is_sqlite(engine):
            upsert_stmt = sqlite_insert(StructureServiceElementTypeDBModel).values(element_dicts)
        else:
            raise ValueError(
                f"Unsupported database engine: {engine}. Please use either Postgres or SQLite."
            )

        upsert_stmt = upsert_stmt.on_conflict_do_update(
            index_elements=[
                "external_id",
                "stakeholder_key",
            ],  # Columns where insert looks for a conflict
            set_={
                col: upsert_stmt.excluded[col] for col in element_dicts[0] if col != "id"
            },  # Exclude primary key from update
        ).returning(StructureServiceElementTypeDBModel)  # type: ignore

        # ORM models returned by the upsert query
        element_dbmodels = session.scalars(
            upsert_stmt,
            execution_options={"populate_existing": True},
        )

        # Convert to dictionary indexed by (stakeholder_key, external_id)
        element_dbmodel_dict = {(et.stakeholder_key, et.external_id): et for et in element_dbmodels}

        return element_dbmodel_dict

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceElementTypeDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceElementTypeDBModel"
        ) from e
    except ValueError as e:
        logger.error("Value error while upserting StructureServiceElementTypeDBModel: %s", e)
        raise DBError("Value error while upserting StructureServiceElementTypeDBModel") from e
    except Exception as e:
        logger.error("Unexpected error while upserting StructureServiceElementTypeDBModel: %s", e)
        raise DBUpdateError(
            "Unexpected error while upserting StructureServiceElementTypeDBModel"
        ) from e
