import logging
from itertools import batched
from math import ceil

from sqlalchemy import tuple_
from sqlalchemy.exc import IntegrityError

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession
from hetdesrun.persistence.structure_service_dbmodels import StructureServiceElementTypeDBModel
from hetdesrun.structure.db.exceptions import DBError, DBIntegrityError, DBUpdateError
from hetdesrun.structure.db.utils import get_insert_statement
from hetdesrun.structure.models import StructureServiceElementType

logger = logging.getLogger(__name__)


def fetch_element_types(
    session: SQLAlchemySession, keys: set[tuple[str, str]]
) -> dict[tuple[str, str], StructureServiceElementTypeDBModel]:
    """Fetch element types by stakeholder_key and external_id.

    Retrieves records matching stakeholder_key and external_id, returns as a dictionary.
    """
    existing_ets_mapping: dict[tuple[str, str], StructureServiceElementTypeDBModel] = {}
    if not keys:
        return existing_ets_mapping
    try:
        query = session.query(StructureServiceElementTypeDBModel).filter(
            tuple_(
                StructureServiceElementTypeDBModel.stakeholder_key,
                StructureServiceElementTypeDBModel.external_id,
            ).in_(keys)
        )
        results = query.all()
        for et in results:
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
) -> None:
    """Insert or update element types.

    Updates existing records or creates new ones if they do not exist.
    """
    try:
        insert_data = [
            {
                "id": element.id,
                "external_id": element.external_id,
                "stakeholder_key": element.stakeholder_key,
                "name": element.name,
                "description": element.description,
            }
            for element in elements
        ]

        insert_stmt = get_insert_statement(session, StructureServiceElementTypeDBModel).values(
            insert_data
        )

        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["external_id", "stakeholder_key"],
            set_={
                "name": insert_stmt.excluded.name,
                "description": insert_stmt.excluded.description,
            },
        )

        session.execute(upsert_stmt)

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceElementTypeDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceElementTypeDBModel"
        ) from e
    except Exception as e:
        logger.error("Unexpected error while upserting StructureServiceElementTypeDBModel: %s", e)
        raise DBUpdateError(
            "Unexpected error while upserting StructureServiceElementTypeDBModel"
        ) from e
