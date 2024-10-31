import logging
from itertools import batched
from math import ceil

from sqlalchemy import tuple_
from sqlalchemy.exc import IntegrityError

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession
from hetdesrun.persistence.structure_service_dbmodels import StructureServiceElementTypeDBModel
from hetdesrun.structure.db.exceptions import DBError, DBIntegrityError, DBUpdateError
from hetdesrun.structure.models import StructureServiceElementType

logger = logging.getLogger(__name__)


def fetch_element_types(
    session: SQLAlchemySession, keys: set[tuple[str, str]], batch_size: int = 500
) -> dict[tuple[str, str], StructureServiceElementTypeDBModel]:
    """
    Fetches StructureServiceElementTypeDBModel records from the
    database based on stakeholder_key and external_id.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        keys (Set[tuple[str, str]]): A set of (stakeholder_key, external_id) tuples.

    Returns:
        dict[tuple[str, str], StructureServiceElementTypeDBModel]:
            A mapping from (stakeholder_key, external_id) to StructureServiceElementTypeDBModel.

    Raises:
        DBIntegrityError: If an integrity error occurs during the database operation.
        DBError: If any other database error occurs.
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
            "Fetched %d StructureServiceElementTypeDBModel items from the database.",
            len(existing_ets_mapping),
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
    """
    Searches for StructureServiceElementTypeDBModel records based on a partial or full name match.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        name_query (str): The name or partial name to search for.

    Returns:
        list[StructureServiceElementTypeDBModel]: A list of StructureServiceElementTypeDBModel
        records matching the name query.

    Raises:
        DBIntegrityError: If an integrity error occurs during the database operation.
        DBError: If any other database error occurs.
    """
    try:
        element_types = (
            session.query(StructureServiceElementTypeDBModel)
            .filter(StructureServiceElementTypeDBModel.name.ilike(f"%{name_query}%"))
            .all()
        )
        logger.debug(
            "Found %d StructureServiceElementTypeDBModel items matching name query '%s'.",
            len(element_types),
            name_query,
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
    existing_elements: dict[tuple[str, str], StructureServiceElementTypeDBModel],
) -> None:
    """
    Upserts StructureServiceElementTypeDBModel records efficiently.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        elements (list[StructureServiceElementType]): The list of StructureServiceElementType
            objects to upsert.
        existing_elements (dict[tuple[str, str], StructureServiceElementTypeDBModel]):
            Existing StructureServiceElementTypeDBModel objects
            mapped by (stakeholder_key, external_id).

    Raises:
        DBIntegrityError: If an integrity error occurs during the upsert operation.
        DBUpdateError: If any other error occurs during the upsert operation.
    """
    try:
        for element in elements:
            key = (element.stakeholder_key, element.external_id)
            db_element = existing_elements.get(key)
            if db_element:
                logger.debug("Updating StructureServiceElementTypeDBModel with key %s.", key)
                db_element.name = element.name
                db_element.description = element.description
            else:
                logger.debug("Creating new StructureServiceElementTypeDBModel with key %s.", key)
                new_element = StructureServiceElementTypeDBModel(
                    id=element.id,
                    external_id=element.external_id,
                    stakeholder_key=element.stakeholder_key,
                    name=element.name,
                    description=element.description,
                )
                session.add(new_element)
        # Explicitly flush all changes to ensure data is written to the database
        session.flush()
    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceElementTypeDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceElementTypeDBModel"
        ) from e
    except Exception as e:
        logger.error("Error while upserting StructureServiceElementTypeDBModel: %s", e)
        raise DBUpdateError("Error while upserting StructureServiceElementTypeDBModel") from e
