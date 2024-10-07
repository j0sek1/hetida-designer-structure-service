import logging

from sqlalchemy import tuple_
from sqlalchemy.exc import IntegrityError

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession
from hetdesrun.persistence.structure_service_dbmodels import ElementTypeOrm
from hetdesrun.structure.db.exceptions import DBError, DBIntegrityError, DBUpdateError
from hetdesrun.structure.models import ElementType

logger = logging.getLogger(__name__)


def fetch_element_types(
    session: SQLAlchemySession, keys: set[tuple[str, str]]
) -> dict[tuple[str, str], ElementTypeOrm]:
    """
    Fetches ElementTypeOrm records from the database based on stakeholder_key and external_id.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        keys (Set[Tuple[str, str]]): A set of (stakeholder_key, external_id) tuples.

    Returns:
        Dict[Tuple[str, str], ElementTypeOrm]:
            A mapping from (stakeholder_key, external_id) to ElementTypeOrm.

    Raises:
        DBIntegrityError: If an integrity error occurs during the database operation.
        DBError: If any other database error occurs.
    """
    try:
        element_types = (
            session.query(ElementTypeOrm)
            .filter(tuple_(ElementTypeOrm.stakeholder_key, ElementTypeOrm.external_id).in_(keys))
            .all()
        )
        logger.debug("Fetched %d ElementTypes from the database.", len(element_types))
        return {(et.stakeholder_key, et.external_id): et for et in element_types}
    except IntegrityError as e:
        logger.error("Integrity Error while fetching ElementTypes: %s", e)
        raise DBIntegrityError("Integrity Error while fetching ElementTypes") from e
    except Exception as e:
        logger.error("Unexpected error while fetching ElementTypes: %s", e)
        raise DBError("Unexpected error while fetching ElementTypes") from e


def search_element_types_by_name(
    session: SQLAlchemySession, name_query: str
) -> list[ElementTypeOrm]:
    """
    Searches for ElementTypeOrm records based on a partial or full name match.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        name_query (str): The name or partial name to search for.

    Returns:
        List[ElementTypeOrm]: A list of ElementTypeOrm records matching the name query.

    Raises:
        DBIntegrityError: If an integrity error occurs during the database operation.
        DBError: If any other database error occurs.
    """
    try:
        element_types = (
            session.query(ElementTypeOrm).filter(ElementTypeOrm.name.ilike(f"%{name_query}%")).all()
        )
        logger.debug(
            "Found %d ElementTypeOrm items matching name query '%s'.",
            len(element_types),
            name_query,
        )
        return element_types
    except IntegrityError as e:
        logger.error("Integrity Error while searching ElementTypeOrm by name: %s", e)
        raise DBIntegrityError("Integrity Error while searching ElementTypeOrm by name") from e
    except Exception as e:
        logger.error("Unexpected error while searching ElementTypeOrm by name: %s", e)
        raise DBError("Unexpected error while searching ElementTypeOrm by name") from e


def upsert_element_types(
    session: SQLAlchemySession,
    elements: list[ElementType],
    existing_elements: dict[tuple[str, str], ElementTypeOrm],
) -> None:
    """
    Upserts ElementTypeOrm records using SQLAlchemy's merge and add functionalities.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        elements (List[ElementType]): The list of ElementType objects to upsert.
        existing_elements (Dict[Tuple[str, str], ElementTypeOrm]):
            Existing ElementTypeOrm objects mapped by (stakeholder_key, external_id).

    Raises:
        DBIntegrityError: If an integrity error occurs during the upsert operation.
        DBUpdateError: If any other error occurs during the upsert operation.
    """
    try:
        # Prevents SQLAlchemy from flushing the session automatically during the upsert
        with session.no_autoflush:
            for element in elements:
                key = (element.stakeholder_key, element.external_id)
                db_element = existing_elements.get(key)
                if db_element:
                    logger.debug("Updating ElementTypeOrm with key %s.", key)
                    # Update fields
                    db_element.name = element.name
                    db_element.description = element.description
                    # Merge the updated element into the session
                    session.merge(db_element)
                else:
                    logger.debug("Creating new ElementTypeOrm with key %s.", key)
                    # Create a new ElementTypeOrm object
                    new_element = ElementTypeOrm(
                        id=element.id,
                        external_id=element.external_id,
                        stakeholder_key=element.stakeholder_key,
                        name=element.name,
                        description=element.description,
                    )
                    # Add the new element to the session
                    session.add(new_element)
        # Explicitly flush all changes to ensure data is written to the database
        session.flush()
    except IntegrityError as e:
        logger.error("Integrity Error while upserting ElementTypeOrm: %s", e)
        raise DBIntegrityError("Integrity Error while upserting ElementTypeOrm") from e
    except Exception as e:
        logger.error("Error while upserting ElementTypeOrm: %s", e)
        raise DBUpdateError("Error while upserting ElementTypeOrm") from e
