import logging
from itertools import batched
from math import ceil
from uuid import UUID

from sqlalchemy import select, tuple_
from sqlalchemy.exc import IntegrityError

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession, get_session
from hetdesrun.persistence.structure_service_dbmodels import (
    StructureServiceElementTypeDBModel,
    StructureServiceThingNodeDBModel,
)
from hetdesrun.structure.db.exceptions import (
    DBError,
    DBIntegrityError,
    DBNotFoundError,
    DBUpdateError,
)
from hetdesrun.structure.models import StructureServiceThingNode

logger = logging.getLogger(__name__)


def fetch_single_thing_node_from_db_by_id(tn_id: UUID) -> StructureServiceThingNode:
    """
    Fetches a single StructureServiceThingNode from the database by its unique ID.

    Args:
        tn_id (UUID): The unique identifier of the StructureServiceThingNode.

    Returns:
        StructureServiceThingNode: The StructureServiceThingNode object matching the given ID.

    Raises:
        DBNotFoundError: If no StructureServiceThingNode with the specified ID is found.
    """
    logger.debug("Fetching single StructureServiceThingNode from database with ID: %s", tn_id)
    with get_session()() as session:
        thing_node = (
            session.query(StructureServiceThingNodeDBModel)
            .filter(StructureServiceThingNodeDBModel.id == tn_id)
            .one_or_none()
        )
        if thing_node:
            logger.debug("StructureServiceThingNode with ID %s found.", tn_id)
            return StructureServiceThingNode.from_orm_model(thing_node)

    logger.error("No StructureServiceThingNode found for ID %s. Raising DBNotFoundError.", tn_id)
    raise DBNotFoundError(f"No StructureServiceThingNode found for ID {tn_id}")


def fetch_thing_nodes(
    session: SQLAlchemySession, keys: set[tuple[str, str]], batch_size: int = 500
) -> dict[tuple[str, str], StructureServiceThingNodeDBModel]:
    """
    Fetches StructureServiceThingNodeDBModel records from the database
    based on stakeholder_key and external_id.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        keys (Set[Tuple[str, str]]): A set of (stakeholder_key, external_id) tuples.

    Returns:
        Dict[Tuple[str, str], StructureServiceThingNodeDBModel]:
            A mapping from (stakeholder_key, external_id) to StructureServiceThingNodeDBModel.

    Raises:
        DBIntegrityError: If an integrity error occurs during the database operation.
        DBError: If any other database error occurs.
    """
    existing_tns_mapping: dict[tuple[str, str], StructureServiceThingNodeDBModel] = {}
    if not keys:
        return existing_tns_mapping
    try:
        # Loop through keys in batches of size <batch_size> or less
        for key_batch in batched(keys, ceil(len(keys) / batch_size)):
            batch_query = session.query(StructureServiceThingNodeDBModel).filter(
                tuple_(
                    StructureServiceThingNodeDBModel.stakeholder_key,
                    StructureServiceThingNodeDBModel.external_id,
                ).in_(key_batch)
            )
            batch_results = batch_query.all()
            for tn in batch_results:
                key = (tn.stakeholder_key, tn.external_id)
                existing_tns_mapping[key] = tn
        logger.debug(
            "Fetched %d StructureServiceThingNodeDBModel items from the database.",
            len(existing_tns_mapping),
        )
        return existing_tns_mapping
    except IntegrityError as e:
        logger.error("Integrity Error while fetching StructureServiceThingNodes: %s", e)
        raise DBIntegrityError("Integrity Error while fetching StructureServiceThingNodes") from e
    except Exception as e:
        logger.error("Unexpected error while fetching StructureServiceThingNodes: %s", e)
        raise DBError("Unexpected error while fetching StructureServiceThingNodes") from e


def search_thing_nodes_by_name(
    session: SQLAlchemySession, name_query: str
) -> list[StructureServiceThingNodeDBModel]:
    """
    Searches for StructureServiceThingNodeDBModel records based on a partial or full name match.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        name_query (str): The name or partial name to search for.

    Returns:
        List[StructureServiceThingNodeDBModel]: A list of StructureServiceThingNodeDBModel
        records matching the name query.

    Raises:
        DBIntegrityError: If an integrity error occurs during the database operation.
        DBError: If any other database error occurs.
    """
    try:
        thing_nodes = (
            session.query(StructureServiceThingNodeDBModel)
            .filter(StructureServiceThingNodeDBModel.name.ilike(f"%{name_query}%"))
            .all()
        )
        logger.debug(
            "Found %d StructureServiceThingNodeDBModel items matching name query '%s'.",
            len(thing_nodes),
            name_query,
        )
        return thing_nodes
    except IntegrityError as e:
        logger.error(
            "Integrity Error while searching StructureServiceThingNodeDBModel by name: %s", e
        )
        raise DBIntegrityError(
            "Integrity Error while searching StructureServiceThingNodeDBModel by name"
        ) from e
    except Exception as e:
        logger.error(
            "Unexpected error while searching StructureServiceThingNodeDBModel by name: %s", e
        )
        raise DBError(
            "Unexpected error while searching StructureServiceThingNodeDBModel by name"
        ) from e


def upsert_thing_nodes(
    session: SQLAlchemySession,
    thing_nodes: list[StructureServiceThingNode],
    existing_thing_nodes: dict[tuple[str, str], StructureServiceThingNodeDBModel],
) -> None:
    """
    Upserts StructureServiceThingNodeDBModel records efficiently using bulk operations.
    Creates new records if they do not exist.

    Args:
        session (SQLAlchemySession):
            The SQLAlchemy session used for database transactions.
        thing_nodes (list[StructureServiceThingNode]):
            A list of `StructureServiceThingNode` objects to upsert into the database.
        existing_thing_nodes (dict[tuple[str, str], StructureServiceThingNodeDBModel]):
            A dictionary of existing `StructureServiceThingNodeDBModel` objects, mapped by
            `(stakeholder_key, external_id)` tuples.

    Raises:
        DBIntegrityError:
            If an integrity error occurs during the upsert operation, such as a unique constraint
            violation.
        DBUpdateError:
            If any other error occurs during the upsert operation.
    """

    try:
        required_keys = {
            (node.stakeholder_key, node.element_type_external_id) for node in thing_nodes
        }
        element_type_stmt = select(StructureServiceElementTypeDBModel).where(
            tuple_(
                StructureServiceElementTypeDBModel.stakeholder_key,
                StructureServiceElementTypeDBModel.external_id,
            ).in_(required_keys)
        )
        element_types = {
            (et.stakeholder_key, et.external_id): et
            for et in session.execute(element_type_stmt).scalars().all()
        }

        new_records = []
        for node in thing_nodes:
            key = (node.stakeholder_key, node.external_id)
            db_node = existing_thing_nodes.get(key)

            element_type = element_types.get((node.stakeholder_key, node.element_type_external_id))
            if not element_type:
                logger.warning(
                    "StructureServiceElementType with key (%s, %s) not found for "
                    "StructureServiceThingNode %s. Skipping update.",
                    node.stakeholder_key,
                    node.element_type_external_id,
                    node.name,
                )
                continue

            if db_node:
                logger.debug("Updating StructureServiceThingNodeDBModel with key %s.", key)
                db_node.name = node.name
                db_node.description = node.description
                db_node.element_type_id = element_type.id
                db_node.meta_data = node.meta_data
                db_node.parent_node_id = node.parent_node_id
                db_node.parent_external_node_id = node.parent_external_node_id
            else:
                new_records.append(
                    StructureServiceThingNodeDBModel(
                        id=node.id,
                        external_id=node.external_id,
                        stakeholder_key=node.stakeholder_key,
                        name=node.name,
                        description=node.description,
                        parent_node_id=node.parent_node_id,
                        parent_external_node_id=node.parent_external_node_id,
                        element_type_id=element_type.id,
                        element_type_external_id=node.element_type_external_id,
                        meta_data=node.meta_data,
                    )
                )

        if new_records:
            session.bulk_save_objects(new_records)

        session.flush()

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceThingNodeDBModel"
        ) from e
    except Exception as e:
        logger.error("Error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBUpdateError("Error while upserting StructureServiceThingNodeDBModel") from e
