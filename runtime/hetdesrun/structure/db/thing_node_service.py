import logging
from itertools import batched
from math import ceil
from uuid import UUID

from sqlalchemy import tuple_
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
    Upserts StructureServiceThingNodeDBModel records using SQLAlchemy's merge functionality.
    Creates new records if they do not exist.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        thing_nodes (List[StructureServiceThingNode]):
            The list of StructureServiceThingNode objects to upsert.
        existing_thing_nodes (Dict[Tuple[str, str], StructureServiceThingNodeDBModel]):
            Existing StructureServiceThingNodeDBModel objects
            mapped by (stakeholder_key, external_id).


    Returns:
    None

    Raises:
        DBIntegrityError: If an integrity error occurs during the upsert operation.
        DBUpdateError: If any other error occurs during the upsert operation.
    """
    try:
        # Prevents SQLAlchemy from flushing the session automatically,
        # which could cause foreign key issues.
        with session.no_autoflush:
            for node in thing_nodes:
                key = (node.stakeholder_key, node.external_id)
                db_node = existing_thing_nodes.get(key)

                # Ensure the related StructureServiceElementType exists before
                # attempting to update or create StructureServiceThingNode.
                # This avoids foreign key constraint violations.
                element_type = (
                    session.query(StructureServiceElementTypeDBModel)
                    .filter_by(
                        external_id=node.element_type_external_id,
                        stakeholder_key=node.stakeholder_key,
                    )
                    .first()
                )
                if not element_type:
                    # If the StructureServiceElementType doesn't exist,
                    # skip updating or creating the StructureServiceThingNode.
                    logger.warning(
                        "StructureServiceElementType with key (%s, %s) not found for "
                        "StructureServiceThingNode %s. Skipping update.",
                        node.stakeholder_key,
                        node.element_type_external_id,
                        node.name,
                    )
                    continue  # Skipping this node as the StructureServiceElementType is missing.

                if db_node:
                    # Update existing StructureServiceThingNode if
                    # it already exists in the database.
                    logger.debug("Updating StructureServiceThingNodeDBModel with key %s.", key)
                    db_node.name = node.name
                    db_node.description = node.description
                    db_node.element_type_id = (
                        element_type.id
                    )  # Assign the correct StructureServiceElementType relationship.
                    db_node.meta_data = node.meta_data
                    db_node.parent_node_id = node.parent_node_id
                    db_node.parent_external_node_id = node.parent_external_node_id

                    session.merge(
                        db_node
                    )  # Merge the updated data into the existing StructureServiceThingNode.
                else:
                    # Create a new StructureServiceThingNode if
                    # it doesn't already exist in the database.
                    logger.debug("Creating new StructureServiceThingNodeDBModel with key %s.", key)
                    new_node = StructureServiceThingNodeDBModel(
                        id=node.id,
                        external_id=node.external_id,
                        stakeholder_key=node.stakeholder_key,
                        name=node.name,
                        description=node.description,
                        parent_node_id=node.parent_node_id,
                        parent_external_node_id=node.parent_external_node_id,
                        element_type=element_type,
                        element_type_external_id=node.element_type_external_id,
                        meta_data=node.meta_data,
                    )
                    session.add(new_node)  # Add the new StructureServiceThingNode to the session.

        # Explicitly flush all changes at once to ensure data is written to the database.
        session.flush()

    except IntegrityError as e:
        # Handle IntegrityErrors, typically caused by foreign key or unique constraint violations.
        logger.error("Integrity Error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceThingNodeDBModel"
        ) from e
    except Exception as e:
        # Handle any other general exceptions that occur during the upsert process.
        logger.error("Error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBUpdateError("Error while upserting StructureServiceThingNodeDBModel") from e
