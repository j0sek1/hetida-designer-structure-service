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
    """Retrieve a single StructureServiceThingNode by its unique ID.

    Returns the thing node if found.
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

    logger.warning("No StructureServiceThingNode found for ID %s. Raising DBNotFoundError.", tn_id)
    raise DBNotFoundError(f"No StructureServiceThingNode found for ID {tn_id}")


def fetch_thing_nodes(
    session: SQLAlchemySession, keys: set[tuple[str, str]], batch_size: int = 500
) -> dict[tuple[str, str], StructureServiceThingNodeDBModel]:
    """Retrieve StructureServiceThingNodeDBModel records by stakeholder_key and external_id.

    Returns a dictionary mapping keys to StructureServiceThingNodeDBModel instances  in batches.
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
            "Fetched %d StructureServiceThingNodeDBModel items from the database for %d keys.",
            len(existing_tns_mapping),
            len(keys),
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
    """Search for StructureServiceThingNodeDBModel records by partial or full name match.

    Returns a list of matching StructureServiceThingNodeDBModel instances.
    """
    try:
        thing_nodes = (
            session.query(StructureServiceThingNodeDBModel)
            .filter(StructureServiceThingNodeDBModel.name.ilike(f"%{name_query}%"))
            .all()
        )
        logger.debug(
            "Found %d StructureServiceThingNodeDBModel items matching "
            "name query '%s' from %d total records.",
            len(thing_nodes),
            name_query,
            session.query(StructureServiceThingNodeDBModel).count(),
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
    """Insert or update StructureServiceThingNodeDBModel records.

    Updates existing records or creates new ones if they do not exist.
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

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceThingNodeDBModel"
        ) from e
    except Exception as e:
        logger.error("Error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBUpdateError("Error while upserting StructureServiceThingNodeDBModel") from e
