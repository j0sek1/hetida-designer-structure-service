import logging
from itertools import batched
from math import ceil
from uuid import UUID

from sqlalchemy import bindparam, select, tuple_
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
from hetdesrun.structure.db.utils import get_insert_statement
from hetdesrun.structure.models import StructureServiceThingNode

logger = logging.getLogger(__name__)


def fetch_single_thing_node_from_db_by_id(tn_id: UUID) -> StructureServiceThingNode:
    """Fetch a single thing node by its unique ID.

    Looks up a StructureServiceThingNode in the database by its ID. Returns the node
    if found; raises DBNotFoundError if no matching record is found.
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

    logger.warning("No StructureServiceThingNode found for ID %s.", tn_id)
    raise DBNotFoundError(f"No StructureServiceThingNode found for ID {tn_id}")


def fetch_thing_nodes(
    session: SQLAlchemySession, keys: set[tuple[str, str]]
) -> dict[tuple[str, str], StructureServiceThingNodeDBModel]:
    """Fetch thing nodes records by stakeholder_key and external_id.

    Retrieves StructureServiceThingNodeDBModel records matching the provided keys
    (stakeholder_key, external_id) and returns a dictionary mapping keys to the
    corresponding database instances.
    """
    existing_tns_mapping: dict[tuple[str, str], StructureServiceThingNodeDBModel] = {}
    if not keys:
        return existing_tns_mapping
    try:
        query = session.query(StructureServiceThingNodeDBModel).filter(
            tuple_(
                StructureServiceThingNodeDBModel.stakeholder_key,
                StructureServiceThingNodeDBModel.external_id,
            ).in_(keys)
        )
        results = query.all()
        for tn in results:
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
    """Search for thing nodes by name substring.

    Performs a case-insensitive search for StructureServiceThingNodeDBModel records
    whose names contain the provided substring. Returns a list of matching instances.
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
) -> None:
    """Upsert ThingNodes using INSERT...ON CONFLICT.

    Performs an upsert operation on ThingNodes, using a subquery
    within the INSERT statement to set the element_type_id based on the
    element_type_external_id and stakeholder_key.
    """
    try:
        # Prepare data for upsert without fetching element_type_id
        insert_data = [
            {
                "id": node.id,
                "external_id": node.external_id,
                "stakeholder_key": node.stakeholder_key,
                "name": node.name,
                "description": node.description,
                "parent_node_id": node.parent_node_id,
                "parent_external_node_id": node.parent_external_node_id,
                "element_type_external_id": node.element_type_external_id,
                "meta_data": node.meta_data,
                "et_stakeholder_key": node.stakeholder_key,
                "et_external_id": node.element_type_external_id,
            }
            for node in thing_nodes
        ]

        # Build the subquery to fetch element_type_id based on external_id and stakeholder_key
        element_type_subquery = (
            select(StructureServiceElementTypeDBModel.id)
            .where(
                StructureServiceElementTypeDBModel.stakeholder_key
                == bindparam("et_stakeholder_key"),
                StructureServiceElementTypeDBModel.external_id == bindparam("et_external_id"),
            )
            .scalar_subquery()
        )

        # Construct the insert statement with the subquery for element_type_id
        insert_stmt = get_insert_statement(session, StructureServiceThingNodeDBModel).values(
            id=bindparam("id"),
            external_id=bindparam("external_id"),
            stakeholder_key=bindparam("stakeholder_key"),
            name=bindparam("name"),
            description=bindparam("description"),
            parent_node_id=bindparam("parent_node_id"),
            parent_external_node_id=bindparam("parent_external_node_id"),
            element_type_id=element_type_subquery,
            element_type_external_id=bindparam("element_type_external_id"),
            meta_data=bindparam("meta_data"),
        )

        # Define the upsert statement with conflict handling
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["external_id", "stakeholder_key"],
            set_={
                "name": insert_stmt.excluded.name,
                "description": insert_stmt.excluded.description,
                "parent_node_id": insert_stmt.excluded.parent_node_id,
                "parent_external_node_id": insert_stmt.excluded.parent_external_node_id,
                "element_type_id": insert_stmt.excluded.element_type_id,
                "element_type_external_id": insert_stmt.excluded.element_type_external_id,
                "meta_data": insert_stmt.excluded.meta_data,
            },
        )

        # Execute the upsert statement with the prepared data
        session.execute(upsert_stmt, insert_data)

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceThingNodeDBModel"
        ) from e
    except Exception as e:
        logger.error("Unexpected error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBUpdateError(
            "Unexpected error while upserting StructureServiceThingNodeDBModel"
        ) from e
