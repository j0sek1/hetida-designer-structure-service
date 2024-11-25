import logging
from itertools import batched
from math import ceil
from uuid import UUID

from sqlalchemy import Connection, Engine, tuple_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.postgresql.dml import Insert as pg_insert_typing
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.sqlite.dml import Insert as sqlite_insert_typing
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
from hetdesrun.structure.utils import is_postgresql, is_sqlite

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
    session: SQLAlchemySession, keys: set[tuple[str, str]], batch_size: int = 500
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
    existing_element_types: dict[tuple[str, str], StructureServiceElementTypeDBModel],
) -> dict[tuple[str, str], StructureServiceThingNodeDBModel]:
    """Insert or update thing node records in the database.

    For each StructureServiceThingNode, updates existing records if they are found;
    otherwise, creates new records. Uses provided element types and returns the
    thing node as a dictionary indexed by (stakeholder_key, external_id).
    """

    # Prepare thing node records
    # Ensure the associated element type exists
    # to prevent foreign key constraint errors
    thing_node_dicts = []
    for node in thing_nodes:
        element_type = existing_element_types.get(
            (node.stakeholder_key, node.element_type_external_id)
        )
        if not element_type:
            logger.warning(
                "StructureServiceElementType with key (%s, %s) not found for "
                "StructureServiceThingNode %s. Skipping update.",
                node.stakeholder_key,
                node.element_type_external_id,
                node.name,
            )
            continue

        # Use node.dict() and add/override specific fields
        node_dict = node.dict()
        node_dict.update(
            {
                "element_type_id": element_type.id,  # Add foreign key
            }
        )
        thing_node_dicts.append(node_dict)

    if not thing_node_dicts:
        return {}

    try:
        engine: Engine | Connection = session.get_bind()
        if isinstance(engine, Connection):
            raise ValueError("The session in use has to be bound to an Engine, not a Connection.")

        upsert_stmt: sqlite_insert_typing | pg_insert_typing

        if is_postgresql(engine):
            upsert_stmt = pg_insert(StructureServiceThingNodeDBModel).values(thing_node_dicts)
        elif is_sqlite(engine):
            upsert_stmt = sqlite_insert(StructureServiceThingNodeDBModel).values(thing_node_dicts)
        else:
            raise ValueError(
                f"Unsupported database engine: {engine}. Please use either Postgres or SQLite."
            )

        excluded_dict = {}
        for col in thing_node_dicts[0]:
            # Exclude primary key (id) and parent_node_id as it is not read from db
            # but created in CompleteStructure
            if col in ("id", "parent_node_id"):
                continue
            excluded_dict[col] = upsert_stmt.excluded[col]

        upsert_stmt = upsert_stmt.on_conflict_do_update(
            index_elements=[
                "external_id",
                "stakeholder_key",
            ],  # Columns where insert looks for a conflict
            set_=excluded_dict,
        ).returning(StructureServiceThingNodeDBModel)  # type: ignore

        # ORM models returned by the upsert query
        thing_node_dbmodels = session.scalars(
            upsert_stmt,
            execution_options={"populate_existing": True},
        )

        # Convert to dictionary indexed by (stakeholder_key, external_id)
        thing_node_dbmodel_dict = {
            (tn.stakeholder_key, tn.external_id): tn for tn in thing_node_dbmodels
        }

        return thing_node_dbmodel_dict

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceThingNodeDBModel"
        ) from e
    except ValueError as e:
        logger.error("Value error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBError("Value error while upserting StructureServiceThingNodeDBModel") from e
    except Exception as e:
        logger.error("Unexpected error while upserting StructureServiceThingNodeDBModel: %s", e)
        raise DBUpdateError(
            "Unexpected error while upserting StructureServiceThingNodeDBModel"
        ) from e
