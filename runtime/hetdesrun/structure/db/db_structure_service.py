import json
import logging
from collections import defaultdict, deque
from uuid import UUID

from sqlalchemy import and_, delete
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession, get_session
from hetdesrun.persistence.structure_service_dbmodels import (
    ElementTypeOrm,
    SinkOrm,
    SourceOrm,
    ThingNodeOrm,
    thingnode_sink_association,
    thingnode_source_association,
)
from hetdesrun.structure.db.element_type_service import (
    fetch_element_types,
    upsert_element_types,
)
from hetdesrun.structure.db.exceptions import (
    DBAssociationError,
    DBConnectionError,
    DBError,
    DBIntegrityError,
    DBParsingError,
    DBUpdateError,
)
from hetdesrun.structure.db.source_sink_service import (
    fetch_sinks,
    fetch_sources,
    upsert_sinks,
    upsert_sources,
)
from hetdesrun.structure.db.thing_node_service import fetch_thing_nodes, upsert_thing_nodes
from hetdesrun.structure.models import (
    CompleteStructure,
    Sink,
    Source,
    ThingNode,
)

logger = logging.getLogger(__name__)


def orm_load_structure_from_json_file(file_path: str) -> CompleteStructure:
    """
    Loads the structure from a JSON file.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        CompleteStructure: The loaded CompleteStructure object.

    Raises:
        FileNotFoundError: If the JSON file is not found at the given path.
        DBParsingError: If an error occurs while parsing or validating the JSON structure.
        DBError: If any other unexpected error occurs.
    """
    logger.debug("Loading structure from JSON file at %s.", file_path)
    try:
        with open(file_path) as file:
            structure_json = json.load(file)
        logger.debug("Successfully loaded JSON from %s.", file_path)

        # Attempt to create a CompleteStructure from the loaded JSON data
        complete_structure = CompleteStructure(**structure_json)
        logger.debug("Successfully created CompleteStructure from JSON data.")

        return complete_structure

    except FileNotFoundError:
        # If the file is not found, log an error and re-raise the exception
        logger.error("File not found: %s", file_path)
        raise

    except json.JSONDecodeError as e:
        # If there is a JSON parsing error, raise a specific DBParsingError
        logger.error("JSON parsing error in file %s: %s", file_path, str(e))
        raise DBParsingError(f"Error parsing JSON structure in file {file_path}: {str(e)}") from e

    except TypeError as e:
        # If there is a TypeError while converting JSON data to CompleteStructure,
        # raise a DBParsingError
        logger.error("Type error while creating CompleteStructure from %s: %s", file_path, str(e))
        raise DBParsingError(
            f"Error converting JSON data to CompleteStructure from file {file_path}: {str(e)}"
        ) from e

    except Exception as e:
        # Catch any other unexpected exceptions and raise a general DBError
        logger.error(
            "Unexpected error while loading or parsing structure from %s: %s", file_path, str(e)
        )
        raise DBError(
            f"Unexpected error while loading or parsing structure from {file_path}: {str(e)}"
        ) from e


def sort_thing_nodes(
    thing_nodes: list[ThingNode], existing_thing_nodes: dict[tuple[str, str], ThingNodeOrm]
) -> list[ThingNode]:
    """
    Sorts ThingNodes into hierarchical levels and flattens the structure,
    excluding orphan nodes (nodes without valid parent).

    Args:
        thing_nodes (list[ThingNode]): The ThingNodes to sort.
        existing_thing_nodes (dict[tuple[str, str], ThingNodeOrm]):
            Existing ThingNodes from the database.

    Returns:
        list[ThingNode]: A flat, sorted list of ThingNodes, excluding orphan nodes.

    Note:
        This function assumes that all ThingNodes have an ID assigned.
    """
    logger.debug("Sorting and flattening ThingNodes, excluding orphan nodes.")

    # Create a mapping for quick parent lookup
    thing_node_map = {(tn.stakeholder_key, tn.external_id): tn for tn in thing_nodes}

    # Assign IDs from existing database entries
    for tn in thing_nodes:
        key = (tn.stakeholder_key, tn.external_id)
        if key in existing_thing_nodes:
            tn.id = existing_thing_nodes[key].id
            logger.debug("ThingNode %s matched existing node with ID %s.", tn.name, tn.id)
        else:
            logger.debug("ThingNode %s is new with ID %s.", tn.name, tn.id)

    # Build child lists per node ID and handle root nodes
    children_by_node_id: dict[UUID, list[ThingNode]] = defaultdict(list)
    root_nodes: list[ThingNode] = []

    for tn in thing_nodes:
        if tn.parent_external_node_id:
            parent_key = (tn.stakeholder_key, tn.parent_external_node_id)
            parent_tn = thing_node_map.get(parent_key)

            if parent_tn:
                children_by_node_id[parent_tn.id].append(tn)
                logger.debug("ThingNode %s added as child to parent ID %s.", tn.name, parent_tn.id)
                tn.parent_node_id = parent_tn.id
            else:
                # Exclude orphan nodes with missing parent
                logger.warning(
                    "Orphan node detected: Parent ThingNode with key %s not found for ThingNode %s."
                    "Excluding from sort.",
                    parent_key,
                    tn.name,
                )
        else:
            root_nodes.append(tn)
            logger.debug("ThingNode %s identified as root node.", tn.name)

    # Sort using BFS
    sorted_nodes_by_level = defaultdict(list)
    queue = deque([(root_nodes, 0)])

    while queue:
        current_level_nodes, level = queue.popleft()
        next_level_nodes = []

        logger.debug("Processing level %d with %d nodes.", level, len(current_level_nodes))

        for node in current_level_nodes:
            sorted_nodes_by_level[level].append(node)
            children = children_by_node_id.get(node.id, [])
            children_sorted = sorted(children, key=lambda x: x.external_id)
            next_level_nodes.extend(children_sorted)

        if next_level_nodes:
            queue.append((next_level_nodes, level + 1))
            logger.debug("Queueing %d nodes for level %d.", len(next_level_nodes), level + 1)

    # Flatten the sorted levels, excluding orphan nodes
    flattened_nodes = [
        node
        for level in sorted(sorted_nodes_by_level.keys())
        for node in sorted_nodes_by_level[level]
    ]
    logger.debug(
        "Flattened ThingNodes into a list of %d nodes, excluding orphan nodes.",
        len(flattened_nodes),
    )

    return flattened_nodes


def populate_element_type_ids(
    thing_nodes: list[ThingNode], existing_element_types: dict[tuple[str, str], ElementTypeOrm]
) -> None:
    """
    Sets the element_type_id for each ThingNode based on existing ElementTypes.

    Args:
        thing_nodes (list[ThingNode]): The ThingNodes to populate element_type_id.
        existing_element_types (dict[tuple[str, str], ElementTypeOrm]):
            Existing ElementTypes from the database.
    """
    logger.debug("Populating element_type_id for ThingNodes.")
    for tn in thing_nodes:
        if tn.element_type_external_id:
            key = (tn.stakeholder_key, tn.element_type_external_id)
            db_et = existing_element_types.get(key)
            if db_et:
                tn.element_type_id = db_et.id
                logger.debug("Set element_type_id %s for ThingNode %s.", db_et.id, tn.external_id)
            else:
                logger.warning(
                    "ElementType with key %s not found for ThingNode %s.", key, tn.external_id
                )


def orm_update_structure(complete_structure: CompleteStructure) -> CompleteStructure:
    logger.debug("Starting update or insert operation for the complete structure in the database.")
    try:
        with get_session()() as session, session.begin():
            # Disable autoflush temporarily to prevent premature inserts
            with session.no_autoflush:
                # 1. Extract relevant keys
                element_type_keys = {
                    (et.stakeholder_key, et.external_id) for et in complete_structure.element_types
                }
                thing_node_keys = {
                    (tn.stakeholder_key, tn.external_id) for tn in complete_structure.thing_nodes
                }
                source_keys = {
                    (src.stakeholder_key, src.external_id) for src in complete_structure.sources
                }
                sink_keys = {
                    (snk.stakeholder_key, snk.external_id) for snk in complete_structure.sinks
                }

                # 2. Batch query existing records
                existing_element_types = fetch_element_types(session, element_type_keys)
                existing_thing_nodes = fetch_thing_nodes(session, thing_node_keys)
                existing_sources = fetch_sources(session, source_keys)
                existing_sinks = fetch_sinks(session, sink_keys)

            # 3. Upsert ElementTypes
            upsert_element_types(session, complete_structure.element_types, existing_element_types)

            # 4. Re-fetch existing_element_types after Upsert
            existing_element_types = fetch_element_types(session, element_type_keys)

            # 5. Upsert ThingNodes
            sorted_thing_nodes = sort_thing_nodes(
                complete_structure.thing_nodes, existing_thing_nodes
            )
            populate_element_type_ids(sorted_thing_nodes, existing_element_types)
            upsert_thing_nodes(session, sorted_thing_nodes, existing_thing_nodes)

            # Update the mapping of ThingNodes after upsert
            existing_thing_nodes = fetch_thing_nodes(session, thing_node_keys)

            # 6. Upsert Sources and Sinks
            upsert_sources(
                session, complete_structure.sources, existing_sources, existing_thing_nodes
            )
            upsert_sinks(session, complete_structure.sinks, existing_sinks, existing_thing_nodes)

            # Update the mapping of Sources and Sinks after upsert
            existing_sources = fetch_sources(session, source_keys)
            existing_sinks = fetch_sinks(session, sink_keys)

    except IntegrityError as e:
        logger.error("Integrity Error while updating or inserting the structure: %s", e)
        raise DBIntegrityError("Integrity Error while updating or inserting the structure") from e
    except OperationalError as e:
        logger.error("Operational Error while updating or inserting the structure: %s", e)
        raise DBConnectionError(
            "Operational Error while updating or inserting the structure"
        ) from e
    except DBAssociationError as e:
        logger.error("Association Error: %s", e)
        raise
    except DBUpdateError as e:
        logger.error("Update Error: %s", e)
        raise
    except DBError as e:
        logger.error("General DB Error: %s", e)
        raise
    except Exception as e:
        logger.error("Unexpected Error while updating or inserting the structure: %s", e)
        raise DBError("Unexpected Error while updating or inserting the structure") from e

    logger.debug("Completed update or insert operation for the complete structure.")
    return complete_structure


def update_structure_from_file(file_path: str) -> CompleteStructure:
    """
    Aktualisiert die Struktur basierend auf einer JSON-Datei.

    Args:
        file_path (str): Der Pfad zur JSON-Datei, die die Struktur definiert.

    Returns:
        CompleteStructure: Die aktualisierte Struktur nach dem Updaten.
    """
    logger.debug("Updating structure from JSON file at path: %s.", file_path)

    try:
        complete_structure: CompleteStructure = orm_load_structure_from_json_file(file_path)
        logger.debug("Successfully loaded structure from JSON file.")

        updated_structure: CompleteStructure = orm_update_structure(complete_structure)
        logger.debug("Successfully updated structure in the database.")

        return updated_structure

    except SQLAlchemyError as e:
        logger.error("Database error occurred while updating structure: %s", e)
        raise
    except Exception as e:
        logger.error("An unexpected error occurred while updating structure: %s", e)
        raise


def orm_is_database_empty() -> bool:
    logger.debug("Checking if the database is empty.")
    with get_session()() as session:
        element_type_exists = session.query(ElementTypeOrm).first() is not None
        thing_node_exists = session.query(ThingNodeOrm).first() is not None
        source_exists = session.query(SourceOrm).first() is not None
        sink_exists = session.query(SinkOrm).first() is not None
        # TODO: Shorten function by only checking for ElementTypes?

    is_empty = not (element_type_exists or thing_node_exists or source_exists or sink_exists)
    logger.debug("Database empty status: %s", is_empty)

    return is_empty


def orm_get_children(
    parent_id: UUID | None,
) -> tuple[list[ThingNode], list[Source], list[Sink]]:
    """
    Retrieves the child nodes, sources, and sinks associated with a given parent
    node from the database.

    If `parent_id` is None, it returns the root nodes (nodes without a parent),
    along with any sources and sinks associated with the root nodes. Otherwise,
    it fetches the direct child nodes, sources, and sinks associated with the
    specified parent node.
    """

    logger.debug("Fetching children for parent_id: %s", parent_id)

    with get_session()() as session:
        # Fetch ThingNodes where parent_id matches
        child_nodes_orm = (
            session.query(ThingNodeOrm).filter(ThingNodeOrm.parent_node_id == parent_id).all()
        )
        logger.debug("Fetched %d child nodes.", len(child_nodes_orm))

        if parent_id is None:
            # Handle root nodes separately if needed
            logger.debug("Fetching sources and sinks for root nodes.")
            sources_orm = []
            sinks_orm = []
        else:
            # Fetch the parent node to get its stakeholder_key and external_id
            parent_node = (
                session.query(ThingNodeOrm).filter(ThingNodeOrm.id == parent_id).one_or_none()
            )

            if parent_node is None:
                logger.warning("Parent node with id %s not found.", parent_id)
                return ([], [], [])

            parent_stakeholder_key = parent_node.stakeholder_key
            parent_external_id = parent_node.external_id

            # Fetch Sources associated with this ThingNode
            sources_orm = (
                session.query(SourceOrm)
                .join(
                    thingnode_source_association,
                    and_(
                        SourceOrm.stakeholder_key
                        == thingnode_source_association.c.source_stakeholder_key,
                        SourceOrm.external_id == thingnode_source_association.c.source_external_id,
                    ),
                )
                .filter(
                    thingnode_source_association.c.thing_node_stakeholder_key
                    == parent_stakeholder_key,
                    thingnode_source_association.c.thing_node_external_id == parent_external_id,
                )
                .all()
            )
            logger.debug("Fetched %d sources.", len(sources_orm))

            # Fetch Sinks associated with this ThingNode
            sinks_orm = (
                session.query(SinkOrm)
                .join(
                    thingnode_sink_association,
                    and_(
                        SinkOrm.stakeholder_key
                        == thingnode_sink_association.c.sink_stakeholder_key,
                        SinkOrm.external_id == thingnode_sink_association.c.sink_external_id,
                    ),
                )
                .filter(
                    thingnode_sink_association.c.thing_node_stakeholder_key
                    == parent_stakeholder_key,
                    thingnode_sink_association.c.thing_node_external_id == parent_external_id,
                )
                .all()
            )
            logger.debug("Fetched %d sinks.", len(sinks_orm))

        return (
            [ThingNode.from_orm_model(node) for node in child_nodes_orm],
            [Source.from_orm_model(source) for source in sources_orm],
            [Sink.from_orm_model(sink) for sink in sinks_orm],
        )


def orm_delete_structure(session: SQLAlchemySession) -> None:
    """
    Deletes all structure-related data from the database, including ThingNodes, Sources, Sinks,
    ElementTypes, and their associations.

    This function ensures that records are deleted in the correct order
    to maintain referential integrity.
    Association tables are cleared first, followed by dependent ORM classes.

    Args:
        session (SQLAlchemySession): The active SQLAlchemy session to use for deletion.

    Raises:
        DBIntegrityError: If an integrity error occurs during the deletion process.
        DBError: If any other database error occurs.
    """
    logger.debug("Starting deletion of all structure data from the database.")

    # Define the order of deletion to maintain referential integrity:
    # Association tables first, followed by dependent ORM classes.
    deletion_order = [
        thingnode_source_association,
        thingnode_sink_association,
        SourceOrm,
        SinkOrm,
        ThingNodeOrm,
        ElementTypeOrm,
    ]

    try:
        for table in deletion_order:
            table_name = table.name if hasattr(table, "name") else table.__tablename__  # type: ignore
            logger.debug("Deleting records from table: %s", table_name)
            session.execute(delete(table))
        session.commit()
        logger.debug("Successfully deleted all structure data from the database.")

    except IntegrityError as e:
        msg = f"Integrity Error while deleting structure: {str(e)}"
        logger.error(msg)
        raise DBIntegrityError(msg) from e
    except SQLAlchemyError as e:
        msg = f"Database Error while deleting structure: {str(e)}"
        logger.error(msg)
        raise DBError(msg) from e
    except Exception as e:
        msg = f"Unexpected Error while deleting structure: {str(e)}"
        logger.error(msg)
        raise DBError(msg) from e
