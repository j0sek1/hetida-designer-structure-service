import logging
from itertools import batched
from math import ceil
from uuid import UUID

from sqlalchemy import and_, bindparam, delete, insert, select, tuple_
from sqlalchemy.exc import IntegrityError

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession, get_session
from hetdesrun.persistence.structure_service_dbmodels import (
    StructureServiceSinkDBModel,
    StructureServiceSourceDBModel,
    StructureServiceThingNodeDBModel,
    thingnode_sink_association,
    thingnode_source_association,
)
from hetdesrun.structure.db.exceptions import (
    DBError,
    DBIntegrityError,
    DBNotFoundError,
    DBUpdateError,
)
from hetdesrun.structure.db.utils import get_insert_statement
from hetdesrun.structure.models import (
    StructureServiceSink,
    StructureServiceSource,
)

logger = logging.getLogger(__name__)


def fetch_all_sources_from_db() -> list[StructureServiceSource]:
    """Fetch all source records from the database.

    Retrieves all instances of StructureServiceSource from the database and returns
    them as a list of StructureServiceSource objects.
    """
    logger.debug("Fetching all StructureServiceSources from the database.")
    with get_session()() as session:
        sources = session.query(StructureServiceSourceDBModel).all()

    logger.debug("Successfully fetched %d sources from the database.", len(sources))
    return [StructureServiceSource.from_orm_model(source) for source in sources]


def fetch_all_sinks_from_db() -> list[StructureServiceSink]:
    """Fetch all sink records from the database.

    Retrieves all instances of StructureServiceSink from the database and returns
    them as a list of StructureServiceSink objects.
    """
    logger.debug("Fetching all StructureServiceSinks from the database.")
    with get_session()() as session:
        sinks = session.query(StructureServiceSinkDBModel).all()

    logger.debug("Successfully fetched %d sinks from the database.", len(sinks))
    return [StructureServiceSink.from_orm_model(sink) for sink in sinks]


def fetch_single_sink_from_db_by_id(sink_id: UUID) -> StructureServiceSink:
    """Fetch a single sink by its ID.

    Looks up a StructureServiceSink object using its unique identifier.
    """
    logger.debug("Fetching single StructureServiceSink from database with ID: %s", sink_id)
    with get_session()() as session:
        sink = (
            session.query(StructureServiceSinkDBModel)
            .filter(StructureServiceSinkDBModel.id == sink_id)
            .one_or_none()
        )
        if sink:
            logger.debug("StructureServiceSink with ID %s found.", sink_id)
            return StructureServiceSink.from_orm_model(sink)

    logger.warning("No StructureServiceSink found for ID %s.", sink_id)
    raise DBNotFoundError(f"No StructureServiceSink found for ID {sink_id}")


def fetch_single_source_from_db_by_id(src_id: UUID) -> StructureServiceSource:
    """Fetch a single source by its ID.

    Looks up a StructureServiceSource object using its unique identifier.
    """
    logger.debug("Fetching single StructureServiceSource from database with ID: %s", src_id)
    with get_session()() as session:
        source = (
            session.query(StructureServiceSourceDBModel)
            .filter(StructureServiceSourceDBModel.id == src_id)
            .one_or_none()
        )
        if source:
            logger.debug("StructureServiceSource with ID %s found.", src_id)
            return StructureServiceSource.from_orm_model(source)

    logger.warning("No StructureServiceSource found for ID %s.", src_id)
    raise DBNotFoundError(f"No StructureServiceSource found for ID {src_id}")


def fetch_collection_of_sources_from_db_by_id(
    src_ids: list[UUID], batch_size: int = 500
) -> dict[UUID, StructureServiceSource]:
    """Fetch multiple sources by their unique IDs.

    Retrieves a collection of StructureServiceSource records from the database,
    returning a dictionary that maps each source ID to its corresponding record.
    """
    sources: dict[UUID, StructureServiceSource] = {}
    if not src_ids:
        return sources

    logger.debug(
        "Successfully fetched collection of %d StructureServiceSources "
        "from the database for %d IDs. StructureServiceSources with IDs: %s",
        len(sources),
        len(src_ids),
        src_ids,
    )
    with get_session()() as session:
        for id_batch in batched(src_ids, ceil(len(src_ids) / batch_size)):
            batch_query = session.query(StructureServiceSourceDBModel).filter(
                StructureServiceSourceDBModel.id.in_(id_batch)
            )
            batch_results = batch_query.all()
            for src in batch_results:
                sources[src.id] = StructureServiceSource.from_orm_model(src)

    if not sources:
        raise DBNotFoundError(f"No StructureServiceSources found for IDs {src_ids}")

    logger.debug("Successfully fetched collection of StructureServiceSources.")
    return sources


def fetch_collection_of_sinks_from_db_by_id(
    sink_ids: list[UUID], batch_size: int = 500
) -> dict[UUID, StructureServiceSink]:
    """Fetch multiple sinks by their unique IDs.

    Retrieves a collection of StructureServiceSink records from the database,
    returning a dictionary that maps each sink ID to its corresponding record.
    """
    sinks: dict[UUID, StructureServiceSink] = {}
    if not sink_ids:
        return sinks

    logger.debug("Fetching collection of StructureServiceSinks with IDs: %s", sink_ids)

    logger.debug(
        "Successfully fetched collection of %d StructureServiceSinks from the database for %d IDs. "
        "StructureServiceSinks with IDs: %s",
        len(sinks),
        len(sink_ids),
        sink_ids,
    )
    with get_session()() as session:
        for id_batch in batched(sink_ids, ceil(len(sink_ids) / batch_size)):
            batch_query = session.query(StructureServiceSinkDBModel).filter(
                StructureServiceSinkDBModel.id.in_(id_batch)
            )
            batch_results = batch_query.all()
            for sink in batch_results:
                sinks[sink.id] = StructureServiceSink.from_orm_model(sink)

    if not sinks:
        raise DBNotFoundError(f"No StructureServiceSinks found for IDs {sink_ids}")

    logger.debug("Successfully fetched collection of StructureServiceSinks.")
    return sinks


def fetch_sources(
    session: SQLAlchemySession, keys: set[tuple[str, str]]
) -> dict[tuple[str, str], StructureServiceSourceDBModel]:
    """Fetch source records by stakeholder_key and external_id.

    Retrieves StructureServiceSourceDBModel records matching the provided keys
    (stakeholder_key, external_id) and returns a dictionary mapping keys to the
    corresponding database instances.
    """
    existing_sources_mapping: dict[tuple[str, str], StructureServiceSourceDBModel] = {}
    if not keys:
        return existing_sources_mapping
    try:
        query = session.query(StructureServiceSourceDBModel).filter(
            tuple_(
                StructureServiceSourceDBModel.stakeholder_key,
                StructureServiceSourceDBModel.external_id,
            ).in_(keys)
        )
        results = query.all()
        for source in results:
            key = (source.stakeholder_key, source.external_id)
            existing_sources_mapping[key] = source
        logger.debug(
            "Fetched %d StructureServiceSourceDBModel items from the database for %d keys.",
            len(existing_sources_mapping),
            len(keys),
        )
        return existing_sources_mapping
    except IntegrityError as e:
        logger.error("Integrity Error while fetching StructureServiceSourceDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while fetching StructureServiceSourceDBModel"
        ) from e
    except Exception as e:
        logger.error("Unexpected error while fetching StructureServiceSourceDBModel: %s", e)
        raise DBError("Unexpected error while fetching StructureServiceSourceDBModel") from e


def fetch_sinks(
    session: SQLAlchemySession, keys: set[tuple[str, str]]
) -> dict[tuple[str, str], StructureServiceSinkDBModel]:
    """Fetch sink records by stakeholder_key and external_id.

    Retrieves StructureServiceSinkDBModel records matching the provided keys
    (stakeholder_key, external_id) and returns a dictionary mapping keys to the
    corresponding database instances.
    """
    existing_sinks_mapping: dict[tuple[str, str], StructureServiceSinkDBModel] = {}
    if not keys:
        return existing_sinks_mapping
    try:
        query = session.query(StructureServiceSinkDBModel).filter(
            tuple_(
                StructureServiceSinkDBModel.stakeholder_key,
                StructureServiceSinkDBModel.external_id,
            ).in_(keys)
        )
        results = query.all()
        for sink in results:
            key = (sink.stakeholder_key, sink.external_id)
            existing_sinks_mapping[key] = sink
        logger.debug(
            "Fetched %d StructureServiceSinkDBModel items from the database for %d keys.",
            len(existing_sinks_mapping),
            len(keys),
        )
        return existing_sinks_mapping
    except IntegrityError as e:
        logger.error("Integrity Error while fetching StructureServiceSinkDBModel: %s", e)
        raise DBIntegrityError("Integrity Error while fetching StructureServiceSinkDBModel") from e
    except Exception as e:
        logger.error("Unexpected error while fetching StructureServiceSinkDBModel: %s", e)
        raise DBError("Unexpected error while fetching StructureServiceSinkDBModel") from e


def fetch_sources_by_substring_match(filter_string: str) -> list[StructureServiceSource]:
    """Search for sources with names matching a substring.

    Performs a case-insensitive search for StructureServiceSource records
    whose names contain the given substring. Returns a list of matching
    source instances.
    """
    with get_session()() as session:
        try:
            matching_sources = (
                session.query(StructureServiceSourceDBModel)
                .filter(StructureServiceSourceDBModel.name.ilike(f"%{filter_string}%"))
                .all()
            )
            logger.debug(
                "Found %d StructureServiceSourceDBModel items matching filter "
                "string '%s' from %d total records.",
                len(matching_sources),
                filter_string,
                session.query(StructureServiceSourceDBModel).count(),
            )
            return [StructureServiceSource.from_orm_model(src) for src in matching_sources]
        except IntegrityError as e:
            logger.error(
                "Integrity Error while filtering StructureServiceSourceDBModel "
                "by substring match: %s",
                e,
            )
            raise DBIntegrityError(
                "Integrity Error while filtering StructureServiceSourceDBModel by substring match"
            ) from e
        except Exception as e:
            logger.error(
                "Unexpected error while filtering StructureServiceSourceDBModel "
                "by substring match: %s",
                e,
            )
            raise DBError(
                "Unexpected error while filtering StructureServiceSourceDBModel by substring match"
            ) from e


def fetch_sinks_by_substring_match(filter_string: str) -> list[StructureServiceSink]:
    """Search for sinks with names matching a substring.

    Performs a case-insensitive search for StructureServiceSink records
    whose names contain the given substring. Returns a list of matching
    sink instances.
    """
    with get_session()() as session:
        try:
            matching_sinks = (
                session.query(StructureServiceSinkDBModel)
                .filter(StructureServiceSinkDBModel.name.ilike(f"%{filter_string}%"))
                .all()
            )
            logger.debug(
                "Found %d StructureServiceSinkDBModel items matching "
                "filter string '%s' from %d total records.",
                len(matching_sinks),
                filter_string,
                session.query(StructureServiceSinkDBModel).count(),
            )
            return [StructureServiceSink.from_orm_model(sink) for sink in matching_sinks]
        except IntegrityError as e:
            logger.error(
                "Integrity Error while filtering StructureServiceSinkDBModel "
                "by substring match: %s",
                e,
            )
            raise DBIntegrityError(
                "Integrity Error while filtering StructureServiceSourceDBModel by substring match"
            ) from e
        except Exception as e:
            logger.error(
                "Unexpected error while filtering StructureServiceSinkDBModel "
                "by substring match: %s",
                e,
            )
            raise DBError(
                "Unexpected error while filtering StructureServiceSinkDBModel by substring match"
            ) from e


def upsert_sources(
    session: SQLAlchemySession,
    sources: list[StructureServiceSource],
) -> None:
    """Insert or update source records in the database.

    For each StructureServiceSource, updates existing records if they are found;
    otherwise, creates new records.
    """
    try:
        # Prepare data for upsert
        insert_data = [
            {
                "id": src.id,
                "external_id": src.external_id,
                "stakeholder_key": src.stakeholder_key,
                "name": src.name,
                "type": src.type,
                "visible": src.visible,
                "display_path": src.display_path,
                "preset_filters": src.preset_filters,
                "passthrough_filters": [f.dict() for f in src.passthrough_filters]
                if src.passthrough_filters
                else None,
                "adapter_key": src.adapter_key,
                "source_id": src.source_id,
                "ref_key": src.ref_key,
                "ref_id": src.ref_id,
                "meta_data": src.meta_data,
            }
            for src in sources
        ]

        # Create upsert statement
        insert_stmt = get_insert_statement(session, StructureServiceSourceDBModel).values(
            insert_data
        )

        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["external_id", "stakeholder_key"],
            set_={
                "name": insert_stmt.excluded.name,
                "type": insert_stmt.excluded.type,
                "visible": insert_stmt.excluded.visible,
                "display_path": insert_stmt.excluded.display_path,
                "preset_filters": insert_stmt.excluded.preset_filters,
                "passthrough_filters": insert_stmt.excluded.passthrough_filters,
                "adapter_key": insert_stmt.excluded.adapter_key,
                "source_id": insert_stmt.excluded.source_id,
                "ref_key": insert_stmt.excluded.ref_key,
                "ref_id": insert_stmt.excluded.ref_id,
                "meta_data": insert_stmt.excluded.meta_data,
            },
        )

        session.execute(insert_stmt)

        # Now, update associations in batch

        # Identify sources with thing_node_external_ids
        sources_with_tn_ids = [src for src in sources if src.thing_node_external_ids]

        if not sources_with_tn_ids:
            return  # No associations to update

        # Build mappings for source and thing node identifiers
        source_thingnode_mappings = []
        for src in sources_with_tn_ids:
            for tn_external_id in src.thing_node_external_ids:
                source_thingnode_mappings.append(
                    {
                        "source_external_id": src.external_id,
                        "source_stakeholder_key": src.stakeholder_key,
                        "thing_node_external_id": tn_external_id,
                        "thing_node_stakeholder_key": src.stakeholder_key,  # Assuming same stakeholder_key
                    }
                )

        # Get unique source identifiers
        unique_source_identifiers = {
            (mapping["source_external_id"], mapping["source_stakeholder_key"])
            for mapping in source_thingnode_mappings
        }

        # Get unique thing node identifiers
        unique_thingnode_identifiers = {
            (mapping["thing_node_external_id"], mapping["thing_node_stakeholder_key"])
            for mapping in source_thingnode_mappings
        }

        # Query source IDs
        source_id_rows = (
            session.query(
                StructureServiceSourceDBModel.external_id,
                StructureServiceSourceDBModel.stakeholder_key,
                StructureServiceSourceDBModel.id,
            )
            .filter(
                tuple_(
                    StructureServiceSourceDBModel.external_id,
                    StructureServiceSourceDBModel.stakeholder_key,
                ).in_(unique_source_identifiers)
            )
            .all()
        )

        source_id_map = {(row.external_id, row.stakeholder_key): row.id for row in source_id_rows}

        # Query thing node IDs
        thingnode_id_rows = (
            session.query(
                StructureServiceThingNodeDBModel.external_id,
                StructureServiceThingNodeDBModel.stakeholder_key,
                StructureServiceThingNodeDBModel.id,
            )
            .filter(
                tuple_(
                    StructureServiceThingNodeDBModel.external_id,
                    StructureServiceThingNodeDBModel.stakeholder_key,
                ).in_(unique_thingnode_identifiers)
            )
            .all()
        )

        thingnode_id_map = {
            (row.external_id, row.stakeholder_key): row.id for row in thingnode_id_rows
        }

        # Build association insert data
        association_insert_data = []
        for mapping in source_thingnode_mappings:
            source_id = source_id_map.get(
                (mapping["source_external_id"], mapping["source_stakeholder_key"])
            )
            thingnode_id = thingnode_id_map.get(
                (mapping["thing_node_external_id"], mapping["thing_node_stakeholder_key"])
            )
            if source_id and thingnode_id:
                association_insert_data.append(
                    {
                        "source_id": source_id,
                        "thingnode_id": thingnode_id,
                    }
                )
            else:
                logger.error(
                    "Missing ID for source (%s, %s) or thing node (%s, %s)",
                    mapping["source_external_id"],
                    mapping["source_stakeholder_key"],
                    mapping["thing_node_external_id"],
                    mapping["thing_node_stakeholder_key"],
                )

        # Collect source IDs for deletion
        source_ids_to_delete = list(source_id_map.values())

        if source_ids_to_delete:
            # Delete existing associations
            delete_stmt = delete(thingnode_source_association).where(
                thingnode_source_association.c.source_id.in_(source_ids_to_delete)
            )
            session.execute(delete_stmt)

        # Insert new associations
        if association_insert_data:
            association_insert_stmt = get_insert_statement(
                session, thingnode_source_association
            ).values(association_insert_data)

            # Use on_conflict_do_nothing to avoid duplicate entries
            association_insert_stmt = association_insert_stmt.on_conflict_do_nothing(
                index_elements=["source_id", "thingnode_id"]
            )

            session.execute(association_insert_stmt)

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceSourceDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceSourceDBModel"
        ) from e
    except Exception as e:
        logger.error("Unexpected error while upserting StructureServiceSourceDBModel: %s", e)
        raise DBUpdateError("Unexpected error while upserting StructureServiceSourceDBModel") from e


def upsert_sinks(
    session: SQLAlchemySession,
    sinks: list[StructureServiceSink],
) -> None:
    """Insert or update sink records in the database.

    For each StructureServiceSink, updates existing records if they are found;
    otherwise, creates new records.
    """
    try:
        # Prepare data for upsert
        insert_data = [
            {
                "id": sink.id,
                "external_id": sink.external_id,
                "stakeholder_key": sink.stakeholder_key,
                "name": sink.name,
                "type": sink.type,
                "visible": sink.visible,
                "display_path": sink.display_path,
                "preset_filters": sink.preset_filters,
                "passthrough_filters": [f.dict() for f in sink.passthrough_filters]
                if sink.passthrough_filters
                else None,
                "adapter_key": sink.adapter_key,
                "sink_id": sink.sink_id,
                "ref_key": sink.ref_key,
                "ref_id": sink.ref_id,
                "meta_data": sink.meta_data,
            }
            for sink in sinks
        ]

        # Create upsert statement
        insert_stmt = get_insert_statement(session, StructureServiceSinkDBModel).values(insert_data)

        insert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["external_id", "stakeholder_key"],
            set_={
                "name": insert_stmt.excluded.name,
                "type": insert_stmt.excluded.type,
                "visible": insert_stmt.excluded.visible,
                "display_path": insert_stmt.excluded.display_path,
                "preset_filters": insert_stmt.excluded.preset_filters,
                "passthrough_filters": insert_stmt.excluded.passthrough_filters,
                "adapter_key": insert_stmt.excluded.adapter_key,
                "sink_id": insert_stmt.excluded.sink_id,
                "ref_key": insert_stmt.excluded.ref_key,
                "ref_id": insert_stmt.excluded.ref_id,
                "meta_data": insert_stmt.excluded.meta_data,
            },
        )

        session.execute(insert_stmt)

        # Now, update associations in batch

        # Identify sinks with thing_node_external_ids
        sinks_with_tn_ids = [sink for sink in sinks if sink.thing_node_external_ids]

        if not sinks_with_tn_ids:
            return  # No associations to update

        # Build mappings for sink and thing node identifiers
        sink_thingnode_mappings = []
        for sink in sinks_with_tn_ids:
            for tn_external_id in sink.thing_node_external_ids:
                sink_thingnode_mappings.append(
                    {
                        "sink_external_id": sink.external_id,
                        "sink_stakeholder_key": sink.stakeholder_key,
                        "thing_node_external_id": tn_external_id,
                        "thing_node_stakeholder_key": sink.stakeholder_key,  # Assuming same stakeholder_key
                    }
                )

        # Get unique sink identifiers
        unique_sink_identifiers = {
            (mapping["sink_external_id"], mapping["sink_stakeholder_key"])
            for mapping in sink_thingnode_mappings
        }

        # Get unique thing node identifiers
        unique_thingnode_identifiers = {
            (mapping["thing_node_external_id"], mapping["thing_node_stakeholder_key"])
            for mapping in sink_thingnode_mappings
        }

        # Query sink IDs
        sink_id_rows = (
            session.query(
                StructureServiceSinkDBModel.external_id,
                StructureServiceSinkDBModel.stakeholder_key,
                StructureServiceSinkDBModel.id,
            )
            .filter(
                tuple_(
                    StructureServiceSinkDBModel.external_id,
                    StructureServiceSinkDBModel.stakeholder_key,
                ).in_(unique_sink_identifiers)
            )
            .all()
        )

        sink_id_map = {(row.external_id, row.stakeholder_key): row.id for row in sink_id_rows}

        # Query thing node IDs
        thingnode_id_rows = (
            session.query(
                StructureServiceThingNodeDBModel.external_id,
                StructureServiceThingNodeDBModel.stakeholder_key,
                StructureServiceThingNodeDBModel.id,
            )
            .filter(
                tuple_(
                    StructureServiceThingNodeDBModel.external_id,
                    StructureServiceThingNodeDBModel.stakeholder_key,
                ).in_(unique_thingnode_identifiers)
            )
            .all()
        )

        thingnode_id_map = {
            (row.external_id, row.stakeholder_key): row.id for row in thingnode_id_rows
        }

        # Build association insert data
        association_insert_data = []
        for mapping in sink_thingnode_mappings:
            sink_id = sink_id_map.get(
                (mapping["sink_external_id"], mapping["sink_stakeholder_key"])
            )
            thingnode_id = thingnode_id_map.get(
                (mapping["thing_node_external_id"], mapping["thing_node_stakeholder_key"])
            )
            if sink_id and thingnode_id:
                association_insert_data.append(
                    {
                        "sink_id": sink_id,
                        "thingnode_id": thingnode_id,
                    }
                )
            else:
                logger.error(
                    "Missing ID for sink (%s, %s) or thing node (%s, %s)",
                    mapping["sink_external_id"],
                    mapping["sink_stakeholder_key"],
                    mapping["thing_node_external_id"],
                    mapping["thing_node_stakeholder_key"],
                )

        # Collect sink IDs for deletion
        sink_ids_to_delete = list(sink_id_map.values())

        if sink_ids_to_delete:
            # Delete existing associations
            delete_stmt = delete(thingnode_sink_association).where(
                thingnode_sink_association.c.sink_id.in_(sink_ids_to_delete)
            )
            session.execute(delete_stmt)

        # Insert new associations
        if association_insert_data:
            association_insert_stmt = get_insert_statement(
                session, thingnode_sink_association
            ).values(association_insert_data)

            # Use on_conflict_do_nothing to avoid duplicate entries
            association_insert_stmt = association_insert_stmt.on_conflict_do_nothing(
                index_elements=["sink_id", "thingnode_id"]
            )

            session.execute(association_insert_stmt)

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceSinkDBModel: %s", e)
        raise DBIntegrityError("Integrity Error while upserting StructureServiceSinkDBModel") from e
    except Exception as e:
        logger.error("Unexpected error while upserting StructureServiceSinkDBModel: %s", e)
        raise DBUpdateError("Unexpected error while upserting StructureServiceSinkDBModel") from e
