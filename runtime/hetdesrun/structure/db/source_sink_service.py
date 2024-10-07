import logging

from sqlalchemy import tuple_
from sqlalchemy.exc import IntegrityError

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession, get_session
from hetdesrun.persistence.structure_service_dbmodels import (
    SinkOrm,
    SourceOrm,
    ThingNodeOrm,
)
from hetdesrun.structure.db.exceptions import DBError, DBIntegrityError, DBUpdateError
from hetdesrun.structure.models import (
    Sink,
    Source,
)

logger = logging.getLogger(__name__)


def fetch_sources(
    session: SQLAlchemySession, keys: set[tuple[str, str]]
) -> dict[tuple[str, str], SourceOrm]:
    """
    Fetches SourceOrm records from the database based on stakeholder_key and external_id.

    Args:
        session (Session): The SQLAlchemy session.
        keys (set[Tuple[str, str]]): A set of (stakeholder_key, external_id) tuples.

    Returns:
        Dict[Tuple[str, str], SourceOrm]:
        A mapping from (stakeholder_key, external_id) to SourceOrm.

    Raises:
        IntegrityError: If an integrity error occurs during the database operation.
        DBError: If any other database error occurs.
    """
    try:
        items = (
            session.query(SourceOrm)
            .filter(tuple_(SourceOrm.stakeholder_key, SourceOrm.external_id).in_(keys))
            .all()
        )
        logger.debug("Fetched %d SourceOrm items from the database.", len(items))
        return {(item.stakeholder_key, item.external_id): item for item in items}
    except IntegrityError as e:
        logger.error("Integrity Error while fetching SourceOrm: %s", e)
        raise DBIntegrityError("Integrity Error while fetching SourceOrm") from e
    except Exception as e:
        logger.error("Unexpected error while fetching SourceOrm: %s", e)
        raise DBError("Unexpected error while fetching SourceOrm") from e


def fetch_sinks(
    session: SQLAlchemySession, keys: set[tuple[str, str]]
) -> dict[tuple[str, str], SinkOrm]:
    """
    Fetches SinkOrm records from the database based on stakeholder_key and external_id.

    Args:
        session (Session): The SQLAlchemy session.
        keys (set[Tuple[str, str]]): A set of (stakeholder_key, external_id) tuples.

    Returns:
        Dict[Tuple[str, str], SinkOrm]: A mapping from (stakeholder_key, external_id) to SinkOrm.

    Raises:
        IntegrityError: If an integrity error occurs during the database operation.
        DBError: If any other database error occurs.
    """
    try:
        items = (
            session.query(SinkOrm)
            .filter(tuple_(SinkOrm.stakeholder_key, SinkOrm.external_id).in_(keys))
            .all()
        )
        logger.debug("Fetched %d SinkOrm items from the database.", len(items))
        return {(item.stakeholder_key, item.external_id): item for item in items}
    except IntegrityError as e:
        logger.error("Integrity Error while fetching SinkOrm: %s", e)
        raise DBIntegrityError("Integrity Error while fetching SinkOrm") from e
    except Exception as e:
        logger.error("Unexpected error while fetching SinkOrm: %s", e)
        raise DBError("Unexpected error while fetching SinkOrm") from e


def orm_get_sources_by_substring_match(filter_string: str) -> list[Source]:
    with get_session()() as session:
        try:
            matching_sources = (
                session.query(SourceOrm).filter(SourceOrm.name.ilike(f"%{filter_string}%")).all()
            )
            logger.debug(
                "Found %d SourceOrm items matching filter string '%s'.",
                len(matching_sources),
                filter_string,
            )
            return [Source.from_orm_model(src) for src in matching_sources]
        except IntegrityError as e:
            logger.error("Integrity Error while filtering SourceOrm by substring match: %s", e)
            raise DBIntegrityError(
                "Integrity Error while filtering SourceOrm by substring match"
            ) from e
        except Exception as e:
            logger.error("Unexpected error while filtering SourceOrm by substring match: %s", e)
            raise DBError("Unexpected error while filtering SourceOrm by substring match") from e


def orm_get_sinks_by_substring_match(filter_string: str) -> list[Sink]:
    with get_session()() as session:
        try:
            matching_sinks = (
                session.query(SinkOrm).filter(SinkOrm.name.ilike(f"%{filter_string}%")).all()
            )
            logger.debug(
                "Found %d SinkOrm items matching filter string '%s'.",
                len(matching_sinks),
                filter_string,
            )
            return [Sink.from_orm_model(sink) for sink in matching_sinks]
        except IntegrityError as e:
            logger.error("Integrity Error while filtering SinkOrm by substring match: %s", e)
            raise DBIntegrityError(
                "Integrity Error while filtering SourceOrm by substring match"
            ) from e
        except Exception as e:
            logger.error("Unexpected error while filtering SinkOrm by substring match: %s", e)
            raise DBError("Unexpected error while filtering SinkOrm by substring match") from e


def upsert_sources(
    session: SQLAlchemySession,
    sources: list[Source],
    existing_sources: dict[tuple[str, str], SourceOrm],
    existing_thing_nodes: dict[tuple[str, str], ThingNodeOrm],
) -> None:
    """
    Upserts SourceOrm records using SQLAlchemy's merge and add functionalities.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        sources (List[Source]): The list of Source objects to upsert.
        existing_sources (Dict[Tuple[str, str], SourceOrm]):
            Existing SourceOrm objects mapped by (stakeholder_key, external_id).
        existing_thing_nodes (Dict[Tuple[str, str], ThingNodeOrm]):
            Existing ThingNodeOrm objects mapped by (stakeholder_key, external_id).

    Raises:
        DBIntegrityError: If an integrity error occurs during the upsert operation.
        DBUpdateError: If any other error occurs during the upsert operation.
    """
    try:
        # Prevents SQLAlchemy from flushing the session automatically during the upsert
        with session.no_autoflush:
            for source in sources:
                key = (source.stakeholder_key, source.external_id)
                db_source = existing_sources.get(key)
                if db_source:
                    logger.debug("Updating SourceOrm with key %s.", key)
                    # Update fields
                    db_source.name = source.name
                    db_source.type = source.type
                    db_source.visible = source.visible
                    db_source.display_path = source.display_path
                    db_source.adapter_key = source.adapter_key
                    db_source.source_id = source.source_id
                    db_source.ref_key = source.ref_key
                    db_source.ref_id = source.ref_id
                    db_source.meta_data = source.meta_data
                    db_source.preset_filters = source.preset_filters
                    db_source.passthrough_filters = source.passthrough_filters
                    # Update relationships
                    db_source.thing_nodes = []
                    for tn_external_id in source.thing_node_external_ids or []:
                        tn_key = (source.stakeholder_key, tn_external_id)
                        db_thing_node = existing_thing_nodes.get(tn_key)
                        if db_thing_node:
                            db_source.thing_nodes.append(db_thing_node)
                    # Merge the updated source into the session
                    session.merge(db_source)
                else:
                    logger.debug("Creating new SourceOrm with key %s.", key)
                    # Create a new SourceOrm object
                    new_source = SourceOrm(
                        id=source.id,
                        external_id=source.external_id,
                        stakeholder_key=source.stakeholder_key,
                        name=source.name,
                        type=source.type,
                        visible=source.visible,
                        display_path=source.display_path,
                        adapter_key=source.adapter_key,
                        source_id=source.source_id,
                        ref_key=source.ref_key,
                        ref_id=source.ref_id,
                        meta_data=source.meta_data,
                        preset_filters=source.preset_filters,
                        passthrough_filters=source.passthrough_filters,
                    )
                    # Set relationships
                    for tn_external_id in source.thing_node_external_ids or []:
                        tn_key = (source.stakeholder_key, tn_external_id)
                        db_thing_node = existing_thing_nodes.get(tn_key)
                        if db_thing_node:
                            new_source.thing_nodes.append(db_thing_node)
                    # Add the new source to the session
                    session.add(new_source)
        # Explicitly flush all changes to ensure data is written to the database
        session.flush()
    except IntegrityError as e:
        logger.error("Integrity Error while upserting SourceOrm: %s", e)
        raise DBIntegrityError("Integrity Error while upserting SourceOrm") from e
    except Exception as e:
        logger.error("Error while upserting SourceOrm: %s", e)
        raise DBUpdateError("Error while upserting SourceOrm") from e


def upsert_sinks(
    session: SQLAlchemySession,
    sinks: list[Sink],
    existing_sinks: dict[tuple[str, str], SinkOrm],
    existing_thing_nodes: dict[tuple[str, str], ThingNodeOrm],
) -> None:
    """
    Upserts SinkOrm records using SQLAlchemy's merge and add functionalities.

    Args:
        session (SQLAlchemySession): The SQLAlchemy session.
        sinks (List[Sink]): The list of Sink objects to upsert.
        existing_sinks (Dict[Tuple[str, str], SinkOrm]):
            Existing SinkOrm objects mapped by (stakeholder_key, external_id).
        existing_thing_nodes (Dict[Tuple[str, str], ThingNodeOrm]):
            Existing ThingNodeOrm objects mapped by (stakeholder_key, external_id).

    Raises:
        DBIntegrityError: If an integrity error occurs during the upsert operation.
        DBUpdateError: If any other error occurs during the upsert operation.
    """
    try:
        # Prevents SQLAlchemy from flushing the session automatically during the upsert
        with session.no_autoflush:
            for sink in sinks:
                key = (sink.stakeholder_key, sink.external_id)
                db_sink = existing_sinks.get(key)
                if db_sink:
                    logger.debug("Updating SinkOrm with key %s.", key)
                    # Update fields
                    db_sink.name = sink.name
                    db_sink.type = sink.type
                    db_sink.visible = sink.visible
                    db_sink.display_path = sink.display_path
                    db_sink.adapter_key = sink.adapter_key
                    db_sink.sink_id = sink.sink_id
                    db_sink.ref_key = sink.ref_key
                    db_sink.ref_id = sink.ref_id
                    db_sink.meta_data = sink.meta_data
                    db_sink.preset_filters = sink.preset_filters
                    db_sink.passthrough_filters = sink.passthrough_filters
                    # Update relationships
                    db_sink.thing_nodes = []
                    for tn_external_id in sink.thing_node_external_ids or []:
                        tn_key = (sink.stakeholder_key, tn_external_id)
                        db_thing_node = existing_thing_nodes.get(tn_key)
                        if db_thing_node:
                            db_sink.thing_nodes.append(db_thing_node)
                    # Merge the updated sink into the session
                    session.merge(db_sink)
                else:
                    logger.debug("Creating new SinkOrm with key %s.", key)
                    # Create a new SinkOrm object
                    new_sink = SinkOrm(
                        id=sink.id,
                        external_id=sink.external_id,
                        stakeholder_key=sink.stakeholder_key,
                        name=sink.name,
                        type=sink.type,
                        visible=sink.visible,
                        display_path=sink.display_path,
                        adapter_key=sink.adapter_key,
                        sink_id=sink.sink_id,
                        ref_key=sink.ref_key,
                        ref_id=sink.ref_id,
                        meta_data=sink.meta_data,
                        preset_filters=sink.preset_filters,
                        passthrough_filters=sink.passthrough_filters,
                    )
                    # Set relationships
                    for tn_external_id in sink.thing_node_external_ids or []:
                        tn_key = (sink.stakeholder_key, tn_external_id)
                        db_thing_node = existing_thing_nodes.get(tn_key)
                        if db_thing_node:
                            new_sink.thing_nodes.append(db_thing_node)
                    # Add the new sink to the session
                    session.add(new_sink)
        # Explicitly flush all changes to ensure data is written to the database
        session.flush()
    except IntegrityError as e:
        logger.error("Integrity Error while upserting SinkOrm: %s", e)
        raise DBIntegrityError("Integrity Error while upserting SinkOrm") from e
    except Exception as e:
        logger.error("Error while upserting SinkOrm: %s", e)
        raise DBUpdateError("Error while upserting SinkOrm") from e
