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
    StructureServiceSinkDBModel,
    StructureServiceSourceDBModel,
    StructureServiceThingNodeDBModel,
)
from hetdesrun.structure.db.exceptions import (
    DBError,
    DBIntegrityError,
    DBNotFoundError,
    DBUpdateError,
)
from hetdesrun.structure.models import (
    StructureServiceSink,
    StructureServiceSource,
)
from hetdesrun.structure.utils import is_postgresql, is_sqlite

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
    session: SQLAlchemySession, keys: set[tuple[str, str]], batch_size: int = 500
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
        # Loop through keys in batches of size <batch_size> or less
        for key_batch in batched(keys, ceil(len(keys) / batch_size)):
            batch_query = session.query(StructureServiceSourceDBModel).filter(
                tuple_(
                    StructureServiceSourceDBModel.stakeholder_key,
                    StructureServiceSourceDBModel.external_id,
                ).in_(key_batch)
            )
            batch_results = batch_query.all()
            for source in batch_results:
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
    session: SQLAlchemySession, keys: set[tuple[str, str]], batch_size: int = 500
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
        # Loop through keys in batches of size <batch_size> or less
        for key_batch in batched(keys, ceil(len(keys) / batch_size)):
            batch_query = session.query(StructureServiceSinkDBModel).filter(
                tuple_(
                    StructureServiceSinkDBModel.stakeholder_key,
                    StructureServiceSinkDBModel.external_id,
                ).in_(key_batch)
            )
            batch_results = batch_query.all()
            for sink in batch_results:
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
    existing_thing_nodes: dict[tuple[str, str], StructureServiceThingNodeDBModel],
) -> None:
    """Insert or update source records in the database.

    For each StructureServiceSource, updates existing records if they are found;
    otherwise, creates new records.
    """
    if not sources:
        return
    source_dicts = [src.dict() for src in sources]

    try:
        engine: Engine | Connection = session.get_bind()
        if isinstance(engine, Connection):
            raise ValueError("The session in use has to be bound to an Engine not a Connection.")

        upsert_stmt: sqlite_insert_typing | pg_insert_typing

        if is_postgresql(engine):
            upsert_stmt = pg_insert(StructureServiceSourceDBModel).values(source_dicts)
        elif is_sqlite(engine):
            upsert_stmt = sqlite_insert(StructureServiceSourceDBModel).values(source_dicts)
        else:
            raise ValueError(
                f"Unsupported database engine: {engine}. Please use either Postgres or SQLITE."
            )

        upsert_stmt = upsert_stmt.on_conflict_do_update(
            index_elements=[
                "external_id",
                "stakeholder_key",
            ],  # Columns where insert looks for a conflict
            set_={
                col: upsert_stmt.excluded[col] for col in source_dicts[0] if col != "id"
            },  # Exclude primary key from update
        ).returning(StructureServiceSourceDBModel)  # type: ignore

        # ORM models returned by the upsert query
        sources_dbmodels = session.scalars(
            upsert_stmt,
            execution_options={"populate_existing": True},
        )

        # Assign relationships
        for source in sources_dbmodels:
            source.thing_nodes = [
                existing_thing_nodes.get((source.stakeholder_key, tn_external_id))
                for tn_external_id in source.thing_node_external_ids or []
                if (source.stakeholder_key, tn_external_id) in existing_thing_nodes
            ]

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceSourceDBModel: %s", e)
        raise DBIntegrityError(
            "Integrity Error while upserting StructureServiceSourceDBModel"
        ) from e
    except ValueError as e:
        logger.error("Value error while upserting StructureServiceSourceDBModel: %s", e)
        raise DBError("Value error while upserting StructureServiceSourceDBModel") from e
    except Exception as e:
        logger.error("Unexpected error while upserting StructureServiceSourceDBModel: %s", e)
        raise DBUpdateError("Unexpected error while upserting StructureServiceSourceDBModel") from e


def upsert_sinks(
    session: SQLAlchemySession,
    sinks: list[StructureServiceSink],
    existing_sinks: dict[tuple[str, str], StructureServiceSinkDBModel],
    existing_thing_nodes: dict[tuple[str, str], StructureServiceThingNodeDBModel],
) -> None:
    """Insert or update sink records in the database.

    For each StructureServiceSink, updates existing records if they are found;
    otherwise, creates new records.
    """
    try:
        new_records = []

        for sink in sinks:
            key = (sink.stakeholder_key, sink.external_id)
            db_sink = existing_sinks.get(key)

            if db_sink:
                logger.debug("Updating StructureServiceSinkDBModel with key %s.", key)
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

                # Clear and set relationships
                db_sink.thing_nodes = [
                    existing_thing_nodes.get((sink.stakeholder_key, tn_external_id))
                    for tn_external_id in sink.thing_node_external_ids or []
                    if (sink.stakeholder_key, tn_external_id) in existing_thing_nodes
                ]
            else:
                logger.debug("Creating new StructureServiceSinkDBModel with key %s.", key)
                new_sink = StructureServiceSinkDBModel(
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
                    passthrough_filters=sink.passthrough_filters,  # type: ignore
                )

                # Assign relationships
                new_sink.thing_nodes = [
                    existing_thing_nodes.get((sink.stakeholder_key, tn_external_id))
                    for tn_external_id in sink.thing_node_external_ids or []
                    if (sink.stakeholder_key, tn_external_id) in existing_thing_nodes
                ]
                new_records.append(new_sink)

        if new_records:
            session.add_all(new_records)

    except IntegrityError as e:
        logger.error("Integrity Error while upserting StructureServiceSinkDBModel: %s", e)
        raise DBIntegrityError("Integrity Error while upserting StructureServiceSinkDBModel") from e
    except Exception as e:
        logger.error("Unexpected error while upserting StructureServiceSinkDBModel: %s", e)
        raise DBUpdateError("Unexpected error while upserting StructureServiceSinkDBModel") from e
