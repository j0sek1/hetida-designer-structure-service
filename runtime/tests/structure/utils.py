import logging
from itertools import batched
from math import ceil

from sqlalchemy import tuple_
from sqlalchemy.exc import IntegrityError

from hetdesrun.persistence.db_engine_and_session import SQLAlchemySession
from hetdesrun.persistence.structure_service_dbmodels import (
    StructureServiceSinkDBModel,
    StructureServiceSourceDBModel,
)
from hetdesrun.structure.db.exceptions import (
    DBError,
    DBIntegrityError,
)

logger = logging.getLogger(__name__)


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
