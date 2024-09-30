import logging
from uuid import UUID

from fastapi import HTTPException

from hetdesrun.adapters.virtual_structure_adapter.models import (
    VirtualStructureAdapterResponse,
    VirtualStructureAdapterSink,
    VirtualStructureAdapterSource,
    VirtualStructureAdapterThingNode,
)
from hetdesrun.adapters.virtual_structure_adapter.structure import (
    get_single_sink,
    get_single_source,
    get_single_thingnode,
    get_structure,
)
from hetdesrun.structure.db.exceptions import DBNotFoundError
from hetdesrun.webservice.auth_dependency import get_auth_deps
from hetdesrun.webservice.router import HandleTrailingSlashAPIRouter

logger = logging.getLogger(__name__)
virtual_structure_adapter_router = HandleTrailingSlashAPIRouter(
    prefix="/adapters/vst", tags=["virtual structure adapter"]
)


@virtual_structure_adapter_router.get(
    "/structure",
    response_model=VirtualStructureAdapterResponse,
    dependencies=get_auth_deps(),
)
async def get_structure_endpoint(parentId: UUID | None = None) -> VirtualStructureAdapterResponse:
    """Returns one level of the thingnode hierarchy for lazy-loading in the frontend"""
    return get_structure(parent_id=parentId)


@virtual_structure_adapter_router.get(
    "/thingNodes/{node_id}/metadata/",
    response_model=list,
    dependencies=get_auth_deps(),
)
async def get_thingnode_metadata_endpoint(node_id: UUID) -> list:  # noqa: ARG001
    """Get metadata attached to thing nodes

    This adapter does not implement metadata yet.
    """
    return []


@virtual_structure_adapter_router.get(
    "/thingNodes/{node_id}",
    response_model=VirtualStructureAdapterThingNode,
    dependencies=get_auth_deps(),
)
async def get_single_thingnode_endpoint(node_id: UUID) -> VirtualStructureAdapterThingNode:
    try:
        node = get_single_thingnode(node_id)
    except DBNotFoundError as exc:
        logger.info("No ThingNode found for provided UUID (%s)", node_id)
        raise HTTPException(
            status_code=404, detail=f"No ThingNode found for provided UUID ({node_id})"
        ) from exc

    return node


@virtual_structure_adapter_router.get(
    "/sources/{source_id}/metadata/",
    response_model=list,
    dependencies=get_auth_deps(),
)
async def get_source_metadata_endpoint(source_id: UUID) -> list:  # noqa: ARG001
    """Get metadata attached to sources

    This adapter does not implement metadata yet.
    """
    return []


@virtual_structure_adapter_router.get(
    "/sources/{source_id}",
    response_model=VirtualStructureAdapterSource,
    dependencies=get_auth_deps(),
)
async def get_single_source_endpoint(source_id: UUID) -> VirtualStructureAdapterSource:
    try:
        source = get_single_source(source_id)
    except DBNotFoundError as exc:
        logger.info("No Source found for provided UUID (%s)", source_id)
        raise HTTPException(
            status_code=404, detail=f"No Source found for provided UUID ({source_id})"
        ) from exc

    return source


@virtual_structure_adapter_router.get(
    "/sinks/{sink_id}/metadata/",
    response_model=list,
    dependencies=get_auth_deps(),
)
async def get_sink_metadata_endpoint(sink_id: UUID) -> list:  # noqa: ARG001
    """Get metadata attached to sinks

    This adapter does not implement metadata yet.
    """
    return []


@virtual_structure_adapter_router.get(
    "/sinks/{sink_id}",
    response_model=VirtualStructureAdapterSink,
    dependencies=get_auth_deps(),
)
async def get_single_sink_endpoint(sink_id: UUID) -> VirtualStructureAdapterSink:
    try:
        sink = get_single_sink(sink_id)
    except DBNotFoundError as exc:
        logger.info("No Sink found for provided UUID (%s)", sink_id)
        raise HTTPException(
            status_code=404, detail=f"No Sink found for provided UUID ({sink_id})"
        ) from exc

    return sink
