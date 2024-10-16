import logging
from uuid import UUID

from hetdesrun.adapters.virtual_structure_adapter.models import (
    VirtualStructureAdapterResponse,
    VirtualStructureAdapterSink,
    VirtualStructureAdapterSource,
    VirtualStructureAdapterThingNode,
)
from hetdesrun.structure.db.db_structure_service import get_children
from hetdesrun.structure.db.exceptions import DBNotFoundError
from hetdesrun.structure.db.source_sink_service import (
    fetch_single_sink_from_db_by_id,
    fetch_single_source_from_db_by_id,
    fetch_sinks_by_substring_match,
    fetch_sources_by_substring_match,
)
from hetdesrun.structure.db.thing_node_service import fetch_single_thing_node_by_id

logger = logging.getLogger(__name__)


def get_children_from_structure_service(
    parent_id: UUID | None = None,
) -> tuple[
    list[VirtualStructureAdapterThingNode],
    list[VirtualStructureAdapterSource],
    list[VirtualStructureAdapterSink],
]:
    """Retrieves all children of the node with the given parent_id.
    And returns their respective structure-representation (for the frontend).
    """
    thing_nodes, sources, sinks = get_children(parent_id)
    struct_thing_nodes = [
        VirtualStructureAdapterThingNode.from_structure_service_thingnode(node)
        for node in thing_nodes
    ]
    struct_sources = [
        VirtualStructureAdapterSource.from_structure_service_source(source) for source in sources
    ]
    struct_sinks = [VirtualStructureAdapterSink.from_structure_service_sink(sink) for sink in sinks]

    return struct_thing_nodes, struct_sources, struct_sinks


def get_structure(parent_id: UUID | None = None) -> VirtualStructureAdapterResponse:
    nodes, sources, sinks = get_children_from_structure_service(parent_id)

    return VirtualStructureAdapterResponse(
        id="vst-adapter",
        name="Virtual Structure Adapter",
        thingNodes=nodes,
        sources=sources,
        sinks=sinks,
    )


def get_single_thingnode(
    tn_id: UUID,
) -> VirtualStructureAdapterThingNode | None:
    try:
        node = fetch_single_thing_node_by_id(tn_id)
    except DBNotFoundError as e:
        logger.error("Could not retrieve thingnode for ID: %s, due to: %s", tn_id, e)
        return None
    return VirtualStructureAdapterThingNode.from_structure_service_thingnode(node)


def get_single_source(
    src_id: UUID,
) -> VirtualStructureAdapterSource | None:
    try:
        source = fetch_single_source_from_db_by_id(src_id)
    except DBNotFoundError as e:
        logger.error("Could not retrieve source for ID: %s, due to: %s", src_id, e)
        return None
    return VirtualStructureAdapterSource.from_structure_service_source(source)


def get_filtered_sources(filter_string: str | None) -> list[VirtualStructureAdapterSource]:
    if filter_string is None:
        return []
    sources = fetch_sources_by_substring_match(filter_string)
    return [VirtualStructureAdapterSource.from_structure_service_source(src) for src in sources]


def get_single_sink(
    sink_id: UUID,
) -> VirtualStructureAdapterSink | None:
    try:
        sink = fetch_single_sink_from_db_by_id(sink_id)
    except DBNotFoundError as e:
        logger.error("Could not retrieve sink for ID: %s, due to: %s", sink_id, e)
        return None
    return VirtualStructureAdapterSink.from_structure_service_sink(sink)


def get_filtered_sinks(filter_string: str | None) -> list[VirtualStructureAdapterSink]:
    if filter_string is None:
        return []
    sinks = fetch_sinks_by_substring_match(filter_string)
    return [VirtualStructureAdapterSink.from_structure_service_sink(sink) for sink in sinks]
