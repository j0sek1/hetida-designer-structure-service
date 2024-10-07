import logging
from uuid import UUID

from hetdesrun.adapters.virtual_structure_adapter.models import (
    VirtualStructureAdapterResponse,
    VirtualStructureAdapterSink,
    VirtualStructureAdapterSource,
    VirtualStructureAdapterThingNode,
)
from hetdesrun.structure.vst_structure_service import (
    get_children,
    get_single_sink_from_db,
    get_single_source_from_db,
    get_single_thingnode_from_db,
    get_sinks_by_substring_match,
    get_sources_by_substring_match,
)

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
) -> VirtualStructureAdapterThingNode:
    node = get_single_thingnode_from_db(tn_id)
    return VirtualStructureAdapterThingNode.from_structure_service_thingnode(node)


def get_single_source(
    src_id: UUID,
) -> VirtualStructureAdapterSource:
    source = get_single_source_from_db(src_id)
    return VirtualStructureAdapterSource.from_structure_service_source(source)


def get_filtered_sources(filter_string: str | None) -> list[VirtualStructureAdapterSource]:
    if filter_string is None:
        return []
    sources = get_sources_by_substring_match(filter_string)
    return [VirtualStructureAdapterSource.from_structure_service_source(src) for src in sources]


def get_single_sink(
    sink_id: UUID,
) -> VirtualStructureAdapterSink:
    sink = get_single_sink_from_db(sink_id)
    return VirtualStructureAdapterSink.from_structure_service_sink(sink)


def get_filtered_sinks(filter_string: str | None) -> list[VirtualStructureAdapterSink]:
    if filter_string is None:
        return []
    sinks = get_sinks_by_substring_match(filter_string)
    return [VirtualStructureAdapterSink.from_structure_service_sink(sink) for sink in sinks]
