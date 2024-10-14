import uuid

import pytest

from hetdesrun.adapters.virtual_structure_adapter.models import (
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


@pytest.mark.usefixtures("_fill_db")
def test_get_structure_with_none():
    structure = get_structure(None)

    # Verify that the root node of the structure is returned
    assert structure.sources == structure.sinks == []
    assert len(structure.thingNodes) == 1
    assert isinstance(structure.thingNodes[0], VirtualStructureAdapterThingNode)
    assert structure.thingNodes[0].parentId is None
    assert structure.thingNodes[0].name == "Waterworks 1"


@pytest.mark.usefixtures("_fill_db")
def test_get_structure_with_non_existent_uuid():
    structure = get_structure(uuid.uuid4())

    # Verify that an empty structure response object is returned
    assert structure.thingNodes == []
    assert structure.sources == []
    assert structure.sinks == []


@pytest.mark.usefixtures("_fill_db")
def test_get_structure_with_existing_uuid():
    # Make multiple calls to get structure, as the IDs are not known
    structure = get_structure(None)
    structure = get_structure(structure.thingNodes[0].id)
    structure = get_structure(structure.thingNodes[0].id)
    structure = get_structure(structure.thingNodes[0].id)

    # Check ThingNodes
    assert structure.thingNodes == []

    # Check Sources
    assert len(structure.sources) == 3
    assert isinstance(structure.sources[0], VirtualStructureAdapterSource)
    expected_source_names = [
        "Energy usage with preset filter",
        "Energy usage with passthrough filters",
        "Test source for type metadata(any)",
    ]
    for source in structure.sources:
        assert source.name in expected_source_names

    # Check Sinks
    assert len(structure.sinks) == 1
    assert isinstance(structure.sinks[0], VirtualStructureAdapterSink)
    assert (
        structure.sinks[0].name
        == "Anomaly score for the energy usage of the pump system in Storage Tank"
    )


@pytest.mark.usefixtures("_fill_db")
def test_get_single_element_functions_with_non_existent_id():
    random_uuid = uuid.uuid4()  # Non-existent UUID

    source = get_single_source(random_uuid)
    sink = get_single_sink(random_uuid)
    thingnode = get_single_thingnode(random_uuid)

    assert source == sink == thingnode
    assert source is None
