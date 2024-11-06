import json

import pytest
from pydantic import ValidationError

from hetdesrun.structure.db.structure_service import (
    update_structure,
)
from hetdesrun.structure.models import (
    CompleteStructure,
    Filter,
    StructureServiceElementType,
    StructureServiceSource,
    StructureServiceThingNode,
)


def test_external_id_stakeholder_key_name_non_empty():
    with pytest.raises(ValueError, match="The external id cannot be empty"):
        StructureServiceElementType(external_id="", stakeholder_key="valid_key", name="TestElement")

    with pytest.raises(ValueError, match="The stakeholder key cannot be empty"):
        StructureServiceThingNode(
            external_id="valid_id", stakeholder_key="", name="TestStructureServiceThingNode"
        )

    with pytest.raises(ValueError, match="The name cannot be empty"):
        StructureServiceSource(external_id="valid_id", stakeholder_key="valid_key", name="")


def test_complete_structure_initialization_from_json():
    file_path = "tests/structure/data/db_test_structure.json"
    with open(file_path) as file:
        structure_json = json.load(file)

    complete_structure = CompleteStructure(**structure_json)

    # Check the number of elements based on JSON content
    assert len(complete_structure.element_types) == len(structure_json["element_types"])
    assert len(complete_structure.thing_nodes) == len(structure_json["thing_nodes"])
    assert len(complete_structure.sources) == len(structure_json["sources"])
    assert len(complete_structure.sinks) == len(structure_json["sinks"])

    # Extract thing node names dynamically from the JSON file and validate
    expected_names = [tn["name"] for tn in structure_json["thing_nodes"]]
    thing_node_names = [tn.name for tn in complete_structure.thing_nodes]

    for name in expected_names:
        assert name in thing_node_names, f"Expected thing node name '{name}' not found."


def test_complete_structure_element_type_not_empty_validator():
    with pytest.raises(
        ValidationError,
        match=(
            "The structure must include at least one StructureServiceElementType object "
            "to be valid."
        ),
    ):
        _ = CompleteStructure(element_types=[])


def test_complete_structure_duplicate_key_id_validator():
    file_path = "tests/structure/data/db_test_invalid_structure_no_duplicate_id.json"
    with open(file_path) as file:
        structure_json = json.load(file)

    with pytest.raises(
        ValidationError,
        match="The stakeholder key and external id pair",
    ):
        _ = CompleteStructure(**structure_json)


def test_complete_structure_duplicate_thingnode_external_id_validator():
    file_path = "tests/structure/data/db_test_no_duplicate_tn_id.json"
    with open(file_path) as file:
        structure_json = json.load(file)

    with pytest.raises(
        ValidationError,
        match="The thing_node_external_ids attribute",
    ):
        _ = CompleteStructure(**structure_json)


@pytest.fixture()
def filter_json():
    file_path = "tests/structure/data/test_filter_creation.json"
    with open(file_path) as file:
        f_json = json.load(file)
    return f_json


def test_filter_class_internal_name_field_creation(filter_json):
    # No value is provided
    filter_with_no_internal_name_provided = Filter(**filter_json["filter_without_internal_name"])
    assert filter_with_no_internal_name_provided.internal_name == "upper_threshold"

    # A value is provided
    filter_with_internal_name_provided = Filter(**filter_json["filter_with_internal_name"])
    assert filter_with_internal_name_provided.internal_name == "lower_threshold"

    # A value with uncommon whitespace is provided
    filter_with_weird_name_provided = Filter(
        **filter_json["filter_without_internal_name_with_uncommon_whitespace"]
    )
    assert filter_with_weird_name_provided.internal_name == "min_max"


def test_filter_class_initialization_with_empty_string_as_name(filter_json):
    with pytest.raises(ValidationError, match="The name of the filter must be set"):
        _ = Filter(**filter_json["filter_with_empty_string_as_name"])


def test_validate_root_nodes_parent_ids_are_none(mocked_clean_test_db_session):
    invalid_structure = {
        "element_types": [
            {
                "external_id": "Type1",
                "stakeholder_key": "SK1",
                "name": "Type 1",
            }
        ],
        "thing_nodes": [
            {
                "external_id": "Node1",
                "stakeholder_key": "SK1",
                "name": "Node 1",
                "parent_external_node_id": None,
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node2",
                "stakeholder_key": "SK1",
                "name": "Node 2",
                "parent_external_node_id": "InvalidNodeID",  # invalid reference
                "element_type_external_id": "Type1",
            },
        ],
    }

    with pytest.raises(
        ValueError,
        match="Root node 'Node 2' has an invalid parent_external_node_id "
        "'InvalidNodeID' that does not reference any existing StructureServiceThingNode.",
    ):
        CompleteStructure(**invalid_structure)


def test_circular_tn_relation(mocked_clean_test_db_session):
    circular_data = {
        "element_types": [
            {
                "external_id": "Type1",
                "stakeholder_key": "SK1",
                "name": "Type 1",
            }
        ],
        "thing_nodes": [
            {
                "external_id": "Node1",
                "stakeholder_key": "SK1",
                "name": "Node 1",
                "parent_external_node_id": None,
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node2",
                "stakeholder_key": "SK1",
                "name": "Node 2",
                "parent_external_node_id": "Node4",
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node3",
                "stakeholder_key": "SK1",
                "name": "Node 3",
                "parent_external_node_id": "Node2",
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node4",
                "stakeholder_key": "SK1",
                "name": "Node 4",
                "parent_external_node_id": "Node3",  # Circular reference
                "element_type_external_id": "Type1",
            },
        ],
    }

    with pytest.raises(ValueError, match="Circular reference detected in node"):
        CompleteStructure(**circular_data)


def test_stakeholder_key_consistency(mocked_clean_test_db_session):
    conflicting_structure = {
        "element_types": [
            {
                "external_id": "Type1",
                "stakeholder_key": "SK1",
                "name": "Type 1",
            },
            {
                "external_id": "Type2",
                "stakeholder_key": "SK2",
                "name": "Type 2",
            },
        ],
        "thing_nodes": [
            {
                "external_id": "Node1",
                "stakeholder_key": "SK1",
                "name": "Node 1",
                "parent_external_node_id": None,
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node1_1",
                "stakeholder_key": "SK1",
                "name": "Node 1.1",
                "parent_external_node_id": "Node1",
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node1_2",
                "stakeholder_key": "SK1",
                "name": "Node 1.2",
                "parent_external_node_id": "Node1",
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node2",
                "stakeholder_key": "SK2",
                "name": "Node 2",
                "parent_external_node_id": None,
                "element_type_external_id": "Type2",
            },
            {
                "external_id": "Node2_1",
                "stakeholder_key": "SK2",
                "name": "Node 2.1",
                "parent_external_node_id": "Node2",
                "element_type_external_id": "Type2",
            },
            {
                "external_id": "Node2_2",
                "stakeholder_key": "SK2",
                "name": "Node 2.2",
                "parent_external_node_id": "Node2",
                "element_type_external_id": "Type2",
            },
            {
                "external_id": "Node1_1_1",
                "stakeholder_key": "SK2",  # Inconsistent stakeholder_key
                "name": "Node 1.1.1",
                "parent_external_node_id": "Node1_1",
                "element_type_external_id": "Type2",
            },
        ],
    }

    with pytest.raises(ValueError, match="Inconsistent stakeholder_key at node"):
        CompleteStructure(**conflicting_structure)


def test_update_two_root_nodes(mocked_clean_test_db_session):
    structure = {
        "element_types": [
            {
                "external_id": "Type1",
                "stakeholder_key": "SK1",
                "name": "Type 1",
            },
            {
                "external_id": "Type2",
                "stakeholder_key": "SK2",
                "name": "Type 2",
            },
        ],
        "thing_nodes": [
            {
                "external_id": "Node1",
                "stakeholder_key": "SK1",
                "name": "Node 1",
                "parent_external_node_id": None,
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node1_1",
                "stakeholder_key": "SK1",
                "name": "Node 1.1",
                "parent_external_node_id": "Node1",
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node1_2",
                "stakeholder_key": "SK1",
                "name": "Node 1.2",
                "parent_external_node_id": "Node1",
                "element_type_external_id": "Type1",
            },
            {
                "external_id": "Node2",
                "stakeholder_key": "SK2",
                "name": "Node 2",
                "parent_external_node_id": None,
                "element_type_external_id": "Type2",
            },
            {
                "external_id": "Node2_1",
                "stakeholder_key": "SK2",
                "name": "Node 2.1",
                "parent_external_node_id": "Node2",
                "element_type_external_id": "Type2",
            },
            {
                "external_id": "Node2_2",
                "stakeholder_key": "SK2",
                "name": "Node 2.2",
                "parent_external_node_id": "Node2",
                "element_type_external_id": "Type2",
            },
            {
                "external_id": "Node2_2_2",
                "stakeholder_key": "SK2",  # Inconsistent stakeholder_key
                "name": "Node 2.2.2",
                "parent_external_node_id": "Node2_1",
                "element_type_external_id": "Type2",
            },
        ],
    }

    structure_with_two_root_nodes = CompleteStructure(**structure)
    update_structure(structure_with_two_root_nodes)


def test_validate_source_sink_references(mocked_clean_test_db_session):
    invalid_source_structure = {
        "element_types": [
            {
                "external_id": "Type1",
                "stakeholder_key": "SK1",
                "name": "Type 1",
            }
        ],
        "thing_nodes": [
            {
                "external_id": "Node1",
                "stakeholder_key": "SK1",
                "name": "Node 1",
                "parent_external_node_id": None,
                "element_type_external_id": "Type1",
            }
        ],
        "sources": [
            {
                "external_id": "Source1",
                "stakeholder_key": "SK1",
                "name": "Source 1",
                "type": "multitsframe",
                "adapter_key": "sql-adapter",
                "source_id": "some_id",
                "thing_node_external_ids": ["NonExistentNode"],  # invalid reference
            }
        ],
        "sinks": [
            {
                "external_id": "Sink1",
                "stakeholder_key": "SK1",
                "name": "Sink 1",
                "type": "multitsframe",
                "adapter_key": "sql-adapter",
                "sink_id": "some_id",
                "thing_node_external_ids": ["Node1"],  # valid reference
            }
        ],
    }

    with pytest.raises(
        ValueError,
        match=r"StructureServiceSource 'Source1' references non-existing "
        r"StructureServiceThingNode 'NonExistentNode'\.",
    ):
        CompleteStructure(**invalid_source_structure)

    invalid_sink_structure = {
        "element_types": [
            {
                "external_id": "Type1",
                "stakeholder_key": "SK1",
                "name": "Type 1",
            }
        ],
        "thing_nodes": [
            {
                "external_id": "Node1",
                "stakeholder_key": "SK1",
                "name": "Node 1",
                "parent_external_node_id": None,
                "element_type_external_id": "Type1",
            }
        ],
        "sources": [
            {
                "external_id": "Source1",
                "stakeholder_key": "SK1",
                "name": "Source 1",
                "type": "multitsframe",
                "adapter_key": "sql-adapter",
                "source_id": "some_id",
                "thing_node_external_ids": ["Node1"],  # valid reference
            }
        ],
        "sinks": [
            {
                "external_id": "Sink1",
                "stakeholder_key": "SK1",
                "name": "Sink 1",
                "type": "multitsframe",
                "adapter_key": "sql-adapter",
                "sink_id": "some_id",
                "thing_node_external_ids": ["NonExistentNode"],  # invalid reference
            }
        ],
    }

    with pytest.raises(
        ValueError,
        match=r"StructureServiceSink 'Sink1' references non-existing"
        r" StructureServiceThingNode 'NonExistentNode'\.",
    ):
        CompleteStructure(**invalid_sink_structure)
