import json

import pytest
from pydantic import ValidationError

from hetdesrun.structure.models import CompleteStructure, Filter


def test_complete_structure_initialization_from_json():
    file_path = "tests/structure/data/db_test_structure.json"
    with open(file_path) as file:
        structure_json = json.load(file)
    complete_structure = CompleteStructure(**structure_json)
    assert len(complete_structure.element_types) == 3
    assert len(complete_structure.thing_nodes) == 7
    assert len(complete_structure.sources) == 3
    assert len(complete_structure.sinks) == 3

    thing_node_names = [tn.name for tn in complete_structure.thing_nodes]

    expected_names = [
        "Waterworks 1",
        "Plant 1",
        "Plant 2",
        "Storage Tank 1, Plant 1",
        "Storage Tank 2, Plant 1",
        "Storage Tank 1, Plant 2",
        "Storage Tank 2, Plant 2",
    ]

    for name in expected_names:
        assert name in thing_node_names


def test_complete_structure_elment_type_not_empty_validator():
    with pytest.raises(
        ValidationError,
        match="The structure must include at least one ElementType object to be valid.",
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
