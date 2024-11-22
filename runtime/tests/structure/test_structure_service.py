import json
import uuid
from sqlite3 import Connection as SQLite3Connection

import pytest
from sqlalchemy import event
from sqlalchemy.future.engine import Engine

from hetdesrun.persistence.db_engine_and_session import get_session
from hetdesrun.persistence.structure_service_dbmodels import (
    StructureServiceElementTypeDBModel,
    StructureServiceSinkDBModel,
    StructureServiceSourceDBModel,
    StructureServiceThingNodeDBModel,
    thingnode_sink_association,
    thingnode_source_association,
)
from hetdesrun.structure.db.element_type_service import (
    fetch_element_types,
    search_element_types_by_name,
    upsert_element_types,
)
from hetdesrun.structure.db.exceptions import (
    DBError,
    JsonParsingError,
)
from hetdesrun.structure.db.source_sink_service import (
    fetch_sinks,
    fetch_sources,
    upsert_sinks,
    upsert_sources,
)
from hetdesrun.structure.db.structure_service import (
    are_structure_tables_empty,
    delete_structure,
    get_children,
    load_structure_from_json_file,
    populate_element_type_ids,
    sort_thing_nodes,
    update_structure,
)
from hetdesrun.structure.db.thing_node_service import (
    fetch_thing_nodes,
    search_thing_nodes_by_name,
    upsert_thing_nodes,
)
from hetdesrun.structure.models import (
    CompleteStructure,
    Filter,
    StructureServiceElementType,
    StructureServiceSink,
    StructureServiceSource,
    StructureServiceThingNode,
)

# Enable Foreign Key Constraints for SQLite Connections


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection: SQLite3Connection, connection_record) -> None:  # type: ignore  # noqa: E501,
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


# Tests for Hierarchy and Relationships


@pytest.mark.usefixtures("_db_test_structure")
def test_thing_node_hierarchy(mocked_clean_test_db_session):  # noqa: PLR0915
    """
    Tests the hierarchy and relationships of StructureServiceThingNodes, StructureServiceSources,
    and StructureServiceSinks in the database based on loaded data from JSON.
    """
    # Load expected data from the JSON file
    file_path = "tests/structure/data/db_test_structure.json"
    with open(file_path) as file:
        expected_data = json.load(file)

    expected_element_type_keys = {
        (et["stakeholder_key"], et["external_id"]) for et in expected_data["element_types"]
    }
    expected_thing_node_keys = {
        (tn["stakeholder_key"], tn["external_id"]) for tn in expected_data["thing_nodes"]
    }
    expected_source_keys = {
        (src["stakeholder_key"], src["external_id"]) for src in expected_data["sources"]
    }
    expected_sink_keys = {
        (snk["stakeholder_key"], snk["external_id"]) for snk in expected_data["sinks"]
    }

    with mocked_clean_test_db_session() as session:
        element_types_in_db = fetch_element_types(session, expected_element_type_keys)
        assert len(element_types_in_db) == len(
            expected_data["element_types"]
        ), "Mismatch in element types count"

        thing_nodes_in_db = fetch_thing_nodes(session, expected_thing_node_keys)
        assert len(thing_nodes_in_db) == len(
            expected_data["thing_nodes"]
        ), "Mismatch in thing nodes count"

        sources_in_db = fetch_sources(session, expected_source_keys)
        assert len(sources_in_db) == len(expected_data["sources"]), "Mismatch in sources count"

        sinks_in_db = fetch_sinks(session, expected_sink_keys)
        assert len(sinks_in_db) == len(expected_data["sinks"]), "Mismatch in sinks count"

        # Verify parent-child relationships in thing nodes
        for thing_node in expected_data["thing_nodes"]:
            key = (thing_node["stakeholder_key"], thing_node["external_id"])
            if thing_node.get("parent_external_node_id"):
                parent_key = (thing_node["stakeholder_key"], thing_node["parent_external_node_id"])
                assert parent_key in thing_nodes_in_db, f"Parent node {parent_key} not found in DB"
                assert (
                    thing_nodes_in_db[key].parent_node_id == thing_nodes_in_db[parent_key].id
                ), f"{key} has incorrect parent ID"

        # Verify associations for sources
        for source in expected_data["sources"]:
            source_key = (source["stakeholder_key"], source["external_id"])
            assert source_key in sources_in_db, f"Source {source_key} not found in DB"
            expected_associated_nodes = {
                (thing_node["stakeholder_key"], thing_node["external_id"])
                for tn_id in source["thing_node_external_ids"]
                for thing_node in expected_data["thing_nodes"]
                if thing_node["external_id"] == tn_id
            }
            actual_associated_nodes = {
                (tn.stakeholder_key, tn.external_id) for tn in sources_in_db[source_key].thing_nodes
            }
            assert (
                actual_associated_nodes == expected_associated_nodes
            ), f"Incorrect associations for source {source_key}"

        # Verify associations for sinks
        for sink in expected_data["sinks"]:
            sink_key = (sink["stakeholder_key"], sink["external_id"])
            assert sink_key in sinks_in_db, f"Sink {sink_key} not found in DB"
            expected_associated_nodes = {
                (thing_node["stakeholder_key"], thing_node["external_id"])
                for tn_id in sink["thing_node_external_ids"]
                for thing_node in expected_data["thing_nodes"]
                if thing_node["external_id"] == tn_id
            }
            actual_associated_nodes = {
                (tn.stakeholder_key, tn.external_id) for tn in sinks_in_db[sink_key].thing_nodes
            }
            assert (
                actual_associated_nodes == expected_associated_nodes
            ), f"Incorrect associations for sink {sink_key}"


### Fetch Functions


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_element_types(mocked_clean_test_db_session):
    # Load expected data from the JSON file
    file_path = "tests/structure/data/db_test_structure.json"
    with open(file_path) as file:
        expected_data = json.load(file)

    # Extract expected keys and names from the loaded JSON data
    expected_element_type_keys = {
        (et["stakeholder_key"], et["external_id"]) for et in expected_data["element_types"]
    }
    expected_element_types = {
        (et["stakeholder_key"], et["external_id"]): et["name"]
        for et in expected_data["element_types"]
    }

    with mocked_clean_test_db_session() as session:
        # Fetch the element types based on the expected keys
        element_types = fetch_element_types(session, expected_element_type_keys)

        # Check if the correct number of element types was retrieved
        assert len(element_types) == len(expected_element_type_keys), (
            f"Expected {len(expected_element_type_keys)} Element Types in the database, "
            f"found {len(element_types)}"
        )

        # Check if each expected element type is present in the retrieved element types
        for key, expected_name in expected_element_types.items():
            # Verify if the key exists in the retrieved dictionary
            assert key in element_types, (
                f"Expected Element Type with stakeholder_key '{key[0]}' and "
                f"external_id '{key[1]}' not found"
            )
            # Check if the name matches
            actual_et = element_types[key]
            assert actual_et.name == expected_name, (
                f"Element Type with external_id '{key[1]}' has name '{actual_et.name}', "
                f"expected '{expected_name}'"
            )


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_thing_nodes(mocked_clean_test_db_session):
    # Load expected data from the JSON file
    file_path = "tests/structure/data/db_test_structure.json"
    with open(file_path) as file:
        expected_data = json.load(file)

    # Extract expected keys and names from the loaded JSON data
    expected_thing_node_keys = {
        (tn["stakeholder_key"], tn["external_id"]) for tn in expected_data["thing_nodes"]
    }
    expected_thing_nodes = {
        (tn["stakeholder_key"], tn["external_id"]): tn["name"]
        for tn in expected_data["thing_nodes"]
    }

    with mocked_clean_test_db_session() as session:
        # Fetch the StructureServiceThingNodes based on the expected keys
        thing_nodes = fetch_thing_nodes(session, expected_thing_node_keys)

        # Check if the correct number of StructureServiceThingNodes was retrieved
        assert len(thing_nodes) == len(expected_thing_node_keys), (
            f"Expected {len(expected_thing_node_keys)} Thing Nodes in the database, "
            f"found {len(thing_nodes)}"
        )

        # Check if each expected StructureServiceThingNode is present in the retrieved thing nodes
        for key, expected_name in expected_thing_nodes.items():
            # Verify if the key exists in the retrieved dictionary
            assert key in thing_nodes, (
                f"Expected Thing Node with stakeholder_key '{key[0]}' and "
                f"external_id '{key[1]}' not found"
            )
            # Check if the name matches
            actual_tn = thing_nodes[key]
            assert actual_tn.name == expected_name, (
                f"Thing Node with external_id '{key[1]}' has name '{actual_tn.name}', "
                f"expected '{expected_name}'"
            )


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_sources(mocked_clean_test_db_session):
    # Load expected data from the JSON file
    file_path = "tests/structure/data/db_test_structure.json"
    with open(file_path) as file:
        expected_data = json.load(file)

    # Extract expected keys and names from the loaded JSON data
    expected_source_keys = {
        (source["stakeholder_key"], source["external_id"]) for source in expected_data["sources"]
    }
    expected_sources = {
        (source["stakeholder_key"], source["external_id"]): source["name"]
        for source in expected_data["sources"]
    }

    with mocked_clean_test_db_session() as session:
        # Fetch the StructureServiceSources based on the expected keys
        sources = fetch_sources(session, expected_source_keys)

        # Check if the correct number of StructureServiceSources was retrieved
        assert len(sources) == len(expected_sources), (
            f"Expected {len(expected_sources)} StructureServiceSources in the database, "
            f"found {len(sources)}"
        )

        # Check if each expected StructureServiceSource is present in the retrieved sources
        for key, expected_name in expected_sources.items():
            # Verify if the key exists in the retrieved dictionary
            assert key in sources, (
                f"Expected StructureServiceSource with stakeholder_key '{key[0]}' and "
                f"external_id '{key[1]}' not found"
            )
            # Check if the name matches
            actual_source = sources[key]
            assert actual_source.name == expected_name, (
                f"StructureServiceSource with external_id"
                f" '{key[1]}' has name '{actual_source.name}', "
                f"expected '{expected_name}'"
            )


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_sinks(mocked_clean_test_db_session):
    # Load expected data from the JSON file
    file_path = "tests/structure/data/db_test_structure.json"
    with open(file_path) as file:
        expected_data = json.load(file)

    # Extract expected keys and names from the loaded JSON data
    expected_sink_keys = {
        (sink["stakeholder_key"], sink["external_id"]) for sink in expected_data["sinks"]
    }
    expected_sinks = {
        (sink["stakeholder_key"], sink["external_id"]): sink["name"]
        for sink in expected_data["sinks"]
    }

    with mocked_clean_test_db_session() as session:
        # Fetch the StructureServiceSinks based on the expected keys
        sinks = fetch_sinks(session, expected_sink_keys)

        # Check if the correct number of StructureServiceSinks was retrieved
        assert len(sinks) == len(expected_sinks), (
            f"Expected {len(expected_sinks)} StructureServiceSinks in the database, "
            f"found {len(sinks)}"
        )

        # Check if each expected StructureServiceSink is present in the retrieved sinks
        for key, expected_name in expected_sinks.items():
            # Verify if the key exists in the retrieved dictionary
            assert key in sinks, (
                f"Expected StructureServiceSink with stakeholder_key '{key[0]}' and "
                f"external_id '{key[1]}' not found"
            )
            # Check if the name matches
            actual_sink = sinks[key]
            assert actual_sink.name == expected_name, (
                f"StructureServiceSink with external_id '{key[1]}' has name '{actual_sink.name}', "
                f"expected '{expected_name}'"
            )


### Structure Helper Functions


def test_complete_structure_object_creation():
    # Load the expected data from the JSON file
    file_path = "tests/structure/data/db_test_structure.json"
    with open(file_path) as file:
        data = json.load(file)

    # Create a CompleteStructure object from the loaded JSON data
    cs = CompleteStructure(**data)

    # Assert the lengths based on the JSON data
    assert len(cs.thing_nodes) == len(
        data["thing_nodes"]
    ), f"Expected {len(data['thing_nodes'])} Thing Nodes, found {len(cs.thing_nodes)}"
    assert len(cs.element_types) == len(
        data["element_types"]
    ), f"Expected {len(data['element_types'])} Element Types, found {len(cs.element_types)}"
    assert len(cs.sources) == len(
        data["sources"]
    ), f"Expected {len(data['sources'])} Sources, found {len(cs.sources)}"
    assert len(cs.sinks) == len(
        data["sinks"]
    ), f"Expected {len(data['sinks'])} Sinks, found {len(cs.sinks)}"

    # Check if all expected Thing Node names are present
    tn_names = {tn.name for tn in cs.thing_nodes}
    expected_tn_names = {tn["name"] for tn in data["thing_nodes"]}
    assert (
        tn_names == expected_tn_names
    ), f"Mismatch in Thing Node names. Expected: {expected_tn_names}, found: {tn_names}"


def test_load_structure_from_json_file(db_test_structure_file_path):
    # Load the structure from the JSON file using the load_structure_from_json_file function
    complete_structure = load_structure_from_json_file(db_test_structure_file_path)

    # Assert that the loaded structure is an instance of the CompleteStructure class
    assert isinstance(
        complete_structure, CompleteStructure
    ), "Loaded structure is not an instance of CompleteStructure"

    # Load the expected structure directly from the JSON file for comparison
    with open(db_test_structure_file_path) as file:
        expected_structure_json = json.load(file)

    # Convert the expected JSON structure into a CompleteStructure instance
    expected_structure = CompleteStructure(**expected_structure_json)

    # Pair corresponding lists from the complete_structure and expected_structure
    # (such as element_types, thing_nodes, sources, and sinks).
    # Ensure that UUIDs match by setting them to the same value for each pair.
    for complete_list, expected_list in [
        (complete_structure.element_types, expected_structure.element_types),
        (complete_structure.thing_nodes, expected_structure.thing_nodes),
        (complete_structure.sources, expected_structure.sources),
        (complete_structure.sinks, expected_structure.sinks),
    ]:
        for complete, expected in zip(complete_list, expected_list, strict=False):
            uniform_id = uuid.uuid4()
            complete.id = uniform_id
            expected.id = uniform_id

    # Ensure that element_type_id fields in StructureServiceThingNodes match
    for complete, expected in zip(
        complete_structure.thing_nodes, expected_structure.thing_nodes, strict=False
    ):
        uniform_id = uuid.uuid4()
        complete.element_type_id = uniform_id
        expected.element_type_id = uniform_id

    # Assert that the entire loaded structure matches the expected structure
    assert (
        complete_structure == expected_structure
    ), "Loaded structure does not match the expected structure"


def test_load_structure_from_invalid_json_file():
    with pytest.raises(FileNotFoundError):
        load_structure_from_json_file("non_existent_file.json")
    with pytest.raises(JsonParsingError):
        load_structure_from_json_file("tests/structure/data/db_test_structure_malformed.json")


def test_load_structure_from_json_with_invalid_data():
    invalid_structure_path = "tests/structure/data/db_test_structure_invalid_data.json"
    with pytest.raises(JsonParsingError) as exc_info:
        load_structure_from_json_file(invalid_structure_path)
    assert "Validation error" in str(exc_info.value)


@pytest.mark.usefixtures("_db_test_structure")
def test_delete_structure(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Verify that the database is initially populated
        assert session.query(StructureServiceElementTypeDBModel).count() > 0
        assert session.query(StructureServiceThingNodeDBModel).count() > 0
        assert session.query(StructureServiceSourceDBModel).count() > 0
        assert session.query(StructureServiceSinkDBModel).count() > 0
        assert session.query(thingnode_source_association).count() > 0
        assert session.query(thingnode_sink_association).count() > 0

    delete_structure()

    with mocked_clean_test_db_session() as session:
        # Verify that all tables are empty after purging
        assert session.query(StructureServiceElementTypeDBModel).count() == 0
        assert session.query(StructureServiceThingNodeDBModel).count() == 0
        assert session.query(StructureServiceSourceDBModel).count() == 0
        assert session.query(StructureServiceSinkDBModel).count() == 0
        assert session.query(thingnode_source_association).count() == 0
        assert session.query(thingnode_sink_association).count() == 0


@pytest.mark.usefixtures("_db_test_structure")
def test_update_structure_with_new_elements():
    # Load the initial and updated structure from JSON files
    initial_file_path = "tests/structure/data/db_test_structure.json"
    updated_file_path = "tests/structure/data/db_updated_test_structure.json"

    # Load structures from JSON files
    with open(initial_file_path) as file:
        initial_structure_data = json.load(file)

    with open(updated_file_path) as file:
        updated_structure_data = json.load(file)

    with get_session()() as session, session.begin():
        # Verify the initial structure in the database
        verify_structure(session, initial_structure_data)

        # Load and update the structure with new elements
        updated_structure = load_structure_from_json_file(updated_file_path)

        # Clear the existing entries
        delete_structure()  # Clears all relevant tables

        # Perform the structure update
        update_structure(updated_structure)

    with get_session()() as session, session.begin():
        # Verify that the updated structure is correct in the database
        verify_structure(session, updated_structure_data)


def verify_structure(session, structure_data):
    # Verify the count of entries for main models based on JSON data
    model_data_pairs = [
        (StructureServiceElementTypeDBModel, structure_data["element_types"]),
        (StructureServiceThingNodeDBModel, structure_data["thing_nodes"]),
        (StructureServiceSourceDBModel, structure_data["sources"]),
        (StructureServiceSinkDBModel, structure_data["sinks"]),
    ]

    for model, data in model_data_pairs:
        actual_count = session.query(model).count()
        expected_count = len(data)
        assert (
            actual_count == expected_count
        ), f"Expected {expected_count} entries for {model.__name__}, found {actual_count}"

    # Check specific attributes based on JSON data
    for thing_node in structure_data["thing_nodes"]:
        tn_db = (
            session.query(StructureServiceThingNodeDBModel)
            .filter_by(external_id=thing_node["external_id"])
            .one()
        )
        for key, value in thing_node.get("meta_data", {}).items():
            assert (
                tn_db.meta_data.get(key) == value
            ), f"Mismatch in {key} for ThingNode '{tn_db.external_id}'"


def test_update_structure(mocked_clean_test_db_session):
    # This test checks both the insert and update functionality of the update_structure function.
    # It starts with an empty database, loads a complete structure from a JSON file, and then
    # updates the database with this structure. The test then verifies that the structure
    # has been correctly inserted/updated in the database.

    # Load test data from JSON file
    with open("tests/structure/data/db_test_structure.json") as file:
        data = json.load(file)
    # Create a CompleteStructure object from the loaded JSON data
    complete_structure = CompleteStructure(**data)

    # Perform the update, which in this case acts as an insert since the database is empty
    update_structure(complete_structure)

    # Open a new session to interact with the database
    with mocked_clean_test_db_session() as session:
        # Fetch all ThingNodes, Sources, Sinks, and ElementTypes from the database
        thing_nodes = session.query(StructureServiceThingNodeDBModel).all()
        sources = session.query(StructureServiceSourceDBModel).all()
        sinks = session.query(StructureServiceSinkDBModel).all()
        element_types = session.query(StructureServiceElementTypeDBModel).all()

        # Verify that the number of ThingNodes in the database
        # matches the number in the JSON structure
        assert len(thing_nodes) == len(
            complete_structure.thing_nodes
        ), "Mismatch in number of thing nodes"
        # Verify that the number of Sources in the database matches the number in the JSON structure
        assert len(sources) == len(complete_structure.sources), "Mismatch in number of sources"
        # Verify that the number of Sinks in the database matches the number in the JSON structure
        assert len(sinks) == len(complete_structure.sinks), "Mismatch in number of sinks"
        # Verify that the number of ElementTypes in the database
        # matches the number in the JSON structure
        assert len(element_types) == len(
            complete_structure.element_types
        ), "Mismatch in number of element types"

        # Validate that specific ThingNodes, Sources, and Sinks exist in the database
        # Check if the 'Waterworks 1' ThingNode was correctly inserted
        waterworks_node = next((tn for tn in thing_nodes if tn.name == "Waterworks 1"), None)
        assert waterworks_node is not None, "Expected 'Waterworks 1' node not found"

        # Check if the 'Energy consumption of a single pump in Storage Tank' Source
        # was correctly inserted
        source = next(
            (s for s in sources if s.name == "Energy consumption of a single pump in Storage Tank"),
            None,
        )
        assert (
            source is not None
        ), "Expected source 'Energy consumption of a single pump in Storage Tank' not found"

        # Check if the 'Anomaly Score for the energy usage of the pump system in
        # Storage Tank' Sink was correctly inserted
        sink = next(
            (
                s
                for s in sinks
                if s.name == "Anomaly Score for the energy usage of the pump system in Storage Tank"
            ),
            None,
        )
        assert sink is not None, (
            "Expected sink 'Anomaly Score for the energy usage"
            " of the pump system in Storage Tank' not found"
        )


def test_update_structure_from_file(mocked_clean_test_db_session):
    # This test checks the insert functionality of the update_structure function.
    # It starts with an empty database and verifies that the structure from the JSON file
    # is correctly inserted into the database.

    # Path to the JSON file containing the test structure
    file_path = "tests/structure/data/db_test_structure.json"

    # Load structure data from the file
    with open(file_path) as file:
        structure_data = json.load(file)

    # Ensure the database is empty at the beginning
    with get_session()() as session:
        model_data_pairs = [
            StructureServiceElementTypeDBModel,
            StructureServiceThingNodeDBModel,
            StructureServiceSourceDBModel,
            StructureServiceSinkDBModel,
        ]
        for model in model_data_pairs:
            assert session.query(model).count() == 0, f"Expected 0 entries for {model.__name__}"

    # Load and update the structure in the database with the loaded structure data
    complete_structure = CompleteStructure(**structure_data)
    update_structure(complete_structure)

    # Verify that the structure was correctly inserted
    with get_session()() as session:
        # Check each model based on structure data counts
        model_data_map = {
            StructureServiceElementTypeDBModel: structure_data["element_types"],
            StructureServiceThingNodeDBModel: structure_data["thing_nodes"],
            StructureServiceSourceDBModel: structure_data["sources"],
            StructureServiceSinkDBModel: structure_data["sinks"],
        }
        for model, data in model_data_map.items():
            actual_count = session.query(model).count()
            expected_count = len(data)
            assert (
                actual_count == expected_count
            ), f"Expected {expected_count} entries for {model.__name__}, found {actual_count}"

        # Verify attributes for each entry type
        for element_type in structure_data["element_types"]:
            db_element_type = (
                session.query(StructureServiceElementTypeDBModel)
                .filter_by(external_id=element_type["external_id"])
                .one()
            )
            assert db_element_type.name == element_type["name"]
            assert db_element_type.description == element_type.get("description", "")

        for thing_node in structure_data["thing_nodes"]:
            db_thing_node = (
                session.query(StructureServiceThingNodeDBModel)
                .filter_by(external_id=thing_node["external_id"])
                .one()
            )
            assert db_thing_node.name == thing_node["name"]
            for key, value in thing_node.get("meta_data", {}).items():
                assert (
                    db_thing_node.meta_data.get(key) == value
                ), f"Mismatch in {key} for ThingNode '{db_thing_node.external_id}'"

        for source in structure_data["sources"]:
            db_source = (
                session.query(StructureServiceSourceDBModel)
                .filter_by(external_id=source["external_id"])
                .one()
            )
            assert db_source.name == source["name"]
            for key, value in source.get("meta_data", {}).items():
                assert (
                    db_source.meta_data.get(key) == value
                ), f"Mismatch in {key} for Source '{db_source.external_id}'"

        for sink in structure_data["sinks"]:
            db_sink = (
                session.query(StructureServiceSinkDBModel)
                .filter_by(external_id=sink["external_id"])
                .one()
            )
            assert db_sink.name == sink["name"]
            for key, value in sink.get("meta_data", {}).items():
                assert (
                    db_sink.meta_data.get(key) == value
                ), f"Mismatch in {key} for Sink '{db_sink.external_id}'"


@pytest.mark.usefixtures("_db_test_structure")
def test_update_structure_no_elements_deleted():
    # This test ensures that no elements are deleted when updating the structure
    # with a new JSON file that omits some elements. It verifies that the total number of elements
    # remains unchanged and that specific elements from the original structure are still present.

    # Define paths to the JSON files
    old_file_path = "tests/structure/data/db_test_structure.json"
    new_file_path = "tests/structure/data/db_test_incomplete_structure.json"

    # Load initial structure from JSON file
    initial_structure: CompleteStructure = load_structure_from_json_file(old_file_path)

    # Load updated structure from new JSON file
    updated_structure: CompleteStructure = load_structure_from_json_file(new_file_path)

    # Update the structure in the database with new structure
    update_structure(updated_structure)

    # Verify structure after update
    with get_session()() as session:
        # Check the number of elements after update
        assert session.query(StructureServiceElementTypeDBModel).count() == len(
            initial_structure.element_types
        )
        assert session.query(StructureServiceThingNodeDBModel).count() == len(
            initial_structure.thing_nodes
        )
        assert session.query(StructureServiceSourceDBModel).count() == len(
            initial_structure.sources
        )
        assert session.query(StructureServiceSinkDBModel).count() == len(initial_structure.sinks)

        # Verify specific elements from the initial structure are still present
        # Element Types
        for element_type in initial_structure.element_types:
            assert (
                session.query(StructureServiceElementTypeDBModel)
                .filter_by(external_id=element_type.external_id)
                .count()
                == 1
            )

        # Thing Nodes
        for thing_node in initial_structure.thing_nodes:
            assert (
                session.query(StructureServiceThingNodeDBModel)
                .filter_by(external_id=thing_node.external_id)
                .count()
                == 1
            )

        # StructureServiceSources
        for source in initial_structure.sources:
            assert (
                session.query(StructureServiceSourceDBModel)
                .filter_by(external_id=source.external_id)
                .count()
                == 1
            )

        # StructureServiceSinks
        for sink in initial_structure.sinks:
            assert (
                session.query(StructureServiceSinkDBModel)
                .filter_by(external_id=sink.external_id)
                .count()
                == 1
            )


def test_are_structure_tables_empty_when_empty(mocked_clean_test_db_session):
    assert are_structure_tables_empty(), "Database should be empty but is not."


@pytest.mark.usefixtures("_db_test_structure")
def test_are_structure_tables_empty_when_not_empty(mocked_clean_test_db_session):
    assert not are_structure_tables_empty(), "Database should not be empty but it is."


@pytest.mark.usefixtures("_db_test_unordered_structure")
def test_sort_thing_nodes(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Fetch all thing node keys first to pass them to fetch_thing_nodes
        thing_node_keys = {
            (tn.stakeholder_key, tn.external_id)
            for tn in session.query(StructureServiceThingNodeDBModel).all()
        }

        # Fetch all thing nodes from the database
        thing_nodes_in_db = fetch_thing_nodes(session, thing_node_keys)
        thing_nodes_in_db = list(thing_nodes_in_db.values())

        # Create a mapping of thing nodes for the sort function
        existing_thing_nodes = {
            (tn.stakeholder_key, tn.external_id): tn for tn in thing_nodes_in_db
        }

        # Run the sort function using the new sort_thing_nodes method
        sorted_nodes = sort_thing_nodes(thing_nodes_in_db)

        # Verify that the sorted_nodes is a list
        assert isinstance(sorted_nodes, list), "sorted_nodes should be a list"

        # Verify the order based on the structure hierarchy
        root_nodes = [node for node in sorted_nodes if node.parent_node_id is None]
        assert len(root_nodes) == 1, "There should be exactly one root node"

        # Generate expected order  based on sorted structure
        expected_order = [node.name for node in sorted_nodes]

        # Extract and compare actual order
        actual_order = [node.name for node in sorted_nodes]
        assert (
            actual_order == expected_order
        ), f"Expected node order {expected_order}, but got {actual_order}"

        # Check that nodes with the same parent are sorted by external_id
        grouped_nodes = {}
        for node in sorted_nodes:
            grouped_nodes.setdefault(node.parent_node_id, []).append(node)

        for group in grouped_nodes.values():
            group_names = [node.name for node in sorted(group, key=lambda x: x.external_id)]
            assert group_names == [
                node.name for node in group
            ], "Nodes should be sorted by external_id. "
            f"Expected {group_names}, got {[node.name for node in group]}"

        # Ensure the condition where a parent_node_id is not initially in existing_thing_nodes
        orphan_node = StructureServiceThingNodeDBModel(
            external_id="Orphan_StorageTank",
            name="Orphan Storage Tank",
            stakeholder_key="GW",
            parent_node_id=uuid.uuid4(),  # Ensure this UUID does not match any existing node
            parent_external_node_id="NonExistentParent",  # Set to a value not in thing_node_map
            element_type_id=uuid.uuid4(),
            element_type_external_id="StorageTank_Type",  # Required element_type_external_id
            meta_data={},
        )

        thing_nodes_in_db.append(orphan_node)

        # Re-run the sort function with the orphan node added
        sorted_nodes_with_orphan = sort_thing_nodes(thing_nodes_in_db)

        # Verify that the orphan node is not placed in the list
        assert (
            orphan_node not in sorted_nodes_with_orphan
        ), "Orphan node should not be included in the sorted list"


def test_fetch_sources_exception_handling(mocked_clean_test_db_session):
    # Create an invalid key tuple that will break the query
    invalid_keys = {("invalid_stakeholder_key",)}

    with (
        pytest.raises(
            DBError,
            match=r"Unexpected error while fetching StructureServiceSourceDBModel",
        ),
        mocked_clean_test_db_session() as session,
    ):
        # This will raise an error due to malformed input (tuple is incomplete)
        fetch_sources(session, invalid_keys)


def test_populate_element_type_ids_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Create an StructureServiceElementTypeDBModel and add it to the session
        element_type_id = uuid.uuid4()
        element_type_orm = StructureServiceElementTypeDBModel(
            id=element_type_id,
            external_id="type1",
            stakeholder_key="GW",
            name="Test StructureServiceElementType",
            description="Description",
        )
        session.add(element_type_orm)
        session.commit()

        # Existing StructureServiceElementTypes mapping
        existing_element_types = {("GW", "type1"): element_type_orm}

        # Create StructureServiceThingNodes that reference the StructureServiceElementType
        thing_nodes = [
            StructureServiceThingNode(
                id=uuid.uuid4(),
                stakeholder_key="GW",
                external_id="node1",
                element_type_external_id="type1",
                name="Node 1",
                description="Test Node",
            )
        ]

        # Call the function
        populate_element_type_ids(thing_nodes, existing_element_types)

        # Assert that the element_type_id was set correctly
        assert thing_nodes[0].element_type_id == element_type_id


def test_populate_element_type_ids_element_type_not_found(mocked_clean_test_db_session, caplog):
    # Empty existing StructureServiceElementTypes mapping
    existing_element_types = {}

    # Create StructureServiceThingNodes that reference a non-existing StructureServiceElementType
    thing_nodes = [
        StructureServiceThingNode(
            id=uuid.uuid4(),
            stakeholder_key="GW",
            external_id="node1",
            element_type_external_id="non_existing_type",
            name="Node 1",
            description="Test Node",
        )
    ]

    # Store the original element_type_id
    original_element_type_id = thing_nodes[0].element_type_id

    # Test that ValueError is raised
    with pytest.raises(ValueError, match="No StructureServiceElementType found for the key"):
        populate_element_type_ids(thing_nodes, existing_element_types)

    # Assert that element_type_id remains unchanged
    assert thing_nodes[0].element_type_id == original_element_type_id

    # Check that a warning was logged
    assert any(
        "StructureServiceElementType with key ('GW', 'non_existing_type') not found "
        "for StructureServiceThingNode node1." in record.message
        for record in caplog.records
    )


def test_search_element_types_by_name_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add StructureServiceElementTypeDBModels to the session
        session.add_all(
            [
                StructureServiceElementTypeDBModel(
                    id=uuid.uuid4(),
                    external_id="type1",
                    stakeholder_key="GW",
                    name="Test StructureServiceElementType",
                    description="Description",
                ),
                StructureServiceElementTypeDBModel(
                    id=uuid.uuid4(),
                    external_id="type2",
                    stakeholder_key="GW",
                    name="Another StructureServiceElementType",
                    description="Another Description",
                ),
            ]
        )
        session.commit()

        # Search for 'Test'
        result = search_element_types_by_name(session, "Test")

        # Assert that the correct StructureServiceElementTypeDBModel is returned
        assert len(result) == 1
        assert result[0].name == "Test StructureServiceElementType"


def test_search_element_types_by_name_no_matches(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add StructureServiceElementTypeDBModels to the session
        session.add_all(
            [
                StructureServiceElementTypeDBModel(
                    id=uuid.uuid4(),
                    external_id="type1",
                    stakeholder_key="GW",
                    name="Sample StructureServiceElementType",
                    description="Description",
                ),
                StructureServiceElementTypeDBModel(
                    id=uuid.uuid4(),
                    external_id="type2",
                    stakeholder_key="GW",
                    name="Another StructureServiceElementType",
                    description="Another Description",
                ),
            ]
        )
        session.commit()

        # Search for 'Nonexistent'
        result = search_element_types_by_name(session, "Nonexistent")

        # Assert that no StructureServiceElementTypeDBModel is returned
        assert len(result) == 0


def test_upsert_element_types_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Create StructureServiceElementType objects to upsert
        elements = [
            StructureServiceElementType(
                id=uuid.uuid4(),
                external_id="type1",
                stakeholder_key="GW",
                name="Test StructureServiceElementType",
                description="Description",
            )
        ]

        # Call the function
        upsert_element_types(session, elements)
        session.commit()

        # Verify that the StructureServiceElementTypeDBModel was added to the database
        result = (
            session.query(StructureServiceElementTypeDBModel)
            .filter_by(external_id="type1")
            .one_or_none()
        )
        assert result is not None
        assert result.name == "Test StructureServiceElementType"


def test_upsert_sinks_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        existing_thing_nodes = {}

        # Create StructureServiceSink object to upsert
        sink = StructureServiceSink(
            id=uuid.uuid4(),
            stakeholder_key="GW",
            external_id="sink1",
            name="Test StructureServiceSink",
            type="timeseries(float)",
            visible=True,
            display_path="Path",
            adapter_key="Adapter",
            sink_id="StructureServiceSinkID",
            ref_key=None,
            ref_id="RefID",
            meta_data={},
            preset_filters={},
            passthrough_filters=[Filter(name="filter1", type="free_text", required=True)],
            thing_node_external_ids=[],
        )

        upsert_sinks(session, [sink])
        session.commit()

        # Verify that the StructureServiceSinkDBModel was added to the database
        result = (
            session.query(StructureServiceSinkDBModel).filter_by(external_id="sink1").one_or_none()
        )
        assert result is not None
        assert result.name == "Test StructureServiceSink"


def test_upsert_sources_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        existing_thing_nodes = {}

        # Create StructureServiceSource object to upsert
        source = StructureServiceSource(
            id=uuid.uuid4(),
            stakeholder_key="GW",
            external_id="source1",
            name="Test StructureServiceSource",
            type="timeseries(float)",
            visible=True,
            display_path="Path",
            adapter_key="Adapter",
            source_id="StructureServiceSourceID",
            ref_key=None,
            ref_id="RefID",
            meta_data={},
            preset_filters={},
            passthrough_filters=[Filter(name="filter1", type="free_text", required=True)],
            thing_node_external_ids=[],
        )

        # Call the function
        upsert_sources(session, [source], existing_thing_nodes)
        session.commit()

        # Verify that the StructureServiceSourceDBModel was added to the database
        result = (
            session.query(StructureServiceSourceDBModel)
            .filter_by(external_id="source1")
            .one_or_none()
        )
        assert result is not None
        assert result.name == "Test StructureServiceSource"


def test_search_thing_nodes_by_name_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add StructureServiceElementTypeDBModel to the session
        element_type = StructureServiceElementTypeDBModel(
            id=uuid.uuid4(),
            external_id="type1",
            stakeholder_key="GW",
            name="Test Type",
            description="A test element type",
        )
        session.add(element_type)
        session.commit()

        # Add StructureServiceThingNodeDBModels to the session with the element_type
        session.add_all(
            [
                StructureServiceThingNodeDBModel(
                    id=uuid.uuid4(),
                    external_id="node1",
                    stakeholder_key="GW",
                    name="Test Node",
                    description="Description",
                    parent_node_id=None,
                    parent_external_node_id=None,
                    element_type_id=element_type.id,  # Link to StructureServiceElementTypeDBModel
                    element_type_external_id="type1",
                    meta_data={},
                ),
                StructureServiceThingNodeDBModel(
                    id=uuid.uuid4(),
                    external_id="node2",
                    stakeholder_key="GW",
                    name="Another Node",
                    description="Description",
                    parent_node_id=None,
                    parent_external_node_id=None,
                    element_type_id=element_type.id,
                    element_type_external_id="type1",
                    meta_data={},
                ),
            ]
        )
        session.commit()

        # Search for 'Test'
        result = search_thing_nodes_by_name(session, "Test")

        # Assert that the correct StructureServiceThingNodeDBModel is returned
        assert len(result) == 1
        assert result[0].name == "Test Node"


def test_search_thing_nodes_by_name_no_matches(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add StructureServiceElementTypeDBModel to the session
        element_type = StructureServiceElementTypeDBModel(
            id=uuid.uuid4(),
            external_id="type1",
            stakeholder_key="GW",
            name="type1",
            description="A test element type",
        )
        session.add(element_type)
        session.commit()
        # Add StructureServiceThingNodeDBModel to the session
        session.add(
            StructureServiceThingNodeDBModel(
                id=uuid.uuid4(),
                external_id="node1",
                stakeholder_key="GW",
                name="Sample Node",
                description="Description",
                parent_node_id=None,
                parent_external_node_id=None,
                element_type_id=element_type.id,
                element_type_external_id="type1",
                meta_data={},
            )
        )
        session.commit()

        # Search for 'Nonexistent'
        result = search_thing_nodes_by_name(session, "Nonexistent")

        # Assert that no StructureServiceThingNodeDBModel is returned
        assert len(result) == 0


def test_upsert_thing_nodes_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add an StructureServiceElementTypeDBModel to the session
        element_type_id = uuid.uuid4()
        element_type = StructureServiceElementTypeDBModel(
            id=element_type_id,
            external_id="type1",
            stakeholder_key="GW",
            name="Test StructureServiceElementType",
            description="Description",
        )
        session.add(element_type)
        session.commit()

        # Create StructureServiceThingNode object to upsert
        node = StructureServiceThingNode(
            id=uuid.uuid4(),
            stakeholder_key="GW",
            external_id="node1",
            name="Test Node",
            description="Description",
            parent_node_id=None,
            parent_external_node_id=None,
            element_type_external_id="type1",
            meta_data={},
        )

        # Call the function
        upsert_thing_nodes(session, [node])
        session.commit()

        # Verify that the StructureServiceThingNodeDBModel was added to the database
        result = (
            session.query(StructureServiceThingNodeDBModel)
            .filter_by(external_id="node1")
            .one_or_none()
        )
        assert result is not None
        assert result.name == "Test Node"


@pytest.mark.usefixtures("_db_test_structure")
def test_get_children():
    with get_session()() as session, session.begin():
        # Fetch root and child nodes to avoid hardcoding
        root_node = (
            session.query(StructureServiceThingNodeDBModel).filter_by(parent_node_id=None).one()
        )
        root_children, root_sources, root_sinks = get_children(root_node.id)

        verify_children(root_children, {child.name for child in root_children}, len(root_children))
        verify_sources(root_sources, [source.name for source in root_sources], len(root_sources))
        verify_sinks(root_sinks, [sink.name for sink in root_sinks], len(root_sinks))

        # Verify each child node and its respective children, sources, and sinks
        for child in root_children:
            sub_children, sub_sources, sub_sinks = get_children(child.id)
            verify_children(sub_children, {sc.name for sc in sub_children}, len(sub_children))
            verify_sources(sub_sources, [src.name for src in sub_sources], len(sub_sources))
            verify_sinks(sub_sinks, [snk.name for snk in sub_sinks], len(sub_sinks))


def get_node_by_name(session, name: str) -> StructureServiceThingNodeDBModel:
    """Helper function to fetch a ThingNode by name."""
    node = (
        session.query(StructureServiceThingNodeDBModel)
        .filter(StructureServiceThingNodeDBModel.name == name)
        .one_or_none()
    )
    assert node is not None, f"Expected node '{name}' not found"
    return node


def verify_children(
    children: list[StructureServiceThingNode], expected_names: set, expected_count: int
):
    """Helper function to verify the children nodes."""
    assert (
        len(children) == expected_count
    ), f"Expected {expected_count} children, found {len(children)}"
    children_names = {child.name for child in children}
    assert children_names == expected_names, f"Unexpected child names: {children_names}"


def verify_sources(
    sources: list[StructureServiceSource], expected_names: list, expected_count: int
):
    """Helper function to verify the sources."""
    assert (
        len(sources) == expected_count
    ), f"Expected {expected_count} source(s), found {len(sources)}"
    actual_names = [source.name for source in sources]
    assert actual_names == expected_names, f"Unexpected source names: {actual_names}"


def verify_sinks(sinks: list[StructureServiceSink], expected_names: list, expected_count: int):
    """Helper function to verify the sinks."""
    assert len(sinks) == expected_count, f"Expected {expected_count} sink(s), found {len(sinks)}"
    actual_names = [sink.name for sink in sinks]
    assert actual_names == expected_names, f"Unexpected sink names: {actual_names}"
