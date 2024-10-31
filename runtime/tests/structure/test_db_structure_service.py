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
    DBParsingError,
)
from hetdesrun.structure.db.source_sink_service import (
    fetch_sinks,
    fetch_sources,
    upsert_sinks,
    upsert_sources,
)
from hetdesrun.structure.db.structure_service import (
    delete_structure,
    is_database_empty,
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
def test_thing_node_hierarchy_db_service(mocked_clean_test_db_session):  # noqa: PLR0915
    """
    Tests the hierarchy and relationships of StructureServiceThingNodes, StructureServiceSources,
    and StructureServiceSinks in the database.

    This test verifies that the expected number of StructureServiceElementTypes,
    StructureServiceThingNodes, StructureServiceSources, and StructureServiceSinks
    are present in the database after the test structure is loaded.
    """
    with mocked_clean_test_db_session() as session:
        # Define expected keys based on the fixture data
        expected_element_type_keys = {
            ("GW", "Waterworks_Type"),
            ("GW", "Plant_Type"),
            ("GW", "StorageTank_Type"),
        }

        expected_source_keys = {
            ("GW", "EnergyUsage_PumpSystem_StorageTank"),
            ("GW", "EnergyConsumption_SinglePump_StorageTank"),
            ("GW", "EnergyConsumption_Waterworks1"),
        }

        expected_sink_keys = {
            ("GW", "AnomalyScore_EnergyUsage_PumpSystem_StorageTank"),
            ("GW", "AnomalyScore_EnergyConsumption_SinglePump_StorageTank"),
            ("GW", "AnomalyScore_EnergyConsumption_Waterworks1"),
        }

        expected_thing_node_keys = {
            ("GW", "Waterworks1"),
            ("GW", "Waterworks1_Plant1"),
            ("GW", "Waterworks1_Plant2"),
            ("GW", "Waterworks1_Plant1_StorageTank1"),
            ("GW", "Waterworks1_Plant1_StorageTank2"),
            ("GW", "Waterworks1_Plant2_StorageTank1"),
            ("GW", "Waterworks1_Plant2_StorageTank2"),
        }

        # Fetch StructureServiceElementTypes using the new fetch_element_types function
        element_types_in_db = fetch_element_types(session, expected_element_type_keys)
        assert len(element_types_in_db) == 3, "Mismatch in element types count"

        # Fetch StructureServiceSources using the new fetch_sources function
        sources_in_db = fetch_sources(session, expected_source_keys)
        assert len(sources_in_db) == 3, "Mismatch in sources count"

        # Fetch StructureServiceSinks using the new fetch_sinks function
        sinks_in_db = fetch_sinks(session, expected_sink_keys)
        assert len(sinks_in_db) == 3, "Mismatch in sinks count"

        # Fetch StructureServiceThingNodes using the new fetch_thing_nodes function
        thing_nodes_in_db = fetch_thing_nodes(session, expected_thing_node_keys)
        assert len(thing_nodes_in_db) == 7, "Mismatch in thing nodes count"

        # Optional: Verify specific relationships

        # Verify that "Waterworks1" is the parent of "Plant1" and "Plant2"
        parent_key = ("GW", "Waterworks1")
        plant1_key = ("GW", "Waterworks1_Plant1")
        plant2_key = ("GW", "Waterworks1_Plant2")
        assert plant1_key in thing_nodes_in_db, "Plant1 not found in StructureServiceThingNodes"
        assert plant2_key in thing_nodes_in_db, "Plant2 not found in StructureServiceThingNodes"

        parent_node = thing_nodes_in_db[parent_key]
        plant1_node = thing_nodes_in_db[plant1_key]
        plant2_node = thing_nodes_in_db[plant2_key]

        # Check parent_node_id is set correctly
        assert plant1_node.parent_node_id == parent_node.id, "Plant1 has incorrect parent"
        assert plant2_node.parent_node_id == parent_node.id, "Plant2 has incorrect parent"

        # Verify that storage tanks have correct parents
        storage_tank_keys = {
            ("GW", "Waterworks1_Plant1_StorageTank1"),
            ("GW", "Waterworks1_Plant1_StorageTank2"),
            ("GW", "Waterworks1_Plant2_StorageTank1"),
            ("GW", "Waterworks1_Plant2_StorageTank2"),
        }

        for st_key in storage_tank_keys:
            assert st_key in thing_nodes_in_db, f"{st_key} not found in StructureServiceThingNodes"

            storage_tank_node = thing_nodes_in_db[st_key]
            if "Plant1" in st_key[1]:
                expected_parent = plant1_node.id
            elif "Plant2" in st_key[1]:
                expected_parent = plant2_node.id
            else:
                expected_parent = None  # Should not happen

            assert (
                storage_tank_node.parent_node_id == expected_parent
            ), f"{st_key} has incorrect parent"

        # Verify associations for sources
        # Example: "EnergyUsage_PumpSystem_StorageTank" should be associated with "StorageTank1"
        # and "StorageTank2" in Plant1 and Plant2
        source_key = ("GW", "EnergyUsage_PumpSystem_StorageTank")
        assert (
            source_key in sources_in_db
        ), "EnergyUsage_PumpSystem_StorageTank not found in StructureServiceSources"
        source = sources_in_db[source_key]
        expected_associated_storage_tanks = {
            ("GW", "Waterworks1_Plant1_StorageTank1"),
            ("GW", "Waterworks1_Plant2_StorageTank2"),
        }
        actual_associated_storage_tanks = {
            (tn.stakeholder_key, tn.external_id) for tn in source.thing_nodes
        }
        assert (
            actual_associated_storage_tanks == expected_associated_storage_tanks
        ), "Incorrect associations for source EnergyUsage_PumpSystem_StorageTank"

        # Similarly, verify other source associations
        # "EnergyConsumption_SinglePump_StorageTank" associated with "StorageTank2, Plant1"
        # and "StorageTank1, Plant2"
        source_key = ("GW", "EnergyConsumption_SinglePump_StorageTank")
        assert (
            source_key in sources_in_db
        ), "EnergyConsumption_SinglePump_StorageTank not found in StructureServiceSources"
        source = sources_in_db[source_key]
        expected_associated_storage_tanks = {
            ("GW", "Waterworks1_Plant1_StorageTank2"),
            ("GW", "Waterworks1_Plant2_StorageTank1"),
        }
        actual_associated_storage_tanks = {
            (tn.stakeholder_key, tn.external_id) for tn in source.thing_nodes
        }
        assert (
            actual_associated_storage_tanks == expected_associated_storage_tanks
        ), "Incorrect associations for source EnergyConsumption_SinglePump_StorageTank"

        # "EnergyConsumption_Waterworks1" associated with "Waterworks1"
        source_key = ("GW", "EnergyConsumption_Waterworks1")
        assert (
            source_key in sources_in_db
        ), "EnergyConsumption_Waterworks1 not found in StructureServiceSources"
        source = sources_in_db[source_key]
        expected_associated_storage_tanks = {
            ("GW", "Waterworks1"),
        }
        actual_associated_storage_tanks = {
            (tn.stakeholder_key, tn.external_id) for tn in source.thing_nodes
        }
        assert (
            actual_associated_storage_tanks == expected_associated_storage_tanks
        ), "Incorrect associations for source EnergyConsumption_Waterworks1"

        # Similarly, verify associations for sinks
        # "AnomalyScore_EnergyUsage_PumpSystem_StorageTank" associated with "StorageTank1, Plant1"
        # and "StorageTank2, Plant1"
        sink_key = ("GW", "AnomalyScore_EnergyUsage_PumpSystem_StorageTank")
        assert (
            sink_key in sinks_in_db
        ), "AnomalyScore_EnergyUsage_PumpSystem_StorageTank not found in StructureServiceSinks"
        sink = sinks_in_db[sink_key]
        expected_associated_storage_tanks = {
            ("GW", "Waterworks1_Plant1_StorageTank1"),
            ("GW", "Waterworks1_Plant1_StorageTank2"),
        }
        actual_associated_storage_tanks = {
            (tn.stakeholder_key, tn.external_id) for tn in sink.thing_nodes
        }
        assert (
            actual_associated_storage_tanks == expected_associated_storage_tanks
        ), "Incorrect associations for sink AnomalyScore_EnergyUsage_PumpSystem_StorageTank"

        # "AnomalyScore_EnergyConsumption_SinglePump_StorageTank" associated with
        # "StorageTank2, Plant2" and "StorageTank1, Plant2"
        sink_key = ("GW", "AnomalyScore_EnergyConsumption_SinglePump_StorageTank")
        assert sink_key in sinks_in_db, (
            "AnomalyScore_EnergyConsumption_SinglePump_StorageTank not found in "
            "StructureServiceSinks"
        )
        sink = sinks_in_db[sink_key]
        expected_associated_storage_tanks = {
            ("GW", "Waterworks1_Plant2_StorageTank2"),
            ("GW", "Waterworks1_Plant2_StorageTank1"),
        }
        actual_associated_storage_tanks = {
            (tn.stakeholder_key, tn.external_id) for tn in sink.thing_nodes
        }
        assert (
            actual_associated_storage_tanks == expected_associated_storage_tanks
        ), "Incorrect associations for sink AnomalyScore_EnergyConsumption_SinglePump_StorageTank"

        # "AnomalyScore_EnergyConsumption_Waterworks1" associated with "Waterworks1"
        sink_key = ("GW", "AnomalyScore_EnergyConsumption_Waterworks1")
        assert (
            sink_key in sinks_in_db
        ), "AnomalyScore_EnergyConsumption_Waterworks1 not found in StructureServiceSinks"
        sink = sinks_in_db[sink_key]
        expected_associated_storage_tanks = {
            ("GW", "Waterworks1"),
        }
        actual_associated_storage_tanks = {
            (tn.stakeholder_key, tn.external_id) for tn in sink.thing_nodes
        }
        assert (
            actual_associated_storage_tanks == expected_associated_storage_tanks
        ), "Incorrect associations for sink AnomalyScore_EnergyConsumption_Waterworks1"


### Fetch Functions


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_element_types_db_service(mocked_clean_test_db_session):
    # Start a session with the mocked, clean test database
    with mocked_clean_test_db_session() as session:
        # Define the keys for the expected element types
        keys = {
            ("GW", "Waterworks_Type"),
            ("GW", "Plant_Type"),
            ("GW", "StorageTank_Type"),
        }

        # Fetch the element types based on the defined keys
        element_types = fetch_element_types(session, keys)

        # Check if the correct number of element types was retrieved
        assert (
            len(element_types) == 3
        ), f"Expected 3 Element Types in the database, found {len(element_types)}"

        # Define the expected element types based on the test data
        expected_element_types = [
            {"external_id": "Waterworks_Type", "name": "Waterworks"},
            {"external_id": "Plant_Type", "name": "Plant"},
            {"external_id": "StorageTank_Type", "name": "Storage Tank"},
        ]

        # Check if each expected element type is present in the retrieved element types
        for expected_et in expected_element_types:
            # Generate the key based on stakeholder_key and external_id
            key = ("GW", expected_et["external_id"])
            # Check if the key exists in the retrieved dictionary
            assert key in element_types, (
                f"Expected Element Type with stakeholder_key 'GW'"
                f"and external_id '{expected_et['external_id']}' not found"
            )
            # Check if the name matches
            actual_et = element_types[key]
            assert actual_et.name == expected_et["name"], (
                f"Element Type with external_id '{expected_et['external_id']}'"
                f"has name '{actual_et.name}', "
                f"expected '{expected_et['name']}'"
            )


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_thing_nodes_db_service(mocked_clean_test_db_session):
    # Start a session to interact with the database
    with mocked_clean_test_db_session() as session:
        # Define the keys for the expected StructureServiceThingNodes
        keys = {
            ("GW", "Waterworks1"),
            ("GW", "Waterworks1_Plant1"),
            ("GW", "Waterworks1_Plant2"),
            ("GW", "Waterworks1_Plant1_StorageTank1"),
            ("GW", "Waterworks1_Plant1_StorageTank2"),
            ("GW", "Waterworks1_Plant2_StorageTank1"),
            ("GW", "Waterworks1_Plant2_StorageTank2"),
        }

        # Fetch the StructureServiceThingNodes based on the defined keys
        thing_nodes = fetch_thing_nodes(session, keys)

        # Check if the correct number of StructureServiceThingNodes was retrieved
        assert (
            len(thing_nodes) == 7
        ), f"Expected 7 Thing Nodes in the database, found {len(thing_nodes)}"

        # Define the expected StructureServiceThingNodes with their external_id and name
        expected_thing_nodes = [
            {"external_id": "Waterworks1", "name": "Waterworks 1"},
            {"external_id": "Waterworks1_Plant1", "name": "Plant 1"},
            {"external_id": "Waterworks1_Plant2", "name": "Plant 2"},
            {
                "external_id": "Waterworks1_Plant1_StorageTank1",
                "name": "Storage Tank 1, Plant 1",
            },
            {
                "external_id": "Waterworks1_Plant1_StorageTank2",
                "name": "Storage Tank 2, Plant 1",
            },
            {
                "external_id": "Waterworks1_Plant2_StorageTank1",
                "name": "Storage Tank 1, Plant 2",
            },
            {
                "external_id": "Waterworks1_Plant2_StorageTank2",
                "name": "Storage Tank 2, Plant 2",
            },
        ]

        # Check if each expected StructureServiceThingNode is present in
        # the retrieved StructureServiceThingNodes
        for expected_tn in expected_thing_nodes:
            # Generate the key based on stakeholder_key and external_id
            key = ("GW", expected_tn["external_id"])
            # Check if the key exists in the retrieved dictionary
            assert key in thing_nodes, (
                f"Expected Thing Node with stakeholder_key 'GW' and external_id"
                f"'{expected_tn['external_id']}' not found"
            )
            # Check if the name matches
            actual_tn = thing_nodes[key]
            assert actual_tn.name == expected_tn["name"], (
                f"Thing Node with external_id '{expected_tn['external_id']}'"
                f"has name '{actual_tn.name}', "
                f"expected '{expected_tn['name']}'"
            )


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_sources_db_service(mocked_clean_test_db_session):
    # Start a session to interact with the database
    with mocked_clean_test_db_session() as session:
        # Define the keys for the expected StructureServiceSources
        keys = {
            ("GW", "EnergyUsage_PumpSystem_StorageTank"),
            ("GW", "EnergyConsumption_SinglePump_StorageTank"),
            # Add more keys if there are additional StructureServiceSources
        }

        # Fetch the StructureServiceSources based on the defined keys
        sources = fetch_sources(session, keys)

        # Check if the correct number of StructureServiceSources was retrieved
        assert (
            len(sources) == 2
        ), f"Expected 2 StructureServiceSources in the database, found {len(sources)}"

        # Define the expected StructureServiceSources with their external_id and name
        expected_sources = [
            {
                "external_id": "EnergyUsage_PumpSystem_StorageTank",
                "name": "Energy usage of the pump system in Storage Tank",
            },
            {
                "external_id": "EnergyConsumption_SinglePump_StorageTank",
                "name": "Energy consumption of a single pump in Storage Tank",
            },
        ]

        # Check if each expected StructureServiceSource is present in
        # the retrieved StructureServiceSources
        for expected_source in expected_sources:
            # Generate the key based on stakeholder_key and external_id
            key = ("GW", expected_source["external_id"])
            # Check if the key exists in the retrieved dictionary
            assert key in sources, (
                f"Expected StructureServiceSource with stakeholder_key 'GW' and"
                f"external_id '{expected_source['external_id']}' not found"
            )
            # Check if the name matches
            actual_source = sources[key]
            assert actual_source.name == expected_source["name"], (
                f"StructureServiceSource with external_id '{expected_source['external_id']}'"
                f"has name '{actual_source.name}', "
                f"expected '{expected_source['name']}'"
            )


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_sinks_db_service(mocked_clean_test_db_session):
    # Start a session to interact with the database
    with mocked_clean_test_db_session() as session:
        # Define the keys for the expected StructureServiceSinks
        keys = {
            ("GW", "AnomalyScore_EnergyUsage_PumpSystem_StorageTank"),
            ("GW", "AnomalyScore_EnergyConsumption_SinglePump_StorageTank"),
            ("GW", "AnomalyScore_EnergyConsumption_Waterworks1"),
        }

        # Fetch the StructureServiceSinks based on the defined keys
        sinks = fetch_sinks(session, keys)

        # Check if the correct number of StructureServiceSinks was retrieved
        assert (
            len(sinks) == 3
        ), f"Expected 3 StructureServiceSinks in the database, found {len(sinks)}"

        # Define the expected StructureServiceSinks with their external_id and name
        expected_sinks = [
            {
                "external_id": "AnomalyScore_EnergyUsage_PumpSystem_StorageTank",
                "name": "Anomaly Score for the energy usage of the pump system in Storage Tank",
            },
            {
                "external_id": "AnomalyScore_EnergyConsumption_SinglePump_StorageTank",
                "name": "Anomaly Score for the energy consumption of a single pump in Storage Tank",
            },
            {
                "external_id": "AnomalyScore_EnergyConsumption_Waterworks1",
                "name": "Anomaly Score for the energy consumption of the waterworks",
            },
        ]

        # Check if each expected StructureServiceSink is present in
        # the retrieved StructureServiceSinks
        for expected_sink in expected_sinks:
            # Generate the key based on stakeholder_key and external_id
            key = ("GW", expected_sink["external_id"])
            # Check if the key exists in the retrieved dictionary
            assert key in sinks, (
                f"Expected StructureServiceSink with stakeholder_key 'GW' and external_id"
                f"'{expected_sink['external_id']}' not found"
            )
            # Check if the name matches
            actual_sink = sinks[key]
            assert actual_sink.name == expected_sink["name"], (
                f"StructureServiceSink with external_id '{expected_sink['external_id']}'"
                f"has name '{actual_sink.name}', "
                f"expected '{expected_sink['name']}'"
            )


### Structure Helper Functions


def test_load_structure_from_json_file_db_service(db_test_structure_file_path):
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


def test_load_structure_from_invalid_json_file_db_service():
    with pytest.raises(FileNotFoundError):
        load_structure_from_json_file("non_existent_file.json")
    with pytest.raises(DBParsingError):
        load_structure_from_json_file("tests/structure/data/db_test_structure_malformed.json")


def test_load_structure_from_json_with_invalid_data_db_service():
    invalid_structure_path = "tests/structure/data/db_test_structure_invalid_data.json"
    with pytest.raises(DBParsingError) as exc_info:
        load_structure_from_json_file(invalid_structure_path)
    assert "Validation error" in str(exc_info.value)


@pytest.mark.usefixtures("_db_test_structure")
def test_delete_structure_db_service(mocked_clean_test_db_session):
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
def test_update_structure_with_new_elements_db_service():
    # This test checks the update functionality of the update_structure function.
    # It starts with an existing structure in the database, then updates it with new elements
    # from a JSON file and verifies that the structure in the database reflects these updates.

    with get_session()() as session, session.begin():
        # Verify initial structure in the database
        verify_initial_structure(session)

        # Load the updated structure from a JSON file
        file_path = "tests/structure/data/db_updated_test_structure.json"
        updated_structure = load_structure_from_json_file(file_path)

        # Update the structure in the database with the newly loaded structure
        update_structure(updated_structure)

    # Start another session to verify the updated structure
    with get_session()() as session, session.begin():
        # Verify that the structure in the database matches the updated structure
        verify_updated_structure(session)


def verify_initial_structure(session):
    # Fetch all initial elements from the database
    initial_element_types = session.query(StructureServiceElementTypeDBModel).all()
    initial_thing_nodes = session.query(StructureServiceThingNodeDBModel).all()
    initial_sources = session.query(StructureServiceSourceDBModel).all()
    initial_sinks = session.query(StructureServiceSinkDBModel).all()

    # Verify that the initial structure contains the correct number of elements
    assert len(initial_element_types) == 3, "Expected 3 Element Types in the initial structure"
    assert len(initial_thing_nodes) == 7, "Expected 7 Thing Nodes in the initial structure"
    assert len(initial_sources) == 3, "Expected 3 StructureServiceSources in the initial structure"
    assert len(initial_sinks) == 3, "Expected 3 StructureServiceSinks in the initial structure"

    # Verify specific attributes of the StructureServiceThingNodes before the update
    initial_tn = (
        session.query(StructureServiceThingNodeDBModel)
        .filter_by(external_id="Waterworks1_Plant1_StorageTank1")
        .one()
    )
    assert (
        initial_tn.meta_data["capacity"] == "5000"
    ), "Initial capacity of Storage Tank 1 should be 5000"
    assert initial_tn.meta_data["description"] == "Water storage capacity for Storage Tank 1"

    initial_tn2 = (
        session.query(StructureServiceThingNodeDBModel)
        .filter_by(external_id="Waterworks1_Plant1_StorageTank2")
        .one()
    )
    assert (
        initial_tn2.meta_data["capacity"] == "6000"
    ), "Initial capacity of Storage Tank 2 should be 6000"
    assert initial_tn2.meta_data["description"] == "Water storage capacity for Storage Tank 2"


def verify_updated_structure(session):
    # Fetch all elements from the database after the update
    final_element_types = session.query(StructureServiceElementTypeDBModel).all()
    final_thing_nodes = session.query(StructureServiceThingNodeDBModel).all()
    final_sources = session.query(StructureServiceSourceDBModel).all()
    final_sinks = session.query(StructureServiceSinkDBModel).all()

    # Verify that the structure now contains the updated number of elements
    assert len(final_element_types) == 4, "Expected 4 Element Types after the update"
    assert len(final_thing_nodes) == 8, "Expected 8 Thing Nodes after the update"
    assert len(final_sources) == 4, "Expected 4 StructureServiceSources after the update"
    assert len(final_sinks) == 3, "Expected 3 StructureServiceSinks after the update"

    # Verify the new elements and updated nodes in the structure
    verify_new_elements_and_nodes(session, final_element_types, final_thing_nodes)


def verify_new_elements_and_nodes(session, final_element_types, final_thing_nodes):
    # Verify that a new StructureServiceElementType was added
    new_element_type = next(
        et for et in final_element_types if et.external_id == "FiltrationPlant_Type"
    )
    assert (
        new_element_type.name == "Filtration Plant"
    ), "Expected new Element Type 'Filtration Plant'"
    assert new_element_type.description == "Element type for filtration plants"

    # Verify that a new StructureServiceThingNode was added
    new_tn = next(tn for tn in final_thing_nodes if tn.external_id == "Waterworks1_FiltrationPlant")
    assert new_tn.name == "Filtration Plant 1", "Expected new Thing Node 'Filtration Plant 1'"
    assert new_tn.description == "New filtration plant in Waterworks 1"
    assert (
        new_tn.meta_data["location"] == "Central"
    ), "Expected location 'Central' for the new Thing Node"
    assert (
        new_tn.meta_data["technology"] == "Advanced Filtration"
    ), "Expected technology 'Advanced Filtration'"

    # Verify that the StructureServiceThingNodes were updated correctly
    updated_tn1 = next(
        tn for tn in final_thing_nodes if tn.external_id == "Waterworks1_Plant1_StorageTank1"
    )
    assert (
        updated_tn1.meta_data["capacity"] == "5200"
    ), "Expected updated capacity 5200 for Storage Tank 1"
    assert (
        updated_tn1.meta_data["description"]
        == "Increased water storage capacity for Storage Tank 1"
    )

    updated_tn2 = next(
        tn for tn in final_thing_nodes if tn.external_id == "Waterworks1_Plant1_StorageTank2"
    )
    assert (
        updated_tn2.meta_data["capacity"] == "6100"
    ), "Expected updated capacity 6100 for Storage Tank 2"
    assert (
        updated_tn2.meta_data["description"]
        == "Increased water storage capacity for Storage Tank 2"
    )


def test_update_structure_from_file_db_service(mocked_clean_test_db_session):
    # This test specifically checks the insert functionality of the update_structure function.
    # It starts with an empty database and verifies that the structure from the JSON file is
    # correctly inserted into the database.

    # Path to the JSON file containing the test structure
    file_path = "tests/structure/data/db_test_structure.json"

    # Ensure the database is empty at the beginning
    with get_session()() as session:
        assert session.query(StructureServiceElementTypeDBModel).count() == 0
        assert session.query(StructureServiceThingNodeDBModel).count() == 0
        assert session.query(StructureServiceSourceDBModel).count() == 0
        assert session.query(StructureServiceSinkDBModel).count() == 0

    # Load structure from the file
    complete_structure = load_structure_from_json_file(file_path)

    # Update the structure in the database with the newly loaded structure
    update_structure(complete_structure)

    # Verify that the structure was correctly inserted
    with get_session()() as session:
        assert session.query(StructureServiceElementTypeDBModel).count() == 3
        assert session.query(StructureServiceThingNodeDBModel).count() == 7
        assert session.query(StructureServiceSourceDBModel).count() == 3
        assert session.query(StructureServiceSinkDBModel).count() == 3

        # Verify specific StructureServiceElementType
        waterworks_type = (
            session.query(StructureServiceElementTypeDBModel)
            .filter_by(external_id="Waterworks_Type")
            .one()
        )
        assert waterworks_type.name == "Waterworks"
        assert waterworks_type.description == "Element type for waterworks"

        # Verify specific StructureServiceThingNode
        waterworks1 = (
            session.query(StructureServiceThingNodeDBModel)
            .filter_by(external_id="Waterworks1")
            .one()
        )
        assert waterworks1.name == "Waterworks 1"
        assert waterworks1.meta_data["location"] == "Main Site"

        # Verify specific StructureServiceSource
        source = (
            session.query(StructureServiceSourceDBModel)
            .filter_by(external_id="EnergyUsage_PumpSystem_StorageTank")
            .one()
        )
        assert source.name == "Energy usage of the pump system in Storage Tank"
        assert source.meta_data["1010001"]["unit"] == "kW/h"

        # Verify specific StructureServiceSink
        sink = (
            session.query(StructureServiceSinkDBModel)
            .filter_by(external_id="AnomalyScore_EnergyUsage_PumpSystem_StorageTank")
            .one()
        )
        assert sink.name == "Anomaly Score for the energy usage of the pump system in Storage Tank"


@pytest.mark.usefixtures("_db_test_structure")
def test_update_structure_no_elements_deleted_db_service():
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


def test_is_database_empty_when_empty_db_service(mocked_clean_test_db_session):
    assert is_database_empty(), "Database should be empty but is not."


@pytest.mark.usefixtures("_db_test_structure")
def test_is_database_empty_when_not_empty_db_service(mocked_clean_test_db_session):
    assert not is_database_empty(), "Database should not be empty but it is."


@pytest.mark.usefixtures("_db_test_unordered_structure")
def test_sort_thing_nodes_db_service(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Fetch all thing node keys first to pass them to fetch_thing_nodes
        thing_node_keys = {
            (tn.stakeholder_key, tn.external_id)
            for tn in session.query(StructureServiceThingNodeDBModel).all()
        }

        # Fetch all thing nodes from the database
        thing_nodes_in_db = fetch_thing_nodes(session, thing_node_keys)

        # Since fetch_thing_nodes returns a dictionary of
        # StructureServiceThingNodeDBModel instances,
        #  no need to unpack tuples
        thing_nodes_in_db = list(thing_nodes_in_db.values())

        # Create a mapping of thing nodes for the sort function
        existing_thing_nodes = {
            (tn.stakeholder_key, tn.external_id): tn for tn in thing_nodes_in_db
        }

        # Run the sort function using the new sort_thing_nodes method
        sorted_nodes = sort_thing_nodes(thing_nodes_in_db, existing_thing_nodes)

        # Verify that the sorted_nodes is a list
        assert isinstance(sorted_nodes, list), "sorted_nodes should be a list"

        # Check that the nodes are in the expected order:
        # Waterworks 1 (root), Plant 1, Plant 2 (children), and the storage tanks
        expected_order = [
            "Waterworks 1",  # Root node
            "Plant 1",
            "Plant 2",  # First level
            "Storage Tank 1, Plant 1",
            "Storage Tank 2, Plant 1",  # Second level
            "Storage Tank 1, Plant 2",
            "Storage Tank 2, Plant 2",  # Second level
        ]
        actual_order = [node.name for node in sorted_nodes]
        assert (
            actual_order == expected_order
        ), f"Expected node order {expected_order}, but got {actual_order}"

        # Check that nodes with the same parent are sorted by external_id
        expected_sorted_by_external_id = [
            "Storage Tank 1, Plant 1",
            "Storage Tank 2, Plant 1",
            "Storage Tank 1, Plant 2",
            "Storage Tank 2, Plant 2",
        ]
        sorted_by_external_id = [node.name for node in sorted_nodes if "Storage Tank" in node.name]
        assert sorted_by_external_id == expected_sorted_by_external_id, (
            f"Nodes should be sorted by external_id. Expected {expected_sorted_by_external_id},"
            f"got {sorted_by_external_id}"
        )

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
        sorted_nodes_with_orphan = sort_thing_nodes(thing_nodes_in_db, existing_thing_nodes)

        # Verify that the orphan node is not placed in the list
        assert (
            orphan_node not in sorted_nodes_with_orphan
        ), "Orphan node should not be included in the sorted list"


def test_fetch_sources_exception_handling_db_service(mocked_clean_test_db_session):
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

    # Call the function
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
        existing_elements = {}

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
        upsert_element_types(session, elements, existing_elements)
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
        existing_sinks = {}
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

        # Call the function
        upsert_sinks(session, [sink], existing_sinks, existing_thing_nodes)
        session.commit()

        # Verify that the StructureServiceSinkDBModel was added to the database
        result = (
            session.query(StructureServiceSinkDBModel).filter_by(external_id="sink1").one_or_none()
        )
        assert result is not None
        assert result.name == "Test StructureServiceSink"


def test_upsert_sources_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        existing_sources = {}
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
        upsert_sources(session, [source], existing_sources, existing_thing_nodes)
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
        existing_thing_nodes = {}
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
        upsert_thing_nodes(session, [node], existing_thing_nodes)
        session.commit()

        # Verify that the StructureServiceThingNodeDBModel was added to the database
        result = (
            session.query(StructureServiceThingNodeDBModel)
            .filter_by(external_id="node1")
            .one_or_none()
        )
        assert result is not None
        assert result.name == "Test Node"
