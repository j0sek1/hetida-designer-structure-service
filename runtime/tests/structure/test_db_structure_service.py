import json
import uuid
from sqlite3 import Connection as SQLite3Connection

import pytest
from sqlalchemy import event
from sqlalchemy.future.engine import Engine

from hetdesrun.persistence.db_engine_and_session import get_session
from hetdesrun.persistence.structure_service_dbmodels import (
    ElementTypeOrm,
    SinkOrm,
    SourceOrm,
    ThingNodeOrm,
    thingnode_sink_association,
    thingnode_source_association,
)
from hetdesrun.structure.db.db_structure_service import (
    orm_delete_structure,
    orm_is_database_empty,
    orm_load_structure_from_json_file,
    orm_update_structure,
    populate_element_type_ids,
    sort_thing_nodes,
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
    search_sinks_by_name,
    search_sources_by_name,
    upsert_sinks,
    upsert_sources,
)
from hetdesrun.structure.db.thing_node_service import (
    fetch_thing_nodes,
    search_thing_nodes_by_name,
    upsert_thing_nodes,
)
from hetdesrun.structure.models import (
    CompleteStructure,
    ElementType,
    Filter,
    Sink,
    Source,
    ThingNode,
)

# Enable Foreign Key Constraints for SQLite Connections


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection: SQLite3Connection, connection_record) -> None:  # type: ignore  # noqa: E501,
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


# Tests for Hierarchy and Relationships


@pytest.mark.usefixtures("_db_test_structure")
def test_thing_node_hierarchy_db_service(mocked_clean_test_db_session):
    """
    Tests the hierarchy and relationships of ThingNodes, Sources, and Sinks in the database.

    This test verifies that the expected number of ElementTypes, ThingNodes, Sources, and Sinks
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

        # Fetch ElementTypes using the new fetch_element_types function
        element_types_in_db = fetch_element_types(session, expected_element_type_keys)
        assert len(element_types_in_db) == 3, "Mismatch in element types count"

        # Fetch Sources using the new fetch_sources function
        sources_in_db = fetch_sources(session, expected_source_keys)
        assert len(sources_in_db) == 3, "Mismatch in sources count"

        # Fetch Sinks using the new fetch_sinks function
        sinks_in_db = fetch_sinks(session, expected_sink_keys)
        assert len(sinks_in_db) == 3, "Mismatch in sinks count"

        # Fetch ThingNodes using the new fetch_thing_nodes function
        thing_nodes_in_db = fetch_thing_nodes(session, expected_thing_node_keys)
        assert len(thing_nodes_in_db) == 7, "Mismatch in thing nodes count"

        # Optional: Verify specific relationships

        # Verify that "Waterworks1" is the parent of "Plant1" and "Plant2"
        parent_key = ("GW", "Waterworks1")
        plant1_key = ("GW", "Waterworks1_Plant1")
        plant2_key = ("GW", "Waterworks1_Plant2")
        assert plant1_key in thing_nodes_in_db, "Plant1 not found in ThingNodes"
        assert plant2_key in thing_nodes_in_db, "Plant2 not found in ThingNodes"

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
            assert st_key in thing_nodes_in_db, f"{st_key} not found in ThingNodes"

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
        # Example: "EnergyUsage_PumpSystem_StorageTank" should be associated with "StorageTank1" and "StorageTank2" in Plant1 and Plant2
        source_key = ("GW", "EnergyUsage_PumpSystem_StorageTank")
        assert (
            source_key in sources_in_db
        ), "EnergyUsage_PumpSystem_StorageTank not found in Sources"
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
        # "EnergyConsumption_SinglePump_StorageTank" associated with "StorageTank2, Plant1" and "StorageTank1, Plant2"
        source_key = ("GW", "EnergyConsumption_SinglePump_StorageTank")
        assert (
            source_key in sources_in_db
        ), "EnergyConsumption_SinglePump_StorageTank not found in Sources"
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
        assert source_key in sources_in_db, "EnergyConsumption_Waterworks1 not found in Sources"
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
        # "AnomalyScore_EnergyUsage_PumpSystem_StorageTank" associated with "StorageTank1, Plant1" and "StorageTank2, Plant1"
        sink_key = ("GW", "AnomalyScore_EnergyUsage_PumpSystem_StorageTank")
        assert (
            sink_key in sinks_in_db
        ), "AnomalyScore_EnergyUsage_PumpSystem_StorageTank not found in Sinks"
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

        # "AnomalyScore_EnergyConsumption_SinglePump_StorageTank" associated with "StorageTank2, Plant2" and "StorageTank1, Plant2"
        sink_key = ("GW", "AnomalyScore_EnergyConsumption_SinglePump_StorageTank")
        assert (
            sink_key in sinks_in_db
        ), "AnomalyScore_EnergyConsumption_SinglePump_StorageTank not found in Sinks"
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
        ), "AnomalyScore_EnergyConsumption_Waterworks1 not found in Sinks"
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
            assert (
                key in element_types
            ), f"Expected Element Type with stakeholder_key 'GW' and external_id '{expected_et['external_id']}' not found"
            # Check if the name matches
            actual_et = element_types[key]
            assert actual_et.name == expected_et["name"], (
                f"Element Type with external_id '{expected_et['external_id']}' has name '{actual_et.name}', "
                f"expected '{expected_et['name']}'"
            )


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_thing_nodes_db_service(mocked_clean_test_db_session):
    # Start a session to interact with the database
    with mocked_clean_test_db_session() as session:
        # Define the keys for the expected ThingNodes
        keys = {
            ("GW", "Waterworks1"),
            ("GW", "Waterworks1_Plant1"),
            ("GW", "Waterworks1_Plant2"),
            ("GW", "Waterworks1_Plant1_StorageTank1"),
            ("GW", "Waterworks1_Plant1_StorageTank2"),
            ("GW", "Waterworks1_Plant2_StorageTank1"),
            ("GW", "Waterworks1_Plant2_StorageTank2"),
        }

        # Fetch the ThingNodes based on the defined keys
        thing_nodes = fetch_thing_nodes(session, keys)

        # Check if the correct number of ThingNodes was retrieved
        assert (
            len(thing_nodes) == 7
        ), f"Expected 7 Thing Nodes in the database, found {len(thing_nodes)}"

        # Define the expected ThingNodes with their external_id and name
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

        # Check if each expected ThingNode is present in the retrieved ThingNodes
        for expected_tn in expected_thing_nodes:
            # Generate the key based on stakeholder_key and external_id
            key = ("GW", expected_tn["external_id"])
            # Check if the key exists in the retrieved dictionary
            assert (
                key in thing_nodes
            ), f"Expected Thing Node with stakeholder_key 'GW' and external_id '{expected_tn['external_id']}' not found"
            # Check if the name matches
            actual_tn = thing_nodes[key]
            assert actual_tn.name == expected_tn["name"], (
                f"Thing Node with external_id '{expected_tn['external_id']}' has name '{actual_tn.name}', "
                f"expected '{expected_tn['name']}'"
            )


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_sources_db_service(mocked_clean_test_db_session):
    # Start a session to interact with the database
    with mocked_clean_test_db_session() as session:
        # Define the keys for the expected Sources
        keys = {
            ("GW", "EnergyUsage_PumpSystem_StorageTank"),
            ("GW", "EnergyConsumption_SinglePump_StorageTank"),
            # Add more keys if there are additional Sources
        }

        # Fetch the Sources based on the defined keys
        sources = fetch_sources(session, keys)

        # Check if the correct number of Sources was retrieved
        assert len(sources) == 2, f"Expected 2 Sources in the database, found {len(sources)}"

        # Define the expected Sources with their external_id and name
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

        # Check if each expected Source is present in the retrieved Sources
        for expected_source in expected_sources:
            # Generate the key based on stakeholder_key and external_id
            key = ("GW", expected_source["external_id"])
            # Check if the key exists in the retrieved dictionary
            assert (
                key in sources
            ), f"Expected Source with stakeholder_key 'GW' and external_id '{expected_source['external_id']}' not found"
            # Check if the name matches
            actual_source = sources[key]
            assert actual_source.name == expected_source["name"], (
                f"Source with external_id '{expected_source['external_id']}' has name '{actual_source.name}', "
                f"expected '{expected_source['name']}'"
            )


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_all_sinks_db_service(mocked_clean_test_db_session):
    # Start a session to interact with the database
    with mocked_clean_test_db_session() as session:
        # Define the keys for the expected Sinks
        keys = {
            ("GW", "AnomalyScore_EnergyUsage_PumpSystem_StorageTank"),
            ("GW", "AnomalyScore_EnergyConsumption_SinglePump_StorageTank"),
            ("GW", "AnomalyScore_EnergyConsumption_Waterworks1"),
        }

        # Fetch the Sinks based on the defined keys
        sinks = fetch_sinks(session, keys)

        # Check if the correct number of Sinks was retrieved
        assert len(sinks) == 3, f"Expected 3 Sinks in the database, found {len(sinks)}"

        # Define the expected Sinks with their external_id and name
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

        # Check if each expected Sink is present in the retrieved Sinks
        for expected_sink in expected_sinks:
            # Generate the key based on stakeholder_key and external_id
            key = ("GW", expected_sink["external_id"])
            # Check if the key exists in the retrieved dictionary
            assert (
                key in sinks
            ), f"Expected Sink with stakeholder_key 'GW' and external_id '{expected_sink['external_id']}' not found"
            # Check if the name matches
            actual_sink = sinks[key]
            assert actual_sink.name == expected_sink["name"], (
                f"Sink with external_id '{expected_sink['external_id']}' has name '{actual_sink.name}', "
                f"expected '{expected_sink['name']}'"
            )


### Structure Helper Functions


def test_load_structure_from_json_file_db_service(db_test_structure_file_path):
    # Load the structure from the JSON file using the load_structure_from_json_file function
    complete_structure = orm_load_structure_from_json_file(db_test_structure_file_path)

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

    # Ensure that element_type_id fields in ThingNodes match
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
        orm_load_structure_from_json_file("non_existent_file.json")
    with pytest.raises(DBParsingError):
        orm_load_structure_from_json_file("tests/structure/data/db_test_structure_malformed.json")


@pytest.mark.usefixtures("_db_test_structure")
def test_delete_structure_db_service(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Verify that the database is initially populated
        assert session.query(ElementTypeOrm).count() > 0
        assert session.query(ThingNodeOrm).count() > 0
        assert session.query(SourceOrm).count() > 0
        assert session.query(SinkOrm).count() > 0
        assert session.query(thingnode_source_association).count() > 0
        assert session.query(thingnode_sink_association).count() > 0

        orm_delete_structure(session)

        # Verify that all tables are empty after purging
        assert session.query(ElementTypeOrm).count() == 0
        assert session.query(ThingNodeOrm).count() == 0
        assert session.query(SourceOrm).count() == 0
        assert session.query(SinkOrm).count() == 0
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
        updated_structure = orm_load_structure_from_json_file(file_path)

        # Update the structure in the database with the newly loaded structure
        orm_update_structure(updated_structure)

    # Start another session to verify the updated structure
    with get_session()() as session, session.begin():
        # Verify that the structure in the database matches the updated structure
        verify_updated_structure(session)


def verify_initial_structure(session):
    # Fetch all initial elements from the database
    initial_element_types = session.query(ElementTypeOrm).all()
    initial_thing_nodes = session.query(ThingNodeOrm).all()
    initial_sources = session.query(SourceOrm).all()
    initial_sinks = session.query(SinkOrm).all()

    # Verify that the initial structure contains the correct number of elements
    assert len(initial_element_types) == 3, "Expected 3 Element Types in the initial structure"
    assert len(initial_thing_nodes) == 7, "Expected 7 Thing Nodes in the initial structure"
    assert len(initial_sources) == 3, "Expected 3 Sources in the initial structure"
    assert len(initial_sinks) == 3, "Expected 3 Sinks in the initial structure"

    # Verify specific attributes of the ThingNodes before the update
    initial_tn = (
        session.query(ThingNodeOrm).filter_by(external_id="Waterworks1_Plant1_StorageTank1").one()
    )
    assert (
        initial_tn.meta_data["capacity"] == "5000"
    ), "Initial capacity of Storage Tank 1 should be 5000"
    assert initial_tn.meta_data["description"] == "Water storage capacity for Storage Tank 1"

    initial_tn2 = (
        session.query(ThingNodeOrm).filter_by(external_id="Waterworks1_Plant1_StorageTank2").one()
    )
    assert (
        initial_tn2.meta_data["capacity"] == "6000"
    ), "Initial capacity of Storage Tank 2 should be 6000"
    assert initial_tn2.meta_data["description"] == "Water storage capacity for Storage Tank 2"


def verify_updated_structure(session):
    # Fetch all elements from the database after the update
    final_element_types = session.query(ElementTypeOrm).all()
    final_thing_nodes = session.query(ThingNodeOrm).all()
    final_sources = session.query(SourceOrm).all()
    final_sinks = session.query(SinkOrm).all()

    # Verify that the structure now contains the updated number of elements
    assert len(final_element_types) == 4, "Expected 4 Element Types after the update"
    assert len(final_thing_nodes) == 8, "Expected 8 Thing Nodes after the update"
    assert len(final_sources) == 4, "Expected 4 Sources after the update"
    assert len(final_sinks) == 3, "Expected 3 Sinks after the update"

    # Verify the new elements and updated nodes in the structure
    verify_new_elements_and_nodes(session, final_element_types, final_thing_nodes)
    # Verify the associations between ThingNodes, Sources, and Sinks
    verify_associations(session)


def verify_new_elements_and_nodes(session, final_element_types, final_thing_nodes):
    # Verify that a new ElementType was added
    new_element_type = next(
        et for et in final_element_types if et.external_id == "FiltrationPlant_Type"
    )
    assert (
        new_element_type.name == "Filtration Plant"
    ), "Expected new Element Type 'Filtration Plant'"
    assert new_element_type.description == "Element type for filtration plants"

    # Verify that a new ThingNode was added
    new_tn = next(tn for tn in final_thing_nodes if tn.external_id == "Waterworks1_FiltrationPlant")
    assert new_tn.name == "Filtration Plant 1", "Expected new Thing Node 'Filtration Plant 1'"
    assert new_tn.description == "New filtration plant in Waterworks 1"
    assert (
        new_tn.meta_data["location"] == "Central"
    ), "Expected location 'Central' for the new Thing Node"
    assert (
        new_tn.meta_data["technology"] == "Advanced Filtration"
    ), "Expected technology 'Advanced Filtration'"

    # Verify that the ThingNodes were updated correctly
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


def verify_associations(session):
    # Fetch all associations between ThingNodes and Sources/Sinks from the database
    source_associations = session.query(thingnode_source_association).all()
    sink_associations = session.query(thingnode_sink_association).all()

    # Define the expected associations between ThingNodes and Sources
    expected_source_associations = [
        ("Waterworks1_Plant1_StorageTank1", "EnergyUsage_PumpSystem_StorageTank"),
        ("Waterworks1_FiltrationPlant", "EnergyUsage_PumpSystem_StorageTank"),
        ("Waterworks1_Plant2_StorageTank1", "EnergyConsumption_SinglePump_StorageTank"),
        ("Waterworks1_FiltrationPlant", "New_EnergySource_FiltrationPlant"),
    ]

    # Define the expected associations between ThingNodes and Sinks
    expected_sink_associations = [
        ("Waterworks1_Plant1_StorageTank1", "AnomalyScore_EnergyUsage_PumpSystem_StorageTank"),
        ("Waterworks1_Plant1_StorageTank2", "AnomalyScore_EnergyUsage_PumpSystem_StorageTank"),
        (
            "Waterworks1_Plant2_StorageTank1",
            "AnomalyScore_EnergyConsumption_SinglePump_StorageTank",
        ),
        (
            "Waterworks1_Plant2_StorageTank2",
            "AnomalyScore_EnergyConsumption_SinglePump_StorageTank",
        ),
        ("Waterworks1_FiltrationPlant", "AnomalyScore_EnergyConsumption_SinglePump_StorageTank"),
    ]

    # Verify that each expected Source association exists in the database
    for tn_external_id, source_external_id in expected_source_associations:
        tn_keys = (
            session.query(ThingNodeOrm.stakeholder_key, ThingNodeOrm.external_id)
            .filter(ThingNodeOrm.external_id == tn_external_id)
            .one()
        )
        source_keys = (
            session.query(SourceOrm.stakeholder_key, SourceOrm.external_id)
            .filter(SourceOrm.external_id == source_external_id)
            .one()
        )

        assert (
            (
                tn_keys.stakeholder_key,
                tn_keys.external_id,
                source_keys.stakeholder_key,
                source_keys.external_id,
            )
            in [
                (
                    assoc.thing_node_stakeholder_key,
                    assoc.thing_node_external_id,
                    assoc.source_stakeholder_key,
                    assoc.source_external_id,
                )
                for assoc in source_associations
            ]
        ), f"Expected association between ThingNode {tn_external_id} and Source {source_external_id} not found"

    # Verify that each expected Sink association exists in the database
    for tn_external_id, sink_external_id in expected_sink_associations:
        tn_keys = (
            session.query(ThingNodeOrm.stakeholder_key, ThingNodeOrm.external_id)
            .filter(ThingNodeOrm.external_id == tn_external_id)
            .one()
        )
        sink_keys = (
            session.query(SinkOrm.stakeholder_key, SinkOrm.external_id)
            .filter(SinkOrm.external_id == sink_external_id)
            .one()
        )

        assert (
            (
                tn_keys.stakeholder_key,
                tn_keys.external_id,
                sink_keys.stakeholder_key,
                sink_keys.external_id,
            )
            in [
                (
                    assoc.thing_node_stakeholder_key,
                    assoc.thing_node_external_id,
                    assoc.sink_stakeholder_key,
                    assoc.sink_external_id,
                )
                for assoc in sink_associations
            ]
        ), f"Expected association between ThingNode {tn_external_id} and Sink {sink_external_id} not found"


@pytest.mark.usefixtures("_db_empty_database")
def test_update_structure_from_file_db_service():
    # This test specifically checks the insert functionality of the update_structure function.
    # It starts with an empty database and verifies that the structure from the JSON file is
    # correctly inserted into the database.

    # Path to the JSON file containing the test structure
    file_path = "tests/structure/data/db_test_structure.json"

    # Ensure the database is empty at the beginning
    with get_session()() as session:
        assert session.query(ElementTypeOrm).count() == 0
        assert session.query(ThingNodeOrm).count() == 0
        assert session.query(SourceOrm).count() == 0
        assert session.query(SinkOrm).count() == 0

    # Load structure from the file
    complete_structure = orm_load_structure_from_json_file(file_path)

    # Update the structure in the database with the newly loaded structure
    orm_update_structure(complete_structure)

    # Verify that the structure was correctly inserted
    with get_session()() as session:
        assert session.query(ElementTypeOrm).count() == 3
        assert session.query(ThingNodeOrm).count() == 7
        assert session.query(SourceOrm).count() == 3
        assert session.query(SinkOrm).count() == 3

        # Verify specific ElementType
        waterworks_type = (
            session.query(ElementTypeOrm).filter_by(external_id="Waterworks_Type").one()
        )
        assert waterworks_type.name == "Waterworks"
        assert waterworks_type.description == "Element type for waterworks"

        # Verify specific ThingNode
        waterworks1 = session.query(ThingNodeOrm).filter_by(external_id="Waterworks1").one()
        assert waterworks1.name == "Waterworks 1"
        assert waterworks1.meta_data["location"] == "Main Site"

        # Verify specific Source
        source = (
            session.query(SourceOrm)
            .filter_by(external_id="EnergyUsage_PumpSystem_StorageTank")
            .one()
        )
        assert source.name == "Energy usage of the pump system in Storage Tank"
        assert source.meta_data["1010001"]["unit"] == "kW/h"

        # Verify specific Sink
        sink = (
            session.query(SinkOrm)
            .filter_by(external_id="AnomalyScore_EnergyUsage_PumpSystem_StorageTank")
            .one()
        )
        assert sink.name == "Anomaly Score for the energy usage of the pump system in Storage Tank"

        # Additional checks for relationships and associations
        # Verify the association between ThingNodes and Sources
        tn_source_associations = session.query(thingnode_source_association).all()
        assert len(tn_source_associations) > 0

        # Example: Check if Waterworks1_Plant1_StorageTank1 is associated with a specific source
        assoc = (
            session.query(thingnode_source_association)
            .filter_by(
                thing_node_external_id="Waterworks1_Plant1_StorageTank1",
                source_external_id="EnergyUsage_PumpSystem_StorageTank",
            )
            .one_or_none()
        )
        assert assoc is not None, "Association between ThingNode and Source is missing"

        # Verify the association between ThingNodes and Sinks
        tn_sink_associations = session.query(thingnode_sink_association).all()
        assert len(tn_sink_associations) > 0

        # Example: Check if Waterworks1_Plant1_StorageTank1 is associated with a specific sink
        assoc_sink = (
            session.query(thingnode_sink_association)
            .filter_by(
                thing_node_external_id="Waterworks1_Plant1_StorageTank1",
                sink_external_id="AnomalyScore_EnergyUsage_PumpSystem_StorageTank",
            )
            .one_or_none()
        )
        assert assoc_sink is not None, "Association between ThingNode and Sink is missing"


@pytest.mark.usefixtures("_db_test_structure")
def test_update_structure_no_elements_deleted_db_service():
    # This test ensures that no elements are deleted when updating the structure
    # with a new JSON file that omits some elements. It verifies that the total number of elements
    # remains unchanged and that specific elements from the original structure are still present.

    # Define paths to the JSON files
    old_file_path = "tests/structure/data/db_test_structure.json"
    new_file_path = "tests/structure/data/db_test_incomplete_structure.json"

    # Load initial structure from JSON file
    initial_structure: CompleteStructure = orm_load_structure_from_json_file(old_file_path)

    # Load updated structure from new JSON file
    updated_structure: CompleteStructure = orm_load_structure_from_json_file(new_file_path)

    # Update the structure in the database with new structure
    orm_update_structure(updated_structure)

    # Verify structure after update
    with get_session()() as session:
        # Check the number of elements after update
        assert session.query(ElementTypeOrm).count() == len(initial_structure.element_types)
        assert session.query(ThingNodeOrm).count() == len(initial_structure.thing_nodes)
        assert session.query(SourceOrm).count() == len(initial_structure.sources)
        assert session.query(SinkOrm).count() == len(initial_structure.sinks)

        # Verify specific elements from the initial structure are still present
        # Element Types
        for element_type in initial_structure.element_types:
            assert (
                session.query(ElementTypeOrm)
                .filter_by(external_id=element_type.external_id)
                .count()
                == 1
            )

        # Thing Nodes
        for thing_node in initial_structure.thing_nodes:
            assert (
                session.query(ThingNodeOrm).filter_by(external_id=thing_node.external_id).count()
                == 1
            )

        # Sources
        for source in initial_structure.sources:
            assert session.query(SourceOrm).filter_by(external_id=source.external_id).count() == 1

        # Sinks
        for sink in initial_structure.sinks:
            assert session.query(SinkOrm).filter_by(external_id=sink.external_id).count() == 1


@pytest.mark.usefixtures("_db_empty_database")
def test_is_database_empty_when_empty_db_service(mocked_clean_test_db_session):
    assert orm_is_database_empty(), "Database should be empty but is not."


@pytest.mark.usefixtures("_db_test_structure")
def test_is_database_empty_when_not_empty_db_service(mocked_clean_test_db_session):
    assert not orm_is_database_empty(), "Database should not be empty but it is."


@pytest.mark.usefixtures("_db_test_unordered_structure")
def test_sort_thing_nodes_db_service(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Fetch all thing node keys first to pass them to fetch_thing_nodes
        thing_node_keys = {
            (tn.stakeholder_key, tn.external_id) for tn in session.query(ThingNodeOrm).all()
        }

        # Fetch all thing nodes from the database
        thing_nodes_in_db = fetch_thing_nodes(session, thing_node_keys)

        # Since fetch_thing_nodes returns a dictionary of ThingNodeOrm instances, no need to unpack tuples
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
        assert (
            sorted_by_external_id == expected_sorted_by_external_id
        ), f"Nodes should be sorted by external_id. Expected {expected_sorted_by_external_id}, got {sorted_by_external_id}"

        # Ensure the condition where a parent_node_id is not initially in existing_thing_nodes
        orphan_node = ThingNodeOrm(
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
            match=r"Unexpected error while fetching SourceOrm",
        ),
        mocked_clean_test_db_session() as session,
    ):
        # This will raise an error due to malformed input (tuple is incomplete)
        fetch_sources(session, invalid_keys)


def test_populate_element_type_ids_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Create an ElementTypeOrm and add it to the session
        element_type_id = uuid.uuid4()
        element_type_orm = ElementTypeOrm(
            id=element_type_id,
            external_id="type1",
            stakeholder_key="GW",
            name="Test ElementType",
            description="Description",
        )
        session.add(element_type_orm)
        session.commit()

        # Existing ElementTypes mapping
        existing_element_types = {("GW", "type1"): element_type_orm}

        # Create ThingNodes that reference the ElementType
        thing_nodes = [
            ThingNode(
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
    with mocked_clean_test_db_session() as session:
        # Empty existing ElementTypes mapping
        existing_element_types = {}

        # Create ThingNodes that reference a non-existing ElementType
        thing_nodes = [
            ThingNode(
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
            "ElementType with key ('GW', 'non_existing_type') not found for ThingNode node1."
            in record.message
            for record in caplog.records
        )


def test_search_element_types_by_name_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add ElementTypeOrms to the session
        session.add_all(
            [
                ElementTypeOrm(
                    id=uuid.uuid4(),
                    external_id="type1",
                    stakeholder_key="GW",
                    name="Test ElementType",
                    description="Description",
                ),
                ElementTypeOrm(
                    id=uuid.uuid4(),
                    external_id="type2",
                    stakeholder_key="GW",
                    name="Another ElementType",
                    description="Another Description",
                ),
            ]
        )
        session.commit()

        # Search for 'Test'
        result = search_element_types_by_name(session, "Test")

        # Assert that the correct ElementTypeOrm is returned
        assert len(result) == 1
        assert result[0].name == "Test ElementType"


def test_search_element_types_by_name_no_matches(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add ElementTypeOrms to the session
        session.add_all(
            [
                ElementTypeOrm(
                    id=uuid.uuid4(),
                    external_id="type1",
                    stakeholder_key="GW",
                    name="Sample ElementType",
                    description="Description",
                ),
                ElementTypeOrm(
                    id=uuid.uuid4(),
                    external_id="type2",
                    stakeholder_key="GW",
                    name="Another ElementType",
                    description="Another Description",
                ),
            ]
        )
        session.commit()

        # Search for 'Nonexistent'
        result = search_element_types_by_name(session, "Nonexistent")

        # Assert that no ElementTypeOrm is returned
        assert len(result) == 0


def test_upsert_element_types_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        existing_elements = {}

        # Create ElementType objects to upsert
        elements = [
            ElementType(
                id=uuid.uuid4(),
                external_id="type1",
                stakeholder_key="GW",
                name="Test ElementType",
                description="Description",
            )
        ]

        # Call the function
        upsert_element_types(session, elements, existing_elements)
        session.commit()

        # Verify that the ElementTypeOrm was added to the database
        result = session.query(ElementTypeOrm).filter_by(external_id="type1").one_or_none()
        assert result is not None
        assert result.name == "Test ElementType"


def test_search_sinks_by_name_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add SinkOrms to the session
        session.add_all(
            [
                SinkOrm(
                    id=uuid.uuid4(),
                    external_id="sink1",
                    stakeholder_key="GW",
                    name="Test Sink",
                    type="timeseries(float)",
                    visible=True,
                    display_path="Path",
                    adapter_key="Adapter",
                    sink_id="SinkID",
                    ref_key=None,
                    ref_id="RefID",
                    meta_data={},
                    preset_filters={},
                    passthrough_filters=[],
                    thing_node_external_ids=[],
                ),
                SinkOrm(
                    id=uuid.uuid4(),
                    external_id="sink2",
                    stakeholder_key="GW",
                    name="Another Sink",
                    type="timeseries(float)",
                    visible=True,
                    display_path="Path",
                    adapter_key="Adapter",
                    sink_id="SinkID2",
                    ref_key=None,
                    ref_id="RefID2",
                    meta_data={},
                    preset_filters={},
                    passthrough_filters=[],
                    thing_node_external_ids=[],
                ),
            ]
        )
        session.commit()

        # Search for 'Test'
        result = search_sinks_by_name(session, "Test")

        # Assert that the correct SinkOrm is returned
        assert len(result) == 1
        assert result[0].name == "Test Sink"


def test_search_sinks_by_name_no_matches(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add SinkOrms to the session
        session.add(
            SinkOrm(
                id=uuid.uuid4(),
                external_id="sink1",
                stakeholder_key="GW",
                name="Sample Sink",
                type="timeseries(float)",
                visible=True,
                display_path="Path",
                adapter_key="Adapter",
                sink_id="SinkID",
                ref_key=None,
                ref_id="RefID",
                meta_data={},
                preset_filters={},
                passthrough_filters=[],
                thing_node_external_ids=[],
            )
        )
        session.commit()

        # Search for 'Nonexistent'
        result = search_sinks_by_name(session, "Nonexistent")

        # Assert that no SinkOrm is returned
        assert len(result) == 0


def test_search_sources_by_name_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add SourceOrms to the session
        session.add_all(
            [
                SourceOrm(
                    id=uuid.uuid4(),
                    external_id="source1",
                    stakeholder_key="GW",
                    name="Test Source",
                    type="timeseries(float)",
                    visible=True,
                    display_path="Path",
                    adapter_key="Adapter",
                    source_id="SourceID",
                    ref_key=None,
                    ref_id="RefID",
                    meta_data={},
                    preset_filters={},
                    passthrough_filters=[],
                    thing_node_external_ids=[],
                ),
                SourceOrm(
                    id=uuid.uuid4(),
                    external_id="source2",
                    stakeholder_key="GW",
                    name="Another Source",
                    type="timeseries(float)",
                    visible=True,
                    display_path="Path",
                    adapter_key="Adapter",
                    source_id="SourceID2",
                    ref_key=None,
                    ref_id="RefID2",
                    meta_data={},
                    preset_filters={},
                    passthrough_filters=[],
                    thing_node_external_ids=[],
                ),
            ]
        )
        session.commit()

        # Search for 'Test'
        result = search_sources_by_name(session, "Test")

        # Assert that the correct SourceOrm is returned
        assert len(result) == 1
        assert result[0].name == "Test Source"


def test_search_sources_by_name_no_matches(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add SourceOrms to the session
        session.add(
            SourceOrm(
                id=uuid.uuid4(),
                external_id="source1",
                stakeholder_key="GW",
                name="Sample Source",
                type="timeseries(float)",
                visible=True,
                display_path="Path",
                adapter_key="Adapter",
                source_id="SourceID",
                ref_key=None,
                ref_id="RefID",
                meta_data={},
                preset_filters={},
                passthrough_filters=[],
                thing_node_external_ids=[],
            )
        )
        session.commit()

        # Search for 'Nonexistent'
        result = search_sources_by_name(session, "Nonexistent")

        # Assert that no SourceOrm is returned
        assert len(result) == 0


def test_upsert_sinks_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        existing_sinks = {}
        existing_thing_nodes = {}

        # Create Sink object to upsert
        sink = Sink(
            id=uuid.uuid4(),
            stakeholder_key="GW",
            external_id="sink1",
            name="Test Sink",
            type="timeseries(float)",
            visible=True,
            display_path="Path",
            adapter_key="Adapter",
            sink_id="SinkID",
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

        # Verify that the SinkOrm was added to the database
        result = session.query(SinkOrm).filter_by(external_id="sink1").one_or_none()
        assert result is not None
        assert result.name == "Test Sink"


def test_upsert_sources_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        existing_sources = {}
        existing_thing_nodes = {}

        # Create Source object to upsert
        source = Source(
            id=uuid.uuid4(),
            stakeholder_key="GW",
            external_id="source1",
            name="Test Source",
            type="timeseries(float)",
            visible=True,
            display_path="Path",
            adapter_key="Adapter",
            source_id="SourceID",
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

        # Verify that the SourceOrm was added to the database
        result = session.query(SourceOrm).filter_by(external_id="source1").one_or_none()
        assert result is not None
        assert result.name == "Test Source"


def test_search_thing_nodes_by_name_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add ElementTypeOrm to the session
        element_type = ElementTypeOrm(
            id=uuid.uuid4(),
            external_id="type1",
            stakeholder_key="GW",
            name="Test Type",
            description="A test element type",
        )
        session.add(element_type)
        session.commit()

        # Add ThingNodeOrms to the session with the element_type
        session.add_all(
            [
                ThingNodeOrm(
                    id=uuid.uuid4(),
                    external_id="node1",
                    stakeholder_key="GW",
                    name="Test Node",
                    description="Description",
                    parent_node_id=None,
                    parent_external_node_id=None,
                    element_type_id=element_type.id,  # Link to ElementTypeOrm
                    element_type_external_id="type1",
                    meta_data={},
                ),
                ThingNodeOrm(
                    id=uuid.uuid4(),
                    external_id="node2",
                    stakeholder_key="GW",
                    name="Another Node",
                    description="Description",
                    parent_node_id=None,
                    parent_external_node_id=None,
                    element_type_id=element_type.id,  # Link to the same ElementTypeOrm
                    element_type_external_id="type1",
                    meta_data={},
                ),
            ]
        )
        session.commit()

        # Search for 'Test'
        result = search_thing_nodes_by_name(session, "Test")

        # Assert that the correct ThingNodeOrm is returned
        assert len(result) == 1
        assert result[0].name == "Test Node"


def test_search_thing_nodes_by_name_no_matches(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Add ElementTypeOrm to the session
        element_type = ElementTypeOrm(
            id=uuid.uuid4(),
            external_id="type1",
            stakeholder_key="GW",
            name="type1",
            description="A test element type",
        )
        session.add(element_type)
        session.commit()
        # Add ThingNodeOrm to the session
        session.add(
            ThingNodeOrm(
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

        # Assert that no ThingNodeOrm is returned
        assert len(result) == 0


def test_upsert_thing_nodes_success(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        existing_thing_nodes = {}
        # Add an ElementTypeOrm to the session
        element_type_id = uuid.uuid4()
        element_type = ElementTypeOrm(
            id=element_type_id,
            external_id="type1",
            stakeholder_key="GW",
            name="Test ElementType",
            description="Description",
        )
        session.add(element_type)
        session.commit()

        # Create ThingNode object to upsert
        node = ThingNode(
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

        # Verify that the ThingNodeOrm was added to the database
        result = session.query(ThingNodeOrm).filter_by(external_id="node1").one_or_none()
        assert result is not None
        assert result.name == "Test Node"
