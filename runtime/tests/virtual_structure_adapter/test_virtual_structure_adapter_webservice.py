import uuid

import pytest


@pytest.mark.asyncio
async def test_access_vst_adapter_info(
    async_test_client_with_vst_adapter,
) -> None:
    response = await async_test_client_with_vst_adapter.get("adapters/virtual_structure/info")
    assert response.status_code == 200
    assert "version" in response.json()


@pytest.mark.asyncio
async def test_vst_adapter_get_structure_with_none_from_webservice(
    async_test_client_with_vst_adapter,
):
    response = await async_test_client_with_vst_adapter.get("/adapters/virtual_structure/structure")

    assert response.status_code == 200

    resp_obj = response.json()

    # Verify that only the root node is returned
    assert len(resp_obj["thingNodes"]) == 1
    assert resp_obj["thingNodes"][0]["parentId"] is None

    first_thing_node = resp_obj["thingNodes"][0]
    first_thing_node_id = resp_obj["thingNodes"][0]["id"]

    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/virtual_structure/thingNodes/{first_thing_node_id}"
    )

    # Verify that the thingNodes endpoint returns the same node
    # given its ID
    assert response.status_code == 200
    assert response.json() == first_thing_node


@pytest.mark.asyncio
async def test_vst_adapter_get_structure_from_webservice(async_test_client_with_vst_adapter):
    # Make multiple calls to the structure endpoint to unravel the hierarchy
    response = await async_test_client_with_vst_adapter.get("/adapters/virtual_structure/structure")

    assert response.status_code == 200

    resp_obj = response.json()

    assert len(resp_obj["thingNodes"]) == 1

    first_thing_node_id = resp_obj["thingNodes"][0]["id"]

    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/virtual_structure/structure?parentId={first_thing_node_id}"
    )

    assert response.status_code == 200

    resp_obj = response.json()

    assert len(resp_obj["thingNodes"]) == 2

    first_thing_node_id = resp_obj["thingNodes"][0]["id"]

    # Get down the hierarchy far enough to arrive at a source
    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/virtual_structure/structure?parentId={first_thing_node_id}"
    )

    assert response.status_code == 200

    resp_obj = response.json()

    first_thing_node_id = resp_obj["thingNodes"][0]["id"]

    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/virtual_structure/structure?parentId={first_thing_node_id}"
    )

    assert response.status_code == 200

    resp_obj = response.json()

    assert len(resp_obj["thingNodes"]) == 0
    assert len(resp_obj["sinks"]) == 1
    assert len(resp_obj["sources"]) == 3

    sink_name = resp_obj["sinks"][0]["name"]
    expected_source_names = [
        "Energy usage with preset filter",
        "Energy usage with passthrough filters",
        "Test source for type metadata(any)",
    ]

    for source in resp_obj["sources"]:
        assert source["name"] in expected_source_names
    assert sink_name == "Anomaly score for the energy usage of the pump system in Storage Tank"


@pytest.mark.asyncio
async def test_vst_adapter_get_metadata_from_webservice(async_test_client_with_vst_adapter):
    # Currently no metadata is returned, every metadata endpoint should return an empty list
    # regardless of the UUID provided
    example_uuid = uuid.uuid4()  # Non-existent UUID

    # Test thingnode metadata
    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/virtual_structure/thingNodes/{example_uuid}/metadata/"
    )
    assert response.status_code == 200
    resp_obj = response.json()
    assert resp_obj == []

    # Test source metadata
    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/virtual_structure/sources/{example_uuid}/metadata/"
    )
    assert response.status_code == 200
    resp_obj = response.json()
    assert resp_obj == []

    # Test sink metadata
    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/virtual_structure/sinks/{example_uuid}/metadata/"
    )
    assert response.status_code == 200
    resp_obj = response.json()
    assert resp_obj == []


@pytest.mark.asyncio
async def test_vst_adapter_sources_endpoint(async_test_client_with_vst_adapter):
    # Without filter string provided
    response = await async_test_client_with_vst_adapter.get("adapters/virtual_structure/sources")

    assert response.status_code == 200

    resp_obj = response.json()
    assert len(resp_obj["sources"]) == 0

    # Filter string provided
    response = await async_test_client_with_vst_adapter.get(
        "adapters/virtual_structure/sources",
        params={"filter": "PrEsEt"},  # Use this capitalization to test case-insensitivity
    )

    assert response.status_code == 200

    resp_obj = response.json()
    assert len(resp_obj["sources"]) == 1
    assert resp_obj["sources"][0]["name"] == "Energy usage with preset filter"


@pytest.mark.asyncio
async def test_vst_adapter_sinks_endpoint(async_test_client_with_vst_adapter):
    # Without filter string provided
    response = await async_test_client_with_vst_adapter.get("adapters/virtual_structure/sinks")

    assert response.status_code == 200

    resp_obj = response.json()
    assert len(resp_obj["sinks"]) == 0

    # Filter string provided
    response = await async_test_client_with_vst_adapter.get(
        "adapters/virtual_structure/sinks",
        params={"filter": "AnOmAly"},  # Use this capitalization to test case-insensitivity
    )

    assert response.status_code == 200

    resp_obj = response.json()
    assert len(resp_obj["sinks"]) == 1
    assert (
        resp_obj["sinks"][0]["name"]
        == "Anomaly score for the energy usage of the pump system in Storage Tank"
    )
