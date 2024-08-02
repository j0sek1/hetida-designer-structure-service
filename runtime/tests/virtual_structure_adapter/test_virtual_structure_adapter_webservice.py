import pytest


@pytest.mark.asyncio
async def test_vst_adapter_get_structure_with_none_from_webservice(
    async_test_client_with_vst_adapter,
):
    response = await async_test_client_with_vst_adapter.get("/adapters/vst/structure")

    assert response.status_code == 200

    resp_obj = response.json()

    assert len(resp_obj["thingNodes"]) == 1

    first_thing_node = resp_obj["thingNodes"][0]
    first_thing_node_id = resp_obj["thingNodes"][0]["id"]

    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/vst/thingNodes/{first_thing_node_id}"
    )

    assert response.status_code == 200
    assert response.json() == first_thing_node


@pytest.mark.asyncio
async def test_vst_adapter_get_structure_from_webservice(async_test_client_with_vst_adapter):
    # Make multiple calls to the structure endpoint to unravel the hierarchy
    response = await async_test_client_with_vst_adapter.get("/adapters/vst/structure")

    assert response.status_code == 200

    resp_obj = response.json()

    assert len(resp_obj["thingNodes"]) == 1

    first_thing_node_id = resp_obj["thingNodes"][0]["id"]

    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/vst/structure?parentId={first_thing_node_id}"
    )

    assert response.status_code == 200

    resp_obj = response.json()

    assert len(resp_obj["thingNodes"]) == 2

    first_thing_node_id = resp_obj["thingNodes"][0]["id"]

    # Get down the hierarchy far enough to arrive at a source
    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/vst/structure?parentId={first_thing_node_id}"
    )

    assert response.status_code == 200

    resp_obj = response.json()

    first_thing_node_id = resp_obj["thingNodes"][0]["id"]

    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/vst/structure?parentId={first_thing_node_id}"
    )

    assert response.status_code == 200

    resp_obj = response.json()

    assert len(resp_obj["thingNodes"]) == 0
    assert len(resp_obj["sinks"]) == 1
    assert len(resp_obj["sources"]) == 2

    sink_name = resp_obj["sinks"][0]["name"]
    expected_source_names = [
        "Energieverbräuche mit preset filter",
        "Energieverbräuche mit passthrough filters",
    ]

    for source in resp_obj["sources"]:
        assert source["name"] in expected_source_names
    assert sink_name == "Anomaliescore des Pumpensystems in Hochbehälter"


@pytest.mark.asyncio
async def test_vst_adapter_get_metadata_from_webservice(async_test_client_with_vst_adapter):
    # Currently no metadata is returned, every metadata endpoint should return an empty list
    # regardless of the UUID provided
    example_uuid = "7cfc4470-65d8-416b-a8c3-c392eaf92b91"  # Non-existent UUID

    # Test thingnode metadata
    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/vst/thingNodes/{example_uuid}/metadata/"
    )
    assert response.status_code == 200
    resp_obj = response.json()
    assert resp_obj == []

    # Test source metadata
    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/vst/sources/{example_uuid}/metadata/"
    )
    assert response.status_code == 200
    resp_obj = response.json()
    assert resp_obj == []

    # Test sink metadata
    response = await async_test_client_with_vst_adapter.get(
        f"/adapters/vst/sinks/{example_uuid}/metadata/"
    )
    assert response.status_code == 200
    resp_obj = response.json()
    assert resp_obj == []
