import json

import pytest


@pytest.mark.asyncio
async def test_update_structure(async_test_client, mocked_clean_test_db_session):
    file_path = "tests/virtual_structure_adapter/data/simple_end_to_end_test.json"
    with open(file_path) as file:
        structure_json = json.load(file)

    async with async_test_client as ac:
        response = await ac.put("/api/structure/update/", json=structure_json)
    assert response.status_code == 204


@pytest.mark.asyncio
async def test_update_structure_with_formally_invalid_structure(
    async_test_client, mocked_clean_test_db_session
):
    async with async_test_client as ac:
        response = await ac.put("/api/structure/update/", json="'nf'")
    assert response.status_code == 422, f"Unexpected status code: {response.status_code}"
    assert "value is not a valid dict" in response.json()["detail"][0]["msg"]


@pytest.mark.asyncio
async def test_update_structure_with_invalid_structure(
    async_test_client, mocked_clean_test_db_session
):
    json_with_type_mismatch = {
        "element_types": [
            {
                "external_id": "Waterworks_Type",
                "stakeholder_key": "GW",
                "name": [42],  # Wrong datatype
                "description": "Element type for waterworks",
            }
        ]
    }

    async with async_test_client as ac:
        response = await ac.put("/api/structure/update/", json=json_with_type_mismatch)
    assert response.status_code == 422, f"Unexpected status code: {response.status_code}"
    assert "str type expected" in response.json()["detail"][0]["msg"]


@pytest.mark.asyncio
async def test_update_structure_with_logically_invalid_structure(
    async_test_client, mocked_clean_test_db_session
):
    file_path = "tests/structure/data/db_test_invalid_structure_no_duplicate_id.json"
    with open(file_path) as file:
        structure_json = json.load(file)

    async with async_test_client as ac:
        response = await ac.put("/api/structure/update/", json=structure_json)
    assert response.status_code == 500, f"Unexpected status code: {response.status_code}"
    assert "Integrity Error while upserting ThingNodeOrm" in response.json()["detail"]
