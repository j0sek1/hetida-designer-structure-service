import uuid

import pytest

from hetdesrun.persistence.structure_service_dbmodels import ThingNodeOrm
from hetdesrun.structure.db.exceptions import DBNotFoundError
from hetdesrun.structure.db.thing_node_service import fetch_single_thing_node_from_db_by_id


@pytest.mark.usefixtures("_db_test_structure")
def test_fetch_single_thing_node_from_db_by_id(mocked_clean_test_db_session):
    with mocked_clean_test_db_session() as session:
        # Fetch an existing ThingNode ID
        existing_tn = session.query(ThingNodeOrm).first()
        assert existing_tn is not None, "No ThingNode found in the test database."

        # Test retrieving the ThingNode by ID
        fetched_tn = fetch_single_thing_node_from_db_by_id(existing_tn.id)
        assert fetched_tn.id == existing_tn.id, "Fetched ThingNode ID does not match."

        # Test that a non-existent ThingNode raises a DBNotFoundError
        non_existent_id = uuid.uuid4()
        with pytest.raises(DBNotFoundError):
            fetch_single_thing_node_from_db_by_id(non_existent_id)
