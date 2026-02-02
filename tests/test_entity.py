import pytest

import strata.core as core


class TestEntity:
    def test_creates_with_valid_inputs(self):
        entity = core.Entity(name="user", join_keys=["user_id"])
        assert entity.name == "user"
        assert entity.join_keys == ["user_id"]

    def test_raises_error_when_join_keys_empty(self):
        with pytest.raises(Exception):  # StrataError
            core.Entity(name="user", join_keys=[])

    def test_accepts_multiple_join_keys(self):
        entity = core.Entity(name="order", join_keys=["user_id", "order_id"])
        assert len(entity.join_keys) == 2

    def test_description_is_optional(self):
        entity = core.Entity(name="user", join_keys=["user_id"])
        assert entity.description is None

        entity_with_desc = core.Entity(
            name="user",
            join_keys=["user_id"],
            description="User entity",
        )
        assert entity_with_desc.description == "User entity"
