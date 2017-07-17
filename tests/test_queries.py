import pytest

from datapipelines import Query, QueryValidationError, QueryValidatorStructureError, validate_query


def test_has():
    valid = Query.has("test")

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    assert valid({"test": "test"})
    assert valid({"test": 0})
    assert valid({"test": "test", "dog": "cat"})


def test_repeat_has():
    with pytest.raises(QueryValidatorStructureError):
        Query.has("test").has("dog")


def test_can_have():
    valid = Query.can_have("test")

    assert valid({})
    assert valid({"dog": "cat"})
    assert valid({"test": "test"})
    assert valid({"test": 0})
    assert valid({"test": "test", "dog": "cat"})


def test_repeat_can_have():
    with pytest.raises(QueryValidatorStructureError):
        Query.can_have("test").can_have("dog")


def test_repeat_have_can_have():
    with pytest.raises(QueryValidatorStructureError):
        Query.has("test").can_have("dog")

    with pytest.raises(QueryValidatorStructureError):
        Query.can_have("test").has("dog")


def test_has_as():
    valid = Query.has("test").as_(str)

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    assert valid({"test": "test"})
    assert valid({"test": "test", "dog": "cat"})


def test_can_have_as():
    valid = Query.can_have("test").as_(str)

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    assert valid({})
    assert valid({"dog": "cat"})
    assert valid({"test": "test"})
    assert valid({"test": "test", "dog": "cat"})


def test_repeat_as():
    with pytest.raises(QueryValidatorStructureError):
        Query.has("test").as_(str).as_(str)

    with pytest.raises(QueryValidatorStructureError):
        Query.can_have("test").as_(str).as_(str)


def test_has_as_any_of():
    valid = Query.has("test").as_any_of({str, int})

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"test": 0.0})

    assert valid({"test": 0})
    assert valid({"test": "test"})
    assert valid({"test": "test", "dog": "cat"})


def test_can_have_as_any_of():
    valid = Query.can_have("test").as_any_of({str, int})

    with pytest.raises(QueryValidationError):
        valid({"test": 0.0})

    assert valid({})
    assert valid({"dog": "cat"})
    assert valid({"test": 0})
    assert valid({"test": "test"})
    assert valid({"test": "test", "dog": "cat"})


def test_repeat_as_any_of():
    with pytest.raises(QueryValidatorStructureError):
        Query.has("test").as_any_of({int, str}).as_any_of({int, str})

    with pytest.raises(QueryValidatorStructureError):
        Query.can_have("test").as_any_of({int, str}).as_any_of({int, str})


def test_repeat_as_as_any_of():
    with pytest.raises(QueryValidatorStructureError):
        Query.has("test").as_(str).as_any_of({int, str})

    with pytest.raises(QueryValidatorStructureError):
        Query.has("test").as_any_of({int, str}).as_(str)

    with pytest.raises(QueryValidatorStructureError):
        Query.can_have("test").as_(str).as_any_of({int, str})

    with pytest.raises(QueryValidatorStructureError):
        Query.can_have("test").as_any_of({int, str}).as_(str)


def test_has_or():
    valid = Query.has("test").or_("dog").or_("foo")

    with pytest.raises(QueryValidationError):
        valid({})

    assert valid({"test": "test"})
    assert valid({"dog": "cat"})
    assert valid({"foo": "bar"})
    assert valid({"test": 0})
    assert valid({"test": "test", "dog": "cat", "foo": "bar"})


def test_can_have_or():
    valid = Query.can_have("test").or_("dog").or_("foo")

    assert valid({})
    assert valid({"test": "test"})
    assert valid({"dog": "cat"})
    assert valid({"foo": "bar"})
    assert valid({"test": 0})
    assert valid({"test": "test", "dog": "cat", "foo": "bar"})


def test_has_as_or():
    valid = Query.has("test").as_(str).or_("dog").as_(int)

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    assert valid({"test": "test"})
    assert valid({"dog": 0})
    assert valid({"test": "test", "dog": 0})


def test_can_have_as_or():
    valid = Query.can_have("test").as_(str).or_("dog").as_(int)

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    assert valid({})
    assert valid({"test": "test"})
    assert valid({"dog": 0})
    assert valid({"test": "test", "dog": 0})


def test_has_as_any_of_or():
    valid = Query.has("test").as_any_of({str, int}).or_("dog").as_any_of({str, int})

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"test": 0.0})

    with pytest.raises(QueryValidationError):
        valid({"dog": 0.0})

    assert valid({"test": "test"})
    assert valid({"test": 0})
    assert valid({"dog": "cat"})
    assert valid({"dog": 0})
    assert valid({"test": "test", "dog": 0})


def test_can_have_as_any_of_or():
    valid = Query.can_have("test").as_any_of({str, int}).or_("dog").as_any_of({str, int})

    with pytest.raises(QueryValidationError):
        valid({"test": 0.0})

    with pytest.raises(QueryValidationError):
        valid({"dog": 0.0})

    assert valid({})
    assert valid({"test": "test"})
    assert valid({"test": 0})
    assert valid({"dog": "cat"})
    assert valid({"dog": 0})
    assert valid({"test": "test", "dog": 0})


def test_and():
    valid = Query.has("test").and_("dog").and_("foo")

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"test": "test"})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"foo": "bar"})

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    assert valid({"test": "test", "dog": "cat", "foo": "bar"})


def test_has_as_and():
    valid = Query.has("test").as_(str).and_("dog").as_(int)

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"test": "test"})

    with pytest.raises(QueryValidationError):
        valid({"dog": 0})

    assert valid({"test": "test", "dog": 0})


def test_can_have_as_and():
    valid = Query.can_have("test").as_(str).and_("dog").as_(int)

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    with pytest.raises(QueryValidationError):
        valid({"test": "test"})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"dog": 0})

    with pytest.raises(QueryValidationError):
        valid({"test": 0, "dog": "cat"})

    assert valid({})
    assert valid({"test": "test", "dog": 0})


def test_has_as_any_of_and():
    valid = Query.has("test").as_any_of({str, int}).and_("dog").as_any_of({str, int})

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"test": 0.0})

    with pytest.raises(QueryValidationError):
        valid({"dog": 0.0})

    with pytest.raises(QueryValidationError):
        valid({"test": "test"})

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"dog": 0})

    assert valid({"test": "test", "dog": "cat"})
    assert valid({"test": "test", "dog": 0})
    assert valid({"test": 0, "dog": "cat"})
    assert valid({"test": 0, "dog": 0})


def test_can_have_as_any_of_and():
    valid = Query.can_have("test").as_any_of({str, int}).and_("dog").as_any_of({str, int})

    with pytest.raises(QueryValidationError):
        valid({"test": 0.0})

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    with pytest.raises(QueryValidationError):
        valid({"test": "test"})

    with pytest.raises(QueryValidationError):
        valid({"dog": 0.0})

    with pytest.raises(QueryValidationError):
        valid({"dog": 0})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"test": 0.0, "dog": 0.0})

    assert valid({})
    assert valid({"test": "test", "dog": "cat"})
    assert valid({"test": "test", "dog": 0})
    assert valid({"test": 0, "dog": "cat"})
    assert valid({"test": 0, "dog": 0})


def test_has_nested_and_or():
    valid = Query.has("test").and_("cat").or_("dog")

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"test": "test"})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"cat": "dog"})

    with pytest.raises(QueryValidationError):
        valid({"test": "test", "foo": "bar"})

    assert valid({"test": "test", "cat": "dog"})
    assert valid({"test": "test", "dog": "cat"})


def test_has_nested_or_and():
    valid = Query.has("test").or_("cat").and_("dog")

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"cat": "dog"})

    assert valid({"test": "test"})
    assert valid({"dog": "cat", "cat": "dog"})


def test_can_have_nested_and_or():
    valid = Query.can_have("test").and_("cat").or_("dog")

    with pytest.raises(QueryValidationError):
        valid({"test": "test"})

    with pytest.raises(QueryValidationError):
        valid({"cat": "dog"})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    assert valid({})
    assert valid({"test": "test", "dog": "cat"})
    assert valid({"test": "test", "cat": "dog"})
    assert valid({"test": "test", "dog": "cat", "cat": "dog"})


def test_can_have_nested_or_and():
    valid = Query.can_have("test").or_("cat").and_("dog")

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"cat": "dog"})

    assert valid({})
    assert valid({"test": "test"})
    assert valid({"dog": "cat", "cat": "dog"})
    assert valid({"test": "test", "foo": "bar"})
    assert valid({"test": "test", "cat": "dog"})
    assert valid({"test": "test", "dog": "cat"})
    assert valid({"test": "test", "dog": "cat", "cat": "dog"})


def test_also():
    valid = Query.has("test").also.has("dog").also.has("foo")

    with pytest.raises(QueryValidationError):
        valid({})

    with pytest.raises(QueryValidationError):
        valid({"test": "test"})

    with pytest.raises(QueryValidationError):
        valid({"dog": "cat"})

    with pytest.raises(QueryValidationError):
        valid({"foo": "bar"})

    with pytest.raises(QueryValidationError):
        valid({"test": 0})

    assert valid({"test": "test", "dog": "cat", "foo": "bar"})


def test_repeat_also():
    with pytest.raises(QueryValidatorStructureError):
        Query.has("test").also.also


def test_default():
    valid = Query.can_have("test").with_default("test")

    query = {}
    assert valid(query)
    assert query == {"test": "test"}


def test_bad_default():
    with pytest.raises(QueryValidatorStructureError):
        Query.has("test").with_default("test")


def test_wrong_default_type():
    valid = Query.can_have("test").with_default("test")

    with pytest.raises(QueryValidationError):
        valid({"test": 1})


def test_no_default_type():
    valid = Query.can_have("test").with_default("test")

    query = {"test": "dog"}
    assert valid(query)
    assert query == {"test": "dog"}


def test_default_supplier():
    x = 0

    def supplier(query, context):
        nonlocal x
        x += 1
        return "test"

    valid = Query.can_have("test").with_default(supplier, str)

    query = {"test": "dog"}
    assert valid(query)
    assert query == {"test": "dog"}
    assert x == 0

    with pytest.raises(QueryValidationError):
        valid({"test": 1})
    assert x == 0

    query = {}
    assert valid(query)
    assert query == {"test": "test"}
    assert x == 1

    query = {}
    assert valid(query)
    assert query == {"test": "test"}
    assert x == 2


def test_validate_decorator():
    def pre_transform(query):
        if "test0" in query:
            query["test1"] = query["test0"]

    def pre_transform2(query):
        if "test1" in query:
            query["test2"] = int(query["test1"])

    validator = Query.has("test1").as_(str).also.has("test2").as_(int)

    @validate_query(validator, pre_transform, pre_transform2)
    def get(self, query, context=None):
        return query["test2"]

    with pytest.raises(QueryValidationError):
        get(None, {"cat": "dog"})

    with pytest.raises(ValueError):
        get(None, {"test1": "one"})

    assert get(None, {"test0": "1"}) == 1
