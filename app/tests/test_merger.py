import sys
sys.path.insert(0, r'D:\\Vento_Timing\\backend')
from app.codec.merger import deep_merge, merge_snapshot

class TestMerger:
    def test_simple_merge(self):
        base = {"a": 1, "b": 2}
        delta = {"b": 3, "c": 4}
        result = deep_merge(base, delta)
        assert result == {"a": 1, "b": 3, "c": 4}
        assert base == {"a": 1, "b": 2}

    def test_nested_merge(self):
        base = {"a": {"x": 1, "y": 2}}
        delta = {"a": {"y": 99, "z": 3}}
        result = deep_merge(base, delta)
        assert result == {"a": {"x": 1, "y": 99, "z": 3}}

    def test_none_delta(self):
        base = {"a": 1, "b": 2}
        result = deep_merge(base, {"c": None})
        assert result == {"a": 1, "b": 2, "c": None}

    def test_array_replacement(self):
        base = {"items": [1, 2, 3]}
        delta = {"items": [4, 5]}
        result = deep_merge(base, delta)
        assert result == {"items": [4, 5]}

    def test_merge_snapshot_none_base(self):
        result = merge_snapshot(None, {"a": 1})
        assert result == {"a": 1}

    def test_merge_snapshot_both_none(self):
        result = merge_snapshot(None, None)
        assert result == {}

    def test_empty_delta(self):
        base = {"a": 1, "b": 2}
        result = deep_merge(base, {})
        assert result == {"a": 1, "b": 2}
