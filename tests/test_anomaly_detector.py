"""Tests for AnomalyDetector — _RollingStats."""

import pytest

from agents.risk.anomaly import _RollingStats


class TestRollingStats:
    def test_mean(self):
        rs = _RollingStats(maxlen=100)
        for v in [10.0, 20.0, 30.0]:
            rs.append(v)
        assert rs.mean() == pytest.approx(20.0)

    def test_std(self):
        rs = _RollingStats(maxlen=100)
        for v in [10.0, 20.0, 30.0]:
            rs.append(v)
        assert rs.std() == pytest.approx(10.0)

    def test_single_value_std_zero(self):
        rs = _RollingStats(maxlen=100)
        rs.append(5.0)
        assert rs.std() == 0.0

    def test_maxlen_eviction(self):
        rs = _RollingStats(maxlen=3)
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            rs.append(v)
        assert rs.count == 3
        assert rs.first() == 3.0
        assert rs.last() == 5.0

    def test_first_last(self):
        rs = _RollingStats(maxlen=100)
        rs.append(1.0)
        rs.append(2.0)
        rs.append(3.0)
        assert rs.first() == 1.0
        assert rs.last() == 3.0

    def test_values(self):
        rs = _RollingStats(maxlen=100)
        rs.append(10.0)
        rs.append(20.0)
        assert rs.values() == [10.0, 20.0]

    def test_empty_mean(self):
        rs = _RollingStats(maxlen=100)
        assert rs.mean() == 0.0

    def test_empty_last(self):
        rs = _RollingStats(maxlen=100)
        assert rs.last() == 0.0
