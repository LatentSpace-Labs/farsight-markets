"""Tests for rolling window data structures."""

import pytest
from datetime import datetime, timedelta
from farsight.markets.engine.window import RollingWindow, VolumeWeightedWindow


class TestRollingWindow:
    def test_empty_window(self):
        w = RollingWindow(timedelta(minutes=5))
        assert w.is_empty()
        assert w.count() == 0
        assert w.mean() is None
        assert w.std() is None
        assert w.delta() is None
        assert w.last() is None
        assert w.first() is None
        assert w.min() is None
        assert w.max() is None

    def test_add_and_query(self):
        w = RollingWindow(timedelta(minutes=5))
        now = datetime.utcnow()
        w.add(now, 10.0)
        w.add(now + timedelta(seconds=1), 20.0)
        w.add(now + timedelta(seconds=2), 30.0)

        assert w.count() == 3
        assert w.mean() == 20.0
        assert w.first() == 10.0
        assert w.last() == 30.0
        assert w.delta() == 20.0
        assert w.min() == 10.0
        assert w.max() == 30.0
        assert w.sum() == 60.0

    def test_eviction(self):
        w = RollingWindow(timedelta(seconds=2))
        old = datetime.utcnow() - timedelta(seconds=10)
        recent = datetime.utcnow()

        w.add(old, 100.0)  # Should be evicted
        w.add(recent, 50.0)

        assert w.count() == 1
        assert w.last() == 50.0

    def test_maxlen(self):
        w = RollingWindow(timedelta(hours=24), maxlen=3)
        now = datetime.utcnow()
        for i in range(5):
            w.add(now + timedelta(seconds=i), float(i))

        # maxlen=3, so only last 3 values kept
        assert w.count() == 3
        assert w.values == [2.0, 3.0, 4.0]

    def test_std(self):
        w = RollingWindow(timedelta(minutes=5))
        now = datetime.utcnow()
        w.add(now, 10.0)
        w.add(now + timedelta(seconds=1), 20.0)
        w.add(now + timedelta(seconds=2), 30.0)

        std = w.std()
        assert std is not None
        assert abs(std - 10.0) < 0.01

    def test_std_single_value_returns_none(self):
        w = RollingWindow(timedelta(minutes=5))
        w.add(datetime.utcnow(), 42.0)
        assert w.std() is None

    def test_last_timestamp(self):
        w = RollingWindow(timedelta(minutes=5))
        now = datetime.utcnow()
        w.add(now, 1.0)
        assert w.last_timestamp() == now

    def test_not_empty_after_add(self):
        w = RollingWindow(timedelta(minutes=5))
        w.add(datetime.utcnow(), 1.0)
        assert not w.is_empty()


class TestVolumeWeightedWindow:
    def test_empty(self):
        vw = VolumeWeightedWindow(timedelta(minutes=5))
        assert vw.is_empty()
        assert vw.vwap() is None
        assert vw.total_volume() == 0.0
        assert vw.trade_count() == 0

    def test_vwap_calculation(self):
        vw = VolumeWeightedWindow(timedelta(minutes=5))
        now = datetime.utcnow()

        # 100 shares at $10, 200 shares at $20
        # VWAP = (100*10 + 200*20) / (100 + 200) = 5000/300 = 16.667
        vw.add(now, 10.0, 100.0)
        vw.add(now + timedelta(seconds=1), 20.0, 200.0)

        vwap = vw.vwap()
        assert vwap is not None
        assert abs(vwap - 16.667) < 0.01

    def test_total_volume(self):
        vw = VolumeWeightedWindow(timedelta(minutes=5))
        now = datetime.utcnow()
        vw.add(now, 10.0, 100.0)
        vw.add(now + timedelta(seconds=1), 20.0, 200.0)

        assert vw.total_volume() == 300.0
        assert vw.trade_count() == 2

    def test_eviction(self):
        vw = VolumeWeightedWindow(timedelta(seconds=2))
        old = datetime.utcnow() - timedelta(seconds=10)
        recent = datetime.utcnow()

        vw.add(old, 10.0, 100.0)
        vw.add(recent, 20.0, 200.0)

        assert vw.trade_count() == 1
        assert vw.total_volume() == 200.0
