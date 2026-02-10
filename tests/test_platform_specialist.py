"""Tests for PlatformSpecialist — fee tiers and asset info."""

import pytest

from agents.ingestion.platform_specialist import PlatformSpecialist
from core.models import AssetInfo
from tests.conftest import MockMessageBus


def _make_specialist() -> PlatformSpecialist:
    return PlatformSpecialist(bus=MockMessageBus())


class TestUpdateFeeTier:
    def test_lowest_tier(self):
        ps = _make_specialist()
        ps._update_fee_tier(0)  # Zero volume
        assert ps._maker_fee_bps == pytest.approx(0.10)
        assert ps._taker_fee_bps == pytest.approx(0.35)

    def test_mid_tier(self):
        ps = _make_specialist()
        ps._update_fee_tier(5_000_000)
        assert ps._maker_fee_bps == pytest.approx(0.06)
        assert ps._taker_fee_bps == pytest.approx(0.28)

    def test_highest_tier(self):
        ps = _make_specialist()
        ps._update_fee_tier(500_000_000)
        assert ps._maker_fee_bps == pytest.approx(0.00)
        assert ps._taker_fee_bps == pytest.approx(0.18)


class TestGetEffectiveFee:
    def test_maker(self):
        ps = _make_specialist()
        assert ps.get_effective_fee(is_maker=True) == pytest.approx(0.10)

    def test_taker(self):
        ps = _make_specialist()
        assert ps.get_effective_fee(is_maker=False) == pytest.approx(0.35)


class TestGetAssetInfo:
    def test_exists(self):
        ps = _make_specialist()
        ps._asset_registry["BTC"] = AssetInfo(
            asset="BTC", sz_decimals=4, max_leverage=50,
        )
        info = ps.get_asset_info("BTC")
        assert info is not None
        assert info.asset == "BTC"

    def test_missing(self):
        ps = _make_specialist()
        info = ps.get_asset_info("NONEXISTENT")
        assert info is None
