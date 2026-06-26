import sys
sys.path.insert(0, r'D:\\Vento_Timing\\backend')
from app.core.timing_state import TimingState

class TestTimingState:
    def setup_method(self):
        self.state = TimingState()

    def test_apply_timing_data(self):
        data = {"Lines": {"44": {"Position": 1, "GapToLeader": "+1.234"}}}
        self.state.apply_event("TimingData", data, 0.0)
        snap = self.state.get_snapshot()
        assert 44 in snap
        assert snap[44]["position"] == 1
        assert snap[44]["gap_to_leader"] == "+1.234"

    def test_multiple_drivers(self):
        data = {"Lines": {"44": {"Position": 1}, "16": {"Position": 2}}}
        self.state.apply_event("TimingData", data, 0.0)
        snap = self.state.get_snapshot()
        assert len(snap) == 2
        assert snap[44]["position"] == 1
        assert snap[16]["position"] == 2

    def test_lap_number(self):
        data = {"Lines": {"44": {"LapNumber": 12}}}
        self.state.apply_event("TimingData", data, 0.0)
        assert self.state.timing[44].lap_number == 12

    def test_sector_times(self):
        data = {"Lines": {"44": {"Sector1": "30.5", "Sector2": "25.3", "Sector3": "28.7"}}}
        self.state.apply_event("TimingData", data, 0.0)
        e = self.state.timing[44]
        assert e.sector1 == "30.5"
        assert e.sector2 == "25.3"
        assert e.sector3 == "28.7"

    def test_wrong_topic_ignored(self):
        self.state.apply_event("OtherTopic", {"Lines": {"44": {"Position": 1}}}, 0.0)
        assert len(self.state.get_snapshot()) == 0

    def test_no_lines(self):
        self.state.apply_event("TimingData", {}, 0.0)
        assert len(self.state.get_snapshot()) == 0

    def test_invalid_driver_number(self):
        data = {"Lines": {"abc": {"Position": 1}}}
        self.state.apply_event("TimingData", data, 0.0)
        assert len(self.state.get_snapshot()) == 0
