import sys
sys.path.insert(0, r'D:\\Vento_Timing\\backend')
from app.core.weather_state import WeatherState

class TestWeatherState:
    def setup_method(self):
        self.state = WeatherState()

    def test_weather_data(self):
        data = {"AirTemp": 25.5, "TrackTemp": 32.0, "Humidity": 60,
                "Pressure": 1013, "WindSpeed": 5.0, "WindDirection": 180,
                "Rainfall": False}
        self.state.apply_event("WeatherData", data, 0.0)
        assert self.state.weather is not None
        assert self.state.weather.air_temperature == 25.5
        assert self.state.weather.humidity == 60

    def test_track_status_green(self):
        self.state.apply_event("TrackStatus", {"Status": "1"}, 0.0)
        assert self.state.track_status is not None
        assert self.state.track_status.status == "Green"

    def test_track_status_red(self):
        self.state.apply_event("TrackStatus", {"Status": "5"}, 0.0)
        assert self.state.track_status.status == "Red"

    def test_track_status_safety_car(self):
        self.state.apply_event("TrackStatus", {"Status": "7"}, 0.0)
        assert self.state.track_status.status == "SafetyCar"

    def test_weather_snapshot(self):
        self.state.apply_event("WeatherData", {"AirTemp": 30.0}, 0.0)
        snap = self.state.get_snapshot()
        assert "weather" in snap
        assert snap["weather"]["air_temperature"] == 30.0

    def test_empty_state(self):
        snap = self.state.get_snapshot()
        assert snap["weather"] is None
        assert snap["track_status"] is None

    def test_wrong_topic_ignored(self):
        self.state.apply_event("TimingData", {}, 0.0)
        assert self.state.weather is None
