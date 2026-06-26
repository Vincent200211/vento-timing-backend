import sys, asyncio
sys.path.insert(0, r'D:\\Vento_Timing\\backend')
from app.core.event_bus import EventBus

class TestEventBus:
    def test_subscribe_and_emit(self):
        bus = EventBus()
        results = []
        async def handler(t, d, ts):
            results.append((t, d))
        bus.subscribe("TestTopic", handler)
        asyncio.run(bus.emit("TestTopic", {"msg": "hello"}, 0.0))
        assert len(results) == 1
        assert results[0] == ("TestTopic", {"msg": "hello"})

    def test_multiple_subscribers(self):
        bus = EventBus()
        r1, r2 = [], []
        async def h1(t, d, ts): r1.append(t)
        async def h2(t, d, ts): r2.append(t)
        bus.subscribe("Topic", h1)
        bus.subscribe("Topic", h2)
        asyncio.run(bus.emit("Topic", {}, 0.0))
        assert len(r1) == 1 and len(r2) == 1

    def test_subscribe_all(self):
        bus = EventBus()
        results = []
        async def h(t, d, ts): results.append(t)
        bus.subscribe_all(h)
        asyncio.run(bus.emit("TopicA", {}, 0.0))
        asyncio.run(bus.emit("TopicB", {}, 0.0))
        assert set(results) == {"TopicA", "TopicB"}

    def test_unsubscribe(self):
        bus = EventBus()
        results = []
        async def h(t, d, ts): results.append(t)
        bus.subscribe("T", h)
        bus.unsubscribe("T", h)
        asyncio.run(bus.emit("T", {}, 0.0))
        assert len(results) == 0

    def test_no_matching_topic(self):
        bus = EventBus()
        results = []
        async def h(t, d, ts): results.append(t)
        bus.subscribe("OnlyThis", h)
        asyncio.run(bus.emit("Other", {}, 0.0))
        assert len(results) == 0

    def test_handler_error_does_not_crash(self):
        bus = EventBus()
        async def bad(t, d, ts):
            raise ValueError("oops")
        async def good(t, d, ts):
            pass
        bus.subscribe("T", bad)
        bus.subscribe("T", good)
        asyncio.run(bus.emit("T", {}, 0.0))
