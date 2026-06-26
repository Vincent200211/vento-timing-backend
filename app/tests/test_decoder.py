import sys, json, base64, zlib
sys.path.insert(0, r'D:\\Vento_Timing\\backend')
from app.codec.decoder import decode_topic_data

class TestDecoder:
    def test_plain_json(self):
        result = decode_topic_data('{"a": 1, "b": "hello"}')
        assert result == {"a": 1, "b": "hello"}

    def test_empty_object(self):
        result = decode_topic_data('{}')
        assert result == {}

    def test_invalid_input(self):
        result = decode_topic_data('not json at all')
        assert result == {}

    def test_empty_string(self):
        result = decode_topic_data('')
        assert result == {}

    def test_quoted_json(self):
        result = decode_topic_data('"' + '{"x": 5}' + '"')
        assert isinstance(result, dict)

    def test_base64_zlib(self):
        original = '{"TimingData": {"Lines": {"44": {"Position": 1}}}}'
        compressor = zlib.compressobj(level=zlib.Z_DEFAULT_COMPRESSION, method=zlib.DEFLATED, wbits=-zlib.MAX_WBITS)
        compressed = compressor.compress(original.encode()) + compressor.flush()
        encoded = base64.b64encode(compressed).decode()
        result = decode_topic_data(encoded)
        assert result == json.loads(original)