import json
import zlib

from optionstrat_flow_monitor import (
    MSG_NEW_OPTIONS_FLOW,
    decode_websocket_message,
    decrypt_protected_array,
)


def protect_array_for_test(decoded: bytes, correction_index: int = 2, xor_value: int = 11) -> bytes:
    """Synthetic encoder used only to verify the public decoder implementation."""
    if correction_index >= len(decoded):
        correction_index = 0

    prepared = bytearray(decoded)
    prepared[correction_index] ^= xor_value

    compressor = zlib.compressobj(level=6, wbits=-zlib.MAX_WBITS)
    compressed = bytearray(compressor.compress(bytes(prepared)) + compressor.flush())

    for index in range(len(compressed)):
        compressed[index] ^= index % xor_value

    return bytes([correction_index, xor_value]) + bytes(compressed)


def encode_7bit(value: int) -> bytes:
    out = bytearray()
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def test_protected_array_round_trip():
    payload = b'{"ok":true,"items":[1,2,3]}'
    protected = protect_array_for_test(payload)
    assert decrypt_protected_array(protected) == payload


def test_websocket_binary_message_decoding():
    body = json.dumps({"symbol": "SPX", "premium": 12345}).encode("utf-8")
    decoded_frame = bytes([MSG_NEW_OPTIONS_FLOW]) + encode_7bit(len(body)) + body
    protected_frame = protect_array_for_test(decoded_frame, correction_index=3, xor_value=13)

    message_type, payload = decode_websocket_message(protected_frame)
    assert message_type == MSG_NEW_OPTIONS_FLOW
    assert payload["symbol"] == "SPX"
    assert payload["premium"] == 12345
