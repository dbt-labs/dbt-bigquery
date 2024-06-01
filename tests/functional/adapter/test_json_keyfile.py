import json
import pytest
from dbt.adapters.bigquery.utility import string_to_base64, is_base64


@pytest.fixture
def example_json_keyfile():
    keyfile = json.dumps(
        {
            "type": "service_account",
            "project_id": "",
            "private_key_id": "",
            "private_key": "-----BEGIN PRIVATE KEY----------END PRIVATE KEY-----\n",
            "client_email": "",
            "client_id": "",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "",
        }
    )

    return keyfile


@pytest.fixture
def example_json_keyfile_b64():
    keyfile = json.dumps(
        {
            "type": "service_account",
            "project_id": "",
            "private_key_id": "",
            "private_key": "-----BEGIN PRIVATE KEY----------END PRIVATE KEY-----\n",
            "client_email": "",
            "client_id": "",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": "",
        }
    )

    return string_to_base64(keyfile)


def test_valid_base64_strings(example_json_keyfile_b64):
    valid_strings = [
        "SGVsbG8gV29ybGQh",  # "Hello World!"
        "Zm9vYmFy",  # "foobar"
        "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5",  # A long string
        "",  # Empty string
        example_json_keyfile_b64.decode("utf-8"),
    ]

    for s in valid_strings:
        assert is_base64(s) is True


def test_valid_base64_bytes(example_json_keyfile_b64):
    valid_bytes = [
        b"SGVsbG8gV29ybGQh",  # "Hello World!"
        b"Zm9vYmFy",  # "foobar"
        b"QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5",  # A long string
        b"",  # Empty bytes
        example_json_keyfile_b64,
    ]
    for s in valid_bytes:
        assert is_base64(s) is True


def test_invalid_base64(example_json_keyfile):
    invalid_inputs = [
        "This is not Base64",
        "SGVsbG8gV29ybGQ",  # Incorrect padding
        "Invalid#Base64",
        12345,  # Not a string or bytes
        b"Invalid#Base64",
        "H\xffGVsbG8gV29ybGQh",  # Contains invalid character \xff
        example_json_keyfile,
    ]
    for s in invalid_inputs:
        assert is_base64(s) is False
