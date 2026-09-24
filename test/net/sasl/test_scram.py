import base64

import pytest

from kafka.errors import KafkaConfigurationError
from kafka.net.sasl.scram import (
    MAX_ITERATIONS,
    SaslMechanismScram,
    ScramClient,
    saslname,
)


# Official test vector from RFC 7677 section 3 (SCRAM-SHA-256)
CLIENT_NONCE = b'rOprNGfwEbeRWgbNEkqO'
SERVER_NONCE = CLIENT_NONCE + b'%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0'
SERVER_FIRST_MESSAGE = (
    b'r=' + SERVER_NONCE + b',s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
)
CLIENT_FINAL_MESSAGE = (
    b'c=biws,r=' + SERVER_NONCE
    + b',p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ='
)
SERVER_FINAL_MESSAGE = b'v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4='

# Same exchange computed for SCRAM-SHA-512
CLIENT_FINAL_MESSAGE_SHA512 = (
    b'c=biws,r=' + SERVER_NONCE
    + b',p=gMGXRcevScNtxZ6/8lQYpGtnsNAc3mGcmNomv+xnoOMw'
    + b'+3R2xNJdMNnzMlTN8PPC6wdp6dybEmDYXYTxwnYPJQ=='
)
SERVER_FINAL_MESSAGE_SHA512 = (
    b'v=ZQnYEgWQMFmmsM8aQMF0nDDCy/AgCzkwk8CmMZYcMg0v'
    + b'SVlKDanekLtifDSeVGT4+5ZxXnJq199RVG2rR7N7Zw=='
)


def make_client(mechanism='SCRAM-SHA-256', user='user', password='pencil'):
    client = ScramClient(user, password, mechanism)
    client.nonce = CLIENT_NONCE
    return client


def server_first(**overrides):
    parts = {
        'r': SERVER_NONCE.decode('utf-8'),
        's': 'W22ZaJ0SNY7soEsUEjb6gQ==',
        'i': '4096',
    }
    parts.update(overrides)
    return ','.join('%s=%s' % (attr, value) for attr, value in parts.items()).encode('utf-8')


def test_rfc7677_scram_sha_256_vector():
    client = make_client()
    assert client.first_message() == b'n,,n=user,r=' + CLIENT_NONCE
    client.process_server_first_message(SERVER_FIRST_MESSAGE)
    assert client.final_message() == CLIENT_FINAL_MESSAGE
    client.process_server_final_message(SERVER_FINAL_MESSAGE)


def test_scram_sha_512_vector():
    client = make_client(mechanism='SCRAM-SHA-512')
    assert client.first_message() == b'n,,n=user,r=' + CLIENT_NONCE
    client.process_server_first_message(SERVER_FIRST_MESSAGE)
    assert client.final_message() == CLIENT_FINAL_MESSAGE_SHA512
    client.process_server_final_message(SERVER_FINAL_MESSAGE_SHA512)


def test_saslname_escapes_comma_and_equals():
    assert saslname(b'user') == b'user'
    assert saslname(b'us,er') == b'us=2Cer'
    assert saslname(b'us=er') == b'us=3Der'
    assert saslname(b'a,b=c') == b'a=2Cb=3Dc'
    # '=' must be escaped before ',' so escape sequences are not re-escaped
    assert saslname(b'=,') == b'=3D=2C'


def test_first_message_escapes_username():
    client = make_client(user='us,er=foo')
    assert client.first_message() == b'n,,n=us=2Cer=3Dfoo,r=' + CLIENT_NONCE


def test_duplicate_attribute_rejected():
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='Duplicate attribute'):
        client.process_server_first_message(
            SERVER_FIRST_MESSAGE + b',r=' + SERVER_NONCE
        )


def test_server_nonce_must_start_with_client_nonce():
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='nonce'):
        client.process_server_first_message(server_first(r='AAAABBBBCCCC'))


def test_server_nonce_must_extend_client_nonce():
    client = make_client()
    client.first_message()
    # identical nonce: server appended nothing
    with pytest.raises(ValueError, match='nonce'):
        client.process_server_first_message(
            server_first(r=CLIENT_NONCE.decode('utf-8'))
        )


def test_unknown_mandatory_extension_rejected():
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='mandatory'):
        client.process_server_first_message(
            b'm=unsupported,' + SERVER_FIRST_MESSAGE
        )


@pytest.mark.parametrize('missing', ['r', 's', 'i'])
def test_missing_required_attribute_rejected(missing):
    client = make_client()
    client.first_message()
    parts = dict(
        pair.split('=', 1)
        for pair in SERVER_FIRST_MESSAGE.decode('utf-8').split(',')
    )
    del parts[missing]
    message = ','.join(
        '%s=%s' % (attr, value) for attr, value in parts.items()
    ).encode('utf-8')
    with pytest.raises(ValueError, match='Missing required attribute'):
        client.process_server_first_message(message)


@pytest.mark.parametrize('salt', [
    '!!!not-base64!!!',
    'W22ZaJ0SNY7soEsUEjb6gQ',  # padding stripped
    'W22ZaJ0SNY7soEsUEjb6gQ==\n',
])
def test_invalid_salt_rejected(salt):
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='salt'):
        client.process_server_first_message(server_first(s=salt))


@pytest.mark.parametrize('iterations', [
    'abc',
    '40.96',
    '-1',
    '0',
    str(MAX_ITERATIONS + 1),
])
def test_invalid_iteration_count_rejected(iterations):
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='[Ii]teration'):
        client.process_server_first_message(server_first(i=iterations))


def test_malformed_message_rejected():
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='Malformed'):
        client.process_server_first_message(b'not-a-scram-message')


def test_failed_challenge_does_not_update_state_or_leak_proof():
    client = make_client(password='sup3r-secret')
    client.first_message()
    auth_message = client.auth_message
    with pytest.raises(ValueError):
        client.process_server_first_message(server_first(s='!!!'))
    # no half-updated state may remain after a rejected challenge
    assert client.auth_message == auth_message
    assert client.nonce == CLIENT_NONCE
    assert client.salted_password is None
    assert client.client_proof is None
    assert client.server_signature is None
    # and no proof derived from the password can be produced
    with pytest.raises(ValueError):
        client.final_message()


def test_server_first_requires_client_first():
    client = make_client()
    with pytest.raises(ValueError, match='client-first'):
        client.process_server_first_message(SERVER_FIRST_MESSAGE)


def test_server_first_processed_only_once():
    client = make_client()
    client.first_message()
    client.process_server_first_message(SERVER_FIRST_MESSAGE)
    with pytest.raises(ValueError, match='already processed'):
        client.process_server_first_message(SERVER_FIRST_MESSAGE)


def test_server_final_error_message():
    client = make_client()
    client.first_message()
    client.process_server_first_message(SERVER_FIRST_MESSAGE)
    with pytest.raises(ValueError, match='invalid-encoding'):
        client.process_server_final_message(b'e=invalid-encoding')


def test_server_final_missing_verifier():
    client = make_client()
    client.first_message()
    client.process_server_first_message(SERVER_FIRST_MESSAGE)
    with pytest.raises(ValueError, match='[Mm]issing verifier'):
        client.process_server_final_message(b'x=optional-extension')


def test_server_final_invalid_verifier_base64():
    client = make_client()
    client.first_message()
    client.process_server_first_message(SERVER_FIRST_MESSAGE)
    with pytest.raises(ValueError, match='verifier'):
        client.process_server_final_message(b'v=!!!not-base64!!!')


def test_server_final_wrong_signature():
    client = make_client()
    client.first_message()
    client.process_server_first_message(SERVER_FIRST_MESSAGE)
    bad_signature = base64.b64encode(b'\x00' * 32)
    with pytest.raises(ValueError, match='signature'):
        client.process_server_final_message(b'v=' + bad_signature)


def test_server_final_requires_server_first():
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='server-first'):
        client.process_server_final_message(SERVER_FINAL_MESSAGE)


def test_errors_do_not_contain_credentials():
    client = make_client(user='sec,ret=user', password='sup3r=secret,password')
    client.first_message()
    challenges = [
        b'garbage',
        b'm=ext,' + SERVER_FIRST_MESSAGE,
        server_first(r='wrong-nonce'),
        server_first(s='!!!'),
        server_first(i='abc'),
    ]
    for challenge in challenges:
        with pytest.raises(ValueError) as exc_info:
            client.process_server_first_message(challenge)
        assert 'sup3r=secret,password' not in str(exc_info.value)
        assert 'sec,ret=user' not in str(exc_info.value)


def test_sasl_mechanism_scram_full_exchange():
    config = {
        'sasl_plain_username': 'user',
        'sasl_plain_password': 'pencil',
        'sasl_mechanism': 'SCRAM-SHA-256',
        'security_protocol': 'SASL_SSL',
    }
    mechanism = SaslMechanismScram(**config)
    mechanism._scram_client.nonce = CLIENT_NONCE
    assert not mechanism.is_done()
    assert mechanism.auth_bytes() == b'n,,n=user,r=' + CLIENT_NONCE
    mechanism.receive(SERVER_FIRST_MESSAGE)
    assert mechanism.auth_bytes() == CLIENT_FINAL_MESSAGE
    mechanism.receive(SERVER_FINAL_MESSAGE)
    assert mechanism.is_done()
    assert mechanism.is_authenticated()


def test_sasl_mechanism_scram_failed_challenge_sends_no_proof():
    config = {
        'sasl_plain_username': 'user',
        'sasl_plain_password': 'pencil',
        'sasl_mechanism': 'SCRAM-SHA-256',
        'security_protocol': 'SASL_SSL',
    }
    mechanism = SaslMechanismScram(**config)
    mechanism._scram_client.nonce = CLIENT_NONCE
    mechanism.auth_bytes()
    with pytest.raises(ValueError):
        mechanism.receive(server_first(s='!!!'))
    # state machine must not advance, so auth_bytes() can only re-emit
    # the client-first-message -- never a proof derived from the password
    assert not mechanism.is_done()
    assert mechanism.auth_bytes() == b'n,,n=user,r=' + CLIENT_NONCE


@pytest.mark.parametrize('config', [
    {'sasl_plain_password': 'pencil', 'sasl_mechanism': 'SCRAM-SHA-256'},
    {'sasl_plain_username': 'user', 'sasl_mechanism': 'SCRAM-SHA-256'},
    {'sasl_plain_username': 'user', 'sasl_plain_password': 'pencil',
     'sasl_mechanism': 'SCRAM-SHA-1'},
])
def test_sasl_mechanism_scram_invalid_config(config):
    with pytest.raises(KafkaConfigurationError):
        SaslMechanismScram(**config)
