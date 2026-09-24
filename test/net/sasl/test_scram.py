import base64
import hashlib
import hmac as hmac_module

import pytest

from kafka.net.sasl.scram import ScramClient


# RFC 7677 section 3 example (SCRAM-SHA-256)
RFC_CLIENT_NONCE = b'rOprNGfwEbeRWgbNEkqO'
RFC_SERVER_FIRST = (
    b'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0'
    b',s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
)
RFC_CLIENT_FINAL = (
    b'c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0'
    b',p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ='
)
RFC_SERVER_FINAL = b'v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4='


def make_client(user='user', password='pencil', mechanism='SCRAM-SHA-256',
                nonce=RFC_CLIENT_NONCE):
    client = ScramClient(user, password, mechanism)
    client.nonce = nonce
    return client


def test_rfc7677_scram_sha_256_vector():
    client = make_client()
    assert client.first_message() == b'n,,n=user,r=' + RFC_CLIENT_NONCE
    client.process_server_first_message(RFC_SERVER_FIRST)
    assert client.final_message() == RFC_CLIENT_FINAL
    client.process_server_final_message(RFC_SERVER_FINAL)


def test_scram_sha_512_vector():
    nonce = b'clientnonce'
    server_nonce = nonce + b'servernonce'
    salt = b'\x00' * 16
    iterations = 4096
    client = make_client(mechanism='SCRAM-SHA-512', nonce=nonce)
    client.first_message()
    server_first = (
        b'r=' + server_nonce
        + b',s=' + base64.b64encode(salt)
        + b',i=%d' % iterations
    )
    client.process_server_first_message(server_first)

    # Independently compute the expected proof and server signature
    auth_message = (
        b'n=user,r=' + nonce + b',' + server_first
        + b',c=biws,r=' + server_nonce
    )
    salted = hashlib.pbkdf2_hmac('sha512', b'pencil', salt, iterations)
    client_key = hmac_module.new(salted, b'Client Key', hashlib.sha512).digest()
    stored_key = hashlib.sha512(client_key).digest()
    signature = hmac_module.new(stored_key, auth_message, hashlib.sha512).digest()
    proof = bytes(lb ^ rb for lb, rb in zip(client_key, signature))
    assert client.final_message() == (
        b'c=biws,r=' + server_nonce + b',p=' + base64.b64encode(proof)
    )

    server_key = hmac_module.new(salted, b'Server Key', hashlib.sha512).digest()
    server_signature = hmac_module.new(server_key, auth_message, hashlib.sha512).digest()
    client.process_server_final_message(b'v=' + base64.b64encode(server_signature))


@pytest.mark.parametrize('mechanism', ['SCRAM-SHA-256', 'SCRAM-SHA-512'])
def test_username_with_comma_and_equals_is_escaped(mechanism):
    client = make_client(user='u=ser,na=me', mechanism=mechanism)
    first = client.first_message()
    assert first == b'n,,n=u=3Dser=2Cna=3Dme,r=' + RFC_CLIENT_NONCE


def test_duplicate_attribute_rejected():
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='[Dd]uplicate'):
        client.process_server_first_message(
            RFC_SERVER_FIRST + b',r=' + RFC_CLIENT_NONCE + b'extra'
        )


def test_nonce_not_extended_rejected():
    client = make_client()
    client.first_message()
    # server nonce identical to client nonce (nothing appended)
    with pytest.raises(ValueError, match='nonce'):
        client.process_server_first_message(
            b'r=' + RFC_CLIENT_NONCE + b',s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
        )
    # server nonce unrelated to client nonce
    with pytest.raises(ValueError, match='nonce'):
        client.process_server_first_message(
            b'r=totallydifferent,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
        )


@pytest.mark.parametrize('salt', [
    b'not!base64!',   # characters outside the base64 alphabet
    b'abc',           # incorrect padding
    b'',              # missing salt attribute value
])
def test_invalid_salt_rejected(salt):
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError):
        client.process_server_first_message(
            b'r=' + RFC_CLIENT_NONCE + b'x,s=' + salt + b',i=4096'
        )


@pytest.mark.parametrize('iterations', [
    b'0',        # must be >= 1
    b'04096',    # leading zero not allowed by RFC 5802
    b'-1',       # not a digit sequence
    b'abc',      # not an integer
    b'4096.0',   # not an integer
    b'1000001',  # above MAX_ITERATIONS
])
def test_invalid_iteration_count_rejected(iterations):
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='[Ii]teration'):
        client.process_server_first_message(
            b'r=' + RFC_CLIENT_NONCE + b'x,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=' + iterations
        )


@pytest.mark.parametrize('attribute', [b'r', b's', b'i'])
def test_missing_required_attribute_rejected(attribute):
    client = make_client()
    client.first_message()
    parts = {
        b'r': b'r=' + RFC_CLIENT_NONCE + b'x',
        b's': b's=W22ZaJ0SNY7soEsUEjb6gQ==',
        b'i': b'i=4096',
    }
    del parts[attribute]
    with pytest.raises(ValueError, match='[Mm]issing'):
        client.process_server_first_message(b','.join(parts.values()))


def test_mandatory_extension_rejected():
    client = make_client()
    client.first_message()
    with pytest.raises(ValueError, match='mandatory extension'):
        client.process_server_first_message(b'm=unsupported,' + RFC_SERVER_FIRST)


def test_optional_extension_ignored():
    client = make_client()
    client.first_message()
    client.process_server_first_message(RFC_SERVER_FIRST + b',x=some-extension')
    assert client.final_message().startswith(b'c=biws,r=')


def test_failed_server_first_does_not_publish_state():
    client = make_client()
    client.first_message()
    auth_message = client.auth_message
    with pytest.raises(ValueError):
        client.process_server_first_message(b'r=bad,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096')
    # no half-updated state may leak into a subsequent proof
    assert client.auth_message == auth_message
    assert client.client_proof is None
    assert client.salted_password is None
    with pytest.raises(ValueError):
        client.final_message()


def test_missing_verifier_rejected():
    client = make_client()
    client.first_message()
    client.process_server_first_message(RFC_SERVER_FIRST)
    with pytest.raises(ValueError, match='[Vv]erifier'):
        client.process_server_final_message(b'x=something')


def test_invalid_verifier_base64_rejected():
    client = make_client()
    client.first_message()
    client.process_server_first_message(RFC_SERVER_FIRST)
    with pytest.raises(ValueError, match='[Vv]erifier'):
        client.process_server_final_message(b'v=not!base64!')


def test_wrong_server_signature_rejected():
    client = make_client()
    client.first_message()
    client.process_server_first_message(RFC_SERVER_FIRST)
    wrong = base64.b64encode(b'\x00' * 32)
    with pytest.raises(ValueError, match='signature'):
        client.process_server_final_message(b'v=' + wrong)


def test_server_error_message_rejected():
    client = make_client()
    client.first_message()
    client.process_server_first_message(RFC_SERVER_FIRST)
    with pytest.raises(ValueError, match='invalid-proof'):
        client.process_server_final_message(b'e=invalid-proof')


def test_out_of_order_calls_rejected():
    client = make_client()
    with pytest.raises(ValueError):
        client.process_server_first_message(RFC_SERVER_FIRST)
    with pytest.raises(ValueError):
        client.final_message()
    with pytest.raises(ValueError):
        client.process_server_final_message(RFC_SERVER_FINAL)
    client.first_message()
    with pytest.raises(ValueError):
        client.first_message()
    client.process_server_first_message(RFC_SERVER_FIRST)
    with pytest.raises(ValueError):
        client.process_server_first_message(RFC_SERVER_FIRST)


def test_errors_do_not_leak_credentials(caplog):
    password = 'sup3r-s3cret'
    client = make_client(password=password)
    client.first_message()
    bad_challenges = [
        b'r=bad,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096',
        b'r=' + RFC_CLIENT_NONCE + b'x,s=!!!,i=4096',
        b'r=' + RFC_CLIENT_NONCE + b'x,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=abc',
        b'r=' + RFC_CLIENT_NONCE + b'x,r=' + RFC_CLIENT_NONCE + b'y,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096',
    ]
    for challenge in bad_challenges:
        with pytest.raises(ValueError) as exc_info:
            client.process_server_first_message(challenge)
        assert password not in str(exc_info.value)
    with pytest.raises(ValueError) as exc_info:
        client.process_server_final_message(b'e=invalid-proof')
    assert password not in str(exc_info.value)
    assert password.encode() not in caplog.text.encode()
