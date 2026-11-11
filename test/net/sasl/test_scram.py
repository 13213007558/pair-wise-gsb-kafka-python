"""SCRAM authentication through the real connection state machine.

Beyond the ScramClient unit tests, these tests drive
``KafkaConnection.initialize()`` against a scripted MockBroker performing the
server side of SCRAM-SHA-256 / SCRAM-SHA-512, and prove that no business
request (Produce/Fetch) reaches the network before the server's proof has
been verified -- and that authentication failures keep the connection
unusable with the error attributed to the right futures.

All challenge data (salt, nonces, iteration count) is constructed locally;
the expected client proof is computed independently of ScramClient. Test
output never contains the password or derived keys: proof verification is a
boolean behind ``hmac.compare_digest``, and the kafka.net / kafka.protocol
debug logs (which would render SASL frames) are silenced for this module.
"""

import base64
import hashlib
import hmac
import logging

import pytest

import kafka.errors as Errors
from kafka.net.connection import KafkaConnection
from kafka.protocol.consumer import FetchRequest, FetchResponse
from kafka.protocol.producer import ProduceRequest
from kafka.protocol.sasl import (
    SaslAuthenticateRequest,
    SaslAuthenticateResponse,
    SaslHandshakeRequest,
    SaslHandshakeResponse,
)

from test.mock_broker import MockBroker


# Test-only credentials, never a real password. Nothing in this module logs
# or prints this value or any key derived from it.
TEST_USERNAME = 'scram-tester'
TEST_PASSWORD = 'test-only-password'

MECHANISMS = ['SCRAM-SHA-256', 'SCRAM-SHA-512']

API_KEY_PRODUCE = ProduceRequest.API_KEY
API_KEY_FETCH = FetchRequest.API_KEY
API_KEY_SASL_AUTHENTICATE = SaslAuthenticateRequest.API_KEY


@pytest.fixture(autouse=True)
def _no_sensitive_debug_logs():
    """Request/response debug logs render SASL frames, including the client
    proof and server signature (derived key material). Keep those loggers at
    INFO so a failing test's captured output never contains them."""
    loggers = [logging.getLogger(name) for name in ('kafka.net', 'kafka.protocol')]
    previous = [logger.level for logger in loggers]
    for logger in loggers:
        logger.setLevel(logging.INFO)
    try:
        yield
    finally:
        for logger, level in zip(loggers, previous):
            logger.setLevel(level)


class ScramServerSide:
    """Independent server-side SCRAM implementation for tests.

    Derives its own keys from the test password and verifies the client's
    proof without borrowing any ScramClient internals, so a client-side
    computation bug cannot be masked by shared code.
    """

    HASHES = {
        'SCRAM-SHA-256': hashlib.sha256,
        'SCRAM-SHA-512': hashlib.sha512,
    }

    def __init__(self, mechanism, password, salt, server_nonce, iterations=4096):
        self.hashfunc = self.HASHES[mechanism]
        self.hashname = mechanism.lower().replace('scram-', '').replace('-', '')
        self.password = password.encode('utf-8')
        self.salt = salt
        self.salt_b64 = base64.b64encode(salt)
        self.iterations = iterations
        self.server_nonce = server_nonce
        self.client_proof_valid = None

    def receive_client_first(self, data):
        assert data.startswith(b'n,,'), 'expected gs2 header in client-first-message'
        self.client_first_bare = data[3:]
        params = dict(pair.split(b'=', 1) for pair in self.client_first_bare.split(b','))
        self.client_nonce = params[b'r']
        self.full_nonce = self.client_nonce + self.server_nonce
        self.server_first = (b'r=' + self.full_nonce
                             + b',s=' + self.salt_b64
                             + b',i=' + str(self.iterations).encode('ascii'))
        return self.server_first

    def receive_client_final(self, data):
        without_proof, sep, proof_b64 = data.rpartition(b',p=')
        assert sep, 'client-final-message missing proof'
        proof = base64.b64decode(proof_b64)
        auth_message = self.client_first_bare + b',' + self.server_first + b',' + without_proof
        salted = hashlib.pbkdf2_hmac(self.hashname, self.password, self.salt, self.iterations)
        client_key = hmac.new(salted, b'Client Key', self.hashfunc).digest()
        stored_key = self.hashfunc(client_key).digest()
        client_signature = hmac.new(stored_key, auth_message, self.hashfunc).digest()
        recovered_client_key = bytes(p ^ s for p, s in zip(proof, client_signature))
        # Boolean only: key material must never reach test output.
        self.client_proof_valid = hmac.compare_digest(
            self.hashfunc(recovered_client_key).digest(), stored_key)
        server_key = hmac.new(salted, b'Server Key', self.hashfunc).digest()
        server_signature = hmac.new(server_key, auth_message, self.hashfunc).digest()
        return b'v=' + base64.b64encode(server_signature)


@pytest.fixture
def scram_broker():
    """MockBroker that records the api_key of every request it sees."""
    broker = MockBroker(broker_version=(2, 5))  # supports SaslAuthenticate v0-2
    broker.api_keys_seen = []
    handle_request = broker.handle_request

    async def recording(api_key, *args, **kwargs):
        broker.api_keys_seen.append(api_key)
        return await handle_request(api_key, *args, **kwargs)

    broker.handle_request = recording
    return broker


def _script_scram(broker, mechanism, server, mutate_first=None, mutate_final=None):
    """Script SaslHandshake + a two-step SCRAM exchange on the mock broker.

    ``mutate_first`` / ``mutate_final`` replace the legitimate server message
    with an attacker-controlled payload built from the server's state.
    """
    broker.respond(SaslHandshakeRequest, SaslHandshakeResponse(
        error_code=0, mechanisms=[mechanism]))

    steps = {'n': 0}

    def handle_auth(api_key, api_version, correlation_id, request_bytes):
        steps['n'] += 1
        request = SaslAuthenticateRequest.decode(
            request_bytes, version=api_version, header=True)
        if steps['n'] == 1:
            payload = server.receive_client_first(request.auth_bytes)
            if mutate_first is not None:
                payload = mutate_first(server)
            # The transcript is built over the bytes actually sent.
            server.server_first = payload
        else:
            payload = server.receive_client_final(request.auth_bytes)
            if mutate_final is not None:
                payload = mutate_final(server)
        return SaslAuthenticateResponse(
            error_code=0,
            error_message=None,
            auth_bytes=payload,
            session_lifetime_ms=0,
        )

    broker.respond_always(SaslAuthenticateRequest, handle_auth)


def _connect(net, broker, mechanism):
    conn = KafkaConnection(
        net,
        node_id='scram-test',
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism=mechanism,
        sasl_plain_username=TEST_USERNAME,
        sasl_plain_password=TEST_PASSWORD,
        request_timeout_ms=5000,
    )
    broker.attach(net)
    net.run(net.create_connection(conn, broker.host, broker.port))
    return conn


def _queue_business_requests(conn):
    """Queue a Produce and a Fetch while the connection is still
    initializing; they must stay buffered until authentication completes."""
    produce_future = conn.send_request(
        ProduceRequest(acks=0, timeout_ms=1000, topic_data=[]))
    fetch_future = conn.send_request(
        FetchRequest(max_wait_ms=100, min_bytes=1, topics=[]))
    return produce_future, fetch_future


def _assert_auth_failure(conn, broker, produce_future, fetch_future, error_type):
    # The connection never became usable...
    assert not conn.connected
    assert conn.closed
    assert conn.init_future.failed()
    assert isinstance(conn.init_future.exception, error_type)
    # ...the queued business requests never reached the network...
    assert API_KEY_PRODUCE not in broker.api_keys_seen
    assert API_KEY_FETCH not in broker.api_keys_seen
    # ...and the authentication error landed on the right futures.
    assert produce_future.failed()
    assert fetch_future.failed()
    assert produce_future.exception is conn.init_future.exception
    assert fetch_future.exception is conn.init_future.exception


class TestScramValidTranscript:
    @pytest.mark.parametrize('mechanism', MECHANISMS)
    def test_valid_transcript(self, net, scram_broker, mechanism):
        server = ScramServerSide(
            mechanism, TEST_PASSWORD,
            salt=b'scr4m-test-salt', server_nonce=b'srvnonce8f2k1')
        _script_scram(scram_broker, mechanism, server)
        scram_broker.respond(ProduceRequest, None)  # acks=0: broker sends nothing
        scram_broker.respond(FetchRequest, FetchResponse(
            throttle_time_ms=0, error_code=0, session_id=0, responses=[]))

        conn = _connect(net, scram_broker, mechanism)
        produce_future, fetch_future = _queue_business_requests(conn)
        net.run(conn.initialize())

        assert conn.connected
        assert conn.init_future.succeeded()
        # The client's proof matched the independently computed expectation.
        assert server.client_proof_valid is True

        net.poll(timeout_ms=1000, future=fetch_future)
        assert produce_future.succeeded()
        assert fetch_future.succeeded()

        # Business traffic went out only after the full SCRAM exchange.
        keys = scram_broker.api_keys_seen
        auth_positions = [i for i, key in enumerate(keys)
                          if key == API_KEY_SASL_AUTHENTICATE]
        assert len(auth_positions) == 2
        assert keys.index(API_KEY_PRODUCE) > max(auth_positions)
        assert keys.index(API_KEY_FETCH) > max(auth_positions)


class TestScramServerProofEnforcement:
    @pytest.mark.parametrize('mechanism', MECHANISMS)
    def test_wrong_server_signature(self, net, scram_broker, mechanism):
        server = ScramServerSide(
            mechanism, TEST_PASSWORD,
            salt=b'scr4m-test-salt', server_nonce=b'srvnonce8f2k1')
        _script_scram(scram_broker, mechanism, server,
                      mutate_final=lambda srv: b'v=' + base64.b64encode(b'\x00' * 64))

        conn = _connect(net, scram_broker, mechanism)
        produce_future, fetch_future = _queue_business_requests(conn)
        net.run(conn.initialize())

        # The client sent a valid proof, but rejected the forged server proof.
        assert server.client_proof_valid is True
        _assert_auth_failure(conn, scram_broker, produce_future, fetch_future, ValueError)

    @pytest.mark.parametrize('mechanism', MECHANISMS)
    def test_server_nonce_not_extended(self, net, scram_broker, mechanism):
        server = ScramServerSide(
            mechanism, TEST_PASSWORD,
            salt=b'scr4m-test-salt', server_nonce=b'srvnonce8f2k1')
        _script_scram(
            scram_broker, mechanism, server,
            mutate_first=lambda srv: (b'r=broker-only-nonce,s=' + srv.salt_b64
                                      + b',i=' + str(srv.iterations).encode('ascii')))

        conn = _connect(net, scram_broker, mechanism)
        produce_future, fetch_future = _queue_business_requests(conn)
        net.run(conn.initialize())

        _assert_auth_failure(conn, scram_broker, produce_future, fetch_future, ValueError)

    @pytest.mark.parametrize('mechanism', MECHANISMS)
    def test_duplicate_attribute(self, net, scram_broker, mechanism):
        server = ScramServerSide(
            mechanism, TEST_PASSWORD,
            salt=b'scr4m-test-salt', server_nonce=b'srvnonce8f2k1')
        # A second 'r' attribute, even with a consistent value, is malformed
        # per RFC 5802 and must be rejected rather than silently merged.
        _script_scram(scram_broker, mechanism, server,
                      mutate_first=lambda srv: srv.server_first + b',r=' + srv.full_nonce)

        conn = _connect(net, scram_broker, mechanism)
        produce_future, fetch_future = _queue_business_requests(conn)
        net.run(conn.initialize())

        _assert_auth_failure(conn, scram_broker, produce_future, fetch_future, ValueError)

    @pytest.mark.parametrize('mechanism', MECHANISMS)
    @pytest.mark.parametrize('bad_iterations', [b'abc', b'2000000', b'0'])
    def test_invalid_iteration_count(self, net, scram_broker, mechanism, bad_iterations):
        server = ScramServerSide(
            mechanism, TEST_PASSWORD,
            salt=b'scr4m-test-salt', server_nonce=b'srvnonce8f2k1')
        _script_scram(
            scram_broker, mechanism, server,
            mutate_first=lambda srv: (b'r=' + srv.full_nonce + b',s=' + srv.salt_b64
                                      + b',i=' + bad_iterations))

        conn = _connect(net, scram_broker, mechanism)
        produce_future, fetch_future = _queue_business_requests(conn)
        net.run(conn.initialize())

        _assert_auth_failure(conn, scram_broker, produce_future, fetch_future, ValueError)

    @pytest.mark.parametrize('mechanism', MECHANISMS)
    def test_disconnect_mid_auth(self, net, scram_broker, mechanism):
        scram_broker.respond(SaslHandshakeRequest, SaslHandshakeResponse(
            error_code=0, mechanisms=[mechanism]))
        # Broker drops the connection when the first SaslAuthenticate arrives.
        scram_broker.fail_next(SaslAuthenticateRequest)

        conn = _connect(net, scram_broker, mechanism)
        produce_future, fetch_future = _queue_business_requests(conn)
        net.run(conn.initialize())

        _assert_auth_failure(conn, scram_broker, produce_future, fetch_future,
                             Errors.KafkaConnectionError)
