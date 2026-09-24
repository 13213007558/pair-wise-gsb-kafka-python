import base64
import binascii
import hashlib
import hmac
import logging
import uuid


from .abc import SaslMechanism
from kafka.errors import KafkaConfigurationError


log = logging.getLogger(__name__)


def xor_bytes(left, right):
    return bytes(lb ^ rb for lb, rb in zip(left, right))


def _escape_saslname(name):
    # RFC 5802 section 5.1: '=' and ',' in usernames are escaped
    # as '=3D' and '=2C' respectively ('=' must be escaped first).
    return name.replace(b'=', b'=3D').replace(b',', b'=2C')


def _parse_scram_message(message):
    # RFC 5802 section 7: attributes are a single ALPHA, values are
    # printable characters excluding ','. Unknown mandatory extensions
    # ('m=') and duplicated attributes must be rejected.
    try:
        decoded = message.decode('utf-8')
    except UnicodeDecodeError:
        raise ValueError('SCRAM message is not valid UTF-8')
    params = {}
    for part in decoded.split(','):
        if len(part) < 3 or part[1] != '=' or not part[0].isascii() or not part[0].isalpha():
            raise ValueError('Malformed attribute in SCRAM message')
        attr, value = part[0], part[2:]
        if not all(0x21 <= ord(char) <= 0x7E for char in value):
            raise ValueError('Invalid character in SCRAM attribute value')
        if attr in params:
            raise ValueError('Duplicate attribute in SCRAM message')
        if attr == 'm':
            raise ValueError('Unsupported mandatory extension in SCRAM message')
        params[attr] = value
    return params


class SaslMechanismScram(SaslMechanism):
    def __init__(self, **config):
        if not config.get('sasl_plain_username', ''):
            KafkaConfigurationError('sasl_plain_username required for SCRAM sasl')
        if not config.get('sasl_plain_password', ''):
            KafkaConfigurationError('sasl_plain_password required for SCRAM sasl')
        if config.get('sasl_mechanism', '') not in ScramClient.MECHANISMS:
            KafkaConfigurationError('Unrecognized SCRAM mechanism')
        if config.get('security_protocol', '') == 'SASL_PLAINTEXT':
            log.warning('Exchanging credentials in the clear during Sasl Authentication')

        self.username = config['sasl_plain_username']
        self.mechanism = config['sasl_mechanism']
        self._scram_client = ScramClient(
            config['sasl_plain_username'],
            config['sasl_plain_password'],
            config['sasl_mechanism']
        )
        self._state = 0

    def auth_bytes(self):
        if self._state == 0:
            return self._scram_client.first_message()
        elif self._state == 1:
            return self._scram_client.final_message()
        else:
            raise ValueError('No auth_bytes for state: %s' % self._state)

    def receive(self, auth_bytes):
        if self._state == 0:
            self._scram_client.process_server_first_message(auth_bytes)
        elif self._state == 1:
            self._scram_client.process_server_final_message(auth_bytes)
        else:
            raise ValueError('Cannot receive bytes in state: %s' % self._state)
        self._state += 1
        return self.is_done()

    def is_done(self):
        return self._state == 2

    def is_authenticated(self):
        # receive raises if authentication fails...?
        return self._state == 2

    def auth_details(self):
        if not self.is_authenticated():
            raise RuntimeError('Not authenticated yet!')
        return 'Authenticated as %s via SASL / %s' % (self.username, self.mechanism)


class ScramClient:
    MECHANISMS = {
        'SCRAM-SHA-256': hashlib.sha256,
        'SCRAM-SHA-512': hashlib.sha512
    }
    MAX_ITERATIONS = 1000000

    def __init__(self, user, password, mechanism):
        self.nonce = str(uuid.uuid4()).replace('-', '').encode('utf-8')
        self.auth_message = b''
        self.salted_password = None
        self.user = user.encode('utf-8')
        self.password = password.encode('utf-8')
        self.hashfunc = self.MECHANISMS[mechanism]
        self.hashname = ''.join(mechanism.lower().split('-')[1:3])
        self.stored_key = None
        self.client_key = None
        self.client_signature = None
        self.client_proof = None
        self.server_key = None
        self.server_signature = None
        self._state = 0

    def first_message(self):
        if self._state != 0:
            raise ValueError('client-first-message already sent')
        client_first_bare = b'n=' + _escape_saslname(self.user) + b',r=' + self.nonce
        self.auth_message = client_first_bare
        self._state = 1
        return b'n,,' + client_first_bare

    def process_server_first_message(self, server_first_message):
        if self._state != 1:
            raise ValueError('Cannot process server-first-message in current state')
        params = _parse_scram_message(server_first_message)
        for required in ('r', 's', 'i'):
            if required not in params:
                raise ValueError('Missing required attribute in server-first-message')

        server_nonce = params['r'].encode('utf-8')
        if not server_nonce.startswith(self.nonce) or len(server_nonce) <= len(self.nonce):
            raise ValueError('Server nonce did not extend the client nonce')

        try:
            salt = base64.b64decode(params['s'].encode('utf-8'), validate=True)
        except (binascii.Error, ValueError):
            raise ValueError('Invalid base64 salt in server-first-message')
        if not salt:
            raise ValueError('Empty salt in server-first-message')

        iterations = self._parse_iteration_count(params['i'])

        # Compute everything locally; internal state is only published
        # once the server-first-message has been fully validated, so a
        # rejected challenge can never leave a half-updated auth_message
        # or a proof derived from it.
        salted_password = hashlib.pbkdf2_hmac(
            self.hashname, self.password, salt, iterations
        )
        auth_message = (
            self.auth_message + b',' + server_first_message
            + b',c=biws,r=' + server_nonce
        )
        client_key = self.hmac(salted_password, b'Client Key')
        stored_key = self.hashfunc(client_key).digest()
        client_signature = self.hmac(stored_key, auth_message)
        client_proof = xor_bytes(client_key, client_signature)
        server_key = self.hmac(salted_password, b'Server Key')
        server_signature = self.hmac(server_key, auth_message)

        self.nonce = server_nonce
        self.auth_message = auth_message
        self.salted_password = salted_password
        self.client_key = client_key
        self.stored_key = stored_key
        self.client_signature = client_signature
        self.client_proof = client_proof
        self.server_key = server_key
        self.server_signature = server_signature
        self._state = 2

    def _parse_iteration_count(self, value):
        # RFC 5802: iteration-count = %x31-39 *DIGIT
        if not value or not value.isascii() or not value.isdigit() or value[0] == '0':
            raise ValueError('Invalid iteration count in server-first-message')
        iterations = int(value)
        if iterations < 1 or iterations > self.MAX_ITERATIONS:
            raise ValueError('Iteration count out of range in server-first-message')
        return iterations

    def hmac(self, key, msg):
        return hmac.new(key, msg, digestmod=self.hashfunc).digest()

    def create_salted_password(self, salt, iterations):
        self.salted_password = hashlib.pbkdf2_hmac(
            self.hashname, self.password, salt, iterations
        )

    def final_message(self):
        if self._state != 2 or self.client_proof is None:
            raise ValueError('Cannot create client-final-message before a valid server-first-message')
        return b'c=biws,r=' + self.nonce + b',p=' + base64.b64encode(self.client_proof)

    def process_server_final_message(self, server_final_message):
        if self._state != 2:
            raise ValueError('Cannot process server-final-message in current state')
        params = _parse_scram_message(server_final_message)
        if 'e' in params:
            raise ValueError('Server returned an error during SCRAM authentication: %s' % params['e'])
        if 'v' not in params:
            raise ValueError('Missing verifier in server-final-message')
        try:
            verifier = base64.b64decode(params['v'].encode('utf-8'), validate=True)
        except (binascii.Error, ValueError):
            raise ValueError('Invalid base64 verifier in server-final-message')
        if self.server_signature is None or not hmac.compare_digest(self.server_signature, verifier):
            raise ValueError("Server sent wrong signature!")
        self._state = 3
