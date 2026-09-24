import base64
import binascii
import hashlib
import hmac
import logging
import string
import uuid


from .abc import SaslMechanism
from kafka.errors import KafkaConfigurationError


log = logging.getLogger(__name__)


# Upper bound on the PBKDF2 iteration count accepted from the server,
# guarding against denial-of-service via a malicious challenge.
MAX_ITERATIONS = 1000000


def xor_bytes(left, right):
    return bytes(lb ^ rb for lb, rb in zip(left, right))


def saslname(name):
    # RFC 5802: '=' is escaped as '=3D' and ',' as '=2C'; escape '=' first
    # so the escape sequences themselves are not re-escaped.
    return name.replace(b'=', b'=3D').replace(b',', b'=2C')


def parse_scram_attributes(message):
    # RFC 5802 section 7: attr-value = ALPHA "=" value, joined by ',';
    # value is 1*value-char, and no attribute may appear more than once.
    attributes = {}
    for part in message.split(','):
        if len(part) < 3 or part[0] not in string.ascii_letters or part[1] != '=':
            raise ValueError('Malformed attribute in SCRAM message')
        attr, value = part[0], part[2:]
        if attr in attributes:
            raise ValueError('Duplicate attribute in SCRAM message: %s' % attr)
        attributes[attr] = value
    return attributes


class SaslMechanismScram(SaslMechanism):
    def __init__(self, **config):
        if not config.get('sasl_plain_username', ''):
            raise KafkaConfigurationError('sasl_plain_username required for SCRAM sasl')
        if not config.get('sasl_plain_password', ''):
            raise KafkaConfigurationError('sasl_plain_password required for SCRAM sasl')
        if config.get('sasl_mechanism', '') not in ScramClient.MECHANISMS:
            raise KafkaConfigurationError('Unrecognized SCRAM mechanism')
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
        if not self.is_authenticated:
            raise RuntimeError('Not authenticated yet!')
        return 'Authenticated as %s via SASL / %s' % (self.username, self.mechanism)


class ScramClient:
    MECHANISMS = {
        'SCRAM-SHA-256': hashlib.sha256,
        'SCRAM-SHA-512': hashlib.sha512
    }

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

    def first_message(self):
        client_first_bare = b'n=' + saslname(self.user) + b',r=' + self.nonce
        self.auth_message = client_first_bare
        return b'n,,' + client_first_bare

    def process_server_first_message(self, server_first_message):
        if not self.auth_message:
            raise ValueError('Cannot process server-first-message before sending client-first-message')
        if self.client_proof is not None:
            raise ValueError('server-first-message already processed')
        try:
            decoded = server_first_message.decode('utf-8')
        except UnicodeDecodeError:
            raise ValueError('Invalid server-first-message: not valid UTF-8')
        params = parse_scram_attributes(decoded)
        if 'm' in params:
            raise ValueError('Server requires an unsupported mandatory SCRAM extension')
        for required in ('r', 's', 'i'):
            if required not in params:
                raise ValueError('Missing required attribute in server-first-message: %s' % required)

        server_nonce = params['r'].encode('utf-8')
        if not server_nonce.startswith(self.nonce) or len(server_nonce) == len(self.nonce):
            raise ValueError('Server nonce does not extend the client nonce')

        try:
            salt = base64.b64decode(params['s'].encode('utf-8'), validate=True)
        except (binascii.Error, ValueError):
            raise ValueError('Invalid base64 salt in server-first-message')

        iterations = params['i']
        if not iterations.isascii() or not iterations.isdigit():
            raise ValueError('Invalid iteration count in server-first-message')
        iterations = int(iterations)
        if iterations <= 0 or iterations > MAX_ITERATIONS:
            raise ValueError('Iteration count out of range in server-first-message')

        # Compute everything into locals first; only commit to instance
        # state once the challenge has been fully validated, so that a
        # rejected challenge cannot leave a half-updated auth_message
        # or leak a proof derived from the password.
        auth_message = b','.join([
            self.auth_message,
            server_first_message,
            b'c=biws,r=' + server_nonce,
        ])
        salted_password = hashlib.pbkdf2_hmac(
            self.hashname, self.password, salt, iterations
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

    def hmac(self, key, msg):
        return hmac.new(key, msg, digestmod=self.hashfunc).digest()

    def create_salted_password(self, salt, iterations):
        self.salted_password = hashlib.pbkdf2_hmac(
            self.hashname, self.password, salt, iterations
        )

    def final_message(self):
        if self.client_proof is None:
            raise ValueError('Cannot create client-final-message before a valid server-first-message')
        return b'c=biws,r=' + self.nonce + b',p=' + base64.b64encode(self.client_proof)

    def process_server_final_message(self, server_final_message):
        if self.server_signature is None:
            raise ValueError('Cannot process server-final-message before server-first-message')
        try:
            decoded = server_final_message.decode('utf-8')
        except UnicodeDecodeError:
            raise ValueError('Invalid server-final-message: not valid UTF-8')
        params = parse_scram_attributes(decoded)
        if 'e' in params:
            raise ValueError('Server returned an error during SCRAM authentication: %s' % params['e'])
        if 'v' not in params:
            raise ValueError('Missing verifier in server-final-message')
        try:
            verifier = base64.b64decode(params['v'].encode('utf-8'), validate=True)
        except (binascii.Error, ValueError):
            raise ValueError('Invalid base64 verifier in server-final-message')
        if not hmac.compare_digest(self.server_signature, verifier):
            raise ValueError('Server signature verification failed')
