from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives import serialization
from pathlib import Path


# ==========================================
# Key Management
# ==========================================

def generate_key_pair():

    private_key = Ed25519PrivateKey.generate()

    public_key = private_key.public_key()

    return private_key, public_key


# ==========================================
# Save Private Key
# ==========================================

def save_private_key(private_key, path):

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    data = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    path.write_bytes(data)


# ==========================================
# Save Public Key
# ==========================================

def save_public_key(public_key, path):

    path = Path(path)

    path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    data = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    path.write_bytes(data)


# ==========================================
# Load Private Key
# ==========================================

def load_private_key(path):

    data = Path(path).read_bytes()

    return serialization.load_pem_private_key(
        data,
        password=None
    )


# ==========================================
# Load Public Key
# ==========================================

def load_public_key(path):

    data = Path(path).read_bytes()

    return serialization.load_pem_public_key(
        data
    )


# ==========================================
# Sign Message
# ==========================================

def sign_message(private_key, message):

    if isinstance(message, str):

        message = message.encode()

    return private_key.sign(message)


# ==========================================
# Verify Signature
# ==========================================

def verify_signature(
    public_key,
    message,
    signature
):

    if isinstance(message, str):

        message = message.encode()

    try:

        public_key.verify(
            signature,
            message
        )

        return True

    except Exception:

        return False