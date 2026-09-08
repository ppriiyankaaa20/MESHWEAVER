from pathlib import Path
from datetime import datetime, timedelta, timezone

from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding


CERT_DIR = Path("certs")
CERT_DIR.mkdir(exist_ok=True)


# ==========================================
# Create Certificate Authority
# ==========================================

ca_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048
)

ca_subject = x509.Name([
    x509.NameAttribute(
        NameOID.COUNTRY_NAME,
        "IN"
    ),
    x509.NameAttribute(
        NameOID.ORGANIZATION_NAME,
        "MeshWeaver"
    ),
    x509.NameAttribute(
        NameOID.COMMON_NAME,
        "MeshWeaver CA"
    ),
])


ca_cert = (
    x509.CertificateBuilder()
    .subject_name(ca_subject)
    .issuer_name(ca_subject)
    .public_key(ca_key.public_key())
    .serial_number(
        x509.random_serial_number()
    )
    .not_valid_before(
        datetime.now(timezone.utc)
    )
    .not_valid_after(
        datetime.now(timezone.utc)
        + timedelta(days=365)
    )
    .add_extension(
        x509.BasicConstraints(
            ca=True,
            path_length=None
        ),
        critical=True
    )
    .sign(
        ca_key,
        hashes.SHA256()
    )
)


# Save CA private key

(CERT_DIR / "ca.key").write_bytes(
    ca_key.private_bytes(
        Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption()
    )
)


# Save CA certificate

(CERT_DIR / "ca.crt").write_bytes(
    ca_cert.public_bytes(Encoding.PEM)
)


# ==========================================
# Create Node Certificates
# ==========================================

for port in [8001, 8002, 8003]:

    node_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )

    node_name = f"node{port}"

    subject = x509.Name([
        x509.NameAttribute(
            NameOID.COUNTRY_NAME,
            "IN"
        ),
        x509.NameAttribute(
            NameOID.ORGANIZATION_NAME,
            "MeshWeaver"
        ),
        x509.NameAttribute(
            NameOID.COMMON_NAME,
            node_name
        ),
    ])


    node_cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(node_key.public_key())
        .serial_number(
            x509.random_serial_number()
        )
        .not_valid_before(
            datetime.now(timezone.utc)
        )
        .not_valid_after(
            datetime.now(timezone.utc)
            + timedelta(days=365)
        )
        .add_extension(
            x509.SubjectAlternativeName([
                x509.DNSName("localhost"),
                x509.IPAddress(
                    __import__("ipaddress").ip_address(
                        "127.0.0.1"
                    )
                ),
            ]),
            critical=False
        )
        .sign(
            ca_key,
            hashes.SHA256()
        )
    )


    # Save node private key

    (CERT_DIR / f"{node_name}_tls.key").write_bytes(
        node_key.private_bytes(
            Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption()
        )
    )


    # Save node certificate

    (CERT_DIR / f"{node_name}_tls.crt").write_bytes(
        node_cert.public_bytes(Encoding.PEM)
    )


    print(
        f"Generated TLS certificate for {node_name}"
    )


print("\nTLS certificate generation complete.")