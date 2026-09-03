from security import (
    generate_key_pair,
    sign_message,
    verify_signature
)


private_key, public_key = generate_key_pair()


message = "TASK|12345|hello"


signature = sign_message(
    private_key,
    message
)


print("Message:")
print(message)

print("\nSignature generated:")
print(signature.hex())


valid = verify_signature(
    public_key,
    message,
    signature
)


print("\nSignature valid:", valid)


# Test tampering

tampered_message = "TASK|12345|HACKED"


tampered_valid = verify_signature(
    public_key,
    tampered_message,
    signature
)


print(
    "Tampered message valid:",
    tampered_valid
)