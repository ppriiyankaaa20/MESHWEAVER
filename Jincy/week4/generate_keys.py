from security import (
    generate_key_pair,
    save_private_key,
    save_public_key
)


for port in [8001, 8002, 8003]:

    private_key, public_key = generate_key_pair()

    save_private_key(
        private_key,
        f"certs/node{port}_private.pem"
    )

    save_public_key(
        public_key,
        f"certs/node{port}_public.pem"
    )

    print(
        f"Generated keys for node {port}"
    )