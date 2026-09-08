# MeshWeaver Week 4

Week 4 adds a secure task path on top of the earlier discovery work:

- TLS 1.2+ with mutual certificate authentication.
- Ed25519 signatures over the task id, sender, and serialized payload.
- Certificate identity must match the signed sender identity.
- Rich live topology and task-state dashboard primitives.
- Async task execution with explicit `running`, `completed`, and `failed` states.

## Setup

From this directory, install the dependencies and generate local development material:

```powershell
python -m pip install -r ../../requirements.txt
python generate_tls_certs.py
python generate_keys.py
```

The generators create certificates and signing keys for nodes `8001` through `8010`.
Do not use the generated development private keys in production.

## Verification

```powershell
python -m pytest -q test_secure_mesh.py
```

`SecureNode` and `submit_task` in `secure_mesh.py` are the reusable entry points. `dashboard.py` exposes `dashboard_loop(nodes)` for a runner that owns a set of live nodes.

## Live CLI

```powershell
python secure_cli.py --nodes 3
```

The dashboard remains active until `Ctrl+C`. Use `--nodes 10` after generating the full ten-node certificate and signing-key set.