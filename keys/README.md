Ed25519 keys for signing Scout outputs.

Generate with: `python -m sweep_scout.fingerprint --generate-keypair ./keys`

- `private.pem` — NEVER commit, NEVER share, NEVER paste into chat/issues.
- `public.pem` — safe to share; copy into Intel's trust_store.json to enable verification.

Permissions on private.pem should be 0600 (set automatically by --generate-keypair on POSIX).
