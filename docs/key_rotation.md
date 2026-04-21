# Key rotation

Scout signs `domain_fingerprints.json` with a private key. The corresponding public key must be listed in each downstream verifier's trust store. Rotation is manual today; automation is a future concern (see `docs/KEY_DISTRIBUTION.md` in Sweeps_Intel for the spec, or TODO.md for the near-term bullet).

## When to rotate

- **Suspected compromise.** Any reason to believe the private key leaked: machine compromise, accidental commit, unexpected copy. Rotate immediately.
- **Scheduled rotation.** Good practice annually even without incident. Put it on a calendar.
- **Personnel change.** If the operator who generated the key hands off the project.

## Rotation procedure

1. **Generate the new keypair** with a new version suffix:

   ```bash
   python -m sweep_scout.fingerprint --generate-keypair ./keys-new
   ```

   Write to a new directory first; don't overwrite `keys/` until the new key is confirmed working end-to-end.

2. **Produce the trust-store entry** for downstream verifiers:

   ```bash
   python scripts/trust_store_entry.py \
     --pem keys-new/public.pem \
     --key-id scout-fingerprint-key-v2 \
     --authorized-for domain_fingerprints
   ```

   Copy the stdout output into Sweeps_Intel's `trust_store.json` as a new entry in the `keys` array. Do NOT delete the `-v1` entry — both keys remain trusted during the transition.

3. **Commit the trust store update in Intel** with a clear message:

   ```bash
   cd ~/Sweeps_Intel
   # Edit trust_store.json, paste the new entry
   git add trust_store.json
   git commit -m "trust: add scout-fingerprint-key-v2 during rotation"
   git push
   ```

4. **Switch Scout to the new key:** in the signing workflow, use `--key-id scout-fingerprint-key-v2` and `--private-key keys-new/private.pem`. Verify that signed output validates in Intel before proceeding.

5. **Revoke the old key** after a grace period (minimum 30 days, longer if older signed artifacts are still in use). Edit Intel's trust_store.json:

   ```json
   {
     "key_id": "scout-fingerprint-key-v1",
     "revoked_at": "2026-05-21T00:00:00Z",
     "revocation_reason": "scheduled rotation to v2"
   }
   ```

   Commit this change separately from the key addition. Revocations should be audit-visible.

6. **Move the new key into place.** Once the old key is revoked:

   ```bash
   mv keys keys-archived-v1
   mv keys-new keys
   ```

   Keep archived keys around — they're useful for forensic verification of old signed artifacts.

## What NOT to do

- **Do not delete old key entries from trust stores.** Revocation preserves audit history; deletion erases it. Always set `revoked_at` and `revocation_reason`.
- **Do not reuse a `key_id`** across different keypairs. Increment the version suffix.
- **Do not commit private keys.** Double-check `git status` before pushing during rotation when multiple key directories exist.
- **Do not skip the grace period.** Revoking the old key before downstream verifiers have the new key breaks the pipeline mid-rotation.
