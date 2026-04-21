## Key distribution

- [ ] **Automated key distribution / mini-PKI**
      Current state (Step 5, manual with helper script): each repo has its
      own trust_store.json, operators manually copy public keys between
      repos and run `scripts/trust_store_entry.py` to format them. Works for
      single-operator deployments.

      When to revisit: when there are multiple independent operators running
      Relief on their own machines, or when key rotation frequency makes
      manual distribution impractical. At that point the options widen to:
      root-signed key manifests, HTTPS-fetched trust updates with
      timestamp-based freshness checks, or a peer-to-peer gossip approach.
      Each has real tradeoffs. Do not build until the use case exists.
