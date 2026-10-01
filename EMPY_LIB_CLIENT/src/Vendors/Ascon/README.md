# Ascon-AEAD128 reference

Unmodified files from the Ascon team's `crypto_aead/asconaead128/ref` directory,
plus `tests/crypto_aead.h` and the upstream CC0 license.

- Repository: https://github.com/ascon/ascon-c
- Commit: `446347f21b209f3921c65ece70027c366cbe1693`
- Archive SHA-256: `9418ad1afc005d37094dfed202d86ee08c26a174dfba929fb24901d72d0a5b95`
- Algorithm: NIST SP 800-232 Ascon-AEAD128, selected by the project author on
  2026-09-18. This intentionally updates the paper's ASCON-128 choice.
- Known-answer vectors: `Tests/Vectors/Ascon-AEAD128.txt`, copied unchanged from
  `crypto_aead/asconaead128/LWC_AEAD_KAT_128_128.txt` at the same commit.

The library builds only `aead.c`. State-printing is disabled. Empy's wrapper
does not release candidate plaintext before tag verification and wipes the
candidate buffer on failure. Never enable `ASCON_PRINT_STATE` in a deployment.
