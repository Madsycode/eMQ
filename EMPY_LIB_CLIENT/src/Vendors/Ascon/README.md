# Ascon-AEAD128 reference

Unmodified files from the Ascon team's `crypto_aead/asconaead128/ref` directory,
plus `tests/crypto_aead.h`, the upstream `implementors` list and the CC0 license.

- Repository: https://github.com/ascon/ascon-c
- Commit: `446347f21b209f3921c65ece70027c366cbe1693`
- Archive SHA-256: `9418ad1afc005d37094dfed202d86ee08c26a174dfba929fb24901d72d0a5b95`
- Algorithm: NIST SP 800-232 Ascon-AEAD128, selected by the project author on
  2026-09-18. This intentionally updates the paper's ASCON-128 choice; the two
  variants are not wire compatible.
- Parameters: 128-bit key, 128-bit public nonce, 128-bit tag.
- Known-answer vectors: `EMPY_TESTS/Vectors/Ascon-AEAD128.txt`, copied unchanged
  from `crypto_aead/asconaead128/LWC_AEAD_KAT_128_128.txt` at the same commit.

## Build integration

`EMPY_LIB_CLIENT` compiles only `aead.c` into the client library, with this
directory as a private include path. `printstate.c` is not built and
`ASCON_PRINT_STATE` is never defined; never enable it in a deployment, because it
prints internal state. The broker and the Core/MQ libraries do not link this code:
payload encryption belongs to clients only.

Do not edit these files. To update, replace the directory with the files of a new
upstream commit, record its commit and archive hash here, refresh the vector file
from the same commit, and rerun the `security` test suite.

## How Empy uses it

`eMQ::Ascon` in `EMPY_LIB_CLIENT/src/Security/Payload.cpp` wraps
`crypto_aead_encrypt` and `crypto_aead_decrypt`; `PayloadCipher` builds the
MMQP end-to-end envelope around them:

- **Keys:** a 16-byte leaf key per canonical topic and epoch, derived level by
  level with HKDF-SHA256 (parent key as input, big-endian epoch as salt, level
  bytes as info). A grant is either a 16/32-byte root secret or a 16-byte
  delegated subtree key bound to one epoch.
- **Associated data:** the canonical UTF-8 topic followed by the 4-byte
  big-endian epoch, with no length prefix. It is built in a fixed stack buffer.
- **Nonce:** an 8-byte random publisher prefix followed by an 8-byte big-endian
  counter. With persistent cipher state, counters are reserved in durable blocks
  (1024 by default): a restart resumes after the reserved block, so a crash may
  skip counters but never reuses one. Counter exhaustion is rejected.
- **Envelope:** epoch (4) | nonce (16) | tag (16) | ciphertext (plaintext length).
  Retransmissions reuse the original envelope.

## Safety properties of the wrapper

- Candidate plaintext is never released before the full tag is verified. On
  failure the candidate buffer is wiped and an authentication error is raised.
- Derived leaf keys are copied out of the cache for each operation and wiped when
  that operation ends; the bounded LRU key cache (128 entries by default) wipes
  evicted keys, keys outside a rotated epoch interval, and all keys on destruction.
- The Ascon calls run outside the cipher lock; key derivation, nonce reservation
  and replay-window updates stay under it. A decryption updates the replay window
  for its (topic, epoch, nonce prefix) only after the tag has been verified and
  before plaintext is returned. That window is stored as its own record when
  persistent state is enabled.
- The vendored routines themselves are unchanged reference code. Any timing or
  side-channel properties are those of the upstream reference implementation; no
  constant-time or hardware hardening is claimed beyond it.

## Tests

The `security` CTest suite checks all 1,089 known-answer cases from the vector
file, plus altered topics, epochs, tags and ciphertexts, delegated-key boundaries
and nonce exhaustion. The `recovery` suite checks persistent nonce reservation,
replay-state restart and migration of the earlier single-record cipher state.
These checks establish agreement with the selected vectors; they are not a
cryptographic certification.
