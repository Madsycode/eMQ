# Empy project instructions

Read `docs/PROJECT_CONTEXT.md`, `docs/PROTOCOL_DECISIONS.md` and the relevant
feature in `docs/IMPLEMENTATION_PLAN.md` before extending the framework.

Preserve the library dependency chain: MQ <- Client. The broker and client
implementations are executables. The generic runtime in `eMQ/Core` (ECS, events,
workers, modules, configuration) stays free of protocol/network dependencies.
Follow the existing four-space indentation, Allman braces, PascalCase
methods/types, `m_` members and `EMPY_*` / `MMQP_*` macros/constants.

The PDF is a scientific design with unresolved interoperability details. Record
assumptions; do not claim unimplemented security, durability, cluster consensus
or transport guarantees. Update feature status and verified commands in the
project context after changes. Keep upstream vendor sources and their licenses.

The workspace may contain a user-led rename from `EMQ_*` to `EMPY_*`; preserve
unrelated edits and never restore deleted legacy paths without instruction.
