# Agent Guidelines for Propulsion

This repository contains the **Propulsion** library — a suite of .NET NuGet packages for building reactive event-processing pipelines on top of event stores such as EventStoreDB, MessageDB, and the [Equinox](https://github.com/jet/equinox) stores (CosmosStore, DynamoStore, MemoryStore).

See [`.github/copilot-instructions.md`](.github/copilot-instructions.md) for Copilot-specific instructions.

## Repository layout

| Path | Purpose |
|---|---|
| `src/` | Library source — one directory per NuGet package |
| `tests/` | Test projects |
| `tools/` | Helper tooling |
| `DOCUMENTATION.md` | In-depth documentation |
| `CHANGELOG.md` | Release history |

## Build & test

```bash
# Build
dotnet build Propulsion.sln --configuration Release

# Test (requires a local Postgres instance for MessageDb tests)
dotnet test --solution Propulsion.sln --no-restore --verbosity minimal
```

The CI workflow (`.github/workflows/ci.yaml`) spins up Postgres and installs `message-db` automatically; for local runs without Postgres the MessageDb-related tests will be skipped or fail.

## Language & style

- All library code is written in **F#**.
- Follow the conventions already present in each file — no tabs, 4-space indent.
- Do not add external dependencies unless absolutely necessary.
- Keep public API surface minimal and consistent with existing packages.
