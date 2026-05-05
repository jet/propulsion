# Copilot Instructions

Propulsion is a suite of .NET/F# NuGet packages for reactive event-processing pipelines.

## Build & test

```bash
dotnet build Propulsion.sln --configuration Release
dotnet test  Propulsion.sln --no-restore --verbosity minimal
```

## Key conventions

- All library code is **F#**; follow existing style in each file.
- Keep public API changes minimal and backward-compatible where possible.
- Do not introduce new NuGet dependencies without a strong reason.
- Each package lives in its own subdirectory under `src/`.
