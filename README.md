# @syrokomskyi/pipeline-node

Node.js-specific pipeline environment implementation.

## Usage

Includes filesystem operations, loading declarations from Markdown, path helpers, logging, prompt/template helpers, and shared CLI entry-point utilities.

Artifact reuse is manifest-backed. `createNodePipelineContext` resolves declared
implementation, operation, and upstream inputs; verifies output digests; and records
human decisions or external receipts according to the step execution semantic.
Declared filesystem inputs must resolve inside `workspaceRootDir` (the process
working directory by default), and symlinks are rejected. Declaration-backed
registries bind the selected factory and non-secret configuration into each step's
operation fingerprint.

## Lifecycle tests

Run `pnpm test` in this package. Example-based integration tests cover filesystem,
manifest, tamper, human-decision, and receipt behavior. Property-based tests use
`fast-check` for canonical digest invariants and dependency sensitivity.

## Exports

| Subpath | Purpose |
| --- | --- |
| `.` | General Node.js pipeline utilities (artifact I/O, env, frontmatter, declarations, types, prompts, templates, fetch helpers, LLM artifacts) |
| `./context` | `createNodePipelineContext` and `ensureOutputDir` |
| `./engine` | `createPipelineEngine` factory for app-local `runPipelineEngine` |
| `./declarations` | `createGogolRegistry`, `createPhaseRegistry`, declaration loading helpers |
| `./cli` | `parseRunOptions` (CLI argument parser) and `createMainEntry` (main entry-point factory with dotenv + error handling) |
| `./env` | `getRequiredEnv` |
| `./frontmatter` | Frontmatter parsing utilities |
| `./documentation` | Pipeline documentation helpers |
| `./types` | Node-specific pipeline types |
| `./prompts` | Prompt file reading and validation |
| `./input-validation` | Input validation helpers |
| `./templates` | Handlebars template rendering |
| `./llm-artifacts` | LLM artifact helpers |

### `./cli` — shared CLI and entry-point boilerplate

Eliminates duplicated `main.ts` and `parse-run-options.ts` across pipeline apps.

```typescript
import { createMainEntry } from "@syrokomskyi/pipeline-node/cli";
import { runApp } from "./app/run-app.js";

createMainEntry({ runApp });
```

`createMainEntry` handles `dotenv.config()`, CLI argument parsing (`--dry-run`, `--from`, `--to`, `--only`, `--refresh`), `PipelinePauseError` (exit code 2), and general error formatting (exit code 1). Refresh records a nonce in the selected step fingerprint; it never bypasses validation.

`parseRunOptions` is also exported separately for apps that need it without the full entry-point wrapper (e.g. `observatory` which has a custom entry point).

## Changelog

[CHANGELOG.md](CHANGELOG.md)
