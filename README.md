# @warpgogol/pipeline-node

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE) [![npm](https://img.shields.io/npm/v/@warpgogol/pipeline-node?logo=npm&logoColor=white)](https://www.npmjs.com/package/@warpgogol/pipeline-node)

Node.js runtime for the pipeline framework — filesystem I/O, declaration loading, prompt/template helpers, CLI entry factory, and artifact management.

> Engineered at [Warpgogol](https://warpgogol.com) · Released as open source.

---

## Features

- **Node.js pipeline context** — `createNodePipelineContext()` with manifest-backed artifact reuse
- **Declaration loading** — load phases and gogols from Markdown declaration files
- **Artifact I/O** — read/write artifacts with fingerprinting and validation
- **Frontmatter parsing** — `gray-matter`-based frontmatter extraction
- **Prompt files** — read and validate prompt templates from disk
- **Handlebars templates** — render template-driven artifacts
- **CLI entry factory** — `createMainEntry()` with dotenv, arg parsing, and error handling
- **Environment helpers** — `getRequiredEnv()` with clear error messages
- **LLM artifact helpers** — persist AI call logs, responses, and usage metadata
- **Webhook event bridge** — `createWebhookEventBridge()` for pipeline event forwarding

## Install

```bash
npm install @warpgogol/pipeline-core @warpgogol/pipeline-ai @warpgogol/pipeline-node

# Optional peer deps (only if you use browser/image features)
npm install playwright sharp
```

### Peer dependencies

| Package      | Required for                | Optional |
| ------------ | --------------------------- | -------- |
| `playwright` | Browser automation features | Yes      |
| `sharp`      | Image processing features   | Yes      |

## Quick start

```ts
import { createMainEntry } from "@warpgogol/pipeline-node/cli";
import { createNodePipelineContext } from "@warpgogol/pipeline-node/context";
import { definePipeline } from "@warpgogol/pipeline-core";

// 1. Create a CLI entry point
createMainEntry({
  runApp: async (options) => {
    const ctx = await createNodePipelineContext({
      workspaceRootDir: process.cwd(),
      outputDir: ".output",
      // ... your context inputs
    });

    const pipeline = definePipeline({
      id: "my-pipeline",
      phases: [/* ... */],
    });

    await runPipelineEngine(pipeline, ctx);
  },
});
```

## Exports

| Export | Description |
| --- | --- |
| `createNodePipelineContext(opts)` | Create a Node.js pipeline context with artifact I/O |
| `createMainEntry(opts)` | CLI entry-point factory with dotenv + error handling |
| `parseRunOptions(args)` | CLI argument parser (`--dry-run`, `--from`, `--to`, `--only`, `--refresh`) |
| `createGogolRegistry()` | Registry for gogol factories |
| `createPhaseRegistry()` | Registry for phase definitions |
| `loadDeclarations(dir)` | Load pipeline declarations from Markdown files |
| `getRequiredEnv(name)` | Read required env var with clear error |
| `readPromptFiles(dir)` | Read and validate prompt files |
| `createHandlebarsTemplateRenderer(opts)` | Render Handlebars templates |
| `writeGogolGuideArtifacts(...)` | Write guide artifacts for a gogol |
| `appendJsonLine(path, entry)` | Append a JSON line to a log file |
| `createWebhookEventBridge(url)` | Forward pipeline events to a webhook |

### Subpath exports

| Path                                        | Description                        |
| ------------------------------------------- | ---------------------------------- |
| `@warpgogol/pipeline-node/context`          | Context creation                   |
| `@warpgogol/pipeline-node/engine`           | Pipeline engine factory            |
| `@warpgogol/pipeline-node/declarations`     | Declaration loading and registries |
| `@warpgogol/pipeline-node/cli`              | CLI entry-point and arg parsing    |
| `@warpgogol/pipeline-node/env`              | Environment helpers                |
| `@warpgogol/pipeline-node/frontmatter`      | Frontmatter parsing                |
| `@warpgogol/pipeline-node/documentation`    | Pipeline documentation helpers     |
| `@warpgogol/pipeline-node/types`            | Node-specific pipeline types       |
| `@warpgogol/pipeline-node/prompts`          | Prompt file helpers                |
| `@warpgogol/pipeline-node/input-validation` | Input validation helpers           |
| `@warpgogol/pipeline-node/templates`        | Handlebars template rendering      |
| `@warpgogol/pipeline-node/llm-artifacts`    | LLM artifact helpers               |
| `@warpgogol/pipeline-node/paths`            | App path helpers                   |

## Changelog

[CHANGELOG.md](CHANGELOG.md)

## License

Apache-2.0 — see [LICENSE](LICENSE)

## Open Engineering

This package originated from production engineering work at [Warpgogol](https://warpgogol.com), an engineering studio in Germany.

We publish reusable parts of our infrastructure when they can be useful beyond our own projects. It is published independently of any Warpgogol commercial service. Using this package does not create any dependency on Warpgogol.

Built for real systems. Shared openly.
