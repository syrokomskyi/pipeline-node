/*
<MODULE_CONTRACT>
<purpose>Creates and manages the context for a node-based pipeline, facilitating logging and artifact handling.</purpose>
<non-goals>
  <item>Does not execute pipeline steps or manage step logic.</item>
</non-goals>
</MODULE_CONTRACT>
<CHANGE_SUMMARY>
  <item>Initial implementation of the node pipeline context creation function.</item>
  <item>Consolidated context creation helpers from create-node-pipeline-fs.ts, create-node-pipeline-paths.ts, and create-pipeline-ai-logger.ts into a single file.</item>
  <item>Remove unused exports listFiles and getImageMimeTypeByFileName that have no consumers.</item>
  <item>Expose run namespace metadata on every node pipeline context.</item>
  <item>Add toDisplayPath utility for resolving paths relative to process.cwd() with forward-slash normalization.</item>
</CHANGE_SUMMARY>
*/

import fs from "node:fs/promises";
import path from "node:path";

import sharp from "sharp";
import type {
  PipelineAiLogOptions,
  PipelineArtifacts,
  PipelineStepContext,
  TokenUsage,
} from "@syrokomskyi/pipeline-core";

import { stringifyJsonOutput } from "./json-output.js";
import {
  assertArtifactValid,
  readArtifactBuffer,
  readArtifactJson,
  readArtifactText,
} from "./artifact-io.js";
import type {
  CreateNodePipelineContextOptions,
  NodePipelineContext,
} from "./node-pipeline-types.js";

// File system utilities (from create-node-pipeline-fs.ts)
const textDecoder = new TextDecoder("utf-8");

const ensureOutputDir = async (dirPath: string): Promise<void> => {
  await fs.mkdir(dirPath, { recursive: true });
};

const fileExists = async (filePath: string): Promise<boolean> => {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
};

const readTextFile = async (filePath: string): Promise<string> => {
  const buffer = await fs.readFile(filePath);
  return textDecoder.decode(buffer);
};

const readBinaryFile = async (filePath: string): Promise<Uint8Array> => {
  const buffer = await fs.readFile(filePath);
  return new Uint8Array(buffer);
};

const listFiles = async (dirPath: string): Promise<string[]> => {
  const items = await fs.readdir(dirPath, { withFileTypes: true });
  return items
    .filter((item) => item.isFile())
    .map((item) => item.name)
    .filter((name) => !path.basename(name).startsWith("-"));
};

const getImageMimeTypeByFileName = (fileName: string): string => {
  const ext = path.extname(fileName).toLowerCase();
  switch (ext) {
    case ".png":
      return "image/png";
    case ".jpg":
    case ".jpeg":
      return "image/jpeg";
    case ".webp":
      return "image/webp";
    default:
      throw new Error(`Unsupported image extension: ${ext}`);
  }
};

const readJsonFile = async (filePath: string): Promise<unknown> => {
  const text = await readTextFile(filePath);
  return JSON.parse(text);
};

const writeTextFile = async (filePath: string, content: string): Promise<void> => {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${content}\n`, "utf-8");
};

const writeBinaryFile = async (filePath: string, content: Uint8Array): Promise<void> => {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, content);
};

const writeJsonFile = async (filePath: string, value: unknown): Promise<void> => {
  await writeTextFile(filePath, stringifyJsonOutput(value));
};

const toDisplayPath = (filePath: string): string => {
  const relativePath = path.relative(process.cwd(), filePath);
  if (!relativePath || relativePath.startsWith("..") || path.isAbsolute(relativePath)) {
    return filePath.replaceAll("\\", "/");
  }

  return relativePath.replaceAll("\\", "/");
};

// Path utilities (from create-node-pipeline-paths.ts)
const createNodePipelinePaths = <TContext extends PipelineStepContext>(options: {
  outputDir: string;
  stepArtifactsById: Map<string, PipelineArtifacts<TContext>>;
  stepNumbers: Map<string, number>;
}) => {
  const getStepNumber = (stepId: string): number => {
    const number = options.stepNumbers.get(stepId);
    if (!number) {
      throw new Error(`Unknown pipeline step id: ${stepId}`);
    }
    return number;
  };

  const getStepOutputDir = (stepId: string): string => {
    return path.join(options.outputDir, `${getStepNumber(stepId)}-${stepId}`);
  };

  const getOutputPath = (stepId: string, baseFileName: string): string => {
    return path.join(getStepOutputDir(stepId), baseFileName);
  };

  const getStepArtifactPath = (stepId: string, artifactId: string): string => {
    const artifacts = options.stepArtifactsById.get(stepId);
    if (!artifacts) {
      throw new Error(`Unknown pipeline step id: ${stepId}`);
    }

    const artifact = artifacts[artifactId];
    if (!artifact) {
      throw new Error(`Unknown artifact id: ${artifactId} for step ${stepId}`);
    }

    return path.join(getStepOutputDir(stepId), artifact.relativePath);
  };

  return {
    getStepNumber,
    getStepOutputDir,
    getOutputPath,
    getStepArtifactPath,
  };
};

// AI logger utilities (from create-pipeline-ai-logger.ts)
const formatMarkdownValue = (value: unknown): string => {
  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(value, null, 2);
};

const getResponseFileName = (
  response: {
    fileName?: string;
  },
  index: number,
): string => {
  return response.fileName ?? `response-${index + 1}.md`;
};

const createPipelineAiLogger = (options: {
  getCurrentStepId: () => string | null;
  getStepOutputDir: (stepId: string) => string;
  ensureOutputDir: (dirPath: string) => Promise<void>;
  writeTextFile: (filePath: string, content: string) => Promise<void>;
}) => {
  const aiCallCountersByStepId = new Map<string, number>();

  const writeResponses = async (
    callDir: string,
    logOptions: PipelineAiLogOptions,
  ): Promise<void> => {
    for (const [index, response] of (logOptions.responses ?? []).entries()) {
      await options.writeTextFile(
        path.join(callDir, getResponseFileName(response, index)),
        response.content,
      );
    }
  };

  return {
    logAiCall: async (logOptions: PipelineAiLogOptions): Promise<string | null> => {
      const stepId = options.getCurrentStepId();
      if (!stepId) {
        return null;
      }

      const next = (aiCallCountersByStepId.get(stepId) ?? 0) + 1;
      aiCallCountersByStepId.set(stepId, next);

      const callDir = path.join(options.getStepOutputDir(stepId), "AI", `ai-${next}`);
      await options.ensureOutputDir(callDir);

      if (typeof logOptions.system === "string") {
        await options.writeTextFile(path.join(callDir, "system.md"), logOptions.system);
      }

      if (logOptions.llm) {
        const llmLines = [
          "# LLM",
          "",
          `- provider: ${logOptions.llm.provider}`,
          `- model: ${logOptions.llm.model}`,
          `- version: ${logOptions.llm.version ?? logOptions.llm.model}`,
        ];

        for (const [key, value] of Object.entries(logOptions.llm.parameters ?? {})) {
          llmLines.push(`- ${key}:`);
          llmLines.push("```json");
          llmLines.push(formatMarkdownValue(value));
          llmLines.push("```");
        }

        await options.writeTextFile(path.join(callDir, "llm.md"), llmLines.join("\n"));
      }

      for (const [index, userPrompt] of logOptions.userPrompts.entries()) {
        await options.writeTextFile(path.join(callDir, `user-${index + 1}.md`), userPrompt);
      }

      await writeResponses(callDir, logOptions);

      for (const [index, image] of (logOptions.images ?? []).entries()) {
        try {
          const webp = await sharp(image).webp({ quality: 100 }).toBuffer();
          await fs.writeFile(path.join(callDir, `image-${index + 1}.webp`), webp);
        } catch (error) {
          console.error("Failed to convert image to webp for ai call logging:", error);
        }
      }

      for (const [index, data] of (logOptions.data ?? []).entries()) {
        await fs.writeFile(
          path.join(callDir, `data-${index + 1}.${data.extension.replace(/^\.+/, "")}`),
          data.buffer,
        );
      }

      return callDir;
    },
    writeAiResponses: async (
      callDir: string | null,
      responses: PipelineAiLogOptions["responses"],
    ): Promise<void> => {
      if (!callDir || !responses?.length) {
        return;
      }

      await writeResponses(callDir, {
        userPrompts: [],
        responses,
      });
    },
    writeAiUsage: async (callDir: string | null, usage: TokenUsage): Promise<void> => {
      if (!callDir) {
        return;
      }
      await options.writeTextFile(path.join(callDir, "usage.json"), JSON.stringify(usage, null, 2));
    },
  };
};

export const createNodePipelineContext = <
  TState,
  TServices,
  TExtra extends object = Record<string, never>,
>(
  options: CreateNodePipelineContextOptions<TState, TServices, TExtra>,
): NodePipelineContext<TState, TServices> & TExtra => {
  let currentStepId: string | null = null;
  const paths = createNodePipelinePaths({
    outputDir: options.outputDir,
    stepArtifactsById: options.stepArtifactsById,
    stepNumbers: options.stepNumbers,
  });
  const runNamespace = options.runNamespace ?? {
    outputRootDir: options.outputDir,
    lockedInputs: {},
    reuseSource: "local_artifacts" as const,
  };

  const aiLogger = createPipelineAiLogger({
    getCurrentStepId: () => currentStepId,
    getStepOutputDir: paths.getStepOutputDir,
    ensureOutputDir,
    writeTextFile,
  });

  const logStepEvent: NodePipelineContext<TState, TServices>["logStepEvent"] = async (event) => {
    const stepId = event.stepId ?? currentStepId;
    if (!stepId) {
      return;
    }

    const stepOutputDir = paths.getStepOutputDir(stepId);
    const shouldCreateStepOutputDir = event.allowCreateStepOutputDir ?? true;

    const payload = {
      timestamp: new Date().toISOString(),
      stepId,
      stepNumber: paths.getStepNumber(stepId),
      ...event,
    };

    if (!shouldCreateStepOutputDir) {
      const outputDirExists = await fileExists(stepOutputDir);
      if (!outputDirExists) {
        return;
      }
    }

    await ensureOutputDir(stepOutputDir);
    await fs.appendFile(
      paths.getOutputPath(stepId, "log.txt"),
      `${JSON.stringify(payload)}\n`,
      "utf-8",
    );
  };

  const baseContext = {
    inputDir: options.inputDir,
    outputDir: options.outputDir,
    promptsDir: options.promptsDir,
    runNamespace,
    services: options.services,
    state: options.state,
    getPipelineOutputDir: () => options.outputDir,
    get currentStepId() {
      return currentStepId;
    },
    set currentStepId(value: string | null) {
      currentStepId = value;
    },
    ...paths,
    ensureOutputDir,
    fileExists,
    readJsonFile,
    readTextFile,
    writeTextFile,
    writeJsonFile,
    assertStepArtifactValid: async (stepId: string, artifactId: string) => {
      await assertArtifactValid({
        ctx,
        stepId,
        artifactId,
        artifactsByStepId: options.stepArtifactsById,
      });
    },
    readStepArtifactText: async (stepId: string, artifactId: string) => {
      return readArtifactText({
        ctx,
        stepId,
        artifactId,
        artifactsByStepId: options.stepArtifactsById,
      });
    },
    readStepArtifactJson: async (stepId: string, artifactId: string) => {
      return readArtifactJson({
        ctx,
        stepId,
        artifactId,
        artifactsByStepId: options.stepArtifactsById,
      });
    },
    readStepArtifactBuffer: async (stepId: string, artifactId: string) => {
      return readArtifactBuffer({
        ctx,
        stepId,
        artifactId,
        artifactsByStepId: options.stepArtifactsById,
      });
    },
    logStepEvent,
    logAiCall: aiLogger.logAiCall,
    writeAiResponses: aiLogger.writeAiResponses,
    writeAiUsage: aiLogger.writeAiUsage,
  } satisfies NodePipelineContext<TState, TServices>;

  const ctx = baseContext as NodePipelineContext<TState, TServices> & TExtra;
  const extension = options.extendContext?.(baseContext) ?? ({} as TExtra);
  Object.defineProperties(ctx, Object.getOwnPropertyDescriptors(extension));

  return ctx;
};

// Export consolidated utilities for external use
export {
  ensureOutputDir,
  fileExists,
  readTextFile,
  readBinaryFile,
  listFiles,
  getImageMimeTypeByFileName,
  readJsonFile,
  writeTextFile,
  writeBinaryFile,
  writeJsonFile,
  createNodePipelinePaths,
  createPipelineAiLogger,
  toDisplayPath,
};
