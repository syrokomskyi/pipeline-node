/*
<MODULE_CONTRACT>
<purpose>Creates and manages the context for a node-based pipeline, facilitating logging and artifact handling.</purpose>
<non-goals>
  <item>Does not execute pipeline steps or manage step logic.</item>
  <item>Does not implement FS utilities, path resolution, or AI logging directly (see node-fs-utils.ts, node-pipeline-paths.ts, pipeline-ai-logger-factory.ts).</item>
</non-goals>
</MODULE_CONTRACT>
<CHANGE_SUMMARY>
  <item>Initial implementation of the node pipeline context creation function.</item>
  <item>Consolidated context creation helpers from create-node-pipeline-fs.ts, create-node-pipeline-paths.ts, and create-pipeline-ai-logger.ts into a single file.</item>
  <item>Remove unused exports listFiles and getImageMimeTypeByFileName that have no consumers.</item>
  <item>Expose run namespace metadata on every node pipeline context.</item>
  <item>Add toDisplayPath utility for resolving paths relative to process.cwd() with forward-slash normalization.</item>
  <item>Extract FS utilities into node-fs-utils.ts, path logic into node-pipeline-paths.ts, and AI logger into pipeline-ai-logger-factory.ts.</item>
</CHANGE_SUMMARY>
*/

import fs from "node:fs/promises";
import matter from "gray-matter";
import { randomUUID } from "node:crypto";

import {
  assertArtifactValid,
  readArtifactBuffer,
  readArtifactJson,
  readArtifactText,
} from "./artifact-io.js";
import { digestDirectory, digestFile, digestValue } from "./artifact-fingerprint.js";
import { readArtifactManifest, writeArtifactManifest } from "./artifact-manifest.js";
import {
  ensureOutputDir,
  fileExists,
  readJsonFile,
  readTextFile,
  writeTextFile,
  writeJsonFile,
} from "./node-fs-utils.js";
import { createNodePipelinePaths } from "./node-pipeline-paths.js";
import { createPipelineAiLogger } from "./pipeline-ai-logger-factory.js";
import type {
  CreateNodePipelineContextOptions,
  NodePipelineContext,
} from "./node-pipeline-types.js";

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
  const outputTransactions = new Map<string, string>();

  const resolveFingerprint = async <TStepContext extends import("@syrokomskyi/pipeline-core").PipelineStepContext<TState>>(
    stepId: string,
    fingerprint: import("@syrokomskyi/pipeline-core").PipelineFingerprintContract<TStepContext>,
  ): Promise<import("@syrokomskyi/pipeline-core").PipelineFingerprintResolution> => {
    const fingerprintContext = ctx as unknown as TStepContext;
    const implementationInputs = await fingerprint.implementationInputs(fingerprintContext);
    const operationInputs = await fingerprint.operationInputs(fingerprintContext);
    if (implementationInputs.length === 0) throw new Error(`Step ${stepId} has no declared implementation inputs`);
    if (operationInputs.length === 0) throw new Error(`Step ${stepId} has no declared operation inputs`);
    const resolveInputs = async (inputs: readonly import("@syrokomskyi/pipeline-core").PipelineFingerprintInput[]) => Promise.all(inputs.map(async (input) => {
      if (input.kind === "file") return { id: input.id, kind: input.kind, ...await digestFile(input.path) };
      if (input.kind === "directory") return { id: input.id, kind: input.kind, ...await digestDirectory(input.path) };
      if (input.kind === "upstream_artifact") {
        const artifactPath = paths.getStepArtifactPath(input.stepId, input.artifactId);
        const stat = await fs.lstat(artifactPath);
        const digest = stat.isDirectory() ? await digestDirectory(artifactPath) : await digestFile(artifactPath);
        return { id: `${input.stepId}:${input.artifactId}`, kind: input.kind, ...digest };
      }
      return { id: input.id, kind: input.kind, ...digestValue(input.kind === "value" ? input.value : input.version) };
    }));
    const implementationParts = await resolveInputs(implementationInputs);
    const operationParts = await resolveInputs(operationInputs);
    const byId = [...implementationParts, ...operationParts].map((part) => part.id);
    if (new Set(byId).size !== byId.length) throw new Error(`Step ${stepId} has duplicate fingerprint input ids`);
    const upstream = operationInputs.flatMap((input, index) => input.kind === "upstream_artifact" ? [{ stepId: input.stepId, artifactId: input.artifactId, sha256: operationParts[index]!.sha256 }] : []);
    const implementationFingerprint = digestValue([...implementationParts].sort((a, b) => a.id.localeCompare(b.id))).sha256;
    const operationFingerprint = digestValue([...operationParts].sort((a, b) => a.id.localeCompare(b.id))).sha256;
    const dependencyFingerprint = digestValue({ stepId, executionSemantics: fingerprint.executionSemantics, implementationFingerprint, operationFingerprint }).sha256;
    return { dependencyFingerprint, implementationFingerprint, operationFingerprint, upstream };
  };

  const readCompletionProof = async (stepId: string, artifactId: string): Promise<Record<string, unknown>> => {
    const source = await fs.readFile(paths.getStepArtifactPath(stepId, artifactId), "utf8");
    try {
      const parsed: unknown = JSON.parse(source);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed as Record<string, unknown>;
    } catch {
      // Human decisions commonly use Markdown with YAML frontmatter.
    }
    return matter(source).data as Record<string, unknown>;
  };

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
    isStepReusable: async ({ stepId, artifacts, fingerprint }) => {
      const manifest = await readArtifactManifest(paths.getStepOutputDir(stepId)).catch(() => null);
      const resolution = await resolveFingerprint(stepId, fingerprint);
      if (!manifest || manifest.stepId !== stepId || manifest.executionSemantics !== fingerprint.executionSemantics || manifest.dependencyFingerprint !== resolution.dependencyFingerprint || manifest.implementationFingerprint !== resolution.implementationFingerprint || manifest.operationFingerprint !== resolution.operationFingerprint) return false;
      for (const artifactId of artifacts) {
        const spec = options.stepArtifactsById.get(stepId)?.[artifactId];
        const recorded = manifest.outputs.find((output) => output.artifactId === artifactId);
        if (!spec || !recorded) return false;
        const artifactPath = paths.getStepArtifactPath(stepId, artifactId);
        const stat = await fs.lstat(artifactPath).catch(() => null);
        if (!stat) return false;
        const digest = stat.isDirectory() ? await digestDirectory(artifactPath) : await digestFile(artifactPath);
        if (digest.sha256 !== recorded.sha256 || digest.bytes !== recorded.bytes) return false;
      }
      return true;
    },
    recordStepCompletion: async ({ stepId, artifacts, fingerprint }) => {
      const resolution = await resolveFingerprint(stepId, fingerprint);
      const outputs = await Promise.all(artifacts.map(async (artifactId) => {
        const spec = options.stepArtifactsById.get(stepId)?.[artifactId];
        if (!spec) throw new Error(`Unknown output artifact ${stepId}:${artifactId}`);
        const artifactPath = paths.getStepArtifactPath(stepId, artifactId);
        const stat = await fs.lstat(artifactPath);
        const digest = stat.isDirectory() ? await digestDirectory(artifactPath) : await digestFile(artifactPath);
        return { artifactId, kind: spec.kind === "dir" ? "directory" as const : "file" as const, ...digest };
      }));
      let completion: import("./artifact-manifest.js").ArtifactManifest["completion"] = { status: "complete" };
      if (fingerprint.executionSemantics === "human_gate") {
        if (fingerprint.completion?.kind !== "human_decision") throw new Error(`Human gate ${stepId} must declare a decision artifact`);
        const proof = await readCompletionProof(stepId, fingerprint.completion.artifactId);
        if (proof.schema !== "pipeline-human-decision@1" || proof.reviewedFingerprint !== resolution.dependencyFingerprint || typeof proof.decision !== "string" || proof.decision.length === 0) throw new Error(`Human decision ${stepId} does not review the current dependency fingerprint`);
        completion = { status: "human_accepted", decisionArtifactId: fingerprint.completion.artifactId, reviewedFingerprint: resolution.dependencyFingerprint };
      } else if (fingerprint.executionSemantics === "external_effect") {
        if (fingerprint.completion?.kind !== "external_receipt") throw new Error(`External effect ${stepId} must declare a receipt artifact`);
        const proof = await readCompletionProof(stepId, fingerprint.completion.artifactId);
        if (proof.schema !== "pipeline-external-effect-receipt@1" || proof.idempotencyKey !== resolution.operationFingerprint || typeof proof.externalId !== "string" || proof.externalId.length === 0) throw new Error(`External receipt ${stepId} has no matching idempotency key and external identifier`);
        completion = { status: "external_effect_complete", receiptArtifactId: fingerprint.completion.artifactId, idempotencyKey: resolution.operationFingerprint };
      }
      await writeArtifactManifest(paths.getStepOutputDir(stepId), {
        schema: "pipeline-artifact-manifest@1",
        pipelineId: options.outputDir,
        stepId,
        executionSemantics: fingerprint.executionSemantics,
        ...resolution,
        outputs,
        completion,
      });
    },
    resolveStepFingerprint: async ({ stepId, fingerprint }) => resolveFingerprint(stepId, fingerprint),
    beginStepOutputTransaction: async (stepId: string) => {
      if (outputTransactions.has(stepId)) throw new Error(`Step output transaction already active: ${stepId}`);
      const canonical = paths.getCanonicalStepOutputDir(stepId);
      const staging = `${canonical}.staging-${randomUUID()}`;
      await fs.mkdir(staging, { recursive: true });
      outputTransactions.set(stepId, staging);
      paths.setStepOutputOverride(stepId, staging);
    },
    commitStepOutputTransaction: async (stepId: string) => {
      const staging = outputTransactions.get(stepId);
      if (!staging) throw new Error(`No step output transaction active: ${stepId}`);
      const canonical = paths.getCanonicalStepOutputDir(stepId);
      const previous = `${canonical}.previous`;
      await fs.rm(previous, { recursive: true, force: true });
      const canonicalExists = await fileExists(canonical);
      if (canonicalExists) await fs.rename(canonical, previous);
      try {
        await fs.rename(staging, canonical);
      } catch (error) {
        if (canonicalExists) await fs.rename(previous, canonical);
        throw error;
      }
      paths.clearStepOutputOverride(stepId);
      outputTransactions.delete(stepId);
    },
    abortStepOutputTransaction: async (stepId: string) => {
      const staging = outputTransactions.get(stepId);
      if (!staging) return;
      paths.clearStepOutputOverride(stepId);
      outputTransactions.delete(stepId);
      const diagnostic = `${paths.getCanonicalStepOutputDir(stepId)}.incomplete-${randomUUID()}`;
      await fs.rename(staging, diagnostic).catch(() => undefined);
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

  const ctx = baseContext as unknown as NodePipelineContext<TState, TServices> & TExtra;
  const extension = options.extendContext?.(baseContext) ?? ({} as TExtra);
  Object.defineProperties(ctx, Object.getOwnPropertyDescriptors(extension));

  return ctx;
};

// Re-export utilities for external consumers
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
  toDisplayPath,
} from "./node-fs-utils.js";
export { createNodePipelinePaths } from "./node-pipeline-paths.js";
export { createPipelineAiLogger } from "./pipeline-ai-logger-factory.js";
