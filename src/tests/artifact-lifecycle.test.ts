import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import type { PipelineFingerprintContract } from "@syrokomskyi/pipeline-core";
import { afterEach, expect, test } from "vitest";

import { createNodePipelineContext } from "../index.js";

const temporaryDirectories: string[] = [];
afterEach(async () => Promise.all(temporaryDirectories.splice(0).map((directory) => fs.rm(directory, { recursive: true }))));

const createHarness = async () => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "pipeline-lifecycle-"));
  temporaryDirectories.push(root);
  const artifacts = new Map([["build", { result: { kind: "file" as const, relativePath: "result.txt" } }]]);
  const ctx = createNodePipelineContext({ inputDir: path.join(root, "input"), outputDir: path.join(root, "output"), promptsDir: path.join(root, "prompts"), stepArtifactsById: artifacts, stepNumbers: new Map([["build", 1]]), state: {}, services: {} });
  await ctx.ensureOutputDir(ctx.getStepOutputDir("build"));
  return { ctx };
};

const contract = (operation: string, completion?: PipelineFingerprintContract["completion"]): PipelineFingerprintContract => ({
  schema: "pipeline-fingerprint-contract@1",
  executionSemantics: completion?.kind === "human_decision" ? "human_gate" : completion?.kind === "external_receipt" ? "external_effect" : "pure_artifact",
  implementationInputs: async () => [{ kind: "runtime", id: "implementation", version: "1" }],
  operationInputs: async () => [{ kind: "value", id: "operation", value: operation }],
  completion,
});

test("legacy and tampered outputs are never reusable", async () => {
  const { ctx } = await createHarness();
  await ctx.writeTextFile(ctx.getStepArtifactPath("build", "result"), "original");
  await expect(ctx.isStepReusable?.({ stepId: "build", artifacts: ["result"], fingerprint: contract("v1") })).resolves.toBe(false);
  await ctx.recordStepCompletion?.({ stepId: "build", artifacts: ["result"], fingerprint: contract("v1") });
  await expect(ctx.isStepReusable?.({ stepId: "build", artifacts: ["result"], fingerprint: contract("v1") })).resolves.toBe(true);
  await ctx.writeTextFile(ctx.getStepArtifactPath("build", "result"), "tampered");
  await expect(ctx.isStepReusable?.({ stepId: "build", artifacts: ["result"], fingerprint: contract("v1") })).resolves.toBe(false);
});

test("operation changes invalidate only the declared consumer manifest", async () => {
  const { ctx } = await createHarness();
  await ctx.writeTextFile(ctx.getStepArtifactPath("build", "result"), "stable");
  await ctx.recordStepCompletion?.({ stepId: "build", artifacts: ["result"], fingerprint: contract("v1") });
  await expect(ctx.isStepReusable?.({ stepId: "build", artifacts: ["result"], fingerprint: contract("v2") })).resolves.toBe(false);
});

test("human completion must bind the decision to the reviewed fingerprint", async () => {
  const { ctx } = await createHarness();
  const fingerprint = contract("reviewed", { kind: "human_decision", artifactId: "result" });
  const resolved = await ctx.resolveStepFingerprint?.({ stepId: "build", fingerprint });
  await ctx.writeTextFile(ctx.getStepArtifactPath("build", "result"), `---\nschema: pipeline-human-decision@1\nreviewedFingerprint: ${resolved?.dependencyFingerprint}\ndecision: accepted\n---\napproved\n`);
  await ctx.recordStepCompletion?.({ stepId: "build", artifacts: ["result"], fingerprint });
  await expect(ctx.isStepReusable?.({ stepId: "build", artifacts: ["result"], fingerprint: contract("changed", { kind: "human_decision", artifactId: "result" }) })).resolves.toBe(false);
});

test("external completion rejects a local receipt without an idempotency key", async () => {
  const { ctx } = await createHarness();
  const fingerprint = contract("publish", { kind: "external_receipt", artifactId: "result" });
  await ctx.writeTextFile(ctx.getStepArtifactPath("build", "result"), "{}\n");
  await expect(ctx.recordStepCompletion?.({ stepId: "build", artifacts: ["result"], fingerprint })).rejects.toThrow("has no matching idempotency key");
});

test("external completion accepts a receipt bound to the operation fingerprint", async () => {
  const { ctx } = await createHarness();
  const fingerprint = contract("publish", { kind: "external_receipt", artifactId: "result" });
  const resolved = await ctx.resolveStepFingerprint?.({ stepId: "build", fingerprint });
  await ctx.writeTextFile(ctx.getStepArtifactPath("build", "result"), JSON.stringify({ schema: "pipeline-external-effect-receipt@1", idempotencyKey: resolved?.operationFingerprint, externalId: "remote-42" }));
  await ctx.recordStepCompletion?.({ stepId: "build", artifacts: ["result"], fingerprint });
  await expect(ctx.isStepReusable?.({ stepId: "build", artifacts: ["result"], fingerprint })).resolves.toBe(true);
});

test("a step output transaction atomically promotes validated staging bytes", async () => {
  const { ctx } = await createHarness();
  const canonical = ctx.getStepArtifactPath("build", "result");
  await ctx.writeTextFile(canonical, "old");
  await ctx.beginStepOutputTransaction?.("build");
  const staged = ctx.getStepArtifactPath("build", "result");
  expect(staged).not.toBe(canonical);
  await ctx.writeTextFile(staged, "new");
  await expect(fs.readFile(canonical, "utf8")).resolves.toBe("old\n");
  await ctx.commitStepOutputTransaction?.("build");
  await expect(fs.readFile(ctx.getStepArtifactPath("build", "result"), "utf8")).resolves.toBe("new\n");
});

test("an interrupted transaction preserves canonical output and isolates partial bytes", async () => {
  const { ctx } = await createHarness();
  const canonical = ctx.getStepArtifactPath("build", "result");
  await ctx.writeTextFile(canonical, "old");
  await ctx.beginStepOutputTransaction?.("build");
  await ctx.writeTextFile(ctx.getStepArtifactPath("build", "result"), "partial");
  await ctx.abortStepOutputTransaction?.("build");
  await expect(fs.readFile(ctx.getStepArtifactPath("build", "result"), "utf8")).resolves.toBe("old\n");
});
