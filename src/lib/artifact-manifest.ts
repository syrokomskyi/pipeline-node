/*
<MODULE_CONTRACT>
  <purpose>Read and atomically write validated YAML manifests for pipeline artifact lineage.</purpose>
  <non-goals>
    <item>Does not decide artifact reuse or execute external effects.</item>
    <item>Does not serialize secrets or accept arbitrary unvalidated YAML data.</item>
  </non-goals>
</MODULE_CONTRACT>
<CHANGE_SUMMARY>
  <item>RFC-0094: Add atomic YAML manifest persistence for fingerprinted artifacts.</item>
</CHANGE_SUMMARY>
*/

import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import matter from "gray-matter";

export const artifactManifestFileName = "artifact-manifest.yaml";
export type ArtifactManifest = {
  schema: "pipeline-artifact-manifest@1";
  pipelineId: string;
  stepId: string;
  dependencyFingerprint: string;
  outputs: Array<{ artifactId: string; sha256: string; bytes: number }>;
};

const assertSha256: (value: unknown, field: string) => asserts value is string = (value, field) => {
  if (typeof value !== "string" || !/^[a-f0-9]{64}$/.test(value)) {
    throw new Error(`Invalid artifact manifest ${field}`);
  }
};

const parseManifest = (value: unknown): ArtifactManifest => {
  if (!value || typeof value !== "object") throw new Error("Invalid artifact manifest");
  const manifest = value as Partial<ArtifactManifest>;
  if (manifest.schema !== "pipeline-artifact-manifest@1") throw new Error("Unknown artifact manifest schema");
  if (!manifest.pipelineId || !manifest.stepId || !Array.isArray(manifest.outputs)) {
    throw new Error("Artifact manifest is missing required fields");
  }
  assertSha256(manifest.dependencyFingerprint, "dependencyFingerprint");
  for (const output of manifest.outputs) {
    if (!output || typeof output.artifactId !== "string" || !Number.isSafeInteger(output.bytes)) {
      throw new Error("Invalid artifact manifest output");
    }
    assertSha256(output.sha256, `outputs.${output.artifactId}.sha256`);
  }
  return manifest as ArtifactManifest;
};

export const readArtifactManifest = async (stepOutputDirectory: string): Promise<ArtifactManifest | null> => {
  const filePath = path.join(stepOutputDirectory, artifactManifestFileName);
  try {
    const source = await fs.readFile(filePath, "utf8");
    return parseManifest(matter(source).data);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
};

export const writeArtifactManifest = async (
  stepOutputDirectory: string,
  manifest: ArtifactManifest,
): Promise<void> => {
  parseManifest(manifest);
  const destination = path.join(stepOutputDirectory, artifactManifestFileName);
  const temporaryPath = `${destination}.${randomUUID()}.tmp`;
  await fs.mkdir(stepOutputDirectory, { recursive: true });
  await fs.writeFile(temporaryPath, matter.stringify("", manifest), "utf8");
  await fs.rename(temporaryPath, destination);
};
