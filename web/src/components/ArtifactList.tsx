import { useEffect, useMemo, useRef, useState } from "react";
import { AuthError, download, DownloadAbortedError, type DownloadProgress } from "../api";
import { fmtBytes, fullTime, shortId, shortSha256, timeAgo } from "../format";
import { toast } from "../toast";
import type { Artifact } from "../types";
import { CopyIcon, DownloadIcon, FileIcon, XIcon } from "./Icons";
import { Badge, Card, Empty, PlainBadge, SectionHead } from "./ui";

export const LARGE_ARTIFACT_BYTES = 5 * 1024 * 1024;

interface ActiveDownload {
  progress: DownloadProgress;
  controller: AbortController;
}

export function ArtifactList({ artifacts }: { artifacts: Artifact[] }) {
  const [downloads, setDownloads] = useState<Record<string, ActiveDownload>>({});
  const controllersRef = useRef<Map<string, AbortController>>(new Map());

  useEffect(() => {
    const controllers = controllersRef.current;
    return () => {
      controllers.forEach((controller) => controller.abort());
      controllers.clear();
    };
  }, []);

  const counts = useMemo(() => {
    let uploaded = 0;
    let pending = 0;
    for (const artifact of artifacts) {
      if (artifact.uploaded) {
        uploaded += 1;
      } else {
        pending += 1;
      }
    }
    return { uploaded, pending };
  }, [artifacts]);

  async function startDownload(artifact: Artifact) {
    if (!artifact.uploaded || !artifact.downloadPath) {
      return;
    }
    if (controllersRef.current.has(artifact.id)) {
      return;
    }

    const controller = new AbortController();
    controllersRef.current.set(artifact.id, controller);
    setDownloads((current) => ({
      ...current,
      [artifact.id]: {
        controller,
        progress: {
          loaded: 0,
          total: artifact.sizeBytes > 0 ? artifact.sizeBytes : null,
          percent: artifact.sizeBytes > 0 ? 0 : null,
        },
      },
    }));

    try {
      await download(artifact.downloadPath, artifact.name, {
        signal: controller.signal,
        expectedSizeBytes: artifact.sizeBytes,
        onProgress: (progress) => {
          setDownloads((current) => {
            const existing = current[artifact.id];
            if (!existing) return current;
            return {
              ...current,
              [artifact.id]: { ...existing, progress },
            };
          });
        },
      });
      toast(`Downloaded ${artifact.name}`, "ok");
    } catch (downloadError) {
      if (downloadError instanceof DownloadAbortedError) {
        toast(`Cancelled download of ${artifact.name}`, "ok");
      } else if (!(downloadError instanceof AuthError)) {
        const message =
          downloadError instanceof Error ? downloadError.message : String(downloadError);
        toast(`Failed to download ${artifact.name}: ${message}`, "err");
      }
    } finally {
      controllersRef.current.delete(artifact.id);
      setDownloads((current) => {
        if (!(artifact.id in current)) return current;
        const next = { ...current };
        delete next[artifact.id];
        return next;
      });
    }
  }

  function cancelDownload(artifactId: string) {
    const controller = controllersRef.current.get(artifactId);
    if (controller) {
      controller.abort();
    }
  }

  async function copySha256(sha256: string) {
    try {
      await navigator.clipboard.writeText(sha256);
      toast("Copied SHA-256", "ok");
    } catch {
      toast("Unable to copy SHA-256 to clipboard", "err");
    }
  }

  const summaryAction =
    artifacts.length > 0 ? (
      <div className="artifact-summary-counts">
        <span className="mono">{counts.uploaded} uploaded</span>
        {counts.pending > 0 ? (
          <>
            <span>·</span>
            <span className="mono artifact-pending-count">{counts.pending} pending</span>
          </>
        ) : null}
      </div>
    ) : undefined;

  return (
    <div className="section">
      <SectionHead title="Artifacts" count={artifacts.length} action={summaryAction} />
      <Card>
        {artifacts.length === 0 ? (
          <Empty
            title="No artifacts yet"
            sub="Files, patches, logs, and screenshots registered on this thread will appear here."
            icon={<FileIcon />}
          />
        ) : (
          <div className="list">
            {artifacts.map((artifact) => {
              const activeDownload = downloads[artifact.id];
              const isReady = artifact.uploaded && Boolean(artifact.downloadPath);
              const isLarge = artifact.uploaded && artifact.sizeBytes >= LARGE_ARTIFACT_BYTES;
              const timestamp = artifact.uploadedAt ?? artifact.createdAt;
              const sizeLabel =
                artifact.uploaded || artifact.sizeBytes > 0
                  ? fmtBytes(artifact.sizeBytes)
                  : "Size pending";
              const mediaTypeLabel = artifact.mediaType || "application/octet-stream";

              return (
                <div
                  key={artifact.id}
                  id={`artifact-${artifact.id}`}
                  className={`artifact-row anchor-target${artifact.uploaded ? "" : " pending"}`}
                >
                  <div className="artifact-main">
                    <div className="artifact-head">
                      <span className="artifact-name" title={artifact.relativePath || artifact.name}>
                        {artifact.name}
                      </span>
                      <Badge
                        status={artifact.uploaded ? "uploaded" : "pending_upload"}
                        label={artifact.uploaded ? "uploaded" : "pending upload"}
                      />
                      {artifact.kind ? <PlainBadge>{artifact.kind}</PlainBadge> : null}
                      {isLarge ? <PlainBadge>large file</PlainBadge> : null}
                    </div>

                    {artifact.relativePath && artifact.relativePath !== artifact.name ? (
                      <div className="artifact-path mono" title={artifact.relativePath}>
                        {artifact.relativePath}
                      </div>
                    ) : null}

                    {artifact.summary ? (
                      <div className="artifact-summary">{artifact.summary}</div>
                    ) : null}

                    <div className="artifact-meta">
                      <span className="chip">{mediaTypeLabel}</span>
                      <span className="chip">{sizeLabel}</span>
                      {artifact.sha256 ? (
                        <button
                          type="button"
                          className="artifact-hash"
                          title={`SHA-256: ${artifact.sha256} (click to copy)`}
                          onClick={() => copySha256(artifact.sha256!)}
                        >
                          <span>sha256 {shortSha256(artifact.sha256)}</span>
                          <CopyIcon />
                        </button>
                      ) : (
                        <span className="chip artifact-hash-empty">sha256 —</span>
                      )}
                      <span className="muted mono" style={{ fontSize: 11 }}>
                        {shortId(artifact.id)}
                      </span>
                      {artifact.taskId ? (
                        <span className="muted mono" style={{ fontSize: 11 }}>
                          task {shortId(artifact.taskId)}
                        </span>
                      ) : null}
                      <span className="muted" style={{ fontSize: 11 }} title={fullTime(timestamp)}>
                        {artifact.uploaded ? `uploaded ${timeAgo(timestamp)}` : `created ${timeAgo(timestamp)}`}
                      </span>
                    </div>

                    {activeDownload ? (
                      <div className="artifact-progress">
                        <div className="artifact-progress-meta mono">
                          <span>
                            Downloading {fmtBytes(activeDownload.progress.loaded)}
                            {activeDownload.progress.total
                              ? ` / ${fmtBytes(activeDownload.progress.total)}`
                              : ""}
                          </span>
                          {activeDownload.progress.percent != null ? (
                            <span>{activeDownload.progress.percent}%</span>
                          ) : null}
                        </div>
                        <div className="bar artifact-progress-bar">
                          <span
                            style={{
                              transform: `scaleX(${
                                activeDownload.progress.percent != null
                                  ? activeDownload.progress.percent / 100
                                  : 0.35
                              })`,
                            }}
                          />
                        </div>
                      </div>
                    ) : null}
                  </div>

                  <div className="artifact-actions">
                    {activeDownload ? (
                      <button
                        type="button"
                        className="btn danger"
                        onClick={() => cancelDownload(artifact.id)}
                      >
                        <XIcon />
                        <span>Cancel</span>
                      </button>
                    ) : isReady ? (
                      <button
                        type="button"
                        className="btn"
                        onClick={() => startDownload(artifact)}
                      >
                        <DownloadIcon />
                        <span>Download</span>
                      </button>
                    ) : (
                      <button
                        type="button"
                        className="btn"
                        disabled
                        aria-disabled="true"
                        title="Artifact content has not been uploaded yet"
                      >
                        <DownloadIcon />
                        <span>Awaiting upload</span>
                      </button>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </Card>
    </div>
  );
}
