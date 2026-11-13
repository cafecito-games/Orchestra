import { useEffect, useState } from "react";
import { useAuthenticatedBlobUrl } from "../api";
import { fmtBytes, fullTime, shortId, timeAgo } from "../format";
import type { Artifact } from "../types";
import {
  ChevronLeftIcon,
  ChevronRightIcon,
  DownloadIcon,
  ImageIcon,
  XIcon,
} from "./Icons";
import { Card, SectionHead } from "./ui";

const IMAGE_EXTENSION_PATTERN = /\.(png|jpe?g|webp|gif)$/i;

export function isUploadedImageArtifact(artifact: Artifact): boolean {
  return (
    artifact.uploaded &&
    (artifact.kind === "image" ||
      Boolean(artifact.mediaType?.startsWith("image/")) ||
      IMAGE_EXTENSION_PATTERN.test(artifact.name))
  );
}

function ReelFrameCard({
  artifact,
  frameNumber,
  totalFrames,
  onSelect,
}: {
  artifact: Artifact;
  frameNumber: number;
  totalFrames: number;
  onSelect: () => void;
}) {
  const { blobUrl, loading, error } = useAuthenticatedBlobUrl(artifact.downloadPath);
  const timestamp = artifact.uploadedAt ?? artifact.createdAt;

  return (
    <button type="button" className="reel-frame" onClick={onSelect}>
      <div className="reel-thumb">
        {blobUrl ? (
          <img src={blobUrl} alt={artifact.name} loading="lazy" />
        ) : loading ? (
          <div className="reel-thumb-placeholder">
            <div className="spinner" />
          </div>
        ) : (
          <div className="reel-thumb-placeholder">
            <ImageIcon />
            <span>{error ?? "Preview unavailable"}</span>
          </div>
        )}
        <span className="reel-step-pill">
          {frameNumber}/{totalFrames}
        </span>
      </div>
      <div className="reel-meta">
        <div className="reel-title" title={artifact.name}>
          {artifact.name}
        </div>
        {artifact.summary ? (
          <div className="reel-summary" title={artifact.summary}>
            {artifact.summary}
          </div>
        ) : null}
        <div className="reel-sub">
          <span>{fmtBytes(artifact.sizeBytes)}</span>
          <span>·</span>
          <span title={fullTime(timestamp)}>{timeAgo(timestamp)}</span>
          {artifact.taskId ? (
            <>
              <span>·</span>
              <span>{shortId(artifact.taskId)}</span>
            </>
          ) : null}
        </div>
      </div>
    </button>
  );
}

function ReelLightbox({
  artifacts,
  activeIndex,
  onClose,
  onSelectIndex,
  onDownload,
}: {
  artifacts: Artifact[];
  activeIndex: number;
  onClose: () => void;
  onSelectIndex: (nextIndex: number) => void;
  onDownload: (name: string, path: string) => void;
}) {
  const activeArtifact = artifacts[activeIndex];
  const { blobUrl, loading, error } = useAuthenticatedBlobUrl(activeArtifact?.downloadPath);

  useEffect(() => {
    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        onClose();
      } else if (event.key === "ArrowLeft" && artifacts.length > 1) {
        event.preventDefault();
        onSelectIndex((activeIndex - 1 + artifacts.length) % artifacts.length);
      } else if (event.key === "ArrowRight" && artifacts.length > 1) {
        event.preventDefault();
        onSelectIndex((activeIndex + 1) % artifacts.length);
      }
    }
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [activeIndex, artifacts.length, onClose, onSelectIndex]);

  if (!activeArtifact) {
    return null;
  }

  const timestamp = activeArtifact.uploadedAt ?? activeArtifact.createdAt;

  return (
    <div
      className="lightbox-backdrop"
      role="dialog"
      aria-modal="true"
      aria-label={activeArtifact.name}
      onClick={onClose}
    >
      <div className="lightbox-dialog" onClick={(event) => event.stopPropagation()}>
        <div className="lightbox-head">
          <div className="lightbox-title-group">
            <div className="lightbox-title">{activeArtifact.name}</div>
            <div className="lightbox-sub">
              <span className="mono">
                {activeIndex + 1} of {artifacts.length}
              </span>
              <span>·</span>
              <span className="mono">{fmtBytes(activeArtifact.sizeBytes)}</span>
              {activeArtifact.mediaType ? (
                <>
                  <span>·</span>
                  <span className="mono">{activeArtifact.mediaType}</span>
                </>
              ) : null}
              <span>·</span>
              <span title={fullTime(timestamp)}>{timeAgo(timestamp)}</span>
              {activeArtifact.taskId ? (
                <>
                  <span>·</span>
                  <span className="mono">task {shortId(activeArtifact.taskId)}</span>
                </>
              ) : null}
            </div>
          </div>
          <div className="lightbox-actions">
            {artifacts.length > 1 ? (
              <>
                <button
                  type="button"
                  className="btn"
                  onClick={() =>
                    onSelectIndex((activeIndex - 1 + artifacts.length) % artifacts.length)
                  }
                  aria-label="Previous image"
                >
                  <ChevronLeftIcon />
                </button>
                <button
                  type="button"
                  className="btn"
                  onClick={() => onSelectIndex((activeIndex + 1) % artifacts.length)}
                  aria-label="Next image"
                >
                  <ChevronRightIcon />
                </button>
              </>
            ) : null}
            <button
              type="button"
              className="btn"
              onClick={() => onDownload(activeArtifact.name, activeArtifact.downloadPath)}
            >
              <DownloadIcon />
              <span>Download</span>
            </button>
            <button type="button" className="btn ghost" onClick={onClose} aria-label="Close preview">
              <XIcon />
            </button>
          </div>
        </div>

        <div className="lightbox-stage">
          {blobUrl ? (
            <img src={blobUrl} alt={activeArtifact.name} className="lightbox-image" />
          ) : loading ? (
            <div className="loading">
              <div className="spinner" />
            </div>
          ) : (
            <div className="muted">{error ?? "Unable to load image artifact"}</div>
          )}
        </div>

        {activeArtifact.summary ? (
          <div className="lightbox-caption">{activeArtifact.summary}</div>
        ) : null}
      </div>
    </div>
  );
}

export function ProgressReel({
  artifacts,
  onDownload,
}: {
  artifacts: Artifact[];
  onDownload: (name: string, path: string) => void;
}) {
  const [selectedArtifactId, setSelectedArtifactId] = useState<string | null>(null);

  if (artifacts.length === 0) {
    return null;
  }

  const orderedArtifacts = [...artifacts].sort((left, right) => {
    const leftTime = new Date(left.uploadedAt ?? left.createdAt).getTime();
    const rightTime = new Date(right.uploadedAt ?? right.createdAt).getTime();
    if (Number.isNaN(leftTime) || Number.isNaN(rightTime) || leftTime === rightTime) {
      return left.id.localeCompare(right.id);
    }
    return leftTime - rightTime;
  });

  const selectedIndex =
    selectedArtifactId !== null
      ? orderedArtifacts.findIndex((artifact) => artifact.id === selectedArtifactId)
      : -1;

  return (
    <div className="section">
      <SectionHead title="Visual progress reel" count={orderedArtifacts.length} />
      <Card className="reel-card">
        <div className="reel-track">
          {orderedArtifacts.map((artifact, index) => (
            <ReelFrameCard
              key={artifact.id}
              artifact={artifact}
              frameNumber={index + 1}
              totalFrames={orderedArtifacts.length}
              onSelect={() => setSelectedArtifactId(artifact.id)}
            />
          ))}
        </div>
      </Card>

      {selectedIndex >= 0 ? (
        <ReelLightbox
          artifacts={orderedArtifacts}
          activeIndex={selectedIndex}
          onClose={() => setSelectedArtifactId(null)}
          onSelectIndex={(nextIndex) => {
            const target = orderedArtifacts[nextIndex];
            if (target) {
              setSelectedArtifactId(target.id);
            }
          }}
          onDownload={onDownload}
        />
      ) : null}
    </div>
  );
}
