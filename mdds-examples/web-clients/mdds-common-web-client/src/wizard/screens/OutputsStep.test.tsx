// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/** @vitest-environment jsdom */

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { DownloadState } from "../WizardState";
import { OutputsStep } from "./OutputsStep";

afterEach(() => {
  cleanup();
});

function renderOutputsStep({
  downloadState = "IDLE",
  onDownload = vi.fn(),
  onCancelDownload = vi.fn(),
}: {
  downloadState?: DownloadState;
  onDownload?: () => void;
  onCancelDownload?: () => void;
} = {}) {
  return render(
    <OutputsStep
      downloadState={downloadState}
      onDownload={onDownload}
      onCancelDownload={onCancelDownload}
    />,
  );
}

describe("OutputsStep", () => {
  it.each<{
    state: DownloadState;
    expectedText: string;
  }>([
    {
      state: "IDLE",
      expectedText: "Available",
    },
    {
      state: "REQUESTING_URL",
      expectedText: "Preparing...",
    },
    {
      state: "DOWNLOADING",
      expectedText: "Downloading...",
    },
    {
      state: "DOWNLOADED",
      expectedText: "Downloaded",
    },
    {
      state: "FAILED_DOWNLOAD",
      expectedText: "Download failed",
    },
    {
      state: "CANCELLED_DOWNLOAD",
      expectedText: "Cancelled",
    },
  ])("shows $expectedText for $state", ({ state, expectedText }) => {
    renderOutputsStep({
      downloadState: state,
    });

    expect(screen.getByText(expectedText)).toBeTruthy();
  });

  it("calls onDownload from the initial state", () => {
    const onDownload = vi.fn();

    renderOutputsStep({
      downloadState: "IDLE",
      onDownload,
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Download",
      }),
    );

    expect(onDownload).toHaveBeenCalledOnce();
  });

  it.each<DownloadState>(["REQUESTING_URL", "DOWNLOADING"])(
    "calls onCancelDownload while state is %s",
    (downloadState) => {
      const onCancelDownload = vi.fn();

      renderOutputsStep({
        downloadState,
        onCancelDownload,
      });

      fireEvent.click(
        screen.getByRole("button", {
          name: "Cancel download",
        }),
      );

      expect(onCancelDownload).toHaveBeenCalledOnce();
    },
  );

  it.each<DownloadState>([
    "DOWNLOADED",
    "FAILED_DOWNLOAD",
    "CANCELLED_DOWNLOAD",
  ])("allows Download again after %s", (downloadState) => {
    const onDownload = vi.fn();

    renderOutputsStep({
      downloadState,
      onDownload,
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Download again",
      }),
    );

    expect(onDownload).toHaveBeenCalledOnce();
  });
});
