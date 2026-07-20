// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/** @vitest-environment jsdom */

import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MonitorStep } from "./MonitorStep";
import type {
  CancellationState,
  MonitorState,
  PublicJobStatus,
} from "../WizardState";

afterEach(() => {
  cleanup();
});

function renderMonitorStep({
  jobStatus = "SUBMITTED",
  jobProgress = 0,
  jobMessage = "Waiting for a worker.",
  monitorState = "RUNNING",
  cancellationState = "IDLE",
  onRequestCancellation = vi.fn(),
  onDismissCancellation = vi.fn(),
  onConfirmCancellation = vi.fn(),
  onStartNewJob = vi.fn(),
  onRetryMonitoring = vi.fn(),
}: {
  jobStatus?: PublicJobStatus;
  jobProgress?: number;
  jobMessage?: string;
  monitorState?: MonitorState;
  cancellationState?: CancellationState;
  onRequestCancellation?: () => void;
  onDismissCancellation?: () => void;
  onConfirmCancellation?: () => void;
  onStartNewJob?: () => void;
  onRetryMonitoring?: () => void;
} = {}) {
  return render(
    <MonitorStep
      jobStatus={jobStatus}
      jobProgress={jobProgress}
      jobMessage={jobMessage}
      monitorState={monitorState}
      cancellationState={cancellationState}
      onRequestCancellation={onRequestCancellation}
      onDismissCancellation={onDismissCancellation}
      onConfirmCancellation={onConfirmCancellation}
      onStartNewJob={onStartNewJob}
      onRetryMonitoring={onRetryMonitoring}
    />,
  );
}

describe("MonitorStep", () => {
  it.each<PublicJobStatus>([
    "SUBMITTED",
    "INPUTS_PREPARED",
    "IN_PROGRESS",
    "DONE",
    "ERROR",
    "CANCELLED",
  ])("shows status %s", (jobStatus) => {
    renderMonitorStep({
      jobStatus,
    });

    expect(screen.getByText(jobStatus)).toBeTruthy();
  });

  it("shows job progress and message", () => {
    renderMonitorStep({
      jobStatus: "IN_PROGRESS",
      jobProgress: 40,
      jobMessage: "Worker is processing the job.",
    });

    expect(screen.getByText("40%")).toBeTruthy();
    expect(screen.getByText("Worker is processing the job.")).toBeTruthy();

    expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe(
      "40",
    );
  });

  it("enables cancellation only for a running in-progress job", () => {
    const { rerender } = render(
      <MonitorStep
        jobStatus="IN_PROGRESS"
        jobProgress={20}
        jobMessage="Running"
        monitorState="RUNNING"
        cancellationState="IDLE"
        onRequestCancellation={vi.fn()}
        onDismissCancellation={vi.fn()}
        onConfirmCancellation={vi.fn()}
        onStartNewJob={vi.fn()}
        onRetryMonitoring={vi.fn()}
      />,
    );

    expect(
      (
        screen.getByRole("button", {
          name: "Cancel job",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false);

    rerender(
      <MonitorStep
        jobStatus="SUBMITTED"
        jobProgress={0}
        jobMessage="Waiting"
        monitorState="RUNNING"
        cancellationState="IDLE"
        onRequestCancellation={vi.fn()}
        onDismissCancellation={vi.fn()}
        onConfirmCancellation={vi.fn()}
        onStartNewJob={vi.fn()}
        onRetryMonitoring={vi.fn()}
      />,
    );

    expect(
      (
        screen.getByRole("button", {
          name: "Cancel job",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);

    rerender(
      <MonitorStep
        jobStatus="IN_PROGRESS"
        jobProgress={20}
        jobMessage="Running"
        monitorState="IDLE"
        cancellationState="IDLE"
        onRequestCancellation={vi.fn()}
        onDismissCancellation={vi.fn()}
        onConfirmCancellation={vi.fn()}
        onStartNewJob={vi.fn()}
        onRetryMonitoring={vi.fn()}
      />,
    );

    expect(
      (
        screen.getByRole("button", {
          name: "Cancel job",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
  });

  it("calls onRequestCancellation", () => {
    const onRequestCancellation = vi.fn();

    renderMonitorStep({
      jobStatus: "IN_PROGRESS",
      monitorState: "RUNNING",
      cancellationState: "IDLE",
      onRequestCancellation,
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Cancel job",
      }),
    );

    expect(onRequestCancellation).toHaveBeenCalledOnce();
  });

  it("shows the cancellation dialog in CONFIRMING", () => {
    renderMonitorStep({
      jobStatus: "IN_PROGRESS",
      cancellationState: "CONFIRMING",
    });

    expect(
      screen.getByRole("dialog", {
        name: "Cancel this running job?",
      }),
    ).toBeTruthy();
  });

  it("calls cancellation dialog callbacks", () => {
    const onDismissCancellation = vi.fn();
    const onConfirmCancellation = vi.fn();

    renderMonitorStep({
      jobStatus: "IN_PROGRESS",
      cancellationState: "CONFIRMING",
      onDismissCancellation,
      onConfirmCancellation,
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Keep running",
      }),
    );

    expect(onDismissCancellation).toHaveBeenCalledOnce();

    const dialog = screen.getByRole("dialog", {
      name: "Cancel this running job?",
    });

    fireEvent.click(
      within(dialog).getByRole("button", {
        name: "Cancel job",
      }),
    );

    expect(onConfirmCancellation).toHaveBeenCalledOnce();
  });

  it("shows the cancelled terminal status", () => {
    renderMonitorStep({
      jobStatus: "CANCELLED",
      monitorState: "COMPLETED",
      cancellationState: "ACCEPTED",
      jobMessage: "Job was cancelled.",
    });

    expect(screen.getByText("CANCELLED")).toBeTruthy();
    expect(screen.getByText("Job was cancelled.")).toBeTruthy();
  });
});

it("shows Start new job only after a job error", () => {
  const { rerender } = render(
    <MonitorStep
      jobStatus="ERROR"
      jobProgress={40}
      jobMessage="Worker-side processing failed."
      monitorState="COMPLETED"
      cancellationState="IDLE"
      onRequestCancellation={vi.fn()}
      onDismissCancellation={vi.fn()}
      onConfirmCancellation={vi.fn()}
      onStartNewJob={vi.fn()}
      onRetryMonitoring={vi.fn()}
    />,
  );

  expect(
    screen.getByRole("button", {
      name: "Start new job",
    }),
  ).toBeTruthy();

  expect(
    screen.queryByRole("button", {
      name: "Cancel job",
    }),
  ).toBeNull();

  rerender(
    <MonitorStep
      jobStatus="IN_PROGRESS"
      jobProgress={40}
      jobMessage="Worker is processing the job."
      monitorState="RUNNING"
      cancellationState="IDLE"
      onRequestCancellation={vi.fn()}
      onDismissCancellation={vi.fn()}
      onConfirmCancellation={vi.fn()}
      onStartNewJob={vi.fn()}
      onRetryMonitoring={vi.fn()}
    />,
  );

  expect(
    screen.queryByRole("button", {
      name: "Start new job",
    }),
  ).toBeNull();
});

it("calls onStartNewJob after a job error", () => {
  const onStartNewJob = vi.fn();

  renderMonitorStep({
    jobStatus: "ERROR",
    monitorState: "COMPLETED",
    jobMessage: "Worker-side processing failed.",
    onStartNewJob,
  });

  fireEvent.click(
    screen.getByRole("button", {
      name: "Start new job",
    }),
  );

  expect(onStartNewJob).toHaveBeenCalledOnce();
});

it("shows recovery actions when monitoring fails", () => {
  renderMonitorStep({
    jobStatus: "IN_PROGRESS",
    jobProgress: 40,
    jobMessage: "Worker is processing the job.",
    monitorState: "FAILED",
  });

  expect(
    screen.getByRole("button", {
      name: "Retry monitoring",
    }),
  ).toBeTruthy();

  expect(
    screen.getByRole("button", {
      name: "Start new job",
    }),
  ).toBeTruthy();

  expect(
    screen.queryByRole("button", {
      name: "Cancel job",
    }),
  ).toBeNull();
});

it("calls monitoring recovery callbacks", () => {
  const onRetryMonitoring = vi.fn();
  const onStartNewJob = vi.fn();

  renderMonitorStep({
    jobStatus: "IN_PROGRESS",
    monitorState: "FAILED",
    onRetryMonitoring,
    onStartNewJob,
  });

  fireEvent.click(
    screen.getByRole("button", {
      name: "Retry monitoring",
    }),
  );

  expect(onRetryMonitoring).toHaveBeenCalledOnce();

  fireEvent.click(
    screen.getByRole("button", {
      name: "Start new job",
    }),
  );

  expect(onStartNewJob).toHaveBeenCalledOnce();
});

it("does not allow cancellation while inputs are being prepared", () => {
  renderMonitorStep({
    jobStatus: "INPUTS_PREPARED",
    jobProgress: 0,
    jobMessage: "Worker prepared the job inputs.",
    monitorState: "RUNNING",
    cancellationState: "IDLE",
  });

  const cancelButton = screen.getByRole("button", {
    name: "Cancel job",
  }) as HTMLButtonElement;

  expect(cancelButton.disabled).toBe(true);
});
