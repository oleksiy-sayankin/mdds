// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/** @vitest-environment jsdom */

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { SubmissionState } from "../WizardState";
import { ReviewStep } from "./ReviewStep";

const matrixFile = new File(["1,0\n0,1"], "matrix.csv", {
  type: "text/csv",
});

const rhsFile = new File(["1\n2"], "rhs.csv", {
  type: "text/csv",
});

function renderReviewStep({
  submissionState = "IDLE",
  onDismissConfirmation = vi.fn(),
  onConfirmSubmission = vi.fn(),
}: {
  submissionState?: SubmissionState;
  onDismissConfirmation?: () => void;
  onConfirmSubmission?: () => void;
} = {}) {
  return render(
    <ReviewStep
      jobType="solving_slae"
      matrixFile={matrixFile}
      rhsFile={rhsFile}
      solvingMethod="numpy_exact_solver"
      submissionState={submissionState}
      onDismissConfirmation={onDismissConfirmation}
      onConfirmSubmission={onConfirmSubmission}
    />,
  );
}

describe("ReviewStep", () => {
  it("shows the complete job summary", () => {
    renderReviewStep();

    expect(screen.getByText("solving_slae")).toBeTruthy();
    expect(screen.getByText("matrix.csv")).toBeTruthy();
    expect(screen.getByText("rhs.csv")).toBeTruthy();
    expect(screen.getByText("numpy_exact_solver")).toBeTruthy();
  });

  it("does not show the dialog outside CONFIRMING", () => {
    renderReviewStep({
      submissionState: "IDLE",
    });

    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("shows the confirmation dialog in CONFIRMING", () => {
    renderReviewStep({
      submissionState: "CONFIRMING",
    });

    expect(
      screen.getByRole("dialog", {
        name: "Submit this job?",
      }),
    ).toBeTruthy();
  });

  it("calls onDismissConfirmation", () => {
    const onDismissConfirmation = vi.fn();

    renderReviewStep({
      submissionState: "CONFIRMING",
      onDismissConfirmation,
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Continue editing",
      }),
    );

    expect(onDismissConfirmation).toHaveBeenCalledOnce();
  });

  it("calls onConfirmSubmission", () => {
    const onConfirmSubmission = vi.fn();

    renderReviewStep({
      submissionState: "CONFIRMING",
      onConfirmSubmission,
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Submit job",
      }),
    );

    expect(onConfirmSubmission).toHaveBeenCalledOnce();
  });
});
