// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/** @vitest-environment jsdom */

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { ParameterUpdateState, SolvingMethod } from "../WizardState";
import { ParametersStep } from "./ParametersStep";

afterEach(() => {
  cleanup();
});

function renderParametersStep({
  solvingMethod = "numpy_exact_solver",
  parameterUpdateState = "PENDING",
  onSolvingMethodChange = vi.fn(),
  onRetryParameterUpdate = vi.fn(),
}: {
  solvingMethod?: SolvingMethod;
  parameterUpdateState?: ParameterUpdateState;
  onSolvingMethodChange?: (solvingMethod: SolvingMethod) => void;
  onRetryParameterUpdate?: () => void;
} = {}) {
  return render(
    <ParametersStep
      solvingMethod={solvingMethod}
      parameterUpdateState={parameterUpdateState}
      onSolvingMethodChange={onSolvingMethodChange}
      onRetryParameterUpdate={onRetryParameterUpdate}
    />,
  );
}

describe("ParametersStep", () => {
  it("shows the selected solving method", () => {
    renderParametersStep({
      solvingMethod: "numpy_pinv_solver",
    });

    expect(screen.getByRole("combobox").textContent).toContain(
      "numpy_pinv_solver",
    );
  });

  it("reports a changed solving method", async () => {
    const onSolvingMethodChange = vi.fn();

    renderParametersStep({
      onSolvingMethodChange,
    });

    fireEvent.mouseDown(screen.getByRole("combobox"));

    const option = await screen.findByRole("option", {
      name: "numpy_lstsq_solver",
    });

    fireEvent.click(option);

    await waitFor(() => {
      expect(onSolvingMethodChange).toHaveBeenCalledWith("numpy_lstsq_solver");
    });
  });

  it.each<{
    state: ParameterUpdateState;
    expectedText: string;
  }>([
    {
      state: "PENDING",
      expectedText: "Pending",
    },
    {
      state: "UPDATING",
      expectedText: "Updating...",
    },
    {
      state: "UPDATED",
      expectedText: "Synchronized",
    },
    {
      state: "FAILED",
      expectedText: "Update failed",
    },
  ])("shows $expectedText for $state", ({ state, expectedText }) => {
    renderParametersStep({
      parameterUpdateState: state,
    });

    expect(screen.getByText(expectedText)).toBeTruthy();
  });

  it("disables the solver selector while updating", () => {
    renderParametersStep({
      parameterUpdateState: "UPDATING",
    });

    const selector = screen.getByRole("combobox");

    expect(selector.getAttribute("aria-disabled")).toBe("true");
  });

  it("shows Retry update only after a failed update", () => {
    const { rerender } = render(
      <ParametersStep
        solvingMethod="numpy_exact_solver"
        parameterUpdateState="PENDING"
        onSolvingMethodChange={vi.fn()}
        onRetryParameterUpdate={vi.fn()}
      />,
    );

    expect(
      screen.queryByRole("button", {
        name: "Retry update",
      }),
    ).toBeNull();

    rerender(
      <ParametersStep
        solvingMethod="numpy_exact_solver"
        parameterUpdateState="FAILED"
        onSolvingMethodChange={vi.fn()}
        onRetryParameterUpdate={vi.fn()}
      />,
    );

    expect(
      screen.getByRole("button", {
        name: "Retry update",
      }),
    ).toBeTruthy();
  });

  it("calls onRetryParameterUpdate", () => {
    const onRetryParameterUpdate = vi.fn();

    renderParametersStep({
      parameterUpdateState: "FAILED",
      onRetryParameterUpdate,
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Retry update",
      }),
    );

    expect(onRetryParameterUpdate).toHaveBeenCalledOnce();
  });
});
