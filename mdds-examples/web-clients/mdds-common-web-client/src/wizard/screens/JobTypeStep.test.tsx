// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/** @vitest-environment jsdom */

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { JobTypeStep } from "./JobTypeStep";

afterEach(() => {
  cleanup();
});

describe("JobTypeStep", () => {
  it("shows the first wizard step", () => {
    render(<JobTypeStep jobType="solving_slae" onJobTypeChange={vi.fn()} />);

    expect(
      screen.getByRole("heading", {
        name: "Select Job Type",
      }),
    ).toBeTruthy();

    expect(screen.getByText("Step 1 of 7")).toBeTruthy();
  });

  it("shows the selected job type", () => {
    render(<JobTypeStep jobType="solving_slae" onJobTypeChange={vi.fn()} />);

    expect(screen.getByRole("combobox").textContent).toContain("solving_slae");
  });

  it("shows solving_slae as an available job type", async () => {
    render(<JobTypeStep jobType="solving_slae" onJobTypeChange={vi.fn()} />);

    fireEvent.mouseDown(screen.getByRole("combobox"));

    expect(
      await screen.findByRole("option", {
        name: "solving_slae",
      }),
    ).toBeTruthy();
  });

  it("reports the selected job type", async () => {
    const onJobTypeChange = vi.fn();

    render(<JobTypeStep jobType="" onJobTypeChange={onJobTypeChange} />);

    fireEvent.mouseDown(screen.getByRole("combobox"));

    const option = await screen.findByRole("option", {
      name: "solving_slae",
    });

    fireEvent.click(option);

    expect(onJobTypeChange).toHaveBeenCalledOnce();
    expect(onJobTypeChange).toHaveBeenCalledWith("solving_slae");
  });
});
