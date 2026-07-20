// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/** @vitest-environment jsdom */

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { InputsStep } from "./InputsStep";

afterEach(() => {
  cleanup();
});

describe("InputsStep", () => {
  it("shows placeholders when no files are selected", () => {
    render(
      <InputsStep
        files={{
          matrix: null,
          rhs: null,
        }}
        onFileChange={vi.fn()}
      />,
    );

    expect(screen.getAllByText("No file selected")).toHaveLength(2);
  });

  it("shows the selected matrix and RHS file names", () => {
    render(
      <InputsStep
        files={{
          matrix: new File(["1,0\n0,1"], "matrix.csv", {
            type: "text/csv",
          }),
          rhs: new File(["1\n2"], "rhs.csv", {
            type: "text/csv",
          }),
        }}
        onFileChange={vi.fn()}
      />,
    );

    expect(screen.getByText("matrix.csv")).toBeTruthy();
    expect(screen.getByText("rhs.csv")).toBeTruthy();
  });

  it("reports the selected matrix file", () => {
    const onFileChange = vi.fn();
    const matrixFile = new File(["1,0\n0,1"], "matrix.csv", {
      type: "text/csv",
    });

    render(
      <InputsStep
        files={{
          matrix: null,
          rhs: null,
        }}
        onFileChange={onFileChange}
      />,
    );

    const matrixInput = document.getElementById(
      "matrix-file",
    ) as HTMLInputElement;

    fireEvent.change(matrixInput, {
      target: {
        files: [matrixFile],
      },
    });

    expect(onFileChange).toHaveBeenCalledOnce();
    expect(onFileChange).toHaveBeenCalledWith("matrix", matrixFile);
  });

  it("reports the selected RHS file", () => {
    const onFileChange = vi.fn();
    const rhsFile = new File(["1\n2"], "rhs.csv", {
      type: "text/csv",
    });

    render(
      <InputsStep
        files={{
          matrix: null,
          rhs: null,
        }}
        onFileChange={onFileChange}
      />,
    );

    const rhsInput = document.getElementById("rhs-file") as HTMLInputElement;

    fireEvent.change(rhsInput, {
      target: {
        files: [rhsFile],
      },
    });

    expect(onFileChange).toHaveBeenCalledOnce();
    expect(onFileChange).toHaveBeenCalledWith("rhs", rhsFile);
  });
});
