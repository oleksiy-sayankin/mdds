// Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

import { downloadSolution, sendFiles } from "../index.js";

// Mock global functions
global.alert = jest.fn();
global.fetch = jest.fn();

describe("index.js tests", () => {
  beforeEach(() => {
    // Clear DOM before each test
    document.body.innerHTML = `
      <input type="file" id="matrixFile" />
      <input type="file" id="rhsFile" />
      <select id="solverMethod">
        <option value="numpy_exact_solver" selected>numpy exact solver</option>
      </select>
      <button id="solveBtn"></button>
      <button id="downloadBtn" disabled></button>
    `;
    jest.clearAllMocks();
    delete window.solutionBlob;
    global.URL.createObjectURL = jest.fn(() => "blob:mock-url");
    global.URL.revokeObjectURL = jest.fn();
  });

  test("sendFiles shows alert if matrix file is missing", async () => {
    const rhsInput = document.getElementById("rhsFile");
    Object.defineProperty(rhsInput, "files", {
      value: [new File(["1"], "rhs.csv")],
    });

    await sendFiles();
    expect(global.alert).toHaveBeenCalledWith("Please select both files.");
  });

  test("sendFiles shows alert if rhs file is missing", async () => {
    const matrixInput = document.getElementById("matrixFile");
    Object.defineProperty(matrixInput, "files", {
      value: [new File(["1"], "matrix.csv")],
    });

    await sendFiles();
    expect(global.alert).toHaveBeenCalledWith("Please select both files.");
  });

  test("sendFiles enables downloadBtn when response ok", async () => {
    const matrixInput = document.getElementById("matrixFile");
    Object.defineProperty(matrixInput, "files", {
      value: [new File(["1"], "matrix.csv")],
    });

    const rhsInput = document.getElementById("rhsFile");
    Object.defineProperty(rhsInput, "files", {
      value: [new File(["1"], "rhs.csv")],
    });

    const blobMock = new Blob(["solution"], { type: "text/csv" });
    global.fetch.mockResolvedValueOnce({
      ok: true,
      blob: () => Promise.resolve(blobMock),
    });

    await sendFiles();
    expect(document.getElementById("downloadBtn").disabled).toBe(false);
    expect(window.solutionBlob).toBe(blobMock);
  });

  test("downloadSolution does nothing if no solutionBlob", () => {
    expect(() => downloadSolution()).not.toThrow();
  });
});
