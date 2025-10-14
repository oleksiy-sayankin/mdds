// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

document.addEventListener("DOMContentLoaded", function () {
  const solveBtn = document.getElementById("solveBtn");
  const downloadBtn = document.getElementById("downloadBtn");

  solveBtn.addEventListener("click", sendFiles);
  downloadBtn.addEventListener("click", downloadSolution);
});

export function downloadSolution() {
  if (!window.solutionBlob) return;

  const url = URL.createObjectURL(window.solutionBlob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "solution.csv";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

export async function sendFiles() {
  const matrixInput = document.getElementById("matrixFile");
  const rhsInput = document.getElementById("rhsFile");
  const solverMethod = document.getElementById("solverMethod").value;

  console.log("matrixInput = " + matrixInput);
  console.log("rhsInput = " + rhsInput);
  console.log("slaeSolvingMethod = " + solverMethod);

  if (!matrixInput.files.length || !rhsInput.files.length) {
    alert("Please select both files.");
    return;
  }

  const matrixFile = matrixInput.files[0];
  const rhsFile = rhsInput.files[0];

  console.log("matrixFile = " + matrixFile);
  console.log("rhsFile = " + rhsFile);

  const formData = new FormData();
  formData.append("matrix", matrixFile);
  formData.append("rhs", rhsFile);
  formData.append("slaeSolvingMethod", solverMethod);

  try {
    const response = await fetch("/solve", {
      method: "POST",
      body: formData,
    });

    if (response.ok) {
      const blob = await response.blob();
      window.solutionBlob = blob;
      document.getElementById("downloadBtn").disabled = false;
    } else {
      alert("Error solving system");
    }
  } catch (error) {
    console.error("Error sending files:", error);
    alert("An error occurred while sending files.");
  }
}
