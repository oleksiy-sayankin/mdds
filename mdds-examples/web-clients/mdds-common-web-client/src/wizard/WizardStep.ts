// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Defines the ordered steps of the job submission wizard.
 *
 * The order matches the intended user flow:
 * Job -> Inputs -> Upload -> Params -> Review/Submit -> Run -> Outputs.
 */

export enum WizardStep {
  Job = "job",
  Inputs = "inputs",
  Upload = "upload",
  Params = "params",
  ReviewSubmit = "review-submit",
  Run = "run",
  Outputs = "outputs",
}
