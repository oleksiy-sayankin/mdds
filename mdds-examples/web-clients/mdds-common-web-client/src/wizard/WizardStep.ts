// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Defines the ordered steps of the job submission wizard.
 *
 * The order matches the intended user flow:
 * Job -> Inputs -> Upload -> Params -> Review/Submit -> Run -> Outputs.
 */

export enum WizardStep {
  JobType = "job-type",
  Inputs = "inputs",
  Upload = "upload",
  Parameters = "parameters",
  Review = "review",
  Monitor = "monitor",
  Outputs = "outputs",
}

export const WIZARD_STEP_ORDER = [
  WizardStep.JobType,
  WizardStep.Inputs,
  WizardStep.Upload,
  WizardStep.Parameters,
  WizardStep.Review,
  WizardStep.Monitor,
  WizardStep.Outputs,
] as const;

export function getWizardStepIndex(step: WizardStep): number {
  return WIZARD_STEP_ORDER.indexOf(step);
}
