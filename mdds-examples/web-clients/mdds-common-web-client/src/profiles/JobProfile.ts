// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Describes a job type that can be rendered by the generic wizard.
 *
 * A profile defines required input slots, editable parameters, and available
 * output slots. The UI should be driven by this model instead of hardcoding
 * SLAE-specific screens.
 */
export interface JobProfile {
  jobType: string;
  inputSlots: InputSlotProfile[];
  params: JobParameterProfile[];
  outputSlots: OutputSlotProfile[];
}

/**
 * Describes one input artifact required or accepted by a job profile.
 *
 * The name is the logical input slot used by the REST API and manifest,
 * for example "matrix" or "rhs".
 */
export interface InputSlotProfile {
  name: string;
  format: string;
  required: boolean;
  fileName: string;
}

/**
 * Describes one editable job parameter rendered by the wizard.
 *
 * Required parameters must be provided before job submission. Optional
 * parameters may be omitted from the PATCH request.
 */
export interface JobParameterProfile {
  name: string;
  type: JobParameterType;
  required: boolean;
  defaultValue?: JobParameterValue;
  enumValues?: JobParameterValue[];
}

/**
 * Describes one output artifact that can be downloaded after a job is DONE.
 *
 * The name is the logical output slot used by the REST API and manifest,
 * for example "solution".
 */
export interface OutputSlotProfile {
  name: string;
  format: string;
  fileName: string;
}

/**
 * Supported parameter editor types for the first version of the wizard.
 */
export type JobParameterType = "string" | "number" | "boolean";

/**
 * Parameter values that can be sent to the MDDS REST API.
 */
export type JobParameterValue = string | number | boolean;
