// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

import { describe, expect, it } from "vitest";
import { WizardStep } from "./WizardStep";
import {
  canGoBackFromUpload,
  getNextLabel,
  getPreviousStep,
  isNextDisabled,
  isSubmissionInProgress,
} from "./WizardValidation";

import type {
  PublicJobStatus,
  SubmissionState,
  UploadManagerState,
  WizardState,
} from "./WizardState";

function createWizardState(overrides: Partial<WizardState> = {}): WizardState {
  return {
    activeStep: WizardStep.JobType,
    jobType: "solving_slae",
    allRequiredFilesSelected: true,
    uploadManagerState: "COMPLETED",
    allRequiredFilesUploaded: true,
    parameterUpdateState: "UPDATED",
    submissionState: "IDLE",
    jobStatus: null,
    monitorState: "IDLE",
    cancellationState: "IDLE",
    downloadState: "IDLE",
    ...overrides,
  };
}

describe("isSubmissionInProgress", () => {
  it.each<SubmissionState>(["CONFIRMING", "REQUESTING", "RECONCILING"])(
    "returns true for %s",
    (submissionState) => {
      expect(isSubmissionInProgress(submissionState)).toBe(true);
    },
  );

  it.each<SubmissionState>(["IDLE", "UNCONFIRMED", "SUBMITTED", "FAILED"])(
    "returns false for %s",
    (submissionState) => {
      expect(isSubmissionInProgress(submissionState)).toBe(false);
    },
  );
});

describe("canGoBackFromUpload", () => {
  it.each<UploadManagerState>([
    "IDLE",
    "COMPLETED",
    "FAILED",
    "STOPPED_BY_USER",
  ])("returns true for %s", (uploadManagerState) => {
    expect(canGoBackFromUpload(uploadManagerState)).toBe(true);
  });

  it("returns false while upload is running", () => {
    expect(canGoBackFromUpload("RUNNING")).toBe(false);
  });
});

describe("getPreviousStep", () => {
  it("returns JobType from Inputs", () => {
    const state = createWizardState({
      activeStep: WizardStep.Inputs,
    });

    expect(getPreviousStep(state)).toBe(WizardStep.JobType);
  });

  it.each<UploadManagerState>([
    "IDLE",
    "COMPLETED",
    "FAILED",
    "STOPPED_BY_USER",
  ])("returns Inputs from Upload when state is %s", (uploadManagerState) => {
    const state = createWizardState({
      activeStep: WizardStep.Upload,
      uploadManagerState,
    });

    expect(getPreviousStep(state)).toBe(WizardStep.Inputs);
  });

  it("does not allow leaving Upload while upload is running", () => {
    const state = createWizardState({
      activeStep: WizardStep.Upload,
      uploadManagerState: "RUNNING",
    });

    expect(getPreviousStep(state)).toBeNull();
  });

  it.each(["PENDING", "UPDATED", "FAILED"] as const)(
    "returns Upload from Parameters when parameter state is %s",
    (parameterUpdateState) => {
      const state = createWizardState({
        activeStep: WizardStep.Parameters,
        parameterUpdateState,
      });

      expect(getPreviousStep(state)).toBe(WizardStep.Upload);
    },
  );

  it("does not allow leaving Parameters while update is running", () => {
    const state = createWizardState({
      activeStep: WizardStep.Parameters,
      parameterUpdateState: "UPDATING",
    });

    expect(getPreviousStep(state)).toBeNull();
  });

  it.each(["IDLE", "FAILED"] as const)(
    "returns Parameters from Review when submission state is %s",
    (submissionState) => {
      const state = createWizardState({
        activeStep: WizardStep.Review,
        submissionState,
      });

      expect(getPreviousStep(state)).toBe(WizardStep.Parameters);
    },
  );

  it.each([
    "CONFIRMING",
    "REQUESTING",
    "RECONCILING",
    "SUBMITTED",
    "UNCONFIRMED",
  ] as const)(
    "does not allow leaving Review when submission state is %s",
    (submissionState) => {
      const state = createWizardState({
        activeStep: WizardStep.Review,
        submissionState,
      });

      expect(getPreviousStep(state)).toBeNull();
    },
  );

  it.each([WizardStep.JobType, WizardStep.Monitor, WizardStep.Outputs])(
    "returns null for %s",
    (activeStep) => {
      const state = createWizardState({ activeStep });

      expect(getPreviousStep(state)).toBeNull();
    },
  );
});

describe("isNextDisabled", () => {
  it("disables JobType when no job type is selected", () => {
    expect(
      isNextDisabled(
        createWizardState({
          activeStep: WizardStep.JobType,
          jobType: "",
        }),
      ),
    ).toBe(true);
  });

  it("enables JobType when a job type is selected", () => {
    expect(
      isNextDisabled(
        createWizardState({
          activeStep: WizardStep.JobType,
        }),
      ),
    ).toBe(false);
  });

  it("disables Inputs when required files are missing", () => {
    expect(
      isNextDisabled(
        createWizardState({
          activeStep: WizardStep.Inputs,
          allRequiredFilesSelected: false,
        }),
      ),
    ).toBe(true);
  });

  it("enables Inputs when all required files are selected", () => {
    expect(
      isNextDisabled(
        createWizardState({
          activeStep: WizardStep.Inputs,
        }),
      ),
    ).toBe(false);
  });

  it.each([
    {
      uploadManagerState: "RUNNING" as const,
      allRequiredFilesUploaded: true,
    },
    {
      uploadManagerState: "COMPLETED" as const,
      allRequiredFilesUploaded: false,
    },
    {
      uploadManagerState: "FAILED" as const,
      allRequiredFilesUploaded: false,
    },
  ])(
    "disables Upload for manager=$uploadManagerState uploaded=$allRequiredFilesUploaded",
    ({ uploadManagerState, allRequiredFilesUploaded }) => {
      const state = createWizardState({
        activeStep: WizardStep.Upload,
        uploadManagerState,
        allRequiredFilesUploaded,
      });

      expect(isNextDisabled(state)).toBe(true);
    },
  );

  it("enables Upload only after all files are uploaded", () => {
    const state = createWizardState({
      activeStep: WizardStep.Upload,
      uploadManagerState: "COMPLETED",
      allRequiredFilesUploaded: true,
    });

    expect(isNextDisabled(state)).toBe(false);
  });

  it.each(["PENDING", "UPDATING", "FAILED"] as const)(
    "disables Parameters when state is %s",
    (parameterUpdateState) => {
      const state = createWizardState({
        activeStep: WizardStep.Parameters,
        parameterUpdateState,
      });

      expect(isNextDisabled(state)).toBe(true);
    },
  );

  it("enables Parameters when synchronization completed", () => {
    const state = createWizardState({
      activeStep: WizardStep.Parameters,
      parameterUpdateState: "UPDATED",
    });

    expect(isNextDisabled(state)).toBe(false);
  });

  it.each(["CONFIRMING", "REQUESTING", "RECONCILING", "SUBMITTED"] as const)(
    "disables Review when submission state is %s",
    (submissionState) => {
      const state = createWizardState({
        activeStep: WizardStep.Review,
        submissionState,
      });

      expect(isNextDisabled(state)).toBe(true);
    },
  );

  it.each(["IDLE", "FAILED", "UNCONFIRMED"] as const)(
    "enables Review when submission state is %s",
    (submissionState) => {
      const state = createWizardState({
        activeStep: WizardStep.Review,
        submissionState,
      });

      expect(isNextDisabled(state)).toBe(false);
    },
  );

  it.each<PublicJobStatus | null>([
    null,
    "SUBMITTED",
    "INPUTS_PREPARED",
    "IN_PROGRESS",
    "ERROR",
  ])("disables Monitor when job status is %s", (jobStatus) => {
    const state = createWizardState({
      activeStep: WizardStep.Monitor,
      jobStatus,
    });

    expect(isNextDisabled(state)).toBe(true);
  });

  it.each(["DONE", "CANCELLED"] as const)(
    "enables Monitor when job status is %s",
    (jobStatus) => {
      const state = createWizardState({
        activeStep: WizardStep.Monitor,
        jobStatus,
      });

      expect(isNextDisabled(state)).toBe(false);
    },
  );

  it("always enables Outputs", () => {
    const state = createWizardState({
      activeStep: WizardStep.Outputs,
    });

    expect(isNextDisabled(state)).toBe(false);
  });
});

describe("getNextLabel", () => {
  it.each([
    WizardStep.JobType,
    WizardStep.Inputs,
    WizardStep.Upload,
    WizardStep.Parameters,
  ])("returns Next > for %s", (activeStep) => {
    const state = createWizardState({ activeStep });

    expect(getNextLabel(state)).toBe("Next >");
  });

  it("returns Submit job for Review", () => {
    const state = createWizardState({
      activeStep: WizardStep.Review,
    });

    expect(getNextLabel(state)).toBe("Submit job");
  });

  it("returns View outputs > for a completed job", () => {
    const state = createWizardState({
      activeStep: WizardStep.Monitor,
      jobStatus: "DONE",
    });

    expect(getNextLabel(state)).toBe("View outputs >");
  });

  it("returns Start new job for a cancelled job", () => {
    const state = createWizardState({
      activeStep: WizardStep.Monitor,
      jobStatus: "CANCELLED",
    });

    expect(getNextLabel(state)).toBe("Start new job");
  });

  it("returns View outputs > for other monitor states", () => {
    const state = createWizardState({
      activeStep: WizardStep.Monitor,
      jobStatus: "IN_PROGRESS",
    });

    expect(getNextLabel(state)).toBe("View outputs >");
  });

  it("returns Start new job for Outputs", () => {
    const state = createWizardState({
      activeStep: WizardStep.Outputs,
    });

    expect(getNextLabel(state)).toBe("Start new job");
  });

  it("returns Check submission status for UNCONFIRMED", () => {
    const state = createWizardState({
      activeStep: WizardStep.Review,
      submissionState: "UNCONFIRMED",
    });

    expect(getNextLabel(state)).toBe("Check submission status");
  });
});
