// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Visual progress indicator for the seven wizard steps.
 *
 * This component displays the current step and completed steps only.
 * It should not change wizard state by itself.
 */

import { Box, Step, StepLabel, Stepper } from "@mui/material";
import { getWizardStepIndex, WizardStep } from "@/wizard/WizardStep";

const WIZARD_STEPS = [
  "Job type",
  "Inputs",
  "Upload",
  "Parameters",
  "Review",
  "Monitor",
  "Outputs",
] as const;

export interface WizardStepperProps {
  activeStep: WizardStep;
}

export function WizardStepper({ activeStep }: Readonly<WizardStepperProps>) {
  return (
    <Box
      sx={{
        overflowX: "auto",
        px: {
          xs: 1,
          sm: 2,
        },
        py: 2,
      }}
    >
      <Stepper
        activeStep={getWizardStepIndex(activeStep)}
        sx={{
          minWidth: 680,

          "& .MuiStepLabel-label": {
            whiteSpace: "nowrap",
          },
        }}
      >
        {WIZARD_STEPS.map((label) => (
          <Step key={label}>
            <StepLabel>{label}</StepLabel>
          </Step>
        ))}
      </Stepper>
    </Box>
  );
}
