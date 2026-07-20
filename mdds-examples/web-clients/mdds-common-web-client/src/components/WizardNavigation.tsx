// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Renders the shared backward and primary forward wizard actions.
 *
 * Action labels and availability are provided by the wizard orchestrator.
 */

import { Box, Button, Stack } from "@mui/material";

export interface WizardNavigationProps {
  previousDisabled?: boolean;
  nextDisabled?: boolean;
  nextLabel?: string;
  showPrevious?: boolean;
  onPrevious: () => void;
  onNext: () => void;
  showNext?: boolean;
}

export function WizardNavigation({
  previousDisabled = false,
  nextDisabled = false,
  nextLabel = "Next >",
  showPrevious = true,
  onPrevious,
  onNext,
  showNext = true,
}: Readonly<WizardNavigationProps>) {
  return (
    <Stack
      direction="row"
      justifyContent="space-between"
      alignItems="center"
      spacing={2}
    >
      {showPrevious ? (
        <Button
          type="button"
          variant="text"
          disabled={previousDisabled}
          onClick={onPrevious}
        >
          {"< Previous"}
        </Button>
      ) : (
        <Box />
      )}

      {showNext ? (
        <Button
          type="button"
          variant="contained"
          disabled={nextDisabled}
          onClick={onNext}
        >
          {nextLabel}
        </Button>
      ) : (
        <Box />
      )}
    </Stack>
  );
}
