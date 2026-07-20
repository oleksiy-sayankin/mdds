// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Displays the SLAE solving-method selection and synchronization state.
 *
 * Parameter persistence is owned by the parameter workflow.
 */

import {
  Box,
  Button,
  Chip,
  CircularProgress,
  FormControl,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  type SelectChangeEvent,
  Stack,
  Typography,
} from "@mui/material";

import {
  SOLVING_METHODS,
  type ParameterUpdateState,
  type SolvingMethod,
} from "../WizardState";

export interface ParametersStepProps {
  solvingMethod: SolvingMethod;
  parameterUpdateState: ParameterUpdateState;
  onSolvingMethodChange: (solvingMethod: SolvingMethod) => void;
  onRetryParameterUpdate: () => void;
}

function ParameterUpdateStatus({
  state,
}: Readonly<{
  state: ParameterUpdateState;
}>) {
  switch (state) {
    case "UPDATING":
      return (
        <Stack direction="row" alignItems="center" spacing={1}>
          <CircularProgress size={18} />

          <Typography variant="body2">Updating...</Typography>
        </Stack>
      );

    case "UPDATED":
      return (
        <Chip
          label="Synchronized"
          size="small"
          color="success"
          variant="outlined"
        />
      );

    case "FAILED":
      return (
        <Chip
          label="Update failed"
          size="small"
          color="error"
          variant="outlined"
        />
      );

    case "PENDING":
    default:
      return <Chip label="Pending" size="small" variant="outlined" />;
  }
}

export function ParametersStep({
  solvingMethod,
  parameterUpdateState,
  onSolvingMethodChange,
  onRetryParameterUpdate,
}: Readonly<ParametersStepProps>) {
  const handleChange = (event: SelectChangeEvent<SolvingMethod>) => {
    onSolvingMethodChange(event.target.value as SolvingMethod);
  };

  return (
    <Paper
      component="section"
      variant="outlined"
      aria-labelledby="params-step-title"
      sx={{
        minHeight: 260,
        p: {
          xs: 2,
          sm: 3,
        },
      }}
    >
      <Stack spacing={4}>
        <Stack
          direction={{
            xs: "column",
            sm: "row",
          }}
          justifyContent="space-between"
          alignItems={{
            xs: "flex-start",
            sm: "center",
          }}
          spacing={1}
        >
          <Typography id="params-step-title" component="h2" variant="h6">
            Set Job Parameters
          </Typography>

          <Typography variant="body2" color="text.secondary">
            Step 4 of 7
          </Typography>
        </Stack>

        <Stack
          direction={{
            xs: "column",
            sm: "row",
          }}
          alignItems={{
            xs: "stretch",
            sm: "center",
          }}
          spacing={2}
        >
          <Box
            sx={{
              width: {
                xs: "100%",
                sm: 360,
              },
            }}
          >
            <FormControl fullWidth required>
              <InputLabel id="solving-method-label">Solving method</InputLabel>

              <Select
                labelId="solving-method-label"
                id="solving-method"
                value={solvingMethod}
                label="Solving method"
                disabled={parameterUpdateState === "UPDATING"}
                onChange={handleChange}
              >
                {SOLVING_METHODS.map((method) => (
                  <MenuItem key={method} value={method}>
                    {method}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          </Box>

          <ParameterUpdateStatus state={parameterUpdateState} />
        </Stack>

        {parameterUpdateState === "FAILED" && (
          <Box>
            <Button
              type="button"
              variant="outlined"
              onClick={onRetryParameterUpdate}
            >
              Retry update
            </Button>
          </Box>
        )}
      </Stack>
    </Paper>
  );
}
