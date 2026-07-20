// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Lets the user select the supported job type.
 */

import {
  Box,
  FormControl,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  type SelectChangeEvent,
  Stack,
  Typography,
} from "@mui/material";

export interface JobTypeStepProps {
  jobType: string;
  onJobTypeChange: (jobType: string) => void;
}

export function JobTypeStep({
  jobType,
  onJobTypeChange,
}: Readonly<JobTypeStepProps>) {
  const handleChange = (event: SelectChangeEvent<string>) => {
    onJobTypeChange(event.target.value);
  };

  return (
    <Paper
      component="section"
      variant="outlined"
      aria-labelledby="job-step-title"
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
          <Typography id="job-step-title" component="h2" variant="h6">
            Select Job Type
          </Typography>

          <Typography variant="body2" color="text.secondary">
            Step 1 of 7
          </Typography>
        </Stack>

        <Box sx={{ maxWidth: 360 }}>
          <FormControl fullWidth required>
            <InputLabel id="job-type-label">Job Type</InputLabel>

            <Select
              labelId="job-type-label"
              id="job-type"
              value={jobType}
              label="Job Type"
              onChange={handleChange}
            >
              <MenuItem value="solving_slae">solving_slae</MenuItem>
            </Select>
          </FormControl>
        </Box>
      </Stack>
    </Paper>
  );
}
