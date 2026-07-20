// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Displays local input-file selections and emits file-change events.
 *
 * It does not request upload URLs or transfer file contents.
 */

import type { ChangeEvent } from "react";
import { Box, Button, Paper, Stack, Typography } from "@mui/material";

import type { InputFiles } from "../WizardState";

export interface InputsStepProps {
  files: InputFiles;
  onFileChange: (inputSlot: keyof InputFiles, file: File | null) => void;
}

interface InputFileRowProps {
  label: string;
  file: File | null;
  inputId: string;
  onChange: (file: File | null) => void;
}

function InputFileRow({
  label,
  file,
  inputId,
  onChange,
}: Readonly<InputFileRowProps>) {
  const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
    const selectedFile = event.target.files?.[0] ?? null;

    onChange(selectedFile);

    /*
     * Clear the native input value so that selecting the same file
     * again still produces a change event.
     */
    event.target.value = "";
  };

  return (
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
      <Typography
        fontWeight={500}
        sx={{
          width: {
            xs: "auto",
            sm: 90,
          },
          flexShrink: 0,
        }}
      >
        {label}
      </Typography>

      <Typography
        color={file ? "text.primary" : "text.secondary"}
        sx={{
          flexGrow: 1,
          minWidth: 0,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
        title={file?.name}
      >
        {file?.name ?? "No file selected"}
      </Typography>

      <Button
        component="label"
        variant="outlined"
        htmlFor={inputId}
        sx={{
          flexShrink: 0,
          alignSelf: {
            xs: "flex-start",
            sm: "center",
          },
        }}
      >
        Choose file
        <Box
          component="input"
          id={inputId}
          type="file"
          accept=".csv,text/csv"
          onChange={handleChange}
          sx={{
            position: "absolute",
            width: 1,
            height: 1,
            p: 0,
            m: -1,
            overflow: "hidden",
            clip: "rect(0 0 0 0)",
            whiteSpace: "nowrap",
            border: 0,
          }}
        />
      </Button>
    </Stack>
  );
}

export function InputsStep({ files, onFileChange }: Readonly<InputsStepProps>) {
  return (
    <Paper
      component="section"
      variant="outlined"
      aria-labelledby="inputs-step-title"
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
          <Typography id="inputs-step-title" component="h2" variant="h6">
            Select Job Inputs
          </Typography>

          <Typography variant="body2" color="text.secondary">
            Step 2 of 7
          </Typography>
        </Stack>

        <Stack spacing={3}>
          <InputFileRow
            label="Matrix"
            inputId="matrix-file"
            file={files.matrix}
            onChange={(file) => {
              onFileChange("matrix", file);
            }}
          />

          <InputFileRow
            label="RHS"
            inputId="rhs-file"
            file={files.rhs}
            onChange={(file) => {
              onFileChange("rhs", file);
            }}
          />
        </Stack>
      </Stack>
    </Paper>
  );
}
