// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/** @vitest-environment jsdom */

import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  act,
  renderHook,
} from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  InputSlotState,
  InputSlotStates,
  UploadManagerState,
} from "../WizardState";
import { UploadStep } from "./UploadStep";
import { useUploadWorkflow } from "@/wizard/useJobWizardWorkflows";
import type { MddsApiClient } from "@/api/MddsApiClient";
import type { ArtifactTransferClient } from "@/artifacts/ArtifactTransferClient";

const files = {
  matrix: new File(["1,0\n0,1"], "matrix.csv", {
    type: "text/csv",
  }),
  rhs: new File(["1\n2"], "rhs.csv", {
    type: "text/csv",
  }),
};

async function rejectUnexpectedCall(): Promise<never> {
  throw new Error("Unexpected client method call.");
}

function createApiClient(
  overrides: Partial<MddsApiClient> = {},
): MddsApiClient {
  return {
    createOrReuseDraftJob: vi
      .fn<MddsApiClient["createOrReuseDraftJob"]>()
      .mockResolvedValue({
        jobId: "job-1",
      }),

    requestInputUploadUrl: vi
      .fn<MddsApiClient["requestInputUploadUrl"]>()
      .mockImplementation(async (_jobId, inputSlot) => ({
        jobId: "job-1",
        uploadUrl: `mock://upload/${inputSlot}`,
        expiresAt: "2026-07-20T12:15:00.000Z",
      })),

    patchJobParams: vi
      .fn<MddsApiClient["patchJobParams"]>()
      .mockImplementation(rejectUnexpectedCall),

    submitJob: vi
      .fn<MddsApiClient["submitJob"]>()
      .mockImplementation(rejectUnexpectedCall),

    getJobStatus: vi
      .fn<MddsApiClient["getJobStatus"]>()
      .mockImplementation(rejectUnexpectedCall),

    cancelJob: vi
      .fn<MddsApiClient["cancelJob"]>()
      .mockImplementation(rejectUnexpectedCall),

    requestOutputDownloadUrl: vi
      .fn<MddsApiClient["requestOutputDownloadUrl"]>()
      .mockImplementation(rejectUnexpectedCall),

    ...overrides,
  };
}

function createArtifactTransferClient(
  overrides: Partial<ArtifactTransferClient> = {},
): ArtifactTransferClient {
  return {
    upload: vi
      .fn<ArtifactTransferClient["upload"]>()
      .mockResolvedValue(undefined),

    download: vi.fn<ArtifactTransferClient["download"]>().mockResolvedValue(
      new Blob(["solution"], {
        type: "text/csv",
      }),
    ),

    ...overrides,
  };
}

function renderUploadStep({
  inputSlotStates = {
    matrix: "FILE_SELECTED",
    rhs: "FILE_SELECTED",
  },
  uploadManagerState = "IDLE",
  onStopUploading = vi.fn(),
  onRetryFailedUploads = vi.fn(),
}: {
  inputSlotStates?: InputSlotStates;
  uploadManagerState?: UploadManagerState;
  onStopUploading?: () => void;
  onRetryFailedUploads?: () => void;
} = {}) {
  return render(
    <UploadStep
      files={files}
      inputSlotStates={inputSlotStates}
      uploadManagerState={uploadManagerState}
      onStopUploading={onStopUploading}
      onRetry={onRetryFailedUploads}
    />,
  );
}

afterEach(() => {
  cleanup();
});

describe("UploadStep", () => {
  it.each<{
    state: InputSlotState;
    managerState: UploadManagerState;
    expectedLabel: string;
  }>([
    {
      state: "FILE_SELECTED",
      managerState: "IDLE",
      expectedLabel: "Ready",
    },
    {
      state: "UPLOADING",
      managerState: "RUNNING",
      expectedLabel: "Uploading...",
    },
    {
      state: "UPLOADED",
      managerState: "COMPLETED",
      expectedLabel: "Uploaded",
    },
    {
      state: "FAILED",
      managerState: "FAILED",
      expectedLabel: "Failed",
    },
  ])(
    "shows $expectedLabel for $state",
    ({ state, managerState, expectedLabel }) => {
      renderUploadStep({
        inputSlotStates: {
          matrix: state,
          rhs: state,
        },
        uploadManagerState: managerState,
      });

      expect(screen.getAllByText(expectedLabel)).toHaveLength(2);
    },
  );

  it("enables Stop uploading only while upload is running", () => {
    const { rerender } = render(
      <UploadStep
        files={files}
        inputSlotStates={{
          matrix: "UPLOADING",
          rhs: "FILE_SELECTED",
        }}
        uploadManagerState="RUNNING"
        onStopUploading={vi.fn()}
        onRetry={vi.fn()}
      />,
    );

    const stopButton = screen.getByRole("button", {
      name: "Stop uploading",
    }) as HTMLButtonElement;

    expect(stopButton.disabled).toBe(false);

    rerender(
      <UploadStep
        files={files}
        inputSlotStates={{
          matrix: "UPLOADED",
          rhs: "UPLOADED",
        }}
        uploadManagerState="COMPLETED"
        onStopUploading={vi.fn()}
        onRetry={vi.fn()}
      />,
    );

    expect(
      (
        screen.getByRole("button", {
          name: "Stop uploading",
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(true);
  });

  it("retries the upload workflow after an input upload fails", () => {
    const onRetry = vi.fn();

    renderUploadStep({
      inputSlotStates: {
        matrix: "FILE_SELECTED",
        rhs: "FILE_SELECTED",
      },
      uploadManagerState: "FAILED",
      onRetryFailedUploads: onRetry,
    });

    const retryButton = screen.getByRole("button", {
      name: "Retry",
    }) as HTMLButtonElement;

    expect(retryButton.disabled).toBe(false);

    fireEvent.click(retryButton);

    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it("continues the upload queue after a transfer fails and retries only the failed input", async () => {
    const apiClient = createApiClient();

    const upload = vi
      .fn<ArtifactTransferClient["upload"]>()
      .mockRejectedValueOnce(new Error("Upload failed."))
      .mockResolvedValue(undefined);

    const artifactTransferClient = createArtifactTransferClient({
      upload,
    });

    const { result } = renderHook(() =>
      useUploadWorkflow({
        apiClient,
        artifactTransferClient,
        jobType: "solving_slae",
        uploadSessionId: "session-1",
        files,
      }),
    );

    act(() => {
      result.current.selectFile("matrix", files.matrix);
      result.current.selectFile("rhs", files.rhs);
    });

    await act(async () => {
      await result.current.startUpload();
    });

    expect(result.current.jobId).toBe("job-1");
    expect(result.current.uploadManagerState).toBe("FAILED");

    expect(result.current.inputSlotStates).toEqual({
      matrix: "FAILED",
      rhs: "UPLOADED",
    });

    expect(upload).toHaveBeenCalledTimes(2);

    expect(apiClient.requestInputUploadUrl).toHaveBeenNthCalledWith(
      1,
      "job-1",
      "matrix",
    );

    expect(apiClient.requestInputUploadUrl).toHaveBeenNthCalledWith(
      2,
      "job-1",
      "rhs",
    );

    act(() => {
      result.current.retryUpload();
    });

    await waitFor(() => {
      expect(result.current.uploadManagerState).toBe("COMPLETED");
    });

    expect(result.current.inputSlotStates).toEqual({
      matrix: "UPLOADED",
      rhs: "UPLOADED",
    });

    expect(apiClient.createOrReuseDraftJob).toHaveBeenCalledTimes(2);

    expect(apiClient.createOrReuseDraftJob).toHaveBeenNthCalledWith(
      1,
      "solving_slae",
      "session-1",
    );

    expect(apiClient.createOrReuseDraftJob).toHaveBeenNthCalledWith(
      2,
      "solving_slae",
      "session-1",
    );

    expect(apiClient.requestInputUploadUrl).toHaveBeenNthCalledWith(
      3,
      "job-1",
      "matrix",
    );

    expect(upload).toHaveBeenCalledTimes(3);
  });

  it("opens and closes the stop-upload dialog", async () => {
    renderUploadStep({
      inputSlotStates: {
        matrix: "UPLOADING",
        rhs: "FILE_SELECTED",
      },
      uploadManagerState: "RUNNING",
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Stop uploading",
      }),
    );

    expect(
      screen.getByRole("dialog", {
        name: "Stop uploading?",
      }),
    ).toBeTruthy();

    fireEvent.click(
      screen.getByRole("button", {
        name: "Continue uploading",
      }),
    );

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });
  });

  it("calls onStopUploading after confirmation", async () => {
    const onStopUploading = vi.fn();

    renderUploadStep({
      inputSlotStates: {
        matrix: "UPLOADING",
        rhs: "FILE_SELECTED",
      },
      uploadManagerState: "RUNNING",
      onStopUploading,
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Stop uploading",
      }),
    );

    fireEvent.click(
      screen.getByRole("button", {
        name: "Stop upload",
      }),
    );

    expect(onStopUploading).toHaveBeenCalledOnce();

    await waitFor(() => {
      expect(screen.queryByRole("dialog")).toBeNull();
    });
  });

  it("calls onRetryFailedUploads", () => {
    const onRetryFailedUploads = vi.fn();

    renderUploadStep({
      inputSlotStates: {
        matrix: "FAILED",
        rhs: "UPLOADED",
      },
      uploadManagerState: "FAILED",
      onRetryFailedUploads,
    });

    fireEvent.click(
      screen.getByRole("button", {
        name: "Retry",
      }),
    );

    expect(onRetryFailedUploads).toHaveBeenCalledOnce();
  });
});
