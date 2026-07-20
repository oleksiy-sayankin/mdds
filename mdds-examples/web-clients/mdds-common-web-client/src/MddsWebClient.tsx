// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

/**
 * Root component for the MDDS Common Web Client.
 *
 * It is the composition root for UI layout and infrastructure adapters.
 */
import { MockMddsRestClient } from "./api/MockMddsRestClient";
import { AppLayout } from "./app/AppLayout";
import { MockArtifactTransferClient } from "./artifacts/MockArtifactTransferClient";
import { JobWizard } from "./wizard/JobWizard";

const apiClient = new MockMddsRestClient({
  delayMs: 350,
});

const artifactTransferClient = new MockArtifactTransferClient({
  delayMs: 500,
});

export default function MddsWebClient() {
  return (
    <AppLayout>
      <JobWizard
        apiClient={apiClient}
        artifactTransferClient={artifactTransferClient}
        onJobTypeSelected={(jobType) => {
          console.info("Selected job type:", jobType);
        }}
        onInputsSelected={(files) => {
          console.info("Selected matrix:", files.matrix.name);
          console.info("Selected RHS:", files.rhs.name);
        }}
        onInputsUploaded={() => {
          console.info("All required inputs are uploaded.");
        }}
        onParametersSynchronized={(solvingMethod) => {
          console.info("Synchronized solving method:", solvingMethod);
        }}
        onJobSubmitted={() => {
          console.info("The job was submitted successfully.");
        }}
        onJobCancellationAccepted={() => {
          console.info("Job cancellation was accepted.");
        }}
        onOutputsRequested={() => {
          console.info("The user opened job outputs.");
        }}
        onOutputDownloaded={(fileName) => {
          console.info("Downloaded output:", fileName);
        }}
        onNewJobStarted={() => {
          console.info("A new wizard workflow was started.");
        }}
      />
    </AppLayout>
  );
}
