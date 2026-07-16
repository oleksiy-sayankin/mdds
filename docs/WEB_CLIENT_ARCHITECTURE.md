<!--
Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
Refer to the LICENSE file in the root directory for full license details.
-->

<!-- TOC -->
* [MDDS Web Client Architecture Specification](#mdds-web-client-architecture-specification)
  * [1. Purpose, scope, and assumptions](#1-purpose-scope-and-assumptions)
  * [2. Document conventions and normative sources](#2-document-conventions-and-normative-sources)
  * [3. Client terminology](#3-client-terminology)
  * [4. Client state composition and ownership](#4-client-state-composition-and-ownership)
  * [5. Global client invariants](#5-global-client-invariants)
  * [6. Common client policies](#6-common-client-policies)
    * [6.1 Asynchronous operation ownership](#61-asynchronous-operation-ownership)
    * [6.2 Request serialization](#62-request-serialization)
    * [6.3 Automatic retry](#63-automatic-retry)
    * [6.4 Operation reconciliation](#64-operation-reconciliation)
    * [6.5 User-initiated cancellation](#65-user-initiated-cancellation)
    * [6.6 Stale and late response handling](#66-stale-and-late-response-handling)
    * [6.7 Navigation locking](#67-navigation-locking)
    * [6.8 Warning and error presentation](#68-warning-and-error-presentation)
    * [6.9 Workflow reset and abandonment](#69-workflow-reset-and-abandonment)
  * [7. Network operation catalog](#7-network-operation-catalog)
  * [8. Client state machines](#8-client-state-machines)
    * [8.1 Wizard Navigation](#81-wizard-navigation)
    * [8.2 Draft Job Creation](#82-draft-job-creation)
    * [8.3 Upload Manager](#83-upload-manager)
    * [8.4 Input Slot](#84-input-slot)
    * [8.5 Job Parameter Update](#85-job-parameter-update)
    * [8.6 Job Submission](#86-job-submission)
    * [8.7 Job Monitor](#87-job-monitor)
    * [8.8 Job Cancellation](#88-job-cancellation)
    * [8.9 Output Download](#89-output-download)
  * [9. Screen-to-state mapping](#9-screen-to-state-mapping)
    * [9.1 Screen 1: Select Job Type](#91-screen-1-select-job-type)
    * [9.2 Screen 2: Select Job Inputs](#92-screen-2-select-job-inputs)
    * [9.3 Screen 3: Upload Job Inputs](#93-screen-3-upload-job-inputs)
    * [9.4 Screen 4: Set Job Parameters](#94-screen-4-set-job-parameters)
    * [9.5 Screen 5: Review Job Summary](#95-screen-5-review-job-summary)
    * [9.6 Screen 6: Monitor Job Progress](#96-screen-6-monitor-job-progress)
    * [9.7 Screen 7: Download Job Outputs](#97-screen-7-download-job-outputs)
  * [10. Acceptance scenarios](#10-acceptance-scenarios)
  * [11. Deferred decisions and out-of-scope behavior](#11-deferred-decisions-and-out-of-scope-behavior)
<!-- TOC -->


# MDDS Web Client Architecture Specification

## 1. Purpose, scope, and assumptions

- The MDDS Web Client v1 renders SLAE-specific screens.
- All input and output slots declared by a job profile are mandatory.
- Optional artifact slots are not supported.
- The `tolerance` parameter is illustrative and ignored by SLAE execution in v1.
- Restoration of an in-progress wizard after a page reload is out of scope.
- Job cancellation is supported only while the public job status is `IN_PROGRESS`.
- Output download is available only while the public job status is `DONE`.
- An abandoned `DRAFT` job may remain on the server because draft deletion is outside the v1 API.

## 2. Document conventions and normative sources

- Global client invariants are normative.
- State transition tables are normative for local client behavior.
- The network operation catalog is normative for request semantics and recovery strategy.
- The screen-to-state mapping is normative for control visibility and availability.
- State diagrams are explanatory visualizations of the corresponding transition tables.
- Screen sketches and wireframes are illustrative.
- User-visible messages are examples unless explicitly marked as exact.
- A rule must be defined in one authoritative section. Other sections should reference that rule instead of redefining it.

## 3. Client terminology

| Term                   | Meaning                                                                                                       |
|------------------------|---------------------------------------------------------------------------------------------------------------|
| `jobStatus`            | The last confirmed public job status obtained from or accepted by the MDDS orchestrator.                      |
| `wizardStep`           | The currently displayed wizard step.                                                                          |
| `draftCreationState`   | Local state of the draft job creation workflow.                                                               |
| `uploadManagerState`   | Local state of the current upload queue run.                                                                  |
| `inputSlotState`       | Local state of one declared input slot.                                                                       |
| `parameterUpdateState` | Local state of synchronization between locally edited job parameters and the current server-side `DRAFT` job. |
| `submissionState`      | Local state of the job submission workflow.                                                                   |
| `monitorState`         | Local state of the job monitoring workflow.                                                                   |
| `cancellationState`    | Local state of the job cancellation workflow.                                                                 |
| `downloadState`        | Local state of one output download workflow.                                                                  |

## 4. Client state composition and ownership

The client state machines are orthogonal and may have active states at the same time.
For example, the wizard may display the monitoring screen while the public job status is `IN_PROGRESS`, the Job Monitor is `RUNNING`, and Job Cancellation is `RECONCILING`.

The public job lifecycle is owned by the MDDS orchestrator and Worker Runtime and is defined by [MDDS Job Orchestrator Architecture](JOB_ORCHESTRATOR_ARCHITECTURE.md).
The Web Client does not redefine that lifecycle. It stores and renders the last confirmed public `jobStatus` and manages only its local client workflow states.

| State machine        |      Instance count | Scope                                      |
|----------------------|--------------------:|--------------------------------------------|
| Wizard Navigation    |                   1 | Current wizard flow                        |
| Draft Job Creation   |                   1 | Current draft creation workflow            |
| Upload Manager       |                   1 | Current upload queue run                   |
| Input Slot           |  One per input slot | `matrix`, `rhs` in SLAE v1                 |
| Job Parameter Update |                   1 | Current parameter synchronization workflow |
| Job Submission       |                   1 | Current submission workflow                |
| Job Monitor          |                   1 | Current job monitoring workflow            |
| Job Cancellation     |                   1 | Current cancellation workflow              |
| Output Download      | One per output slot | `solution` in SLAE v1                      |

## 5. Global client invariants

| Invariant code | Content                                                                                                                                                                                          |
|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `I1`           | One wizard flow has at most one current `jobId`.                                                                                                                                                 |
| `I2`           | An `UPLOADED` input slot always belongs to the current `jobId`.                                                                                                                                  |
| `I3`           | Every input slot declared by the selected job profile is mandatory.                                                                                                                              |
| `I4`           | Navigation from Screen 3 to Screen 4 is allowed only when every input slot is `UPLOADED`.                                                                                                        |
| `I5`           | After the public job status leaves `DRAFT`, the Web Client must not modify job inputs or parameters.                                                                                             |
| `I6`           | Each asynchronous client workflow may own at most one active HTTP request at a time.                                                                                                             |
| `I7`           | A response belonging to a cancelled, abandoned, or stale client operation must not change the current UI state.                                                                                  |
| `I8`           | `POST /jobs/{jobId}/submit` and `POST /jobs/{jobId}/cancel` must not be repeated automatically after an ambiguous result. Their results must be reconciled through a safe observational request. |
| `I9`           | While job monitoring is active, all `GET /jobs/{jobId}/status` requests are owned and serialized by the Job Monitor.                                                                             |
| `I10`          | The Web Client must not replace the last confirmed public job status with an earlier lifecycle status.                                                                                           |
| `I11`          | Every local non-terminal state must have a defined recovery path or exit path.                                                                                                                   |
| `I12`          | UI controls emit client events. Local workflow states may change only through the transition rules of their owning state machines.                                                               |

## 6. Common client policies

### 6.1 Asynchronous operation ownership

### 6.2 Request serialization

### 6.3 Automatic retry

### 6.4 Operation reconciliation

### 6.5 User-initiated cancellation

### 6.6 Stale and late response handling

### 6.7 Navigation locking

### 6.8 Warning and error presentation

### 6.9 Workflow reset and abandonment

## 7. Network operation catalog

| Operation                                            |                     Changes server or storage state | Safe to repeat                                  |                 Ambiguous result possible | Recovery strategy                                       |
|------------------------------------------------------|----------------------------------------------------:|-------------------------------------------------|------------------------------------------:|---------------------------------------------------------|
| `POST /jobs`                                         |                                                 Yes | Yes, with the same upload `sessionId`           |                 Controlled by idempotency | Repeat the same `POST /jobs` operation                  |
| `POST /jobs/{jobId}/inputs`                          |                           No; returns an upload URL | Yes                                             |                                        No | Repeat the same URL request                             |
| `PUT <presigned-upload-url>`                         | Yes; creates or replaces the canonical input object | Yes, preferably with a fresh URL                |                                       Yes | Request a fresh upload URL and repeat the `PUT`         |
| `PATCH /jobs/{jobId}/params`                         |                                                 Yes | Yes, with the same JSON Merge Patch document    | Yes, but repeating the same patch is safe | Repeat the same `PATCH` operation                       |
| `POST /jobs/{jobId}/submit`                          |                                                 Yes | No after an ambiguous result                    |                                       Yes | Reconcile through `GET /jobs/{jobId}/status`            |
| `GET /jobs/{jobId}/status`                           |                                                  No | Yes                                             |                                        No | Repeat the same `GET` operation                         |
| `POST /jobs/{jobId}/cancel`                          |                                                 Yes | Do not repeat blindly after an ambiguous result |                                       Yes | Reconcile through `GET /jobs/{jobId}/status`            |
| `GET /jobs/{jobId}/outputs?outputSlot=<output-slot>` |                          No; returns a download URL | Yes                                             |                                        No | Repeat the same output URL request                      |
| `GET <presigned-download-url>`                       |                                                  No | Yes, preferably with a fresh URL                |                                        No | Restart the complete download workflow with a fresh URL |

## 8. Client state machines

Each subsection defines one local state machine. Its transition table is normative.
A state diagram may be added later as an explanatory visualization of the same table.

### 8.1 Wizard Navigation

| Current state | Event | Condition | Side effects | Next state |
|---------------|-------|-----------|--------------|------------|

```text
JOB_TYPE
INPUTS
UPLOAD
PARAMETERS
REVIEW
MONITOR
OUTPUTS
```

### 8.2 Draft Job Creation

| Current state | Event | Condition | Side effects | Next state |
|---------------|-------|-----------|--------------|------------|

```text
IDLE
CREATING
WAITING_RETRY
READY
FAILED
CANCELLED
```

### 8.3 Upload Manager

| Current state | Event                            | Condition                     | Side effects                                                                   | Next state        |
|---------------|----------------------------------|-------------------------------|--------------------------------------------------------------------------------|-------------------|
| `RUNNING`     | Queue exhausted                  | —                             | —                                                                              | `COMPLETED`       |
| `RUNNING`     | Fatal upload URL request failure | —                             | Current slot → `FAILED`; discard the remaining queue                           | `FAILED`          |
| `RUNNING`     | User confirms stop               | —                             | Abort the active request; active slot → `PENDING`; discard the remaining queue | `STOPPED_BY_USER` |
| `COMPLETED`   | Retry failed slots               | At least one slot is `FAILED` | Build a queue from the failed slots                                            | `RUNNING`         |

```text
IDLE
RUNNING
COMPLETED
FAILED
STOPPED_BY_USER
```

### 8.4 Input Slot

| Current state | Event | Condition | Side effects | Next state |
|---------------|-------|-----------|--------------|------------|

```text
PENDING
UPLOADING
UPLOADED
FAILED
```

### 8.5 Job Parameter Update

| Current state | Event | Condition | Side effects | Next state |
|---------------|-------|-----------|--------------|------------|

```text
UNSAVED
UPDATING
SYNCHRONIZED
FAILED
```

### 8.6 Job Submission

| Current state | Event | Condition | Side effects | Next state |
|---------------|-------|-----------|--------------|------------|

```text
IDLE
SUBMITTING
RECONCILING
SUBMITTED
NOT_SUBMITTED
UNKNOWN
FAILED
```

### 8.7 Job Monitor

| Current state | Event | Condition | Side effects | Next state |
|---------------|-------|-----------|--------------|------------|

```text
IDLE
RUNNING
COMPLETED
FAILED
```

### 8.8 Job Cancellation

| Current state | Event | Condition | Side effects | Next state |
|---------------|-------|-----------|--------------|------------|

```text
IDLE
REQUESTING
RECONCILING
ACCEPTED
NOT_ACCEPTED
FAILED
```

### 8.9 Output Download

| Current state | Event | Condition | Side effects | Next state |
|---------------|-------|-----------|--------------|------------|

```text
IDLE
REQUESTING_URL
DOWNLOADING
COMPLETED
FAILED
CANCELLED
```

## 9. Screen-to-state mapping

This section maps local client states to visible content, control availability,
and generated client events. It does not redefine state transitions or network
recovery policies.

### 9.1 Screen 1: Select Job Type

### 9.2 Screen 2: Select Job Inputs

### 9.3 Screen 3: Upload Job Inputs

### 9.4 Screen 4: Set Job Parameters

### 9.5 Screen 5: Review Job Summary

### 9.6 Screen 6: Monitor Job Progress

### 9.7 Screen 7: Download Job Outputs

## 10. Acceptance scenarios

| Given                                         | When                                  | Then                                                                                                                   |
|-----------------------------------------------|---------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| The current `submissionState` is `SUBMITTING` | `POST /jobs/{jobId}/submit` times out | The client must not repeat the submit request; it must enter `RECONCILING`; it must request `GET /jobs/{jobId}/status` |

## 11. Deferred decisions and out-of-scope behavior
