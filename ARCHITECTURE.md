<!-- 
Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
Refer to the LICENSE file in the root directory for full license details.
-->

# MDDS Job Orchestrator Architecture

<!-- TOC -->
* [MDDS Job Orchestrator Architecture](#mdds-job-orchestrator-architecture)
  * [Overview](#overview)
  * [Architectural Goal](#architectural-goal)
  * [Main Responsibilities](#main-responsibilities)
    * [Web Server (Orchestrator)](#web-server-orchestrator)
    * [Worker](#worker)
    * [Object Storage (S3 / MinIO)](#object-storage-s3--minio)
    * [Metadata Store (RDBMS)](#metadata-store-rdbms)
  * [Job Lifecycle](#job-lifecycle)
    * [Supported statuses](#supported-statuses)
    * [Lifecycle rules](#lifecycle-rules)
  * [Object Storage Layout](#object-storage-layout)
  * [REST API v1](#rest-api-v1)
    * [1. Create or Reuse a Draft Job](#1-create-or-reuse-a-draft-job)
    * [2. Request a Pre-Signed Upload URL for an Input Artifact](#2-request-a-pre-signed-upload-url-for-an-input-artifact)
    * [3. Submit a Job for Execution](#3-submit-a-job-for-execution)
    * [4. Get Job State](#4-get-job-state)
    * [5. Request Job Cancellation](#5-request-job-cancellation)
    * [6. Request a Pre-Signed Download URL for the Result](#6-request-a-pre-signed-download-url-for-the-result)
  * [Manifest v1](#manifest-v1)
    * [Example manifest for SLAE solving](#example-manifest-for-slae-solving)
    * [Field meaning](#field-meaning)
  * [SLAE Job Profile (v1)](#slae-job-profile-v1)
    * [Required input slots](#required-input-slots)
    * [Required execution parameter](#required-execution-parameter)
    * [Expected output](#expected-output)
  * [Extension Model](#extension-model)
  * [Summary](#summary)
<!-- TOC -->

## Overview

This document describes the target architecture of the Modeling of the Dynamic of Distributed Systems (MDDS)
job orchestration platform.

The system is designed as a **generic job orchestrator**. Its main responsibility is **not** to understand the internal business logic of every task. Instead, it manages the full lifecycle of a job:

- creating a job;
- accepting input artifacts for that job;
- scheduling the job for execution;
- tracking job status;
- requesting cancellation;
- providing access to results.

The actual meaning of a job is defined by two things:

1. the job type (`jobType`);
2. the job manifest (`manifest.json`).

This design allows the platform to support multiple job types in the future. At the current stage, the primary 
supported job type is solving Systems of Linear Algebraic Equations (SLAE).

---

## Architectural Goal

The long-term goal is to build a system where:

- the **Web Server** acts as a **job orchestrator**;
- **Workers** execute concrete job types;
- **object storage** (S3/MinIO) stores job inputs, outputs, and manifests;
- the **metadata store** (RDBMS) stores job lifecycle data such as status, timestamps, owner, and progress.

In other words, the platform coordinates work, but the business meaning of the work is delegated to Workers through manifests.

---

## Main Responsibilities

### Web Server (Orchestrator)

The Web Server is responsible for:

- creating jobs and assigning job identifiers;
- issuing pre-signed upload URLs for input artifacts;
- validating whether a job is ready for submission;
- generating the job manifest;
- publishing jobs to the execution queue;
- exposing job status to clients;
- accepting cancellation requests;
- issuing pre-signed download URLs for results.

The Web Server should not contain job-specific execution logic.

### Worker

The Worker is responsible for:

- consuming submitted jobs from the queue;
- reading `manifest.json` from object storage;
- selecting the correct handler for the given `jobType`;
- downloading input artifacts;
- executing the actual job logic;
- uploading output artifacts;
- publishing lifecycle status updates.

### Object Storage (S3 / MinIO)

Object storage is responsible for:

- storing all input artifacts;
- storing output artifacts;
- storing `manifest.json`;
- optionally storing logs and auxiliary execution artifacts.

### Metadata Store (RDBMS)

The relational database is responsible for:

- storing job metadata;
- storing job lifecycle status;
- storing timestamps;
- storing user/job relationships;
- supporting filtering, querying, and future administrative pages.

---

## Job Lifecycle

Each job moves through a defined set of statuses.

### Supported statuses

- `DRAFT` — the job has been created, but input artifacts are still being uploaded;
- `SUBMITTED` — the job has been sent to the execution pipeline;
- `IN_PROGRESS` — a Worker has started executing the job;
- `CANCEL_REQUESTED` — a cancellation request has been accepted and forwarded;
- `CANCELLED` — the Worker confirmed that execution was cancelled;
- `DONE` — the job completed successfully;
- `ERROR` — the job failed.

### Lifecycle rules

- A newly created job always starts in `DRAFT`.
- A job can be submitted only after all required inputs have been uploaded.
- After a job is submitted, its input artifacts must be treated as immutable.
- `POST /jobs/{jobId}/cancel` does not mean the job is already cancelled. It means cancellation has been requested. The final state becomes `CANCELLED` only after confirmation from the execution side.
- Downloading results is allowed only when the job is in `DONE`.
- An upload session id is valid only for the draft phase of a job. Once the job leaves `DRAFT`, that session id is closed and must not be reused.

---

## Object Storage Layout

Each job uses a dedicated object prefix.

File structure for a job:

```text
jobs/{userId}/{jobId}/manifest.json
jobs/{userId}/{jobId}/in/{inputSlot}
jobs/{userId}/{jobId}/out/{outputSlot}
jobs/{userId}/{jobId}/logs/{logFile}
```

Examples:

```text
jobs/42/abc-123/manifest.json
jobs/42/abc-123/in/matrix.csv.gz
jobs/42/abc-123/in/rhs.csv.gz
jobs/42/abc-123/out/solution.csv.gz
```

Note: in S3-compatible storage these are object keys with prefixes, not real directories.

---

## REST API v1

The API below describes the public lifecycle of a job.

### 1. Create or Reuse a Draft Job

**Endpoint**

```http
POST /jobs
```

**Required headers**

```http
X-MDDS-User-Login: <user-login>
X-MDDS-Upload-Session-Id: <upload-session-id>
Content-Type: application/json
```

`X-MDDS-Upload-Session-Id` is a required idempotency key for draft job creation.
For the same pair (`<user-login>`, `<upload-session-id>`), if an existing job is still in DRAFT state, 
the server must return the same draft job.

**Request**

```json
{
  "jobType": "solving_slae"
}
```
Here `solving_slae` denotes a job for solving systems of linear algebraic equations. 

**Response**

- `201 Created` — the job was created successfully.

```json
{
  "jobId": "<new-job-id>"
}
```

- `200 OK` — a draft job for the same user and upload session already exists and was returned.

```json
{
  "jobId": "<existing-job-id>"
}
```

**Meaning**

Creates a new draft job or returns an existing draft job for the same user and upload session id.
The endpoint uses the upload session id as an idempotency key.
When a new job is created, the server assigns a job identifier, and the object storage prefix 
is derived from the user identifier and job identifier.
The `X-MDDS-Upload-Session-Id` may be reused only while the corresponding job remains in `DRAFT` state.
Once the job leaves `DRAFT`, that upload session id is considered closed and must not be reused.

**Possible errors**

- `400 Bad Request` — unknown or unsupported job type;
- `400 Bad Request` — null or blank job type;
- `400 Bad Request` — required headers are missing;
- `400 Bad Request` — request body is missing or malformed;
- `400 Bad Request` — `X-MDDS-User-Login` is blank;
- `400 Bad Request` — `X-MDDS-Upload-Session-Id` is blank;
- `401 Unauthorized` — unknown user login;
- `409 Conflict` — a job already exists for the same upload session id, but with a different `jobType`;
- `409 Conflict` — a job already exists for the same upload session id, but it is no longer in `DRAFT` state. The client must create a new upload session id;
- `415 Unsupported Media Type` — missing or unsupported `Content-Type`; `application/json` is required.

---

### 2. Request a Pre-Signed Upload URL for an Input Artifact

**Endpoint**

```http
POST /jobs/{jobId}/inputs
```

**Request**

```json
{
  "inputSlot": "<input-slot>"
}
```
`inputSlot` is a logical name of an input artifact defined by the selected job profile.

For `jobType` = `solving_slae`, supported values for `<input-slot>` in v1 are:

- `matrix` — matrix of coefficients for System of Linear Algebraic Equations.
- `rhs` — right hand side vector for System of Linear Algebraic Equations.

For jobs with `jobType` = `solving_slae_parallel`, input upload URL requests are not supported in this version.

**Required headers**

```http
X-MDDS-User-Login: <user-login>
Content-Type: application/json
```

**Response**

- `200 OK` — pre-signed upload URL was returned successfully.

```json
{
  "jobId": "<job-id>",
  "inputSlot": "<input-slot>",
  "uploadUrl": "<presigned-upload-url>",
  "expiresAt": "<timestamp>"
}
```

**Meaning**

Returns a pre-signed URL that allows the client to upload an input artifact directly to object storage.
The returned URL must be used with HTTP `PUT` to upload the artifact bytes.
Input artifacts can be uploaded only while the job is in `DRAFT` state. After submission, input artifacts are immutable.
In case of expiration of pre-signed URL, the new one can be requested if job is in `DRAFT` state.
While the job remains in `DRAFT`, the client may request a new upload URL for the same `inputSlot` and re-upload the artifact.
For a given `jobId` and `inputSlot`, the pre-signed upload URL always targets the canonical object key assigned to 
that slot. Re-uploading the same slot replaces the previously uploaded artifact for that slot.

Note: `<timestamp>` uses RFC 3339 format, for example `2026-03-20T14:30:00Z` or `2026-03-20T14:30:00+02:00`.

**Upload via Pre-Signed URL**

After receiving `uploadUrl`, the client uploads the artifact bytes directly to object storage using HTTP `PUT` to the 
returned pre-signed URL. The orchestration server does not proxy artifact bytes through itself for this operation.

The following rules apply:

* the client must use HTTP `PUT` with the artifact bytes as the request body;
* the client must not send an Authorization header when calling the pre-signed URL;
* `Content-Type` is not part of the signed parameters of the pre-signed URL, therefore the client may omit it or send an appropriate value;
* while the job remains in `DRAFT`, the client may request a new upload URL for the same `inputSlot` and upload a replacement artifact;
* the artifact currently stored under the canonical object key for that slot is used at submission time;
* the existence of required uploaded artifacts is validated by the orchestration server during `POST /jobs/{jobId}/submit`, not during pre-signed URL issuance.

For browser-based deployments, the following object storage CORS requirements apply:

* the frontend origin used by the system must be allowed;
* HTTP method `PUT` must be allowed;
* request headers used by the client upload request must be allowed, including `Content-Type` when sent;
* response headers that the frontend is expected to read must be exposed.

If the frontend does not rely on any response headers from object storage, exposing additional response headers is not required.

Illustrative example of the direct upload request:

```http request
PUT <presigned-upload-url>
Content-Type: application/octet-stream

<artifact-bytes>
```

**Possible errors**

The errors listed below apply to `POST /jobs/{jobId}/inputs`.
Errors returned by object storage during direct `PUT` upload are outside of this endpoint contract.

- `400 Bad Request` — unknown or unsupported input slot for the given jobType;
- `400 Bad Request` — `inputSlot` is null or blank;
- `400 Bad Request` — request body is missing or malformed;
- `400 Bad Request` — required headers are missing;
- `400 Bad Request` — `X-MDDS-User-Login` is blank;
- `400 Bad Request` — input upload URL requests are not supported for the given `jobType`;
- `401 Unauthorized` — unknown user login;
- `404 Not Found` — job does not exist (or is not accessible to the current user);
- `409 Conflict` — the job is not in `DRAFT` state and no more input artifacts can be uploaded;
- `415 Unsupported Media Type` — missing or unsupported `Content-Type`; `application/json` is required.

---

### 3. Submit a Job for Execution

**Endpoint**

```http
POST /jobs/{jobId}/submit
```

**Request**

Empty body.

**Response**

- `202 Accepted` — the submit request was accepted.

```json
{
  "jobId": "<job-id>",
  "status": "SUBMITTED"
}
```

**Meaning**

Validates that all required inputs exist, generates `manifest.json`, and publishes the job to the execution queue.
Submission is allowed only when all required input slots defined by the job profile are present.

**Possible errors**

- `404 Not Found` — the job does not exist;
- `409 Conflict` — the job is not in `DRAFT` state (for example, it has already been submitted or is already in a terminal state);
- `400 Bad Request` — required inputs are missing.

---

### 4. Get Job State

**Endpoint**

```http
GET /jobs/{jobId}
```

**Response**

 - `200 OK` — job state was returned successfully.

```json
{
  "jobId": "<job-id>",
  "jobType": "solving_slae",
  "status": "IN_PROGRESS",
  "progress": 42,
  "message": null,
  "createdAt": "<timestamp>",
  "submittedAt": "<timestamp>",
  "startedAt": "<timestamp-or-null>",
  "finishedAt": "<timestamp-or-null>"
}
```

**Meaning**

Returns the current state of the job and lifecycle metadata useful for UI and administration. Note that if a field 
has a `null` or empty value, the response still contains that field with the value `null` or `""`.

**Possible errors**

- `404 Not Found` — the job does not exist.
---

### 5. Request Job Cancellation

**Endpoint**

```http
POST /jobs/{jobId}/cancel
```

**Request**

Empty body.

**Response**

- `202 Accepted` — the cancellation request was accepted.

```json
{
  "jobId": "<job-id>",
  "status": "CANCEL_REQUESTED"
}
```

**Meaning**

Accepts a cancellation request and forwards it to the execution pipeline. If a job is already in `CANCEL_REQUESTED`
state, repeated cancellation returns `202 Accepted`.

**Possible errors**

- `404 Not Found` — the job does not exist;
- `409 Conflict` — the job is already terminal and can no longer be cancelled.

---

### 6. Request a Pre-Signed Download URL for the Result

**Endpoint**

```http
GET /jobs/{jobId}/result-url
```

**Response**

- `200 OK` — the job was found and a pre-signed download URL was returned.

```json
{
  "jobId": "<job-id>",
  "downloadUrl": "<presigned-download-url>",
  "expiresAt": "<timestamp>"
}
```

**Meaning**

Returns a pre-signed URL that allows the client to download the result artifact directly from object storage.
The returned URL is temporary, and its expiration is defined by server configuration.

**Possible errors**

- `404 Not Found` — the job does not exist;
- `409 Conflict` — the result is not available because the job is not `DONE`.

---

## Manifest v1

`manifest.json` is the contract between the Web Server and the Worker.

The Web Server generates the manifest when the job is submitted. The Worker reads the manifest to determine what it must execute.

### Example manifest for SLAE solving

```json
{
  "manifestVersion": 1,
  "jobId": "<job-id>",
  "jobType": "solving_slae",
  "inputs": {
    "matrix": {
      "objectKey": "jobs/<userId>/<jobId>/in/matrix.csv.gz",
      "format": "csv.gz"
    },
    "rhs": {
      "objectKey": "jobs/<userId>/<jobId>/in/rhs.csv.gz",
      "format": "csv.gz"
    }
  },
  "params": {
    "solvingMethod": "numpy_exact_solver"
  },
  "outputs": {
    "solution": {
      "objectKey": "jobs/<userId>/<jobId>/out/solution.csv.gz",
      "format": "csv.gz"
    }
  }
}
```

### Field meaning

- `manifestVersion` — version of the manifest schema;
- `jobId` — identifier of the job;
- `jobType` — logical type of work to execute;
- `inputs` — declared input artifacts;
- `params` — execution parameters for the specific job type;
- `outputs` — declared output artifacts.

This structure is intentionally generic so the same orchestration model can support multiple job types in the future.

---

## SLAE Job Profile (v1)

The first supported job type is:

```text
solving_slae
```

### Required input slots

- `matrix` — matrix of coefficients for System of Linear Algebraic Equations.
- `rhs` — right hand side vector for System of Linear Algebraic Equations.

### Required execution parameter

- `solvingMethod`

### Expected output

- `solution`

This profile defines what the Worker must expect when `jobType = solving_slae`.

---

## Extension Model

The orchestration API should remain stable even when new job types are added.

To add a new job type, the system should define:

- supported input slots;
- required execution parameters;
- expected output artifacts;
- Worker-side execution logic.

This means that the public lifecycle API can remain the same:

- `POST /jobs`
- `POST /jobs/{jobId}/inputs`
- `POST /jobs/{jobId}/submit`
- `GET /jobs/{jobId}`
- `POST /jobs/{jobId}/cancel`
- `GET /jobs/{jobId}/result-url`

Only the `jobType` profile and the Worker logic need to change.

---

## Summary

The MDDS target architecture is a **manifest-driven job orchestration platform**.

- The Web Server manages lifecycle and coordination.
- Workers execute concrete job types.
- Object storage contains artifacts and manifests.
- The relational database stores metadata and job state.

This architecture allows the platform to start with SLAE solving and later evolve into a more general distributed 
job execution system without redesigning the core lifecycle API. The lifecycle API remains stable across job types, 
while job-specific semantics are delegated to manifests and Worker implementations.