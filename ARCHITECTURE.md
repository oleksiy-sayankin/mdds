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
    * [1. Create a Job](#1-create-a-job)
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

This design allows the platform to support multiple job types in the future, not only solving Systems of Linear 
Algebraic Equations (SLAE), which is the primary supported job type at the current stage.

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
- reserving object storage namespaces/prefixes for jobs;
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

### 1. Create a Job

**Endpoint**

```http
POST /jobs
```

**Request**

```json
{
  "jobType": "solving_slae"
}
```
Here `solving_slae` denotes a job for solving Systems of Linear Algebraic Equations. 

**Response**

```json
{
  "jobId": "<new-job-id>",
  "jobType": "solving_slae",
  "status": "DRAFT"
}
```

**Meaning**

Creates a new job resource and reserves a namespace/prefix in object storage for future artifacts.

---

### 2. Request a Pre-Signed Upload URL for an Input Artifact

**Endpoint**

```http
POST /jobs/{jobId}/inputs
```

**Request**

```json
{
  "inputSlot": "matrix"
}
```
`inputSlot` is a logical name of an input artifact defined by the selected job profile.

For `jobType = solving_slae`, supported values in v1 are:

- `matrix` — matrix of coefficients for System of Linear Algebraic Equations.
- `rhs` — right hand side vector for System of Linear Algebraic Equations.

**Response**

```json
{
  "jobId": "<job-id>",
  "inputSlot": "matrix",
  "uploadUrl": "<presigned-upload-url>",
  "expiresAt": "<timestamp>"
}
```

**Meaning**

Returns a pre-signed URL that allows the client to upload an input artifact directly to object storage.

---

### 3. Submit a Job for Execution

**Endpoint**

```http
POST /jobs/{jobId}/submit
```

**Request**

Empty body.

**Response**

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
- `409 Conflict` — the job has already been submitted or is already terminal;
- `400 Bad Request` — required inputs are missing.

---

### 4. Get Job State

**Endpoint**

```http
GET /jobs/{jobId}
```

**Response**

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

Returns the current state of the job and lifecycle metadata useful for UI and administration.

---

### 5. Request Job Cancellation

**Endpoint**

```http
POST /jobs/{jobId}/cancel
```

**Request**

Empty body.

**Response**

```json
{
  "jobId": "<job-id>",
  "status": "CANCEL_REQUESTED"
}
```

**Meaning**

Accepts a cancellation request and forwards it to the execution pipeline.

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

```json
{
  "jobId": "<job-id>",
  "downloadUrl": "<presigned-download-url>"
}
```

**Meaning**

Returns a pre-signed URL that allows the client to download the result artifact directly from object storage.

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