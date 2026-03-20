/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

-- Creates initial tables, indexes and keys for the application.

-- users
create table if not exists users (
  id bigserial primary key,
  login text not null unique,
  password_hash text null,
  created_at timestamptz not null default now()
  );

-- jobs
create table if not exists jobs (
    id text primary key,
    user_id bigint not null references users(id),
    status text not null,
    job_type text not null,
    progress int not null default 0,
    upload_session_id text not null,
    created_at timestamptz not null default now(),
    submitted_at timestamptz null,
    started_at timestamptz null,
    finished_at timestamptz null
    );

create index if not exists idx_jobs_user_id on jobs(user_id);
create index if not exists idx_jobs_status on jobs(status);
create index if not exists idx_jobs_created_at on jobs(created_at);
create unique index if not exists ux_jobs_user_upload_session on jobs(user_id, upload_session_id);