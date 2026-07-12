// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

import { solvingSlaeProfile } from "@/profiles/solvingSlaeProfile";

/**
 * Registry of job profiles supported by the web client.
 *
 * The wizard uses this registry to populate the job type selector and to
 * resolve the selected profile configuration.
 */

export const JOB_PROFILES = [solvingSlaeProfile];
