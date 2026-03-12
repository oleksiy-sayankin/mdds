/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */

-- Creates default users.

insert into users(login, password_hash)
values ('guest', null),
       ('admin', null)
    on conflict (login) do nothing;