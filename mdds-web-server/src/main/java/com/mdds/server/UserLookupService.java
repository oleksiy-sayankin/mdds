/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.mdds.server.jpa.UsersRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * This service finds a User by its Login. This is stub and will be replaced in the future with
 * tenant/token implementation.
 */
@Service
@RequiredArgsConstructor
public class UserLookupService {
  private final UsersRepository usersRepository;

  @Transactional(readOnly = true)
  public long findUserIdOrGuest(String loginHeader) {
    String login = (loginHeader == null || loginHeader.isBlank()) ? "guest" : loginHeader.trim();

    return usersRepository
        .findIdByLogin(login)
        .orElseThrow(() -> new UnknownUserException("Unknown user login: " + login));
  }
}
