/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server.jpa;

import com.mdds.persistence.entity.UserEntity;
import jakarta.persistence.LockModeType;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

/** JPA repository to process UserEntity. */
public interface UsersRepository extends JpaRepository<UserEntity, Long> {

  @Query("select u.id from UserEntity u where u.login = :login")
  Optional<Long> findIdByLogin(@Param("login") String login);

  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("select u from UserEntity u where u.id = :id")
  Optional<UserEntity> lockById(@Param("id") Long id);
}
