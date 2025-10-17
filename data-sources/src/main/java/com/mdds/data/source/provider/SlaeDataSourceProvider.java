/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.data.source.provider;

import com.mdds.api.Processable;

/** Interface for Data sources. */
public interface SlaeDataSourceProvider {
  /**
   * Loads matrix of coefficients for system of linear algebraic equations. If it cannot do that,
   * (e.g. no connection for DB where the data are stored, or no access to S3 filesystem or data is
   * corrupted, or some parts of the data are absent), it returns failure instance.
   *
   * <p>
   *
   * @return instance of <i>Processable</i> that either contains matrix with double values or
   *     contains error message with possible cause as <i>Throwable</i>.
   */
  Processable<double[][]> loadMatrix();

  /**
   * Loads right hand side vector for system of linear algebraic equations.
   *
   * <p>
   *
   * @return instance of <i>Processable</i> that either contains right hand side vector with double
   *     values or contains error message with possible cause as <i>Throwable</i>.
   */
  Processable<double[]> loadRhs();
}
