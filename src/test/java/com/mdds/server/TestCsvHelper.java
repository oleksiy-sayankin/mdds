/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import static com.mdds.server.CsvHelper.convert;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestCsvHelper {
  @Test
  void testConvertVector() {
    var numbers = new String[] {"2.3", "43.654654", "346.8534"};
    double[] expectedVector = {2.3, 43.654654, 346.8534};
    Assertions.assertArrayEquals(expectedVector, convert(numbers));
  }

  @Test
  void testConvertVectorException() {
    var numbers = new String[] {"2.3kjhkj", "43.654654", "346.8534"};
    assertThrows(SolveServletException.class, () -> convert(numbers));
  }

  @Test
  void testConvertMatrix() {
    var numbers =
        new String[][] {
          {"2.3", "43.654654", "346.8534"}, {"5.8", "4.4", "7.5"}, {"1.2", "3.7", "9.8"}
        };
    double[][] expectedMatrix = {{2.3, 43.654654, 346.8534}, {5.8, 4.4, 7.5}, {1.2, 3.7, 9.8}};
    Assertions.assertArrayEquals(expectedMatrix, convert(numbers));
  }

  @Test
  void testConvertMatrixException() {
    var numbers =
        new String[][] {
          {"2.3kjhkj", "43.654654", "346.8534"}, {"5.8", "4.4", "7.5"}, {"1.2", "3.7", "9.8"}
        };
    assertThrows(SolveServletException.class, () -> convert(numbers));
  }
}
