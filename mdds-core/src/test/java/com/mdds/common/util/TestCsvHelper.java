/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.common.util;

import static com.mdds.common.util.CsvHelper.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.opencsv.exceptions.CsvException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class TestCsvHelper {
  @Test
  void testConvertVector() {
    var numbers = new String[] {"2.3", "43.654654", "346.8534"};
    var expectedVector = new double[] {2.3, 43.654654, 346.8534};
    assertArrayEquals(expectedVector, convert(numbers));
  }

  @Test
  void testConvertVectorException() {
    var numbers = new String[] {"2.3kjhkj", "43.654654", "346.8534"};
    assertThrows(ParseException.class, () -> convert(numbers));
  }

  @Test
  void testConvertMatrix() {
    var numbers =
        new String[][] {
          {"2.3", "43.654654", "346.8534"}, {"5.8", "4.4", "7.5"}, {"1.2", "3.7", "9.8"}
        };
    var expectedMatrix =
        new double[][] {{2.3, 43.654654, 346.8534}, {5.8, 4.4, 7.5}, {1.2, 3.7, 9.8}};
    assertArrayEquals(expectedMatrix, convert(numbers));
  }

  @Test
  void testConvertMatrixException() {
    var numbers =
        new String[][] {
          {"2.3kjhkj", "43.654654", "346.8534"}, {"5.8", "4.4", "7.5"}, {"1.2", "3.7", "9.8"}
        };
    assertThrows(ParseException.class, () -> convert(numbers));
  }

  @Test
  void testReadCsvAsVector() throws IOException, CsvException {
    var expectedVector = new String[] {"1.3", "2.2", "3.7"};
    var is =
        new ByteArrayInputStream(
            String.join(System.lineSeparator(), expectedVector).getBytes(UTF_8));
    var actualVector = readCsvAsVector(is);
    assertArrayEquals(expectedVector, actualVector);
  }

  @Test
  void testReadCsvAsMatrix() throws IOException, CsvException {
    var expectedMatrix =
        new String[][] {{"1.3", "2.2", "3.7"}, {"7.7", "2.1", "9.3"}, {"1.1", "4.8", "2.3"}};

    var sb = new StringBuilder();
    for (var row : expectedMatrix) {
      sb.append(String.join(",", row));
      sb.append(System.lineSeparator());
    }
    var is = new ByteArrayInputStream(sb.toString().getBytes(UTF_8));
    var actualMatrix = readCsvAsMatrix(is);
    assertArrayEquals(expectedMatrix, actualMatrix);
  }
}
