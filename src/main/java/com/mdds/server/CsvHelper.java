/*
 * Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
 * Refer to the LICENSE file in the root directory for full license details.
 */
package com.mdds.server;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/** Helper class to read/parse csv files. */
public final class CsvHelper {
  private CsvHelper() {}

  /**
   * Converts array of string into array of double.
   *
   * @param numbers array of float numbers as string data.
   * @return array of double
   * @throws SolveServletException when exception raises during parsing a number.
   */
  public static double[] convert(String[] numbers) throws SolveServletException {
    var result = new double[numbers.length];
    var i = 0;
    for (var number : numbers) {
      try {
        result[i] = Double.parseDouble(number);
      } catch (NumberFormatException e) {
        throw new SolveServletException("Can not convert to double: " + number, e);
      }
      i++;
    }
    return result;
  }

  /**
   * Converts matrix of string into matrix of double.
   *
   * @param matrix matrix of float numbers as string data.
   * @return matrix of double
   * @throws SolveServletException when exception raises during parsing a number.
   */
  public static double[][] convert(String[][] matrix) throws SolveServletException {
    var result = new double[matrix.length][];
    var i = 0;
    for (var row : matrix) {
      result[i] = new double[matrix[i].length];
      var j = 0;
      for (var number : row) {
        try {
          result[i][j] = Double.parseDouble(number);
        } catch (NumberFormatException e) {
          throw new SolveServletException("Can not convert to double: " + number, e);
        }
        j++;
      }
      i++;
    }
    return result;
  }

  /**
   * Reads input stram as comma separated values of a vector.
   *
   * @param inputStream input stram as comma separated values.
   * @return array of strings.
   * @throws IOException when can not read input stream.
   * @throws CsvException when can not parse csv data.
   */
  public static String[] readCsvAsVector(InputStream inputStream) throws IOException, CsvException {
    try (var reader = new BufferedReader(new InputStreamReader(inputStream));
        var csvReader = new CSVReader(reader)) {
      var allData = csvReader.readAll();
      var rhs = new String[allData.size()];
      var i = 0;
      for (var row : allData) {
        rhs[i] = row[0];
        i++;
      }
      return rhs;
    }
  }

  /**
   * Reads input stram as comma separated values of a vector.
   *
   * @param inputStream input stram as comma separated values.
   * @return matrix of strings.
   * @throws IOException when can not read input stream.
   * @throws CsvException when can not parse csv data.
   */
  public static String[][] readCsvAsMatrix(InputStream inputStream)
      throws IOException, CsvException {
    try (var reader = new BufferedReader(new InputStreamReader(inputStream));
        var csvReader = new CSVReader(reader)) {
      var allData = csvReader.readAll();
      return allData.toArray(new String[0][0]);
    }
  }
}
