# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

"""
Helper for parsing CSV files
"""
import csv


def load_matrix(upload_file):
    # Read CSV to the list of lists of float
    reader = csv.reader(upload_file.file.read().decode().splitlines(), delimiter=",")
    return [[float(x) for x in row] for row in reader]
