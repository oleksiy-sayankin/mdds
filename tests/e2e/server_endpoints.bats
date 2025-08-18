# Copyright (c) 2025 Oleksy Oleksandrovych Sayankin. All Rights Reserved.
# Refer to the LICENSE file in the root directory for full license details.

#!/usr/bin/env bats

# Test main page
@test "GET / returns index.html" {
  run curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/
  [ "$status" -eq 0 ]
  [ "$output" -eq 200 ]
}

# Test solve end point
#@test "POST /solve returns solution" {
#  MATRIX="resources/matrix.csv"
#  RHS="resources/rhs.csv"
#  OUTPUT="output.csv"

#  run curl -s -X POST http://localhost:8000/solve \
#      -F "matrix=@$MATRIX" \
#      -F "rhs=@$RHS" \
#      -F "method=gmres" \
#      -o "$OUTPUT"

#  [ "$status" -eq 0 ]
#  [ -s "$OUTPUT" ]
#}