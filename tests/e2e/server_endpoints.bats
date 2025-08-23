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
@test "POST /solve returns solution" {
  MATRIX="tests/e2e/resources/matrix.csv"
  RHS="tests/e2e/resources/rhs.csv"
  OUTPUT_DIR="tests/e2e/output"
  OUTPUT="$OUTPUT_DIR/output.csv"

  run mkdir "$OUTPUT_DIR"

  run curl -s -X POST http://localhost:8000/solve \
      -F "matrix=@$MATRIX" \
      -F "rhs=@$RHS" \
      -F "method=numpy_exact_solver" \
      -o "$OUTPUT"

  [ "$status" -eq 0 ]
  [ -s "$OUTPUT" ]
}

# Test solve SLAE correctness
@test "POST /solve solves SLAE correctly" {
  SOLVERS=(
    "numpy_exact_solver"
    "numpy_lstsq_solver"
    "numpy_pinv_solver"
    "petsc_solver"
    "scipy_gmres_solver"
  )
  MATRIX="tests/e2e/resources/matrix.csv"
  RHS="tests/e2e/resources/rhs.csv"
  EXPECTED="tests/e2e/resources/expected_output.csv"
  OUTPUT_DIR="tests/e2e/output"
  ACTUAL="$OUTPUT_DIR/actual_output.csv"
  ERROR=1e-6

  run mkdir "$OUTPUT_DIR"

  for method in "${SOLVERS[@]}"; do
    echo "=== Testing solver: $method ==="

    # Call solve
    run curl -s -X POST http://localhost:8000/solve \
      -F "matrix=@$MATRIX" \
      -F "rhs=@$RHS" \
      -F "method=$method" \
      -o "$ACTUAL"

    # Check that status is ok and output is nit empty
    [ "$status" -eq 0 ]
    [ -s "$ACTUAL" ]

    # Compare line by line actual and expected result
    expected_lines=$(wc -l < "$EXPECTED")
    actual_lines=$(wc -l < "$ACTUAL")
    [ "$expected_lines" -eq "$actual_lines" ]


    # We read expected file from stdin chanel (has 0 as descriptor)
    # and actual file from free custom (has 3 as descriptor) chanel.
    #
    # This line
    # done < "$EXPECTED" 3<"$ACTUAL"
    # redirects stdin (0) and custom (3) channels to while-do loop.
    #
    # This line
    # read -r exp
    # reads next string from stdin (0) channel.
    #
    # This line
    # read -r act <&3
    # reads next string from custom (3) channel.
    #
    # So we have line by line parallel reading
    # both from $EXPECTED and $ACTUAL files.

    i=1
    while read -r exp && read -r act <&3; do
      diff=$(echo "scale=10; $exp - $act" | bc -l)
      abs_diff=$(echo "if ($diff < 0) -$diff else $diff" | bc -l)
      too_big=$(echo "$abs_diff > $ERROR" | bc -l)
      if [ "$too_big" -eq 1 ]; then
        echo "[ERROR] Line $i mismatch: expected $exp, got $act (diff=$abs_diff)"
        exit 1
      fi
      i=$((i+1))
    done < "$EXPECTED" 3<"$ACTUAL"
  done
}
