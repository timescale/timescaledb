# Code coverage for TimescaleDB

Code coverage can be enabled for TimescaleDB builds by setting the
option `-DCODECOVEAGE=ON` when running CMake (it is off by
default). This enables the necessary compiler option (`--coverage`) to
generate code coverage statistics and should be enough for CI build
reports using, e.g., `codecov.io`. In addition, local code coverage
reports can be generated with the `lcov` tool, when this tool is
installed on the build system.


## Generating local code coverage data files

A code coverage report is generated in three steps using `lcov`:

1. A pre test baseline run to learn what zero coverage looks like (the
   `coverage_base` target).
2. A post test run to learn the test coverage (the `coverage_test`
   target).
3. A final run to combine the pre test and post test output files into
   a final data file (the `coverage_final` target).

Each of these steps can be run manually using the mentioned targets,
but should happen automatically as part of regular build and test
steps. Optionally, this process can be extended with a filtering step
to ignore certain paths that shouldn't be included in the final
report.

## Producing a HTML-based code coverage report

Once the complete test suite has run (`installcheck` target), it is
possible to produce a HTML-based code coverage report that can be
viewed in a web browser. This is automated by the `coverage` target.
Thus, the complete steps to produce a code coverage report are:

1. `cmake --build`
2. `cmake --build --target installcheck`
3. `cmake --build --target coverage`
