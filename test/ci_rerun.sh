#!/bin/bash

subcommand=$1
out_files="regression.out"

case "$subcommand" in
  installcheck)
    out_files=$(find .. -name "regression.out")
    ;;
  regresscheck-shared)
    out_files="shared/regression.out"
    ;;
  isolationcheck|isolationcheck-t)
    out_files="isolation/regression.out"
    ;;
  *)
    out_files="regression.out"
    ;;
esac

failed=$(grep -h -P "^not ok|FAILED" $out_files | sed -r -e 's!^not ok [0-9]+ +[+-] ([a-z0-9_-]+) .*$!\1!' -e 's!^(test|    ) ([a-z0-9_-]+) +... FAILED.*$!\2!')

failed="${failed//$'\n'/ }"

make -k $subcommand TESTS="$failed"

