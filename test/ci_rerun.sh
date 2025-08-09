#!/bin/sh

subcommand=$1
out_files="regression.out"

if [ "$subcommand" == "installcheck" ]; then
  out_files=$(find .. -name "regression.out")
fi

failed=$(grep -h -P "^not ok|FAILED" $out_files | sed -r -e 's!^not ok [0-9]+ +[+-] ([a-z0-9_-]+) .*$!\1!' -e 's!^(test|    ) ([a-z0-9_-]+) +... FAILED.*$!\2!')

failed="${failed//$'\n'/ }"

make -k $subcommand TESTS="$failed"

