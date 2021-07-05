#!/bin/bash

rm -f coverage.info
# capture coverage info
lcov --quiet --directory . --capture --output-file coverage.info
# header files are not covered
lcov --quiet --remove coverage.info "*/odpi/src/*.c" "*.h" --output-file coverage.info

if [ -z "${TRAVIS}" ]; then

  target_coverage=${1:-100}

  # Sample structure of lcov --summary coverage.info
  # Reading tracefile coverage.info
  # Summary coverage rate:
  #   lines......: 13.2% (142 of 1078 lines)
  #   functions..: 21.1% (12 of 57 functions)
  #   branches...: no data found
  #
  # grep to find the 'lines...' line with total % (e,g, 13.2 above)
  lc_summary=$(lcov --summary coverage.info 2>&1 | grep -zo "lines[.]\+: [0-9.]\+" | tr '\0' '\n')
  # stip off the rest to get the number
  current_coverage=$(echo $lc_summary | sed 's/lines\.*: \(.*\)/\1/')

  # calculate difference to target
  result=$(awk 'BEGIN { print('$current_coverage'<'$target_coverage')?"F":"P"}')

  if [ "$result" = "P" ]; then

    echo "===> traget coverage reached: $current_coverage% of $target_coverage%"
    exit 0

  else

    lcov --list coverage.info
    echo "===>  FAILED to reach coverage target: $target_coverage% of $current_coverage%"
    exit 1

  fi

else

  lcov --list coverage.info --list-full-path

fi
