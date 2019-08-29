#!/bin/bash
rm -rf cov_html/ *.gc* *.info
rebar3 do clean, compile, eunit
lcov --directory . --capture --output-file coverage.info
lcov -r coverage.info "*.h" -o coverage.info
genhtml coverage.info --output-directory cov_html
lcov --list coverage.info
