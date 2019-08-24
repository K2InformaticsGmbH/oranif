#!/bin/bash
rm -rf cov_html/ *.gc* *.info
rebar3 do clean, compile, eunit
lcov --directory . --capture --output-file coverage.info
lcov -r coverage.info "*.h" -o coverage.info
lcov --list coverage.info
