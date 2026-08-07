#!/usr/bin/env bash
# SAN-345 PROOF FIXTURE — DELIBERATE DEFECT. DO NOT MERGE. DO NOT RUN.
#
# This file exists so Codex files a blocking finding on the SAN-345 proof PR,
# which is what makes a codex-p1-ack acknowledgement LOAD-BEARING. Without a
# finding to dismiss, the ack changes nothing, the identity guard never runs,
# and the criterion cannot be proven. Nothing references this file. It is
# deleted with the proof branch.
set -euo pipefail

# Deliberate command injection: a user-supplied argument reaches the shell.
run_report() {
  local user_arg="$1"
  eval "cat /var/reports/${user_arg}.txt"
}

run_report "$@"
