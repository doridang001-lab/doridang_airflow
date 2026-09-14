#!/usr/bin/env bash
set -euo pipefail

if [[ ! -e /proc/sys/fs/binfmt_misc/register ]]; then
    mount -t binfmt_misc binfmt_misc /proc/sys/fs/binfmt_misc
fi
if [[ ! -e /proc/sys/fs/binfmt_misc/WSLInterop ]]; then
    printf ':WSLInterop:M::MZ::/init:PF\n' > /proc/sys/fs/binfmt_misc/register
fi

exec python3 /mnt/c/airflow/scripts/repair_baemin_acl_queue_wsl.py "$@"
