"""Entry point: python -m clique_scheduler"""
from __future__ import annotations

import argparse
import logging
import time

from clique_scheduler.k8s import KubeApi
from clique_scheduler.scheduler import Config, Scheduler

log = logging.getLogger('clique_scheduler')


def main() -> None:
    d = Config()
    parser = argparse.ArgumentParser(description='NVLink-clique-aware gang scheduler')
    parser.add_argument('--scheduler-name', default=d.scheduler_name)
    parser.add_argument('--clique-label', default=d.clique_label,
                        help='node label naming the NVLink domain')
    parser.add_argument('--slice-size', type=int, default=d.slice_size,
                        help='default ranks per clique (pods can override)')
    parser.add_argument('--reserve', type=int, default=d.reserve,
                        help='idle spare nodes kept per clique for replacements')
    parser.add_argument('--defrag', choices=['off', 'dry-run', 'on'], default=d.defrag)
    parser.add_argument('--migrate-slices', action='store_true',
                        help='move a slice to another clique when its own has no spare')
    parser.add_argument('--cooldown', type=float, default=d.cooldown,
                        help='seconds between defrag/migration attempts for one gang')
    parser.add_argument('--interval', type=float, default=2.0)
    parser.add_argument('--once', action='store_true', help='run one pass and exit')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    config = Config(scheduler_name=args.scheduler_name, clique_label=args.clique_label,
                    slice_size=args.slice_size, reserve=args.reserve, defrag=args.defrag,
                    migrate_slices=args.migrate_slices, cooldown=args.cooldown)
    scheduler = Scheduler(KubeApi(), config)
    while True:
        try:
            scheduler.run_once()
        except Exception:  # pylint: disable=broad-except
            log.exception('scheduling pass failed')
        if args.once:
            return
        time.sleep(args.interval)


if __name__ == '__main__':
    main()
