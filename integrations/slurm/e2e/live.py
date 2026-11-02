"""Live test of nvlink-rack-slurmd against real Slurm (topology/block,
3 blocks x 18 nodes; see cluster.sh). Runs inside the Slurm VM as a normal
user with passwordless sudo; jobs are submitted as that user, the daemon and
failure injection run as root.

Each scenario runs with stock Slurm (daemon off) and with the daemon on, so
the difference it makes is observed directly.

    # on the host, from integrations/slurm
    GOOS=linux GOARCH=arm64 go build -o /tmp/nvlink-rack-slurmd ./cmd/nvlink-rack-slurmd
    limactl copy /tmp/nvlink-rack-slurmd slurm:/tmp/nvlink-rack-slurmd
    limactl copy e2e/live.py slurm:/tmp/live.py
    limactl shell slurm -- python3 /tmp/live.py
"""
import argparse
import json
import os
import subprocess
import sys
import time

BIN = '/opt/slurm/bin'
SBIN = '/opt/slurm/sbin'
MARKER = 'nvlink-rack.io/preemptible'


def sh(*args, sudo=False, check=True):
    cmd = (['sudo'] if sudo else []) + list(args)
    out = subprocess.run(cmd, capture_output=True, text=True)
    if check and out.returncode != 0:
        raise RuntimeError(f'{" ".join(cmd)}: {out.stderr.strip()}')
    return out.stdout


def scontrol(*args, sudo=True, check=True):
    return sh(f'{BIN}/scontrol', *args, sudo=sudo, check=check)


def hostnames(hostlist):
    return scontrol('show', 'hostnames', hostlist, sudo=False).split() if hostlist else []


def jobs():
    data = json.loads(sh(f'{BIN}/squeue', '--json'))
    return {j['name']: j for j in data['jobs']}


def job(name):
    return jobs().get(name)


def submit(name, nodes, segment=None, preemptible=False, nodelist=None):
    args = [f'{BIN}/sbatch', '-o', '/dev/null', '-J', name, '-N', str(nodes)]
    if segment:
        args += [f'--segment={segment}']
    if preemptible:
        args += ['--requeue', f'--comment={MARKER}']
    else:
        args += ['--requeue']  # requeue on node failure, but not restart-safe for defrag
    if nodelist:
        args += ['-w', nodelist]
    sh(*args, '--wrap', 'sleep 100000')


def running(name):
    j = job(name)
    return j is not None and j['job_state'] == ['RUNNING']


def blocks_of(name):
    return sorted({n[:2] for n in hostnames(job(name)['nodes'])})


def node_states():
    data = json.loads(scontrol('show', 'nodes', '--json', sudo=False))
    return {n['name']: n['state'] for n in data['nodes']}


def idle_in(block):
    return sum(1 for n, s in node_states().items()
               if n.startswith(block) and 'IDLE' in s and 'DOWN' not in s)


def until(cond, what, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return time.time()
        time.sleep(1)
    raise AssertionError(f'timed out waiting for: {what}')


def settle(seconds=12):
    time.sleep(seconds)


def kill_node(node):
    """A real failure: the node's slurmd dies and Slurm marks it down."""
    sh('pkill', '-f', f'slurmd -N {node}$', sudo=True, check=False)
    scontrol('update', f'nodename={node}', 'state=down', 'reason=live-test-failure')


def revive_node(node):
    sh(f'{SBIN}/slurmd', '-N', node, sudo=True)
    time.sleep(2)
    scontrol('update', f'nodename={node}', 'state=resume', check=False)


def cleanup():
    sh(f'{BIN}/scancel', '-u', os.environ.get('USER', ''), check=False)
    until(lambda: not jobs(), 'jobs gone', timeout=60)
    for r in json.loads(scontrol('show', 'reservation', '--json'))['reservations']:
        if r['name'].startswith('nvlr-'):
            scontrol('delete', f'ReservationName={r["name"]}')
    for n, s in node_states().items():
        if 'DOWN' in s or 'DRAIN' in s:
            revive_node(n)
    until(lambda: all('IDLE' in s for s in node_states().values()), 'all nodes idle', 60)


class Daemon:
    def __init__(self, binary, log_path):
        self.log_path = log_path
        sh('rm', '-f', '/tmp/nvlr-state.json', sudo=True)
        self.proc = subprocess.Popen(
            ['sudo', binary, '--defrag=on', '--interval=2s', '--state=/tmp/nvlr-state.json',
             f'--scontrol={BIN}/scontrol', f'--squeue={BIN}/squeue'],
            stdout=open(log_path, 'a'), stderr=subprocess.STDOUT)
        time.sleep(5)

    def stop(self):
        sh('pkill', '-f', 'nvlink-rack-slurmd', sudo=True, check=False)
        self.proc.wait(timeout=10)


def scenario_small_job_beside_segments(daemon_on):
    submit('train', 32, segment=16)
    until(lambda: running('train'), 'train running')
    settle(6)
    submit('small', 2)
    until(lambda: running('small'), 'small running')
    train = blocks_of('train')
    return {'train blocks': train, 'small blocks': blocks_of('small'),
            'idle left in train blocks': {b: idle_in(b) for b in train}}


def scenario_node_failure(daemon_on):
    submit('train', 32, segment=16)
    until(lambda: running('train'), 'train running')
    settle(6)
    before = blocks_of('train')
    victim = hostnames(job('train')['nodes'])[5]
    t0 = time.time()
    kill_node(victim)
    until(lambda: job('train')['restart_cnt'] == 1, 'train requeued', timeout=30)
    t1 = until(lambda: running('train'), 'train running again', timeout=200)
    return {'blocks before': before, 'failed node': victim, 'blocks after': blocks_of('train'),
            'seconds to restart': round(t1 - t0),
            'reservation used': job('train')['resv_name']}


def scenario_fragmentation(daemon_on):
    # One small job per block, placed by hand as finished neighbours would
    # leave them: no block has 16 free nodes plus 2 spares.
    submit('frag-b1', 4, preemptible=True, nodelist='b1n[01-04]')
    submit('frag-b2', 2, preemptible=True, nodelist='b2n[01-02]')
    submit('pinned-b2', 2, nodelist='b2n[03-04]')
    submit('pinned-b3', 4, nodelist='b3n[01-04]')
    for name in ('frag-b1', 'frag-b2', 'pinned-b2', 'pinned-b3'):
        until(lambda: running(name), f'{name} running')
    settle(6)
    submit('big', 16, segment=16)
    try:
        until(lambda: running('big'), 'big running', timeout=45)
    except AssertionError:
        pass
    j = jobs()
    return {'big': j['big']['job_state'][0],
            'big blocks': blocks_of('big') if running('big') else [],
            'requeued': sorted(n for n, x in j.items() if x['restart_cnt'] > 0)}


SCENARIOS = [scenario_small_job_beside_segments, scenario_node_failure, scenario_fragmentation]


def check(mode, name, got):
    fails = []

    def expect(cond, what):
        if not cond:
            fails.append(f'{mode}/{name}: expected {what}; got {got}')

    if name == 'scenario_small_job_beside_segments':
        expect(len(got['train blocks']) == 2, 'two 16-node segments in two blocks')
        if mode == 'stock':
            expect(set(got['small blocks']) <= set(got['train blocks']),
                   "the small job on a segment block's last nodes")
        else:
            expect(not set(got['small blocks']) & set(got['train blocks']),
                   'the small job in the third block')
            expect(all(v == 2 for v in got['idle left in train blocks'].values()),
                   '2 idle nodes left in each segment block')
    elif name == 'scenario_node_failure':
        if mode == 'stock':
            expect(got['seconds to restart'] >= 100, "Slurm's requeue delay (~120 s)")
        else:
            expect(got['blocks after'] == got['blocks before'], 'the same blocks')
            expect(got['seconds to restart'] < 30, 'a restart within seconds')
            expect(got['reservation used'].startswith('nvlr-repl-'), 'the replacement reservation')
    elif name == 'scenario_fragmentation':
        if mode == 'stock':
            expect(got['big'] == 'PENDING', 'the 16-node job to wait')
        else:
            expect(got['big'] == 'RUNNING' and got['big blocks'] == ['b1'], 'the job running in b1')
            expect(got['requeued'] == ['frag-b1'], 'only frag-b1 requeued')
    return fails


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--daemon', default='/tmp/nvlink-rack-slurmd')
    parser.add_argument('--mode', action='append', choices=['stock', 'daemon'])
    parser.add_argument('--only')
    args = parser.parse_args()
    log_path = '/tmp/nvlink-rack-slurmd.log'
    open(log_path, 'w').close()
    failures = []
    for mode in args.mode or ['stock', 'daemon']:
        for scenario in [s for s in SCENARIOS if not args.only or args.only in s.__name__]:
            cleanup()
            daemon = Daemon(args.daemon, log_path) if mode == 'daemon' else None
            try:
                got = scenario(mode == 'daemon')
            finally:
                if daemon:
                    daemon.stop()
            print(f'{mode:7} {scenario.__name__:36} {got}', flush=True)
            failures += check(mode, scenario.__name__, got)
    cleanup()
    for f in failures:
        print('FAIL', f)
    print('all live checks passed' if not failures else f'{len(failures)} failure(s)')
    print(f'daemon log: {log_path}')
    sys.exit(1 if failures else 0)


if __name__ == '__main__':
    main()
