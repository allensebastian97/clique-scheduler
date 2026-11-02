#!/usr/bin/env bash
# A real Slurm cluster for the live test, on a laptop: one Lima VM running
# slurmctld and one slurmd per emulated node (Slurm's multiple-slurmd build),
# 3 topology/block blocks of 18 nodes.
#
#   e2e/cluster.sh vm      # create the VM and build Slurm (once, ~10 min)
#   e2e/cluster.sh start   # write the config and (re)start every daemon
#
# Run from integrations/slurm on the host. Needs Lima (brew install lima).
set -euo pipefail

VM=${VM:-slurm}
SLURM_VERSION=${SLURM_VERSION:-26.05.5}
BLOCKS=${BLOCKS:-3}
NODES_PER_BLOCK=${NODES_PER_BLOCK:-18}

in_vm() { limactl shell "$VM" -- sudo bash -c "$1"; }

vm() {
  limactl start --name="$VM" --cpus=4 --memory=3 --disk=20 --tty=false template:ubuntu-24.04
  in_vm "DEBIAN_FRONTEND=noninteractive apt-get update -qq &&
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq build-essential munge libmunge-dev \
      libjson-c-dev libyaml-dev pkg-config bzip2 python3 >/dev/null"
  in_vm "cd /tmp && curl -fsSL -o slurm.tar.bz2 https://download.schedmd.com/slurm/slurm-$SLURM_VERSION.tar.bz2 &&
    tar xjf slurm.tar.bz2 && cd slurm-$SLURM_VERSION &&
    ./configure --prefix=/opt/slurm --sysconfdir=/etc/slurm --enable-multiple-slurmd \
      --with-json --with-yaml >/tmp/configure.log &&
    make -j4 >/tmp/make.log && make install >/tmp/install.log"
}

start() {
  local nodes="" topo="" port
  for b in $(seq 1 "$BLOCKS"); do
    port=$((17000 + 100 * (b - 1)))
    nodes+="NodeName=b${b}n[01-$(printf %02d "$NODES_PER_BLOCK")] NodeHostname=localhost"
    nodes+=" Port=[$((port + 1))-$((port + NODES_PER_BLOCK))] CPUs=1 RealMemory=100"$'\n'
    topo+="BlockName=b$b Nodes=b${b}n[01-$(printf %02d "$NODES_PER_BLOCK")]"$'\n'
  done
  in_vm "mkdir -p /etc/slurm /var/spool/slurmctld /var/log/slurm
cat > /etc/slurm/slurm.conf <<EOF
ClusterName=nvl
SlurmctldHost=localhost
SlurmUser=root
AuthType=auth/munge
StateSaveLocation=/var/spool/slurmctld
SlurmdSpoolDir=/var/spool/slurmd/%n
SlurmdPidFile=/var/run/slurmd-%n.pid
SlurmctldPidFile=/var/run/slurmctld.pid
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd-%n.log
ProctrackType=proctrack/linuxproc
TaskPlugin=task/none
MpiDefault=none
# topology/block requires select/cons_tres; with select/linear Slurm
# silently ignores block boundaries.
SelectType=select/cons_tres
SelectTypeParameters=CR_CPU
SchedulerType=sched/backfill
SchedulerParameters=bf_interval=5
TopologyPlugin=topology/block
JobRequeue=1
ReturnToService=2
SlurmdTimeout=60
InactiveLimit=0
KillWait=5
${nodes}PartitionName=gpu Nodes=ALL Default=YES MaxTime=INFINITE State=UP OverSubscribe=EXCLUSIVE
EOF
cat > /etc/slurm/topology.conf <<EOF
${topo}BlockSizes=$NODES_PER_BLOCK
EOF
echo CgroupPlugin=disabled > /etc/slurm/cgroup.conf
systemctl start munge
pkill slurmd || true; pkill slurmctld || true; sleep 2
/opt/slurm/sbin/slurmctld -c
for b in \$(seq 1 $BLOCKS); do for i in \$(seq -w 1 $NODES_PER_BLOCK); do
  mkdir -p /var/spool/slurmd/b\${b}n\$i; /opt/slurm/sbin/slurmd -N b\${b}n\$i
done; done
sleep 10
/opt/slurm/bin/sinfo -o '%D %T'"
}

"${1:?usage: cluster.sh vm|start}"
