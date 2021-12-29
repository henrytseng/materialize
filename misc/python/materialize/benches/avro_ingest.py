# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""Ingest some Avro records, and report how long it takes"""

import argparse
import json
import os
import sys
import time
from typing import NamedTuple

from docker.models.containers import Container
import docker
import pg8000
import requests
from pg8000.exceptions import DatabaseError

from materialize import ROOT, mzbuild, spawn


def wait_for_confluent(host: str) -> None:
    url = f"http://{host}:8081/subjects"
    while True:
        try:
            print(f"Checking if schema registry at {url} is accessible...")
            r = requests.get(url)
            if r.status_code == 200:
                print("Schema registry is ready")
                return
        except requests.exceptions.ConnectionError as e:
            print(e)
            time.sleep(5)


class PrevStats(NamedTuple):
    wall_time: float
    user_cpu: float
    system_cpu: float


def print_stats(container: Container, prev: PrevStats) -> PrevStats:
    stats = json.loads(container.stats(stream=False))
    print(stats)
    new_prev = PrevStats(time.time(), cpu.user, cpu.system)
    print(
        f"{memory.rss},{memory.vms},{new_prev.user_cpu - prev.user_cpu},{new_prev.system_cpu - prev.system_cpu},{new_prev.wall_time - prev.wall_time}"
    )
    return new_prev


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--confluent-host",
        default="confluent",
        help="The hostname of a machine running the Confluent Platform",
    )
    parser.add_argument(
        "-n",
        "--trials",
        default=1,
        type=int,
        help="Number of measurements to take",
    )
    parser.add_argument(
        "-r",
        "--records",
        default=1000000,
        type=int,
        help="Number of Avro records to generate",
    )
    parser.add_argument(
        "-d",
        "--distribution",
        default="benchmark",
        type=str,
        help="Distribution to use in kafka-avro-generator",
    )
    args = parser.parse_args()

    os.chdir(ROOT)
    repo = mzbuild.Repository(ROOT)

    if args.confluent_host == "localhost" and sys.platform == "darwin":
        docker_confluent_host = "host.docker.internal"
    else:
        docker_confluent_host = "localhost"

    wait_for_confluent(args.confluent_host)

    images = ["kafka-avro-generator", "materialized"]
    deps = repo.resolve_dependencies([repo.images[name] for name in images])
    deps.acquire()

    docker_client = docker.from_env()

    mz_container = docker_client.containers.run(
        deps["materialized"].spec(), detach=True, network_mode="host",
    )

    loadgen_container = docker_client.containers.run(
        deps["kafka-avro-generator"].spec(),
        [
            "-n",
            str(args.records),
            "-b",
            f"{docker_confluent_host}:9092",
            "-r",
            f"http://{docker_confluent_host}:8081",
            "-t",
            "bench_data",
            "-d",
            args.distribution,
        ],
        detach=True,
        network_mode="host",
    )

    conn = pg8000.connect(host="localhost", port=6875, user="materialize")
    cur = conn.cursor()
    cur.execute(
        f"""CREATE SOURCE src
        FROM KAFKA BROKER '{args.confluent_host}:9093' TOPIC 'bench_data'
        FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://{args.confluent_host}:8081'""")

    print("Rss,Vms,User Cpu,System Cpu,Wall Time")
    prev = PrevStats(time.time(), 0.0, 0.0)
    for _ in range(args.trials):
        cur.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS ct AS SELECT count(*) FROM s")
        while True:
            try:
                cur.execute("SELECT * FROM ct")
                n = cur.fetchone()[0]
                if n == args.records:
                    break
            except DatabaseError:
                pass
            time.sleep(1)
        prev = print_stats(mz_container, prev)


if __name__ == "__main__":
    main()
