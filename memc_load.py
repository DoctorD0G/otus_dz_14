#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import asyncio
import aiofiles
from optparse import OptionParser
import appsinstalled_pb2
import aiomcache
import memcache

NORMAL_ERR_RATE = 0.01
BATCH_SIZE = 100
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return None
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return None
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        logging.info(f"Invalid apps: `{raw_apps}`")
        return None
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info(f"Invalid geo coords: `{line}`")
        return None
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def batch_insert(memc, data, dry_run=False):
    if dry_run:
        for key, packed in data.items():
            logging.debug(f"{key} -> {packed}")
        return True

    try:
        memc.set_multi(data)
        return True
    except Exception as e:
        logging.exception(f"Failed to perform batch insert: {e}")
        return False


def process_file(filename, memc_clients, options):
    processed = errors = 0
    batches = {dev_type: {} for dev_type in memc_clients}

    with gzip.open(filename, "rt") as fd:
        for line in fd:
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = memc_clients.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error(f"Unknown device type: {appsinstalled.dev_type}")
                continue

            ua = appsinstalled_pb2.UserApps()
            ua.lat = appsinstalled.lat
            ua.lon = appsinstalled.lon
            ua.apps.extend(appsinstalled.apps)
            key = f"{appsinstalled.dev_type}:{appsinstalled.dev_id}"
            batches[appsinstalled.dev_type][key] = ua.SerializeToString()

            if len(batches[appsinstalled.dev_type]) >= BATCH_SIZE:
                if batch_insert(memc_addr, batches[appsinstalled.dev_type], options.dry):
                    processed += len(batches[appsinstalled.dev_type])
                else:
                    errors += len(batches[appsinstalled.dev_type])
                batches[appsinstalled.dev_type].clear()

    for dev_type, batch in batches.items():
        if batch:
            if batch_insert(memc_clients[dev_type], batch, options.dry):
                processed += len(batch)
            else:
                errors += len(batch)

    return processed, errors


def main(options):
    memc_clients = {
        "idfa": memcache.Client([options.idfa]),
        "gaid": memcache.Client([options.gaid]),
        "adid": memcache.Client([options.adid]),
        "dvid": memcache.Client([options.dvid]),
    }

    for filename in glob.iglob(options.pattern):
        processed, errors = process_file(filename, memc_clients, options)
        logging.info(f"Processed {processed} lines with {errors} errors in {filename}.")

    for client in memc_clients.values():
        client.disconnect_all()


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)

    (opts, args) = op.parse_args()
    logging.basicConfig(
        filename=opts.log,
        level=logging.DEBUG if opts.dry else logging.INFO,
        format="[%(asctime)s] %(levelname).1s %(message)s",
        datefmt="%Y.%m.%d %H:%M:%S"
    )
    main(opts)
