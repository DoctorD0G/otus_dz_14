#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import threading
import time
import asyncio
import aiofiles
from optparse import OptionParser
from concurrent.futures import ThreadPoolExecutor
import appsinstalled_pb2
import aiomcache
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path):
    head, fn = os.path.split(path)
    os.rename(path, os.path.join(head, "." + fn))


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
        logging.info("Invalid apps: `%s`" % raw_apps)
        return None
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
        return None
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    ua.apps.extend(appsinstalled.apps)
    key = f"{appsinstalled.dev_type}:{appsinstalled.dev_id}"
    packed = ua.SerializeToString()
    try:
        if dry_run:
            logging.debug(f"{memc_addr} - {key} -> {ua}")
        else:
            memc = memcache.Client([memc_addr])
            memc.set(key, packed)
    except Exception as e:
        logging.exception(f"Cannot write to memc {memc_addr}: {e}")
        return False
    return True


async def insert_appsinstalled_async(memc_addr, appsinstalled):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    ua.apps.extend(appsinstalled.apps)
    key = f"{appsinstalled.dev_type}:{appsinstalled.dev_id}"
    packed = ua.SerializeToString()
    try:
        async with aiomcache.Client(*memc_addr.split(":")) as client:
            await client.set(key.encode(), packed)
        return True
    except Exception as e:
        logging.exception(f"Cannot write to memc {memc_addr}: {e}")
        return False


async def process_file_async(filename, options):
    tasks = []
    async with aiofiles.open(filename, "r") as fd:
        async for line in fd:
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                continue
            memc_addr = options.device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                continue
            tasks.append(insert_appsinstalled_async(memc_addr, appsinstalled))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    processed = sum(1 for r in results if r is True)
    errors = len(results) - processed
    return processed, errors


async def process_files_async(file_list, options):
    tasks = []
    for filename in file_list:
        tasks.append(process_file_async(filename, options))
    results = await asyncio.gather(*tasks, return_exceptions=True)
    processed = sum(r[0] for r in results if isinstance(r, tuple))
    errors = sum(r[1] for r in results if isinstance(r, tuple))
    logging.info(f"Total processed: {processed}, Total errors: {errors}")
    return processed, errors


def process_file(filename, options):
    processed = errors = 0
    start_time = time.time()
    with gzip.open(filename, "rt") as fd:
        for line in fd:
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = options.device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error(f"Unknown device type: {appsinstalled.dev_type}")
                continue
            if insert_appsinstalled(memc_addr, appsinstalled, options.dry):
                processed += 1
            else:
                errors += 1
    elapsed_time = time.time() - start_time
    logging.info(f"Processed {processed} lines with {errors} errors in {filename} in {elapsed_time:.2f}s")
    return processed, errors


def main(options):
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    options.device_memc = device_memc

    if options.mode == "linear":
        for filename in glob.iglob(options.pattern):
            processed, errors = process_file(filename, options)
            logging.info(f"Processed {processed} lines with {errors} errors.")

    elif options.mode == "threaded":
        with ThreadPoolExecutor() as executor:
            futures = []
            for filename in glob.iglob(options.pattern):
                futures.append(executor.submit(process_file, filename, options))
            for future in futures:
                processed, errors = future.result()
                logging.info(f"Processed {processed} lines with {errors} errors.")

    elif options.mode == "async":
        asyncio.run(process_files_async(glob.iglob(options.pattern), options))


if __name__ == "__main__":
    op = OptionParser()
    op.add_option("-m", "--mode", action="store", default="linear",
                  help="Mode of execution: linear, threaded, async")
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
