#!/usr/bin/env python3
# Copyright 2026 The Dynaflux Authors
# SPDX-License-Identifier: Apache-2.0

"""S5 cost model for Dynaflux (monthly, per region and total).

A rough model (±30%) to compare designs, not a quote: use the AWS Pricing Calculator
for real numbers. Prices are us-east-1 (official DynamoDB on-demand page, Firehose
pricing page, Lambda arm64 list price); Asia regions are typically higher.
Run: python3 costs.py
"""
import math

MONTH_S = 730 * 3600

# ---- prices (us-east-1) ----
WRU = 0.625 / 1e6        # on-demand write request unit (also replicated writes)
RRU = 0.125 / 1e6        # on-demand read request unit
DDB_GB = 0.25            # Standard table class, per GB-month
DDB_IA_GB = 0.10         # Standard-IA storage (its WRU/RRU are ~25% pricier)
WCU_HOUR = 0.00065       # provisioned WCU-hour (third-party source; verify)
RCU_HOUR = 0.00013       # provisioned RCU-hour (third-party source; verify)
LAMBDA_REQ = 0.20 / 1e6
LAMBDA_GBS = 0.0000133334  # arm64
FIREHOSE_ICEBERG_GB = 0.045
S3_GB = 0.023
XFER_GB = 0.02           # inter-region transfer from US regions; Asia sources ~0.09

PARTITION_GB = 10        # ~10 GB per partition (widely cited; not on the docs page)
SHARD_LIFE_H = 4         # stream shards roll over at most every 4 hours


def scenario(name, events_s=1000, event_kb=1.0, regions=3, retention_days=7,
             archive_regions=None, window_s=1.0, prune="progress", provisioned=False,
             compression=6.0, hot_rows=1000, uniq_new_fraction=0.3, row_split=1,
             lifetime_s=120):
    """Three typical views:
      v1  low-cardinality aggregate, e.g. per page per day (hot_rows active rows)
      v2  per-entity state, e.g. argMax per user (a distinct row per event)
      v3  per-entity-per-day count + uniqExact (a distinct row per event, plus markers)
    """
    archive_regions = regions if archive_regions is None else archive_regions
    E = events_s
    wru_event = math.ceil(event_kb)

    # Stream table size drives partitions, which drive stream shards.
    stored_gb = E * (event_kb + 0.1) * retention_days * 86400 / 1e6
    partitions = max(1, math.ceil(stored_gb / PARTITION_GB), math.ceil(E * wru_event / 1000))
    rate = E / partitions                      # events/s per shard
    w = max(window_s, 0.25)                    # Lambda polls each shard 4x/s
    inv_per_shard = (1 - math.exp(-rate * w)) / w
    invocations = partitions * inv_per_shard   # per region
    batch = E / invocations

    # Rows touched per batch.
    v1_rows = hot_rows * (1 - (1 - 1 / hot_rows) ** batch)
    rows_per_batch = v1_rows + 2 * batch
    # High-water-mark map on a hot (v1) row: one entry per shard ID that touched the row
    # within the window an entry must be kept: 25 h (S1 rule), or, when pruned by progress,
    # the longest invocation lifetime. Splitting a hot row into row_split sub-rows divides
    # the shards that touch each sub-row (queries then read row_split items).
    window = 25 * 3600 if prune == "25h" else lifetime_s
    touches = inv_per_shard * v1_rows / hot_rows            # per shard, per second
    ids_per_partition = 1 + window / (SHARD_LIFE_H * 3600)
    hwm_entries = partitions / row_split * ids_per_partition * (1 - math.exp(-touches * window))
    hot_row_kb = 0.3 + 0.029 * hwm_entries
    hot_wru = math.ceil(hot_row_kb)
    hot_rru = math.ceil(hot_row_kb / 4)

    # Per-region request units per second.
    w_events = E * wru_event                                   # local + replicated: every event once per region
    w_ttl = E * wru_event * (regions - 1) / regions if retention_days < 10**5 else 0  # replicated TTL deletes
    w_views = invocations * (v1_rows * hot_wru + 2 * batch * 1) * 1.1   # +10% conflicts
    r_views = invocations * (v1_rows * hot_rru + 2 * batch * 1) * 1.1   # strongly consistent RMW reads
    r_markers = E * 1                                          # v3: read-before-write marker check
    w_markers = E * uniq_new_fraction                          # v3: new distinct values
    w_progress = invocations                                   # S1b progress item
    wps = w_events + w_ttl + w_views + w_markers + w_progress
    rps = r_views + r_markers

    if provisioned:
        ddb_req = (wps / 0.7 * WCU_HOUR + rps / 0.7 * RCU_HOUR) * 730  # 70% target utilization
    else:
        ddb_req = (wps * WRU + rps * RRU) * MONTH_S
    ddb_storage = stored_gb * DDB_GB
    dur_s = 0.03 + 0.002 * batch                               # rough: 30 ms + 2 ms/record
    lam = invocations * MONTH_S * (LAMBDA_REQ + dur_s * 1.0 * LAMBDA_GBS)  # 1 GB memory
    archive_gb_month = E * event_kb * 1.2 * MONTH_S / 1e6
    firehose = archive_gb_month * FIREHOSE_ICEBERG_GB if archive_regions else 0
    s3_per_year = archive_gb_month / compression * 12 * S3_GB * 0.5  # avg over first year
    xfer = E * event_kb * MONTH_S / 1e6 * (regions - 1) / regions * XFER_GB

    per_region = {
        "DynamoDB requests": ddb_req,
        "DynamoDB storage": ddb_storage,
        "Lambda (projector)": lam,
        "Firehose": firehose * archive_regions / regions,
        "S3 archive (yr-1 avg)": s3_per_year * archive_regions / regions,
        "Replication transfer": xfer,
    }
    return {
        "name": name, "per_region": per_region, "total": sum(per_region.values()) * regions,
        "partitions": partitions, "batch": batch, "invocations": invocations,
        "hot_row_kb": hot_row_kb, "wps": wps, "regions": regions, "hwm": hwm_entries,
    }


def show(s):
    print(f"\n## {s['name']}")
    print(f"   stream shards/region ≈ {s['partitions']:,}   avg batch ≈ {s['batch']:.1f}   "
          f"invocations/s/region ≈ {s['invocations']:,.0f}   hot row ≈ {s['hwm']:,.0f} hwm entries = {s['hot_row_kb']:.1f} KB"
          + ("  ⚠ OVER 400 KB ITEM LIMIT" if s['hot_row_kb'] > 400 else "") + "   "
          f"writes/s/region ≈ {s['wps']:,.0f}")
    for k, v in s["per_region"].items():
        print(f"   {k:<24} ${v:>10,.0f}")
    print(f"   {'= per region':<24} ${sum(s['per_region'].values()):>10,.0f}")
    print(f"   {'= total, ' + str(s['regions']) + ' regions':<24} ${s['total']:>10,.0f}")


if __name__ == "__main__":
    print("Monthly cost at 1,000 events/s × 1 KB, three views, us-east-1 prices (±30%)")
    for s in [
        scenario("A. Log kept 2 days in DynamoDB + archive (3 regions)", retention_days=2),
        scenario("A1. Log kept 7 days", retention_days=7),
        scenario("A2. Log kept 7 days, hot rows split in 4", retention_days=7, row_split=4),
        scenario("A3. Log 2 days, 2 regions", retention_days=2, regions=2),
        scenario("A4. Log 2 days, provisioned capacity", retention_days=2, provisioned=True),
        scenario("A5. Log 2 days, archive in 1 region", retention_days=2, archive_regions=1),
        scenario("B. Events kept 1 year in DynamoDB", retention_days=365),
        scenario("B1. 1 year, S1's 25 h high-water-mark rule", retention_days=365, prune="25h"),
        scenario("C. 100 events/s, log 2 days", events_s=100, retention_days=2),
        scenario("C1. 100 events/s, 1 year in DynamoDB", events_s=100, retention_days=365),
    ]:
        show(s)
