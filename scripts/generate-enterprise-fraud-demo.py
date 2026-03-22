#!/usr/bin/env python3

import argparse
import json
import random
import signal
from datetime import UTC, datetime, timedelta


BUSINESS_UNITS = [
    ("consumer-banking", ["identity-core", "mobile-auth"]),
    ("global-cards", ["issuer-risk", "merchant-routing"]),
    ("treasury-platform", ["wire-screening", "payment-orchestrator"]),
]
CHANNELS = ["mobile", "api", "branch", "atm", "card_present"]
REGIONS = ["us-east", "eu-west", "ap-south"]
PLATFORMS = ["ios", "android", "web", "partner-api"]
TRUST_LEVELS = ["trusted", "unknown", "review"]
FRAUD_RINGS = ["ring-redwood", "ring-lantern", "ring-orbit"]
STATUSES = ["investigating", "auto-blocked", "confirmed"]
CUSTOMER_TIERS = ["bronze", "silver", "gold", "platinum"]
SCHEMA_VARIANTS = [
    ("canonical", 0.58),
    ("risk_score_string", 0.16),
    ("customer_tier_numeric", 0.14),
    ("mfa_required_string", 0.08),
    ("combined_conflict", 0.04),
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate a synthetic enterprise fraud bulk-ingest fixture."
    )
    parser.add_argument("--index", required=True, help="Target Astra dataset name")
    parser.add_argument("--metadata", action="store_true", help="Print metadata JSON only")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed")
    parser.add_argument(
        "--now-epoch-ms",
        type=int,
        help="Anchor time in epoch ms; defaults to current UTC time",
    )
    parser.add_argument("--historical-docs", type=int, default=750)
    parser.add_argument("--live-docs", type=int, default=400)
    parser.add_argument("--historical-days-ago", type=int, default=6)
    parser.add_argument("--historical-window-mins", type=int, default=180)
    parser.add_argument("--live-window-mins", type=int, default=90)
    parser.add_argument(
        "--window",
        choices=("all", "historical", "live"),
        default="all",
        help="Emit only a specific demo window, or all windows in ingest order",
    )
    return parser.parse_args()


def dt_from_epoch_ms(epoch_ms: int) -> datetime:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=UTC)


def iso(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_metadata(args) -> dict:
    anchor = (
        dt_from_epoch_ms(args.now_epoch_ms)
        if args.now_epoch_ms is not None
        else datetime.now(UTC)
    )
    historical_end = anchor - timedelta(days=args.historical_days_ago)
    historical_start = historical_end - timedelta(minutes=args.historical_window_mins)
    live_end = anchor
    live_start = live_end - timedelta(minutes=args.live_window_mins)

    return {
        "index": args.index,
        "anchorEpochMs": int(anchor.timestamp() * 1000),
        "historical": {
            "docs": args.historical_docs,
            "from": iso(historical_start),
            "to": iso(historical_end),
        },
        "live": {
            "docs": args.live_docs,
            "from": iso(live_start),
            "to": iso(live_end),
        },
        "expectedVisibleBeforeRestore": args.live_docs,
        "expectedVisibleAfterRestore": args.historical_docs + args.live_docs,
    }


def choose_variant(rng: random.Random, seq: int) -> str:
    # Keep the earliest documents canonical so Astra registers the desired field types first.
    if seq < 12:
        return "canonical"
    cutoff = rng.random()
    total = 0.0
    for label, weight in SCHEMA_VARIANTS:
        total += weight
        if cutoff <= total:
            return label
    return SCHEMA_VARIANTS[-1][0]


def make_doc(
    index: str,
    rng: random.Random,
    seq: int,
    window_seq: int,
    window_name: str,
    start_dt: datetime,
    end_dt: datetime,
    count: int,
):
    span_ms = max(int((end_dt - start_dt).total_seconds() * 1000), 1)
    event_ms = int(start_dt.timestamp() * 1000) + (span_ms * window_seq) // max(count - 1, 1)
    event_dt = dt_from_epoch_ms(event_ms)

    business_unit, teams = BUSINESS_UNITS[seq % len(BUSINESS_UNITS)]
    team = teams[seq % len(teams)]
    region = REGIONS[(seq // 3) % len(REGIONS)]
    channel = CHANNELS[(seq // 5) % len(CHANNELS)]
    platform = PLATFORMS[(seq // 7) % len(PLATFORMS)]
    trust_level = TRUST_LEVELS[(seq // 11) % len(TRUST_LEVELS)]
    fraud_ring = FRAUD_RINGS[(seq // 13) % len(FRAUD_RINGS)]
    status = STATUSES[(seq // 17) % len(STATUSES)]
    customer_tier = CUSTOMER_TIERS[(seq // 19) % len(CUSTOMER_TIERS)]
    transaction_amount = round(85 + ((seq * 37) % 9000) / 10 + rng.random(), 2)
    risk_score = round(22 + ((seq * 29) % 730) / 10 + rng.random(), 1)
    mfa_required = bool((seq // 2) % 2)
    schema_variant = choose_variant(rng, seq)

    doc = {
        "@timestamp": iso(event_dt),
        "incident_id": "fraud-wave-2026-q1",
        "event_window": window_name,
        "business_unit": business_unit,
        "team": team,
        "region": region,
        "channel": channel,
        "fraud_ring": fraud_ring,
        "detection_status": status,
        "schema_variant": schema_variant,
        "account_id": f"acct-{window_name[:1]}-{seq:05d}",
        "transaction_id": f"txn-{index[-6:]}-{window_name[:1]}-{seq:05d}",
        "transaction_amount": transaction_amount,
        "risk_score": risk_score,
        "customer_tier": customer_tier,
        "mfa_required": mfa_required,
        "device_platform": platform,
        "device_trust_level": trust_level,
        "message": (
            f"{business_unit} flagged {channel} activity tied to {fraud_ring} "
            f"from {region} during {window_name} investigation"
        ),
    }

    if schema_variant == "risk_score_string":
        doc["risk_score"] = f"{risk_score:.1f}"
    elif schema_variant == "customer_tier_numeric":
        doc["customer_tier"] = CUSTOMER_TIERS.index(customer_tier) + 1
    elif schema_variant == "mfa_required_string":
        doc["mfa_required"] = "true" if mfa_required else "false"
    elif schema_variant == "combined_conflict":
        doc["risk_score"] = f"{risk_score:.1f}"
        doc["customer_tier"] = CUSTOMER_TIERS.index(customer_tier) + 1
        doc["mfa_required"] = "true" if mfa_required else "false"

    return {"index": {"_index": index}}, doc


def emit_bulk(args, metadata):
    rng = random.Random(args.seed)
    historical_from = datetime.fromisoformat(metadata["historical"]["from"].replace("Z", "+00:00"))
    historical_to = datetime.fromisoformat(metadata["historical"]["to"].replace("Z", "+00:00"))
    live_from = datetime.fromisoformat(metadata["live"]["from"].replace("Z", "+00:00"))
    live_to = datetime.fromisoformat(metadata["live"]["to"].replace("Z", "+00:00"))

    def emit_window(
        window_name: str,
        start_dt: datetime,
        end_dt: datetime,
        count: int,
        seq_start: int,
    ) -> None:
        for offset in range(count):
            seq = seq_start + offset
            action, doc = make_doc(
                args.index,
                rng,
                seq,
                offset,
                window_name,
                start_dt,
                end_dt,
                count,
            )
            print(json.dumps(action, separators=(",", ":")))
            print(json.dumps(doc, separators=(",", ":")))

    if args.window in ("all", "historical"):
        emit_window(
            "historical",
            historical_from,
            historical_to,
            args.historical_docs,
            0,
        )

    if args.window in ("all", "live"):
        emit_window(
            "live",
            live_from,
            live_to,
            args.live_docs,
            args.historical_docs,
        )


def main():
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    args = parse_args()
    metadata = build_metadata(args)
    if args.metadata:
        print(json.dumps(metadata, indent=2, sort_keys=True))
        return
    emit_bulk(args, metadata)


if __name__ == "__main__":
    main()
