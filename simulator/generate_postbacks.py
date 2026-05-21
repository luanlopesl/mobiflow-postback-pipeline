#!/usr/bin/env python3
"""Send simulated MMP postbacks to the local ingestion API."""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_ENDPOINT = "http://localhost:5000/postback"
DEFAULT_TOTAL = 500
DEFAULT_DELAY_MS = 10

RATIO_VALID = 0.70
RATIO_DUPLICATE = 0.15
RATIO_INVALID = 0.15

_BASE_TS: float = 1746835200.0


PARTNERS: list[str] = ["appsflyer", "adjust", "singular", "tune"]

APPS: list[dict] = [
    {"app_id": "com.demo.wordpuzzle", "category": "casual"},
    {"app_id": "com.demo.spincasual", "category": "casual"},
    {"app_id": "com.demo.candyblast", "category": "casual"},
    {"app_id": "com.demo.learnlingo", "category": "education"},
    {"app_id": "com.demo.tradewallet", "category": "fintech"},
    {"app_id": "com.demo.neobank", "category": "fintech"},
    {"app_id": "com.demo.arcadehunt", "category": "gaming"},
    {"app_id": "com.demo.clanstrike", "category": "gaming"},
    {"app_id": "com.demo.mindfulapp", "category": "health"},
    {"app_id": "com.demo.calmspace", "category": "health"},
]

CAMPAIGNS: list[dict] = [
    {
        "campaign_id": "camp_ios_us_cpi_burst_001",
        "os": "ios",
        "country": "US",
        "goal": "install",
    },
    {
        "campaign_id": "camp_android_us_cpe_reg_002",
        "os": "android",
        "country": "US",
        "goal": "register",
    },
    {
        "campaign_id": "camp_ios_br_cpi_scale_003",
        "os": "ios",
        "country": "BR",
        "goal": "install",
    },
    {
        "campaign_id": "camp_android_de_cpa_purchase_004",
        "os": "android",
        "country": "DE",
        "goal": "purchase",
    },
    {
        "campaign_id": "camp_ios_jp_cpe_retention_005",
        "os": "ios",
        "country": "JP",
        "goal": "register",
    },
    {
        "campaign_id": "camp_android_gb_cpi_ua_006",
        "os": "android",
        "country": "GB",
        "goal": "install",
    },
    {
        "campaign_id": "camp_ios_au_cpa_sub_007",
        "os": "ios",
        "country": "AU",
        "goal": "subscription",
    },
    {
        "campaign_id": "camp_android_ca_cpe_ltv_008",
        "os": "android",
        "country": "CA",
        "goal": "purchase",
    },
]

PUBLISHERS: list[str] = [
    "pub_ironSource_001",
    "pub_unityAds_002",
    "pub_mintegral_003",
    "pub_appLovin_004",
    "pub_vungle_005",
    "pub_chartboost_006",
    "pub_inMobi_007",
    "pub_tapjoy_008",
]

REVENUE_RANGE: dict[str, tuple[float, float]] = {
    "install": (0.00, 0.00),
    "register": (0.00, 0.00),
    "purchase": (0.99, 99.99),
    "subscription": (4.99, 29.99),
    "level_complete": (0.00, 0.00),
}

_InvalidRecipe = tuple[str, Callable[[dict, random.Random], dict]]

INVALID_RECIPES: list[_InvalidRecipe] = [
    ("missing_click_id", lambda p, r: {k: v for k, v in p.items() if k != "click_id"}),
    ("empty_click_id", lambda p, r: {**p, "click_id": ""}),
    ("click_id_too_short", lambda p, r: {**p, "click_id": p["click_id"][:3]}),
    (
        "missing_event_type",
        lambda p, r: {k: v for k, v in p.items() if k != "event_type"},
    ),
    (
        "invalid_event_type",
        lambda p, r: {**p, "event_type": r.choice(["click", "impression", "INSTALL", ""])},
    ),
    ("missing_app_id", lambda p, r: {k: v for k, v in p.items() if k != "app_id"}),
    (
        "missing_campaign_id",
        lambda p, r: {k: v for k, v in p.items() if k != "campaign_id"},
    ),
    (
        "negative_revenue",
        lambda p, r: {**p, "event_revenue": round(r.uniform(-99.99, -0.01), 2)},
    ),
    (
        "revenue_as_string",
        lambda p, r: {**p, "event_revenue": str(round(r.uniform(0.99, 9.99), 2))},
    ),
    (
        "event_time_in_future",
        lambda p, r: {
            **p,
            "event_time": (
                datetime(2035, 1, 1, tzinfo=timezone.utc) + timedelta(days=r.randint(0, 30))
            ).isoformat(),
        },
    ),
    ("null_partner_id", lambda p, r: {**p, "partner_id": None}),
    ("completely_empty", lambda p, r: {}),
]


def make_click_id(device_id: str, campaign_id: str, click_ts: float, nonce: int) -> str:
    raw = f"{device_id}|{campaign_id}|{click_ts:.6f}|{nonce}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def make_event_id(rnd: random.Random) -> str:
    return str(uuid.UUID(int=rnd.getrandbits(128), version=4))


def make_device_id(rnd: random.Random, os: str) -> str:
    identifier = str(uuid.UUID(int=rnd.getrandbits(128), version=4))
    prefix = "idfa" if os == "ios" else "gaid"
    return f"{prefix}-{identifier}"


@dataclass
class Postback:
    click_id: str
    event_id: str
    app_id: str
    campaign_id: str
    partner_id: str
    publisher_id: str
    event_type: str
    event_revenue: float
    event_currency: str
    device_id: str
    os: str
    country: str
    event_time: str

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


def build_postback(campaign: dict, app: dict, nonce: int, rnd: random.Random) -> Postback:
    click_ts = _BASE_TS - rnd.uniform(0, 86_400)
    device_id = make_device_id(rnd, campaign["os"])
    click_id = make_click_id(device_id, campaign["campaign_id"], click_ts, nonce)

    event_type = campaign["goal"]
    rev_min, rev_max = REVENUE_RANGE[event_type]
    revenue = round(rnd.uniform(rev_min, rev_max), 2) if rev_max > 0 else 0.00

    # Event fires a few seconds to a few minutes after the click
    event_ts = click_ts + rnd.uniform(5, 300)
    event_dt = datetime.fromtimestamp(event_ts, tz=timezone.utc)

    return Postback(
        click_id=click_id,
        event_id=make_event_id(rnd),
        app_id=app["app_id"],
        campaign_id=campaign["campaign_id"],
        partner_id=rnd.choice(PARTNERS),
        publisher_id=rnd.choice(PUBLISHERS),
        event_type=event_type,
        event_revenue=revenue,
        event_currency="USD",
        device_id=device_id,
        os=campaign["os"],
        country=campaign["country"],
        event_time=event_dt.isoformat(),
    )


def make_session(max_retries: int = 2) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=max_retries,
        backoff_factor=0.3,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@dataclass
class Stats:
    total: int = 0
    sent_valid: int = 0
    sent_duplicate: int = 0
    sent_invalid: int = 0
    http_2xx: int = 0
    http_4xx: int = 0
    http_5xx: int = 0
    network_error: int = 0
    by_status_code: dict = field(default_factory=dict)
    by_invalid_type: dict = field(default_factory=dict)


def print_stats(stats: Stats, elapsed: float) -> None:
    rps = stats.total / elapsed if elapsed > 0 else 0.0

    print()
    print("Simulation complete")
    print(f"  total_sent:      {stats.total}")
    print(f"  valid_unique:    {stats.sent_valid}")
    print(f"  duplicates:      {stats.sent_duplicate}")
    print(f"  invalid:         {stats.sent_invalid}")
    print(f"  accepted_2xx:    {stats.http_2xx}")
    print(f"  unexpected_4xx:  {stats.http_4xx}")
    print(f"  server_5xx:      {stats.http_5xx}")
    print(f"  network_error:   {stats.network_error}")
    print(f"  elapsed_seconds: {elapsed:.2f}")
    print(f"  throughput_rps:  {rps:.1f}")

    if stats.by_status_code:
        print()
        print("HTTP status breakdown")
        for code, count in sorted(stats.by_status_code.items()):
            print(f"  {code}: {count}")

    if stats.by_invalid_type:
        print()
        print("Invalid payload breakdown")
        for itype, count in sorted(stats.by_invalid_type.items()):
            print(f"  {itype}: {count}")

    print()


def build_event_batch(
    total: int,
    rnd: random.Random,
) -> list[tuple[str, dict, str]]:
    n_valid = int(total * RATIO_VALID)
    n_duplicate = int(total * RATIO_DUPLICATE)
    n_invalid = total - n_valid - n_duplicate

    events: list[tuple[str, dict, str]] = []
    valid_pool: list[dict] = []

    for nonce in range(n_valid):
        campaign = rnd.choice(CAMPAIGNS)
        app = rnd.choice(APPS)
        payload = build_postback(campaign, app, nonce, rnd).to_dict()
        valid_pool.append(payload)
        events.append(("valid", payload, ""))

    for _ in range(n_duplicate):
        if valid_pool:
            original = rnd.choice(valid_pool)
            duplicate = {**original, "event_id": make_event_id(rnd)}
            events.append(("duplicate", duplicate, ""))
        else:
            campaign = rnd.choice(CAMPAIGNS)
            app = rnd.choice(APPS)
            payload = build_postback(campaign, app, nonce=999, rnd=rnd).to_dict()
            events.append(("valid", payload, ""))

    for _ in range(n_invalid):
        campaign = rnd.choice(CAMPAIGNS)
        app = rnd.choice(APPS)
        base = build_postback(campaign, app, nonce=0, rnd=rnd).to_dict()
        label, fn = rnd.choice(INVALID_RECIPES)
        bad_payload = fn(base, rnd)
        events.append(("invalid", bad_payload, label))

    rnd.shuffle(events)
    return events


def run(
    endpoint: str,
    total: int,
    delay_ms: int,
    seed: int | None,
) -> tuple[Stats, float]:
    rnd = random.Random(seed)
    stats = Stats()
    session = make_session()

    events = build_event_batch(total, rnd)

    n_valid = sum(1 for kind, _, _ in events if kind == "valid")
    n_duplicate = sum(1 for kind, _, _ in events if kind == "duplicate")
    n_invalid = sum(1 for kind, _, _ in events if kind == "invalid")

    seed_label = str(seed) if seed is not None else "random"

    print()
    print("MMP simulator")
    print(f"  endpoint: {endpoint}")
    print(f"  total:    {total}")
    print(f"  mix:      valid={n_valid}, duplicates={n_duplicate}, invalid={n_invalid}")
    print(f"  delay:    {delay_ms}ms")
    print(f"  seed:     {seed_label}")
    print()

    start = time.monotonic()

    for i, (kind, payload, invalid_label) in enumerate(events, 1):
        stats.total += 1
        if kind == "valid":
            stats.sent_valid += 1
        elif kind == "duplicate":
            stats.sent_duplicate += 1
        else:
            stats.sent_invalid += 1
            if invalid_label:
                stats.by_invalid_type[invalid_label] = (
                    stats.by_invalid_type.get(invalid_label, 0) + 1
                )

        try:
            response = session.post(
                url=endpoint,
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Simulator": "adtech-postback-pipeline/1.0",
                },
                timeout=5,
            )
            code = response.status_code
            stats.by_status_code[code] = stats.by_status_code.get(code, 0) + 1

            if 200 <= code < 300:
                stats.http_2xx += 1
            elif 400 <= code < 500:
                stats.http_4xx += 1
            else:
                stats.http_5xx += 1

            result = "ok" if 200 <= code < 300 else "fail"
            click_id = str(payload.get("click_id", "N/A"))[:12].ljust(12)
            etype = str(payload.get("event_type", "N/A"))[:14].ljust(14)
            suffix = f"  {invalid_label}" if invalid_label else ""
            print(f"  [{i:>4}] {result:<4} {code}  {kind:<9}  {click_id}  {etype}{suffix}")

        except requests.exceptions.ConnectionError:
            stats.network_error += 1
            if i == 1:
                print(f"\n  Cannot reach {endpoint}")
                print("  Is the server running? Try: docker compose up\n")
                sys.exit(1)
        except requests.exceptions.Timeout:
            stats.network_error += 1
            print(f"  [{i:>4}] timeout")
        except Exception as exc:  # noqa: BLE001
            stats.network_error += 1
            print(f"  [{i:>4}] error: {exc}")

        if delay_ms > 0 and i < total:
            time.sleep(delay_ms / 1_000)

    elapsed = time.monotonic() - start

    return stats, elapsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="generate_postbacks.py",
        description="Simulate MMP postback traffic for adtech-postback-pipeline.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  python generate_postbacks.py
  python generate_postbacks.py --total 100
  python generate_postbacks.py --total 1000 --delay 0
  python generate_postbacks.py --seed 42 --total 200
  python generate_postbacks.py --sample
        """,
    )
    parser.add_argument(
        "--endpoint", default=DEFAULT_ENDPOINT,
        metavar="URL",
        help=f"Target /postback URL  (default: {DEFAULT_ENDPOINT})",
    )
    parser.add_argument(
        "--total", type=int, default=DEFAULT_TOTAL,
        metavar="N",
        help=f"Total postbacks to send  (default: {DEFAULT_TOTAL})",
    )
    parser.add_argument(
        "--delay", type=int, default=DEFAULT_DELAY_MS,
        metavar="MS",
        help=f"Delay between requests in milliseconds  (default: {DEFAULT_DELAY_MS})",
    )
    parser.add_argument(
        "--seed", type=int, default=None,
        metavar="INT",
        help="Seed for reproducible runs (controls mix, shuffle, UUIDs, and timestamps)",
    )
    parser.add_argument(
        "--sample", action="store_true",
        help="Print one sample valid payload and exit (no HTTP requests)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.sample:
        rnd = random.Random(args.seed)
        campaign = rnd.choice(CAMPAIGNS)
        app = rnd.choice(APPS)
        payload = build_postback(campaign, app, nonce=0, rnd=rnd)
        print("\n  Sample valid postback payload:\n")
        print(payload.to_json(indent=2))
        print()
        return

    stats, elapsed = run(
        endpoint=args.endpoint,
        total=args.total,
        delay_ms=args.delay,
        seed=args.seed,
    )
    print_stats(stats, elapsed)


if __name__ == "__main__":
    main()
