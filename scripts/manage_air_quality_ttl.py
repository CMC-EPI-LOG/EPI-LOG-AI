import argparse
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from dotenv import load_dotenv
from pymongo import MongoClient


def _format_date(value: Any) -> str:
    if value is None:
        return "None"
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _print_indexes(indexes: Dict[str, Dict[str, Any]]) -> None:
    for name, spec in indexes.items():
        expire = spec.get("expireAfterSeconds")
        key = spec.get("key")
        if expire is None:
            print(f"- {name}: key={key}")
        else:
            print(f"- {name}: key={key}, expireAfterSeconds={expire}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Manage TTL index for airkorea.air_quality_data."
    )
    parser.add_argument("--db", default="airkorea", help="MongoDB database name")
    parser.add_argument(
        "--collection",
        default="air_quality_data",
        help="MongoDB collection name",
    )
    parser.add_argument(
        "--date-field",
        default="updatedAt",
        help="Date field used for TTL expiration",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Retention days for TTL index (default: 7)",
    )
    parser.add_argument(
        "--index-name",
        default="ttl_updatedAt_7d",
        help="TTL index name",
    )
    parser.add_argument(
        "--prune-now",
        action="store_true",
        help="Immediately delete docs older than retention window",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show changes without creating index or deleting docs",
    )
    args = parser.parse_args()

    if args.retention_days <= 0:
        raise ValueError("--retention-days must be a positive integer")

    load_dotenv()
    mongo_uri = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI or MONGODB_URI is not set")

    expire_seconds = args.retention_days * 24 * 60 * 60
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.retention_days)

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    collection = client[args.db][args.collection]

    print("=== Current status ===")
    total_count = collection.estimated_document_count()
    newest = collection.find_one(
        {},
        {args.date_field: 1},
        sort=[(args.date_field, -1), ("_id", -1)],
    )
    oldest = collection.find_one(
        {},
        {args.date_field: 1},
        sort=[(args.date_field, 1), ("_id", 1)],
    )
    print(f"- target: {args.db}.{args.collection}")
    print(f"- docs: {total_count}")
    print(f"- newest {args.date_field}: {_format_date((newest or {}).get(args.date_field))}")
    print(f"- oldest {args.date_field}: {_format_date((oldest or {}).get(args.date_field))}")
    print("- indexes:")
    _print_indexes(collection.index_information())

    print("\n=== Plan ===")
    print(
        f"- create/update TTL index: name={args.index_name}, "
        f"field={args.date_field}, expireAfterSeconds={expire_seconds}"
    )
    if args.prune_now:
        prune_filter = {args.date_field: {"$lt": cutoff}}
        prune_count = collection.count_documents(prune_filter)
        print(
            f"- prune-now enabled: {prune_count} docs older than {cutoff.isoformat()} will be deleted"
        )
    else:
        print("- prune-now disabled: rely on TTL background deletion only")

    if args.dry_run:
        print("\nDry-run mode: no changes applied.")
        return 0

    index_result = collection.create_index(
        [(args.date_field, 1)],
        expireAfterSeconds=expire_seconds,
        name=args.index_name,
    )
    print(f"\n✅ TTL index ensured: {index_result}")

    deleted_count = 0
    if args.prune_now:
        prune_filter = {args.date_field: {"$lt": cutoff}}
        delete_result = collection.delete_many(prune_filter)
        deleted_count = delete_result.deleted_count
        print(f"🧹 Immediate prune deleted: {deleted_count}")

    print("\n=== Final status ===")
    print(f"- docs: {collection.estimated_document_count()} (deleted_now={deleted_count})")
    print("- indexes:")
    _print_indexes(collection.index_information())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
