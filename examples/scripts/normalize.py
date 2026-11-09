"""Validate and normalize the example support-ticket snapshot."""

import csv
import sys
from pathlib import Path


def main(source: Path, output: Path) -> None:
    with source.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ["ticket_id", "region", "channel", "queue_minutes"]:
            raise ValueError("Unexpected ticket columns")
        rows = list(reader)
    seen = set()
    clean = []
    for row in rows:
        ticket = row["ticket_id"].strip()
        if not ticket or ticket in seen:
            raise ValueError("Missing or duplicate ticket ID")
        seen.add(ticket)
        queue = float(row["queue_minutes"])
        if not 0 <= queue <= 10080:
            raise ValueError("Invalid queue time")
        clean.append({"ticket_id": ticket, "region": row["region"].strip().upper(),
                      "channel": row["channel"].strip().lower(), "queue_minutes": f"{queue:g}"})
    clean.sort(key=lambda item: item["ticket_id"])
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(clean)


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
