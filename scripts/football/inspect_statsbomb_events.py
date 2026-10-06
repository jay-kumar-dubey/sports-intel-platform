import json
import sys
from collections import Counter, defaultdict

SKIP_OBJECTS = {"type", "team", "player", "position", "possession_team", "play_pattern"}
SKIP_NAMES = {"recipient", "replacement"}  # player names: too many values to list


def inspect(path):
    with open(path, encoding="utf-8") as f:
        events = json.load(f)

    print("=" * 70)
    print(f"{path} | events: {len(events)}")
    print("top-level keys:", sorted({k for e in events for k in e}))
    print("periods:", sorted(Counter(e.get("period") for e in events).items()))

    print("\nevent types:")
    for name, n in Counter(e["type"]["name"] for e in events).most_common():
        print(f"  {name}: {n}")

    subkeys = defaultdict(set)
    named = defaultdict(set)
    for e in events:
        for k, v in e.items():
            if not isinstance(v, dict) or k in SKIP_OBJECTS:
                continue
            subkeys[k].update(v.keys())
            for sk, sv in v.items():
                if sk in SKIP_NAMES:
                    continue
                if isinstance(sv, dict) and "name" in sv:
                    named[f"{k}.{sk}"].add(sv["name"])

    print("\nsub-object keys:")
    for k in sorted(subkeys):
        print(f"  {k}: {sorted(subkeys[k])}")

    print("\nnamed values:")
    for k in sorted(named):
        print(f"  {k}: {sorted(named[k])}")

    print("\nlast event clock per period:")
    for p in sorted({e["period"] for e in events}):
        last = max(
            (e for e in events if e["period"] == p),
            key=lambda e: (e["minute"], e["second"]),
        )
        print(
            f"  period {p}: {last['minute']}:{last['second']:02d} ({last['type']['name']})"
        )

    shootout = [e for e in events if e["period"] == 5 and e["type"]["name"] == "Shot"]
    if shootout:
        print("\npenalty shootout shots:")
        for e in shootout:
            print(f"  {e['team']['name']}: {e['shot']['outcome']['name']}")

    for name in ("Shot", "Pass", "Duel", "Goal Keeper"):
        ex = next((e for e in events if e["type"]["name"] == name), None)
        print(
            f"\nexample {name}:",
            json.dumps(ex, ensure_ascii=False)[:1000] if ex else None,
        )
    print()


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    if len(sys.argv) < 2:
        sys.exit(
            "usage: python inspect_statsbomb_events.py <events.json> [<events.json> ...]"
        )
    for path in sys.argv[1:]:
        inspect(path)


if __name__ == "__main__":
    main()
