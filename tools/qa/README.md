# QA scripts (as run on 14 Nov 2026)

The scripts the coordinator used to drive the compose stack through load, faults and bad input (results: `docs/REQUIREMENTS.md` row D3, `.agents/logs/2026-11/14.md`). Standard library only; kept as run, not polished. They are not part of any gate.

They expect the stack on moved ports, because 8080 was taken on the machine they ran on:

```bash
APP_PORT=18080 PROVIDER_PORT=18090 GRAFANA_PORT=13000 docker compose -p notifier-qa up --build --wait
```

| Script | What it does |
|---|---|
| `qa.py` | `load N`, `wait`, `stats`, `reset`: queue messages on every channel, wait for the queue to drain, read the provider's counters (busiest second, keys requested twice) |
| `fuzz.py` | 92 malformed or odd requests; passes when none answers 5xx |
| `apikill.py` | 24 concurrent keyed batch posts, to run while the API container is killed and restarted; counts stored rows afterwards |
| `ws.mjs` | subscribes to a notification's WebSocket and prints the events in order (Node 22 or later) |

Faults were injected by hand between `load` and `wait`: `docker kill`, `docker pause` and `docker start` on the `mysql`, `valkey`, `worker` and `app` containers of the project.
