# Health Monitor xApp: live Evidence API publishing

This xApp subscribes to E2SM-KPM through FlexRIC and publishes the latest
structured observation directly to the Health Evidence Producer:

```text
E2 node -> E2SM-KPM -> Health xApp -> HTTP POST /v1/evidence
```

No JSON summary or raw-log file is used for rApp communication. The KPM
callback only updates an in-memory snapshot. A dedicated publisher thread
copies that snapshot after releasing the mutex, serializes it, and performs the
HTTP request, so a slow/unavailable Evidence API cannot block the E2 callback.

## FlexRIC build dependency

The source uses FlexRIC's relative includes and must be built inside the
external FlexRIC tree. On Ubuntu/Debian, install libcurl headers:

```bash
sudo apt update
sudo apt install libcurl4-openssl-dev
```

Place `xapp_health_moni.c` in the FlexRIC monitor examples directory, normally:

```text
<flexric>/examples/xApp/c/monitor/xapp_health_moni.c
```

Add/update the target in that directory's `CMakeLists.txt`:

```cmake
find_package(CURL REQUIRED)

add_executable(
  xapp_health_moni
  xapp_health_moni.c
  ../../../../src/util/alg_ds/alg/defer.c
)

target_link_libraries(
  xapp_health_moni
  PUBLIC
  e42_xapp
  -pthread
  -lsctp
  -ldl
  CURL::libcurl
)
```

Then reconfigure and build using the same FlexRIC build options already used by
your simulated environment:

```bash
cd <flexric>
cmake -S . -B build
cmake --build build --target xapp_health_moni -j"$(nproc)"
```

If your FlexRIC branch already defines this executable, only add
`find_package(CURL REQUIRED)` and `CURL::libcurl` to its existing target.

## Runtime configuration

The C process reads these environment variables directly:

```bash
export EVIDENCE_API_URL=http://127.0.0.1:9991/v1/evidence
export EVIDENCE_PUBLISH_INTERVAL_MS=1000
export EVIDENCE_HTTP_CONNECT_TIMEOUT_MS=1000
export EVIDENCE_HTTP_TIMEOUT_MS=10000
export HEALTH_XAPP_INSTANCE_ID=flexric-health-xapp
```

`HEALTH_XAPP_INSTANCE_ID` is a logical prefix. The process appends a boot-time
identifier so restarting the xApp safely restarts its sequence counter.

Use `127.0.0.1` only when the xApp and Evidence API run in the same VM/network
namespace. Otherwise use the Evidence API VM/service address and allow TCP port
9991 through the relevant firewall.

## Manual VM test

1. Start DME, the Evidence API, and the rApp.
2. Confirm the Evidence API is reachable:

   ```bash
   curl -f http://127.0.0.1:9991/healthz
   curl -i http://127.0.0.1:9991/readyz
   ```

3. Start FlexRIC and the simulated E2 node/gNB.
4. Start `xapp_health_moni` using your normal FlexRIC xApp arguments.
5. Confirm live evidence:

   ```bash
   curl -s http://127.0.0.1:9991/v1/evidence/latest | python3 -m json.tool
   curl -s 'http://127.0.0.1:9991/v1/evidence/history?limit=3' | python3 -m json.tool
   ```

6. Ask the rApp `Is the system healthy?`.
7. Stop the E2 agent while leaving the xApp alive. After `STALE_AFTER_S`, the
   rApp should report failed KPM freshness.
8. Stop the xApp with Ctrl+C; it removes successful subscriptions and exits
   cleanly.

## Current MVP limitations

- `ric_connected` and `e2_nodes_connected` are established during startup; a
  later transport loss is detected reliably by KPM staleness, but those two
  counters are not dynamically rediscovered after subscription.
- The subscription builder follows FlexRIC's KPM v3 format-3 monitor example
  and currently expects report style/action-definition format 4 in the first
  advertised report-style slot. Confirm that ordering in the exact FlexRIC/E2
  branch used by the VM; another ordering or report style needs a matching
  subscription builder.
- The flat metric object retains the last value encountered in an indication,
  rather than separate per-node/per-UE series.
- PDCP volume is published when the E2 node advertises it, but it is optional
  for the default health decision; the required set is configured in the rApp.
- Publishing every second creates about 86,400 evidence rows/day with the
  PostgreSQL backend. Configure database retention before long-running tests.
- HTTP delivery is latest-value/coalescing behavior. A failed request is not
  queued in the xApp; the following periodic snapshot is attempted normally.
