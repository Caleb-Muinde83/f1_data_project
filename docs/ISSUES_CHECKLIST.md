# Issues checklist

One row per issue found while running the project on real data. **Do not reopen a ✅ item unless a new screenshot or log contradicts it.**

Legend: ✅ verified on real data · 🧪 fixed, verified on the synthetic fixtures only (not yet seen on real data) · 🔍 open, needs a diagnostic ·
⏸ by design / accepted · ⚠ fixed but a part could not be tested · 📋 roadmap (not started)

## Data model (dbt)

| # | Symptom on real data | Root cause | Fix (file) | Status |
|---|---|---|---|---|
| D1 | BigQuery would reject `int_lap_context` | four standalone `OFFSET 0` clauses | removed (`int_lap_context.sql`) | ⚠ DuckDB only: `make bq-emulator-test` never run |
| D2 | Points identical for every season; sprints scored on the race scale | un-keyed points seed | season-aware seed + join (`f1_points_system.csv`, `fct_race_results.sql`) | ✅ totals reconcile (e.g. 25 + 8 + 18 = 51). Assumption to confirm: 2026 Sprint scale is 8-1 |
| D3 | 13,022 pit-stop warnings; circuit pit loss meaningless | OpenF1 reports every practice/qualifying garage visit as a pit stop (median 340 s) | Race/Sprint only (`fct_pit_stops.sql`) | ✅ race median 23.5 s; the 123 remaining long race stops are legitimate review items (⏸) |
| D4 | Telemetry of the last lap held minutes of slow driving; delta traces to 150 s; 93 km/h "top speeds" | samples joined to the latest lap that *started*, no upper bound | lap window (`fct_telemetry.sql`) | ✅ `assert_telemetry_samples_within_lap_window` passes. Older telemetry needs `--full-refresh` (the Production warehouse was never rebuilt) |
| D5 | Starting grid empty for every race | OpenF1 keys grid rows to the *qualifying* session | map via the grid-setting session (`fct_race_results.sql`) | ✅ 93 of 94 races have a grid |
| D6 | Sprints scored 25-18-15…, no grid, counted as races | OpenF1 types a Sprint `session_type = 'Race'` | normalise in `stg_sessions.sql` | ✅ |
| D7 | 140 s spikes in the race trace at retirements; drivers ~100 s "ahead" with no lap-1 data | `SUM(lap_duration)` skips NULLs | cumulative time from lap END timestamps (`int_lap_context.sql`) | ✅ leader-time test passes; spot checks match pit stops and retirements |
| D8 | "35-lap VSC" | VSC upgraded to SC has no "VSC ending" message | a period ends when a more severe one starts; cap otherwise (`int_track_status_periods.sql`) | ✅ longest VSC 3 laps; overlap test passes (9898 was Practice 2) |
| D9 | OCO had two 10 s penalties | "PENALTY SERVED - …" notice parsed as a new penalty | drop served notices, add `served_lap` (`fct_penalties.sql`) | ✅ one penalty, `served_lap = 51` |
| D10 | 18 other (session, driver, seconds, type) groups with repeated penalties | unknown: legitimate repeats or another wording | — | 🔍 see "Needed" |
| D11 | 6–7 corners detected on Shanghai (16 turns) | the detector finds speed minima / braking zones, not turns | tuned run gave 7; defaults unchanged | ⏸ by design; revisit only if several circuits under-detect |
| D12 | ALO's telemetry: speed flat ≈ 300 km/h for 1 km, delta −6 s, apex speeds 35 km/h above the field | suspected sparse/gappy car data for that driver | — | 🔍 see "Needed" |
| D13 | 2025 São Paulo Sprint has no grid | no `starting_grid` rows at the source | optional fallback from Sprint Qualifying classification | 🔍 your call |
| D14 | `telemetry_only_sessions` to build telemetry per session | first run processes every session | new variable (`fct_telemetry.sql`) | ✅ built session 11245 |

## Dashboard

| # | Symptom | Fix | Status |
|---|---|---|---|
| U1 | `NA == NA` crashes (`session_laps`, health checks, a DNF's position); slider with min = max | `pd.notna`; replay stops with "No lap data" | ✅ app runs on real data |
| U2 | `StreamlitDuplicateElementId`; "connection is busy" | unique chart keys; one DuckDB cursor per run | ✅ |
| U3 | Position progression with no lines, race trace as dots | WebGL above 1000 rows → forced SVG (`components/plot.py`) | ✅ 2025 Shanghai screenshot |
| U4 | Timing tower with two P2 | rank by cumulative time | ✅ |
| U5 | Junk practice/qualifying tyre fits; stint projection tilted by restart laps | usable-fit filter; robust Theil-Sen | ✅ "17 of 45 fits hidden" |
| U6 | Simulator defaulted to a session it then rejected; 19-lap Sprint distance | only viable calibration sessions, earlier races at the circuit, distance of the same session kind | ✅ offers "2024 Race at this circuit" |
| U7 | Race distance 58 for a 56-lap race | distance = classified leader's laps (`data.race_distance`) | 🧪 confirm the field now reads 56 |
| U8 | Blank panels when the focus driver did not start | "(no laps)" in the sidebar + banner warning | 🧪 |
| U9 | Race events scattered; no way to jump | events timeline + "Jump to event" | ✅ visible on real pages |
| U10 | Championship chart showed the whole season on a post-race page | stops at the selected race (checkbox for the rest) | 🧪 |
| U11 | Engineer pre-race telemetry/circuit/corner panels blank with no explanation (telemetry built only for the race) | baseline defaults to a session with telemetry; message with the exact build command | 🧪 this round |
| U12 | Anomaly chart flattened by one z = 60 retirement lap | axis clipped at 12, hover keeps the true score | 🧪 this round |
| U13 | KPI tiles broke "YELLOW" into "YELLO W" on narrow screens | tile font scales with width | 🧪 this round |
| U14 | Wrong warehouse used by accident | sidebar shows the file; warns on the synthetic fixtures | ✅ |

## Needed from you (open items)

```bash
# D10: what are the 18 repeated-penalty groups?
dbt show --inline "select s.year, s.location, d.driver_acronym, p.penalty_seconds, p.penalty_type, p.lap_number, p.served_lap, substr(p.message,1,90) msg from fct_penalties p join dim_sessions s using (session_key) left join dim_driver_session d using (session_key, driver_number) where (p.session_key, p.driver_number, p.penalty_seconds, p.penalty_type) in (select session_key, driver_number, penalty_seconds, penalty_type from fct_penalties group by 1,2,3,4 having count(*) > 1) order by 1,2,3,6" --limit 40

# D12: does ALO (or anyone) have far fewer telemetry samples per lap?
dbt show --inline "select d.driver_acronym drv, count(*) samples, count(distinct t.lap_number) laps, round(count(*)*1.0/count(distinct t.lap_number),0) samples_per_lap from fct_telemetry t join dim_driver_session d on d.session_key = t.session_key and d.driver_number = t.driver_number where t.session_key = 11245 and t.lap_number is not null group by 1 order by 4" --limit 25

# D12: the largest gap between samples on the two laps in the overlay (ALO L25, VER L39)
dbt show --inline "select d.driver_acronym drv, t.lap_number, count(*) n, max(g) max_gap_ms from (select driver_number, lap_number, epoch_ms(telemetry_timestamp) - lag(epoch_ms(telemetry_timestamp)) over (partition by driver_number, lap_number order by telemetry_timestamp) g from fct_telemetry where session_key = 11245) t join dim_driver_session d on d.session_key = 11245 and d.driver_number = t.driver_number where (d.driver_acronym = 'ALO' and t.lap_number = 25) or (d.driver_acronym = 'VER' and t.lap_number = 39) group by 1, 2"

# U7: is the 2024 Shanghai race 56 laps, and does the lap table carry extra rows?
dbt show --inline "select (select max(lap_number) from fct_lap_performance where session_key = 9673) max_lap_row, (select max(laps_completed) from fct_race_results where session_key = 9673 and classification_status in ('Finished','Lapped')) leader_laps"
```

## Roadmap (not started)
Synchronised selection across all charts and play/pause replay speed · corner-level drill-down · engineering annotations · real predictive models, SHAP, drift
monitoring · live streaming · BigQuery, Docker and Airflow verification · Production warehouse rebuild.
