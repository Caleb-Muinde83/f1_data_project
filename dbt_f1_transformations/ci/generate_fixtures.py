"""
Generates a small, deterministic, OpenF1-shaped raw dataset (Parquet) so the whole dbt
project can be built and tested without calling the API.

  python ci/generate_fixtures.py <output_dir>

Design notes
  * 3 sessions: 9001 Qualifying, 9002 Race (clean numeric types), 9003 Race (string-typed gap columns),
    which reproduces the cross-file schema drift that needs union_by_name=true.
  * Lap data contains an in-lap, an out-lap, a Safety Car period, a sector yellow, a rain onset, a penalty.
  * location timestamps are offset from car_data timestamps so an equality join finds nothing (ASOF needed).
"""
import json, math, os, random, sys
from datetime import datetime, timedelta, timezone
import pyarrow as pa, pyarrow.parquet as pq

OUT = sys.argv[1] if len(sys.argv) > 1 else "fixtures_raw"
# Optional: FIXTURE_TELEMETRY_LAPS=3 limits car_data/location to the first N laps (a small set for slow engines/emulators)
TEL_LAPS = int(os.environ.get("FIXTURE_TELEMETRY_LAPS", "0")) or None
os.makedirs(OUT, exist_ok=True)
random.seed(7)
UTC = timezone.utc
iso = lambda d: d.astimezone(UTC).isoformat(timespec="milliseconds")

def save(endpoint, session, rows):
    if rows:
        pq.write_table(pa.Table.from_pylist(rows), f"{OUT}/{endpoint}_{session}_extracted.parquet", compression="snappy")

DRIVERS = {1: ("Max Verstappen", "VER", "Red Bull Racing"), 11: ("Sergio Perez", "PER", "Red Bull Racing"),
           44: ("Lewis Hamilton", "HAM", "Mercedes"), 63: ("George Russell", "RUS", "Mercedes")}
SESSIONS = [  # key, meeting, type, name, start
    (9001, 1001, "Qualifying", "Qualifying", datetime(2024, 3, 1, 15, 0, tzinfo=UTC)),
    (9002, 1001, "Race", "Race", datetime(2024, 3, 2, 15, 0, tzinfo=UTC)),
    (9003, 1002, "Race", "Race", datetime(2024, 3, 9, 15, 0, tzinfo=UTC)),
]
NLAPS, SC_LAPS = 12, (7, 8)
# Pit laps in race sessions: 1 and 63 stop first (lap 4), 44 stops lap 5 with a quicker stop, 11 stays out until lap 9.
# This yields an undercut (63 on 44), an overcut (11 on 1) and a slower-stop case.
PIT_LAPS = {1: 4, 63: 4, 44: 5, 11: 9}
OUTLAP_EXTRA = {44: 15}
meetings = [{"meeting_key": m, "year": 2024, "circuit_key": 10 + m % 10, "meeting_name": f"GP {m}", "meeting_official_name": f"Official GP {m}",
             "location": f"City {m}", "country_key": 1, "country_code": "XXX", "circuit_short_name": f"Circuit{m}",
             "date_start": iso(datetime(2024, 3, 1 + 8 * (m - 1001), 11, tzinfo=UTC)), "gmt_offset": "00:00:00"} for m in (1001, 1002)]
save("meetings", "global", meetings)
sessions, drivers_all = [], []
for key, mk, stype, sname, start in SESSIONS:
    sessions.append({"session_key": key, "meeting_key": mk, "circuit_key": 10 + mk % 10, "year": 2024, "session_name": sname,
                     "session_type": stype, "date_start": iso(start), "date_end": iso(start + timedelta(minutes=45)), "gmt_offset": "00:00:00"})
    for n, (full, acr, team) in DRIVERS.items():
        t = "Ferrari" if (key == 9003 and n == 63) else team   # mid-season team change for driver 63
        drivers_all.append({"driver_number": n, "session_key": key, "meeting_key": mk, "full_name": full, "name_acronym": acr,
                            "team_name": t, "team_colour": "FF0000", "country_code": "GBR", "headshot_url": "http://x/h.png"})
save("sessions", "global", sessions); save("drivers", "global", drivers_all)

def build_session(key, mk, stype, start):
    is_race = stype == "Race"
    base = {n: 90.0 + i * 0.35 for i, n in enumerate(DRIVERS)}
    laps, cum, starts, pits, stints = [], {n: 0.0 for n in DRIVERS}, {}, [], []
    for n in DRIVERS:
        pit_lap = PIT_LAPS.get(n) if is_race else None
        stints += ([{"session_key": key, "meeting_key": mk, "driver_number": n, "stint_number": 1, "compound": "MEDIUM", "tyre_age_at_start": 0, "lap_start": 1, "lap_end": pit_lap},
                    {"session_key": key, "meeting_key": mk, "driver_number": n, "stint_number": 2, "compound": "HARD", "tyre_age_at_start": 0, "lap_start": pit_lap + 1, "lap_end": NLAPS}]
                   if pit_lap else [{"session_key": key, "meeting_key": mk, "driver_number": n, "stint_number": 1, "compound": "MEDIUM", "tyre_age_at_start": 1, "lap_start": 1, "lap_end": NLAPS}])
        age0 = 0
        for lap in range(1, NLAPS + 1):
            if pit_lap and lap == pit_lap + 1: age0 = pit_lap + 1
            age = lap - age0
            d = base[n] + 0.06 * age - 0.035 * lap + random.uniform(-0.15, 0.15)
            if lap == 1: d += 6
            if lap in SC_LAPS: d += 32
            if pit_lap and lap == pit_lap: d += 5
            if pit_lap and lap == pit_lap + 1: d += OUTLAP_EXTRA.get(n, 19)
            s1, s2 = d * 0.30, d * 0.36
            ds = start + timedelta(seconds=120 + cum[n])
            laps.append({"session_key": key, "meeting_key": mk, "driver_number": n, "lap_number": lap, "date_start": iso(ds), "lap_duration": round(d, 3),
                         "duration_sector_1": round(s1, 3), "duration_sector_2": round(s2, 3), "duration_sector_3": round(d - s1 - s2, 3),
                         "i1_speed": 280 + n % 7, "i2_speed": 250 + n % 5, "st_speed": 320 + n % 9, "is_pit_out_lap": bool(pit_lap and lap == pit_lap + 1),
                         "segments_sector_1": json.dumps([2049, 2049]), "segments_sector_2": json.dumps([2049]), "segments_sector_3": json.dumps([2048])})
            starts[(n, lap)] = ds
            cum[n] += d
            if pit_lap and lap == pit_lap:
                pits.append({"session_key": key, "meeting_key": mk, "driver_number": n, "lap_number": lap, "date": iso(ds + timedelta(seconds=d - 20)), "pit_duration": 22.4})
    return laps, starts, pits, stints, cum

for key, mk, stype, sname, start in SESSIONS:
    is_race = stype == "Race"
    laps, starts, pits, stints, cum = build_session(key, mk, stype, start)
    save("laps", key, laps); save("stints", key, stints); save("pit", key, pits)
    order = sorted(cum, key=cum.get)
    # position events: one per driver per lap end, ranked by cumulative time
    pos_rows, ccum = [], {n: 0.0 for n in DRIVERS}
    for lap in range(1, NLAPS + 1):
        for n in DRIVERS: ccum[n] += next(l["lap_duration"] for l in laps if l["driver_number"] == n and l["lap_number"] == lap)
        for rank, n in enumerate(sorted(ccum, key=ccum.get), 1):
            pos_rows.append({"session_key": key, "meeting_key": mk, "driver_number": n, "position": rank, "date": iso(start + timedelta(seconds=120 + ccum[n]))})
    save("position", key, pos_rows)
    # intervals every 30s; string-typed (as sanitiser would emit) for session 9003
    iv = []
    if is_race:
        for k in range(0, int(cum[order[-1]] // 30)):
            ts = start + timedelta(seconds=120 + 30 * k)
            for r, n in enumerate(order):
                g = None if r == 0 else round(r * 1.4 + random.uniform(-0.3, 0.3), 3)
                i_ = None if r == 0 else round(1.4 + random.uniform(-0.5, 0.5), 3)
                if key == 9003 and n == 63 and k % 7 == 0: g, i_ = "+1 LAP", "+1 LAP"
                iv.append({"session_key": key, "meeting_key": mk, "driver_number": n, "date": iso(ts), "gap_to_leader": g, "interval": i_})
        if key == 9003:
            for row in iv:
                for c in ("gap_to_leader", "interval"): row[c] = None if row[c] is None else str(row[c])
    save("intervals", key, iv)
    # weather every 60s, rain from minute 14
    wx = [{"session_key": key, "meeting_key": mk, "date": iso(start + timedelta(seconds=60 * m)), "air_temperature": 24.0 - 0.1 * m, "track_temperature": 38.0 - 0.3 * m,
           "humidity": 50.0, "pressure": 1010.0, "rainfall": 1 if m >= 14 else 0, "wind_speed": 2.0, "wind_direction": 180} for m in range(0, 46)]
    save("weather", key, wx)
    # race control
    s_sc = start + timedelta(seconds=120 + sum(l["lap_duration"] for l in laps if l["driver_number"] == 1 and l["lap_number"] < SC_LAPS[0]) + 10)
    e_sc = s_sc + timedelta(seconds=150)
    y_ts = starts[(1, 3)] + timedelta(seconds=40)
    rc = [{"session_key": key, "meeting_key": mk, "date": iso(start + timedelta(seconds=100)), "category": "Flag", "flag": "GREEN", "scope": "Track", "message": "GREEN LIGHT - PIT EXIT OPEN", "lap_number": None, "driver_number": None, "sector": None}]
    if is_race:
        rc += [{"session_key": key, "meeting_key": mk, "date": iso(y_ts), "category": "Flag", "flag": "YELLOW", "scope": "Sector", "message": "YELLOW IN TRACK SECTOR 7", "lap_number": 3, "driver_number": None, "sector": 7},
               {"session_key": key, "meeting_key": mk, "date": iso(s_sc), "category": "SafetyCar", "flag": None, "scope": "Track", "message": "SAFETY CAR DEPLOYED", "lap_number": SC_LAPS[0], "driver_number": None, "sector": None},
               {"session_key": key, "meeting_key": mk, "date": iso(e_sc), "category": "SafetyCar", "flag": None, "scope": "Track", "message": "SAFETY CAR IN THIS LAP", "lap_number": SC_LAPS[1], "driver_number": None, "sector": None},
               {"session_key": key, "meeting_key": mk, "date": iso(e_sc + timedelta(seconds=70)), "category": "Flag", "flag": "GREEN", "scope": "Track", "message": "TRACK CLEAR", "lap_number": SC_LAPS[1] + 1, "driver_number": None, "sector": None},
               {"session_key": key, "meeting_key": mk, "date": iso(starts[(11, 9)] + timedelta(seconds=20)), "category": "Other", "flag": None, "scope": "Driver", "message": "FIA STEWARDS: 5 SECOND TIME PENALTY FOR CAR 11 (PER) - SPEEDING IN THE PIT LANE", "lap_number": None, "driver_number": 11, "sector": None}]
    save("race_control", key, rc)
    # results
    res = []
    for p, n in enumerate(order, 1):
        if is_race:
            lapped = key == 9003 and n == 63
            res.append({"session_key": key, "meeting_key": mk, "driver_number": n, "position": p, "dnf": False, "dns": False, "dsq": False, "number_of_laps": NLAPS - (1 if lapped else 0),
                        "duration": (str(round(cum[n], 3)) if key == 9003 else round(cum[n], 3)), "gap_to_leader": ("+1 LAP" if lapped else (round(cum[n] - cum[order[0]], 3) if key == 9002 else str(round(cum[n] - cum[order[0]], 3))))})
        else:
            q = [round(89.5 + 0.3 * p - i * 0.4, 3) for i in range(3)]
            res.append({"session_key": key, "meeting_key": mk, "driver_number": n, "position": p, "dnf": False, "dns": False, "dsq": False, "number_of_laps": 12,
                        "duration": json.dumps(q), "gap_to_leader": json.dumps([0.0, round(0.3 * (p - 1), 3), round(0.3 * (p - 1), 3)])})
    save("session_result", key, res)
    grid = [{"session_key": key, "meeting_key": mk, "driver_number": n, "position": p, "lap_duration": 89.0 + p * 0.3} for p, n in enumerate(reversed(order) if key == 9002 else order, 1)]
    save("starting_grid", key, grid)
    if is_race:
        save("overtakes", key, [{"session_key": key, "meeting_key": mk, "date": iso(starts[(63, 4)] + timedelta(seconds=30)), "overtaking_driver_number": 63, "overtaken_driver_number": 11, "position": 3},
                                {"session_key": key, "meeting_key": mk, "date": iso(starts[(44, 5)] + timedelta(seconds=75)), "overtaking_driver_number": 11, "overtaken_driver_number": 44, "position": 3}])
        save("championship_drivers", key, [{"session_key": key, "meeting_key": mk, "driver_number": n, "position_start": p, "position_current": p, "points_start": 0.0, "points_current": float(25 - 7 * (p - 1))} for p, n in enumerate(order, 1)])
        save("championship_teams", key, [{"session_key": key, "meeting_key": mk, "team_name": t, "position_start": i, "position_current": i, "points_start": 0.0, "points_current": float(40 - 20 * (i - 1))} for i, t in enumerate(["Red Bull Racing", "Mercedes"], 1)])
    save("team_radio", key, [{"session_key": key, "meeting_key": mk, "driver_number": 1, "date": iso(start + timedelta(seconds=600)), "recording_url": "http://x/r.mp3"}])
    # high-frequency telemetry for two drivers (4 Hz); location is offset by 70 ms
    car, loc = [], []
    for n in (1, 44):
        for l in (x for x in laps if x["driver_number"] == n and (TEL_LAPS is None or x["lap_number"] <= TEL_LAPS)):
            t0 = datetime.fromisoformat(l["date_start"]); steps = int(l["lap_duration"] * 4)
            for k in range(steps):
                ph = k / steps; spd = int(200 + 110 * math.sin(2 * math.pi * ph * 3))
                brk = 100 if math.cos(2 * math.pi * ph * 3) < -0.7 else 0
                car.append({"session_key": key, "meeting_key": mk, "driver_number": n, "date": iso(t0 + timedelta(seconds=k * 0.25)), "rpm": 9000 + spd * 10, "speed": spd,
                            "n_gear": 1 + spd // 45, "throttle": 0 if brk else 100, "brake": brk, "drs": 12 if (spd > 280 and not brk) else 0})
                loc.append({"session_key": key, "meeting_key": mk, "driver_number": n, "date": iso(t0 + timedelta(seconds=k * 0.25 + 0.07)),
                            "x": 1000.0 * math.cos(2 * math.pi * ph), "y": 600.0 * math.sin(2 * math.pi * ph), "z": 10.0})
    save("car_data", key, car); save("location", key, loc)
# radio transcripts (shape written by airflow/dags/modules/radio_transcriber.py); prefix differs from team_radio_* on purpose
for key, mk, stype, sname, start in SESSIONS:
    if stype != "Race": continue
    msgs = [(44, 200, "Box box box, we are switching to hard tyres"), (1, 420, "My tyres are gone, graining on the front left"),
            (63, 700, "Safety car safety car, stay out, stay out"), (11, 800, "DRS is not working in the fast sections"),
            (11, 900, "Copy, five second penalty, stewards are investigating"), (1, 950, "")]
    save("radio_transcripts", key, [{"session_key": key, "meeting_key": mk, "driver_number": n, "date": iso(start + timedelta(seconds=120 + sec)),
        "recording_url": f"http://x/{key}/{i}.mp3", "transcript": txt, "language": "en", "audio_duration_seconds": 4.2, "model": "small.en",
        "error": None, "_transcribed_at_utc": "2026-01-01T00:00:00+00:00"} for i, (n, sec, txt) in enumerate(msgs)])
print("fixtures written to", OUT)
