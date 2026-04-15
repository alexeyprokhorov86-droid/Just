#!/usr/bin/env python3
"""
NKT Dashboard — Просмотр данных посещаемости сотрудников (СКУД НКТ).
Flask-приложение на порту 5555.
"""

import os
import calendar
from datetime import datetime, date, timedelta
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from flask import Flask, request, redirect, url_for, Response

# ── .env ────────────────────────────────────────────────────────────────
load_dotenv(Path(__file__).resolve().parent / ".env")

DB_HOST = "172.20.0.2"
DB_NAME = "knowledge_base"
DB_USER = "knowledge"
DB_PASS = os.getenv("DB_PASSWORD", "")

app = Flask(__name__)


# ── helpers ─────────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def fetch_employee_names():
    """Return sorted list of distinct full_name values."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT full_name FROM c1_nkt_access_log "
                "WHERE full_name IS NOT NULL ORDER BY full_name"
            )
            return [r[0] for r in cur.fetchall()]


def fetch_daily_data(name: str, year: int, month: int):
    """Return list of dicts with daily attendance aggregates."""
    start = date(year, month, 1)
    _, last_day = calendar.monthrange(year, month)
    end = date(year, month, last_day) + timedelta(days=1)

    sql = """
    WITH entries AS (
        SELECT period::date AS day, time_in,
               ROW_NUMBER() OVER (PARTITION BY period::date ORDER BY time_in) AS rn
        FROM c1_nkt_access_log
        WHERE full_name = %(name)s
          AND period >= %(start)s AND period < %(end)s
          AND time_in IS NOT NULL
    ),
    exits AS (
        SELECT period::date AS day, time_out,
               ROW_NUMBER() OVER (PARTITION BY period::date ORDER BY time_out) AS rn
        FROM c1_nkt_access_log
        WHERE full_name = %(name)s
          AND period >= %(start)s AND period < %(end)s
          AND time_out IS NOT NULL
    ),
    paired AS (
        SELECT e.day, e.time_in, x.time_out,
               EXTRACT(EPOCH FROM (x.time_out - e.time_in)) / 3600 AS seg_hours
        FROM entries e
        JOIN exits x ON e.day = x.day AND e.rn = x.rn
        WHERE x.time_out > e.time_in
    ),
    daily AS (
        SELECT day,
               MIN(time_in)        AS first_in,
               MAX(time_out)       AS last_out,
               SUM(seg_hours)      AS hours,
               COUNT(*)            AS entries_count
        FROM paired
        GROUP BY day
    )
    SELECT day, first_in, last_out, hours, entries_count
    FROM daily
    ORDER BY day;
    """
    params = {"name": name, "start": start, "end": end}
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def fetch_after_hours(name: str, year: int, month: int, end_hour: int):
    """For each day, compute minutes on territory after end_hour."""
    start = date(year, month, 1)
    _, last_day = calendar.monthrange(year, month)
    end = date(year, month, last_day) + timedelta(days=1)

    sql = """
    WITH entries AS (
        SELECT period::date AS day, time_in,
               ROW_NUMBER() OVER (PARTITION BY period::date ORDER BY time_in) AS rn
        FROM c1_nkt_access_log
        WHERE full_name = %(name)s
          AND period >= %(start)s AND period < %(end)s
          AND time_in IS NOT NULL
    ),
    exits AS (
        SELECT period::date AS day, time_out,
               ROW_NUMBER() OVER (PARTITION BY period::date ORDER BY time_out) AS rn
        FROM c1_nkt_access_log
        WHERE full_name = %(name)s
          AND period >= %(start)s AND period < %(end)s
          AND time_out IS NOT NULL
    ),
    paired AS (
        SELECT e.day, e.time_in, x.time_out
        FROM entries e
        JOIN exits x ON e.day = x.day AND e.rn = x.rn
        WHERE x.time_out > e.time_in
    ),
    after AS (
        SELECT day,
               GREATEST(
                   EXTRACT(EPOCH FROM (
                       time_out - GREATEST(time_in, (day + interval '1 hour' * %(end_hour)s))
                   )) / 60,
                   0
               ) AS after_min
        FROM paired
        WHERE time_out > (day + interval '1 hour' * %(end_hour)s)
    )
    SELECT day, SUM(after_min) AS after_minutes
    FROM after
    GROUP BY day
    ORDER BY day;
    """
    params = {"name": name, "start": start, "end": end, "end_hour": end_hour}
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()


# ── Russian locale helpers ──────────────────────────────────────────────

MONTHS_RU = [
    "", "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
    "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
]

DAYS_RU_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def fmt_time(dt):
    if dt is None:
        return "&mdash;"
    return dt.strftime("%H:%M")


def fmt_hours(h):
    if h is None:
        return "&mdash;"
    return f"{h:.1f}"


def row_color(hours):
    if hours is None:
        return ""
    if hours < 8:
        return "background-color:#fff3cd;"   # yellow / short
    if hours <= 10:
        return "background-color:#d4edda;"   # green / normal
    if hours <= 12:
        return "background-color:#ffe0b2;"   # orange / long
    return "background-color:#ffccbc;"       # red-orange / very long


# ── CSS ─────────────────────────────────────────────────────────────────

CSS = """
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #f5f7fa; color: #333; padding: 16px; max-width: 1100px; margin: 0 auto; }
h1, h2, h3 { margin-bottom: 12px; }
a { color: #1a73e8; text-decoration: none; }
a:hover { text-decoration: underline; }

.card { background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,.12);
        padding: 20px; margin-bottom: 20px; }

.form-row { display: flex; flex-wrap: wrap; gap: 12px; align-items: end; margin-bottom: 16px; }
.form-group { display: flex; flex-direction: column; }
.form-group label { font-size: 13px; color: #666; margin-bottom: 4px; }
.form-group select, .form-group input {
    padding: 8px 12px; border: 1px solid #ccc; border-radius: 6px; font-size: 14px; }
button { padding: 8px 20px; background: #1a73e8; color: #fff; border: none;
         border-radius: 6px; font-size: 14px; cursor: pointer; }
button:hover { background: #1557b0; }

.summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
           gap: 12px; margin-bottom: 20px; }
.stat { background: #f0f4f8; padding: 14px; border-radius: 8px; text-align: center; }
.stat .val { font-size: 26px; font-weight: 700; color: #1a73e8; }
.stat .lbl { font-size: 12px; color: #666; margin-top: 4px; }

table { width: 100%; border-collapse: collapse; font-size: 14px; }
th { background: #f0f4f8; padding: 10px 8px; text-align: left; font-weight: 600;
     position: sticky; top: 0; }
td { padding: 8px; border-bottom: 1px solid #eee; }
tr:hover td { background: rgba(0,0,0,.02); }

.chart-wrap { max-width: 100%; overflow-x: auto; }
canvas { max-width: 100%; }

@media (max-width: 600px) {
    .form-row { flex-direction: column; }
    .summary { grid-template-columns: 1fr 1fr; }
    table { font-size: 12px; }
    td, th { padding: 6px 4px; }
}
</style>
"""

# ── Pages ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    names = fetch_employee_names()
    now = datetime.now()
    cur_month = now.month
    cur_year = now.year

    options_names = "\n".join(
        f'<option value="{n}">{n}</option>' for n in names
    )

    options_months = "\n".join(
        f'<option value="{m}" {"selected" if m == cur_month else ""}>'
        f'{MONTHS_RU[m]}</option>'
        for m in range(1, 13)
    )

    years = sorted({cur_year - 1, cur_year, cur_year + 1})
    options_years = "\n".join(
        f'<option value="{y}" {"selected" if y == cur_year else ""}>{y}</option>'
        for y in years
    )

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>НКТ — Посещаемость</title>
{CSS}
</head>
<body>
<h1>СКУД НКТ &mdash; Посещаемость сотрудников</h1>
<div class="card">
<form method="get" action="/employee">
  <div class="form-row">
    <div class="form-group">
      <label>Сотрудник</label>
      <select name="name" required style="min-width:260px;">
        <option value="">-- выберите --</option>
        {options_names}
      </select>
    </div>
    <div class="form-group">
      <label>Месяц</label>
      <select name="month">{options_months}</select>
    </div>
    <div class="form-group">
      <label>Год</label>
      <select name="year">{options_years}</select>
    </div>
    <div class="form-group">
      <button type="submit">Показать</button>
    </div>
  </div>
</form>
</div>

<div class="card" style="color:#888; font-size:13px;">
  Всего сотрудников в системе: {len(names)}<br>
  Данные из таблицы c1_nkt_access_log
</div>
</body></html>"""
    return Response(html, content_type="text/html; charset=utf-8")


@app.route("/employee")
def employee_detail():
    name = request.args.get("name", "")
    month = int(request.args.get("month", datetime.now().month))
    year = int(request.args.get("year", datetime.now().year))
    end_hour = int(request.args.get("end_hour", 17))

    if not name:
        return redirect(url_for("index"))

    rows = fetch_daily_data(name, year, month)
    after_rows = fetch_after_hours(name, year, month, end_hour)
    after_map = {r["day"]: r["after_minutes"] for r in after_rows}

    # ── summary stats ───────────────────────────────────────────────
    total_days = len(rows)
    total_hours = sum(r["hours"] or 0 for r in rows)
    avg_hours = total_hours / total_days if total_days else 0
    lt8 = sum(1 for r in rows if r["hours"] is not None and r["hours"] < 8)
    h8_10 = sum(1 for r in rows if r["hours"] is not None and 8 <= r["hours"] < 10)
    h10_12 = sum(1 for r in rows if r["hours"] is not None and 10 <= r["hours"] < 12)
    gt12 = sum(1 for r in rows if r["hours"] is not None and r["hours"] >= 12)
    total_after = sum(after_map.values())

    # ── table rows ──────────────────────────────────────────────────
    table_rows = ""
    chart_labels = []
    chart_data = []

    for r in rows:
        d = r["day"]
        dow = DAYS_RU_SHORT[d.weekday()]
        hours = r["hours"]
        style = row_color(hours)
        after_min = after_map.get(d, 0)

        chart_labels.append(d.strftime("%d.%m"))
        chart_data.append(round(hours, 2) if hours else 0)

        table_rows += (
            f'<tr style="{style}">'
            f'<td>{d.strftime("%d.%m.%Y")}</td>'
            f'<td>{dow}</td>'
            f'<td>{fmt_time(r["first_in"])}</td>'
            f'<td>{fmt_time(r["last_out"])}</td>'
            f'<td><b>{fmt_hours(hours)}</b></td>'
            f'<td>{r["entries_count"]}</td>'
            f'<td>{int(after_min)} мин</td>'
            f'</tr>\n'
        )

    # ── after-hours table ───────────────────────────────────────────
    after_table_rows = ""
    for ar in after_rows:
        d = ar["day"]
        mins = ar["after_minutes"]
        if mins and mins > 0:
            after_table_rows += (
                f'<tr><td>{d.strftime("%d.%m.%Y")}</td>'
                f'<td>{DAYS_RU_SHORT[d.weekday()]}</td>'
                f'<td>{int(mins)} мин ({mins/60:.1f} ч)</td></tr>\n'
            )

    chart_labels_js = str(chart_labels)
    chart_data_js = str(chart_data)

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{name} &mdash; {MONTHS_RU[month]} {year}</title>
{CSS}
<script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
</head>
<body>
<p><a href="/">&larr; К списку сотрудников</a></p>

<h1>{name}</h1>
<h3>{MONTHS_RU[month]} {year}</h3>

<!-- Summary -->
<div class="summary">
  <div class="stat"><div class="val">{total_days}</div><div class="lbl">Дней на территории</div></div>
  <div class="stat"><div class="val">{total_hours:.1f}</div><div class="lbl">Часов всего</div></div>
  <div class="stat"><div class="val">{avg_hours:.1f}</div><div class="lbl">Среднее ч/день</div></div>
  <div class="stat"><div class="val">{lt8}</div><div class="lbl">&lt; 8 ч</div></div>
  <div class="stat"><div class="val">{h8_10}</div><div class="lbl">8 &ndash; 10 ч</div></div>
  <div class="stat"><div class="val">{h10_12}</div><div class="lbl">10 &ndash; 12 ч</div></div>
  <div class="stat"><div class="val">{gt12}</div><div class="lbl">&ge; 12 ч</div></div>
</div>

<!-- Daily table -->
<div class="card" style="overflow-x:auto;">
<h2>Детализация по дням</h2>
<table>
<thead>
<tr><th>Дата</th><th>День</th><th>Вход</th><th>Выход</th><th>Часы</th><th>Входов</th><th>После {end_hour}:00</th></tr>
</thead>
<tbody>
{table_rows}
</tbody>
<tfoot>
<tr style="font-weight:700; background:#f0f4f8;">
  <td colspan="4">Итого</td>
  <td>{total_hours:.1f}</td>
  <td></td>
  <td>{int(total_after)} мин</td>
</tr>
</tfoot>
</table>
</div>

<!-- After-hours config -->
<div class="card">
<h2>Время после окончания рабочего дня</h2>
<form method="get" action="/employee">
  <input type="hidden" name="name" value="{name}">
  <input type="hidden" name="month" value="{month}">
  <input type="hidden" name="year" value="{year}">
  <div class="form-row">
    <div class="form-group">
      <label>Конец рабочего дня (час)</label>
      <input type="number" name="end_hour" value="{end_hour}" min="0" max="23" style="width:80px;">
    </div>
    <div class="form-group">
      <button type="submit">Пересчитать</button>
    </div>
  </div>
</form>

{f'''<table style="margin-top:12px;">
<thead><tr><th>Дата</th><th>День</th><th>После {end_hour}:00</th></tr></thead>
<tbody>{after_table_rows}</tbody>
<tfoot><tr style="font-weight:700;background:#f0f4f8;">
  <td colspan="2">Итого</td><td>{int(total_after)} мин ({total_after/60:.1f} ч)</td>
</tr></tfoot>
</table>''' if after_table_rows else '<p style="margin-top:8px;color:#888;">Нет переработок за выбранный период.</p>'}
</div>

<!-- Chart -->
<div class="card">
<h2>Часы на территории по дням</h2>
<div class="chart-wrap">
<canvas id="hoursChart" height="100"></canvas>
</div>
</div>

<script>
(function() {{
  const labels = {chart_labels_js};
  const data = {chart_data_js};
  const ctx = document.getElementById('hoursChart').getContext('2d');
  new Chart(ctx, {{
    type: 'bar',
    data: {{
      labels: labels,
      datasets: [{{
        label: 'Часы на территории',
        data: data,
        backgroundColor: data.map(v =>
          v < 8 ? '#fff3cd' : v <= 10 ? '#81c784' : v <= 12 ? '#ffb74d' : '#ef5350'
        ),
        borderRadius: 4
      }}]
    }},
    options: {{
      responsive: true,
      scales: {{
        y: {{
          beginAtZero: true,
          title: {{ display: true, text: 'Часы' }}
        }}
      }},
      plugins: {{
        legend: {{ display: false }},
        tooltip: {{
          callbacks: {{
            label: (ctx) => ctx.parsed.y.toFixed(1) + ' ч'
          }}
        }}
      }}
    }}
  }});
}})();
</script>

<p style="margin-top:20px;font-size:12px;color:#aaa;">
  Цвета: <span style="background:#fff3cd;padding:2px 8px;">&#60;8 ч</span>
  <span style="background:#d4edda;padding:2px 8px;">8-10 ч</span>
  <span style="background:#ffe0b2;padding:2px 8px;">10-12 ч</span>
  <span style="background:#ffccbc;padding:2px 8px;">&ge;12 ч</span>
</p>
</body></html>"""
    return Response(html, content_type="text/html; charset=utf-8")


# ── main ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5580, debug=False)
