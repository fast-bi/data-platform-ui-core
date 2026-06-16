/*
 * BigQuery FinOps tab (dbt-bigquery-monitoring).
 *
 * Drives the "BigQuery FinOps" tab on /stats:
 *  - top-level tab switching (Warehouse Stats <-> BigQuery FinOps)
 *  - sub-tab switching (Overview / Compute / Users / dbt / Storage / Savings)
 *  - global filter bar (date range, project, user, model, category)
 *  - fetches data from /stats/bq-audit/api/* and renders Chart.js charts +
 *    auto-generated tables, reusing the console's card styling.
 *
 * No framework dependency beyond Chart.js (loaded before this file) and the
 * native fetch API. Sub-panes render lazily on first activation and re-render
 * when filters are applied.
 */
(function () {
    'use strict';

    var API = '/stats/bq-audit/api';
    var charts = {};       // canvasId -> Chart instance
    var loaded = {};       // subId -> bool (rendered at least once for current filters)
    var currentSub = 'overview';
    var PALETTE = ['#5e72e4', '#2dce89', '#fb6340', '#11cdef', '#f5365c', '#ffd600', '#8965e0', '#5603ad'];

    // ---------- environment helpers ----------
    function darkMode() { return document.body.className.indexOf('rj-dark-mode') !== -1; }
    function axisColor() { return darkMode() ? '#cfd6e4' : '#344767'; }
    function gridColor() { return darkMode() ? 'rgba(255,255,255,.08)' : 'rgba(0,0,0,.06)'; }
    function round2(v) { return v == null ? 0 : Math.round(v * 100) / 100; }

    function val(id) { var el = document.getElementById(id); return el && el.value ? el.value : ''; }

    function getFilters() {
        return {
            date_from: val('finops-date-from'),
            date_to: val('finops-date-to'),
            project_id: val('finops-project'),
            user_email: val('finops-user'),
            dbt_model_name: val('finops-model'),
            cost_category: val('finops-category')
        };
    }

    function qs(filters) {
        var p = new URLSearchParams();
        Object.keys(filters).forEach(function (k) { if (filters[k]) p.append(k, filters[k]); });
        var s = p.toString();
        return s ? ('?' + s) : '';
    }

    function fetchJSON(url) {
        return fetch(url, { headers: { 'Accept': 'application/json' } }).then(function (res) {
            return res.json().catch(function () { return { success: false, error: 'Bad response' }; })
                .then(function (json) {
                    if (!res.ok || !json.success) { throw new Error(json.error || ('HTTP ' + res.status)); }
                    return json.data;
                });
        });
    }

    // ---------- formatting ----------
    function fmtNum(v) { if (v == null) return '—'; return Number(v).toLocaleString('en-US'); }
    function fmtCur(v) { if (v == null) return '—'; return '€' + Number(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }); }
    function fmtBytes(v) {
        if (v == null) return '—';
        v = Number(v); var u = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']; var i = 0;
        while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
        return v.toFixed(2) + ' ' + u[i];
    }
    function prettyHeader(k) { return k.replace(/_/g, ' ').replace(/\b\w/g, function (c) { return c.toUpperCase(); }); }

    function colKind(key) {
        var k = key.toLowerCase();
        if (/ratio/.test(k)) return 'ratio';
        if (/(_pct|pct_|_rate$|rate_|percentage)/.test(k)) return 'pct';
        if (/bytes/.test(k)) return 'bytes';
        if (/(^|_)(cost|savings|forecast|spend|wasted|difference)/.test(k)) return 'cur';
        if (/(time|date|occurrence)/.test(k) || k === 'day' || k === 'hour') return 'date';
        if (/(count|_ms|seconds|rows|partitions|jobs|rank|days_|reference|users|number|hours)/.test(k)) return 'num';
        return 'text';
    }

    function fmtCell(key, v) {
        if (v == null || v === '') return '—';
        switch (colKind(key)) {
            case 'cur': return fmtCur(v);
            case 'bytes': return fmtBytes(v);
            case 'pct': return (typeof v === 'number' ? v.toFixed(2) : v) + '%';
            case 'ratio': return (typeof v === 'number' ? v.toFixed(2) : v);
            case 'num': return (typeof v === 'number' ? fmtNum(v) : v);
            case 'date': return String(v).replace('T', ' ').slice(0, 19);
            default: return escapeHtml(String(v));
        }
    }

    function isRightAligned(kind) { return kind === 'cur' || kind === 'num' || kind === 'bytes' || kind === 'pct' || kind === 'ratio'; }

    function escapeHtml(s) {
        return s.replace(/[&<>"']/g, function (c) {
            return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
        });
    }

    // ---------- DOM builders ----------
    function card(title, innerHtml, extraClass) {
        return '<div class="card mb-4 ' + (extraClass || '') + '">'
            + '<div class="card-header pb-0"><h6>' + title + '</h6></div>'
            + '<div class="card-body px-3 pt-2 pb-2">' + innerHtml + '</div></div>';
    }
    function chartCard(title, canvasId) {
        return card(title, '<div style="position:relative;height:320px;"><canvas id="' + canvasId + '"></canvas></div>');
    }
    function tableCard(title, mountId) {
        return card(title, '<div id="' + mountId + '"><div class="finops-loading">Loading…</div></div>');
    }
    function row(inner) { return '<div class="row">' + inner + '</div>'; }
    function col(inner, cls) { return '<div class="' + (cls || 'col-12') + '">' + inner + '</div>'; }

    function buildTable(rows) {
        if (!rows || !rows.length) { return '<div class="finops-loading">No data for the selected filters.</div>'; }
        var cols = Object.keys(rows[0]);
        var html = '<div class="finops-table-wrap"><table><thead><tr>';
        cols.forEach(function (c) { html += '<th>' + prettyHeader(c) + '</th>'; });
        html += '</tr></thead><tbody>';
        rows.forEach(function (r) {
            html += '<tr>';
            cols.forEach(function (c) {
                var kind = colKind(c);
                var cls = isRightAligned(kind) ? ' class="num"' : '';
                var cell;
                if (c === 'query' && r[c]) {
                    var full = String(r[c]);
                    var prev = full.replace(/\s+/g, ' ').trim();
                    cell = '<div class="finops-q" title="Click to view / copy full query">'
                        + '<code class="finops-q-prev">' + escapeHtml(prev.slice(0, 110)) + (prev.length > 110 ? '…' : '') + '</code>'
                        + '<i class="fa-solid fa-up-right-from-square finops-q-ico"></i>'
                        + '<span class="finops-q-full" style="display:none">' + escapeHtml(full) + '</span></div>';
                } else if (c === 'priority' && r[c]) {
                    cell = '<span class="badge-priority ' + String(r[c]).toUpperCase() + '">' + escapeHtml(String(r[c])) + '</span>';
                } else {
                    cell = fmtCell(c, r[c]);
                }
                var tdcls = (c === 'query') ? ' class="qcell"' : cls;
                html += '<td' + tdcls + '>' + cell + '</td>';
            });
            html += '</tr>';
        });
        html += '</tbody></table></div>';
        return html;
    }

    // ---------- chart helpers ----------
    function destroyChart(id) { if (charts[id]) { charts[id].destroy(); delete charts[id]; } }

    function baseOptions(extra) {
        var o = {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { labels: { color: axisColor(), font: { family: 'Roboto', size: 13 } } } },
            scales: {
                x: { ticks: { color: axisColor() }, grid: { color: gridColor() } },
                y: { ticks: { color: axisColor() }, grid: { color: gridColor() } }
            }
        };
        if (extra) { Object.keys(extra).forEach(function (k) { o[k] = extra[k]; }); }
        return o;
    }

    function lineByCategory(rows, labelKey, catKey, valKey) {
        var days = Array.from(new Set(rows.map(function (r) { return String(r[labelKey]).slice(0, 10); }))).sort();
        var cats = Array.from(new Set(rows.map(function (r) { return r[catKey]; })));
        var idx = {}; days.forEach(function (d, i) { idx[d] = i; });
        var datasets = cats.map(function (cat, ci) {
            var data = new Array(days.length).fill(0);
            rows.filter(function (r) { return r[catKey] === cat; }).forEach(function (r) {
                data[idx[String(r[labelKey]).slice(0, 10)]] = round2(r[valKey]);
            });
            var color = PALETTE[ci % PALETTE.length];
            return { label: cat, data: data, borderColor: color, backgroundColor: color + '33', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 };
        });
        return { labels: days, datasets: datasets };
    }

    function aggByDay(rows, dayKey, valKeys) {
        var map = {};
        rows.forEach(function (r) {
            var d = String(r[dayKey]).slice(0, 10);
            if (!map[d]) { map[d] = {}; }
            valKeys.forEach(function (k) { map[d][k] = (map[d][k] || 0) + (r[k] || 0); });
        });
        return { days: Object.keys(map).sort(), map: map };
    }

    function sumByKey(rows, key, valKey, n) {
        var m = {};
        rows.forEach(function (r) { m[r[key]] = (m[r[key]] || 0) + (r[valKey] || 0); });
        return Object.keys(m).map(function (k) { return [k, m[k]]; })
            .sort(function (a, b) { return b[1] - a[1]; }).slice(0, n);
    }

    // ---------- async loaders ----------
    function loadTable(mountId, datamart, filters) {
        var el = document.getElementById(mountId);
        if (!el) return;
        fetchJSON(API + '/datamart/' + datamart + qs(filters)).then(function (rows) {
            el.innerHTML = buildTable(rows);
        }).catch(function (e) {
            el.innerHTML = '<div class="finops-error">⚠ ' + escapeHtml(e.message) + '</div>';
        });
    }

    function loadChart(canvasId, datamart, filters, builder) {
        fetchJSON(API + '/datamart/' + datamart + qs(filters)).then(function (rows) {
            var cv = document.getElementById(canvasId);
            if (!cv) return;
            destroyChart(canvasId);
            if (!rows || !rows.length) { cv.parentElement.innerHTML = '<div class="finops-loading">No data for the selected filters.</div>'; return; }
            charts[canvasId] = builder(rows, cv.getContext('2d'));
        }).catch(function (e) {
            var cv = document.getElementById(canvasId);
            if (cv) { cv.parentElement.innerHTML = '<div class="finops-error">⚠ ' + escapeHtml(e.message) + '</div>'; }
        });
    }

    function loadKpis(mountId, filters) {
        var el = document.getElementById(mountId);
        if (!el) return;
        fetchJSON(API + '/summary' + qs(filters)).then(function (d) {
            // label, value, icon, accent colour
            var kpis = [
                ['Spend · This Month', fmtCur(d.spend_mtd), 'fa-coins', '#5e72e4'],
                ['Compute · This Month', fmtCur(d.compute_mtd), 'fa-bolt', '#11cdef'],
                ['Storage · This Month', fmtCur(d.storage_mtd), 'fa-hard-drive', '#2dce89'],
                ['Month Forecast', fmtCur(d.spend_month_forecast), 'fa-chart-line', '#fb6340'],
                ['Potential Savings', fmtCur(d.potential_savings), 'fa-piggy-bank', '#2dce89'],
                ['Tables Monitored', fmtNum(d.tables_monitored), 'fa-table-cells', '#8965e0'],
                ['Active Recommendations', fmtNum(d.active_recommendations), 'fa-lightbulb', '#ff7701'],
                ['Unique Users', fmtNum(d.unique_users), 'fa-users', '#f5365c']
            ];
            el.innerHTML = '<div class="finops-kpi-grid">' + kpis.map(function (k) {
                return '<div class="finops-kpi-card">'
                    + '<div class="kpi-icon" style="background:' + k[3] + '1f;color:' + k[3] + '"><i class="fa-solid ' + k[2] + '"></i></div>'
                    + '<div class="kpi-body"><p class="kpi-label">' + k[0] + '</p><h3>' + k[1] + '</h3></div>'
                    + '</div>';
            }).join('') + '</div>';
        }).catch(function (e) {
            el.innerHTML = '<div class="finops-error">⚠ ' + escapeHtml(e.message) + '</div>';
        });
    }

    // ---------- sub-tab renderers ----------
    function renderOverview(f) {
        var pane = document.getElementById('sub-overview');
        pane.innerHTML =
            '<div id="ov-kpis" class="mb-3"><div class="finops-loading">Loading…</div></div>'
            + row(col(chartCard('Daily Spend (compute vs storage)', 'ov-daily-spend'), 'col-lg-7 mt-2')
                + col(chartCard('Cost Trend (daily vs 7d / 30d avg)', 'ov-cost-trend'), 'col-lg-5 mt-2'))
            + row(col(card('Estimated Cost by Project',
                '<div class="finops-note"><b>On-demand estimate</b> (bytes billed × on-demand rate) over the package retention window — <b>not your actual invoice</b>. '
                + 'Actual billed spend (including Editions / flat-rate) is in the KPI cards and "Daily Spend" above, sourced from the GCP billing export. '
                + 'The billing export has no per-project dimension, so per-project cost can only be estimated here.</div>'
                + '<div id="ov-project"><div class="finops-loading">Loading…</div></div>')))
            + row(col(tableCard('Top Recommendations', 'ov-recs')));

        loadKpis('ov-kpis', f);
        loadChart('ov-daily-spend', 'daily_spend', f, function (rows, ctx) {
            return new Chart(ctx, { type: 'line', data: lineByCategory(rows, 'day', 'cost_category', 'cost'), options: baseOptions() });
        });
        loadChart('ov-cost-trend', 'cost_trend_comparison', f, function (rows, ctx) {
            var a = aggByDay(rows, 'day', ['daily_cost', 'rolling_7d_avg_cost', 'rolling_30d_avg_cost']);
            function ds(k, c) { return { label: prettyHeader(k), data: a.days.map(function (d) { return round2(a.map[d][k]); }), borderColor: c, backgroundColor: c + '22', tension: 0.3, pointRadius: 0, borderWidth: 2, fill: false }; }
            return new Chart(ctx, { type: 'line', data: { labels: a.days, datasets: [ds('daily_cost', PALETTE[0]), ds('rolling_7d_avg_cost', PALETTE[1]), ds('rolling_30d_avg_cost', PALETTE[2])] }, options: baseOptions() });
        });
        loadTable('ov-project', 'project_cost_summary', f);
        loadTable('ov-recs', 'recommendations', f);
    }

    function renderCompute(f) {
        var pane = document.getElementById('sub-compute');
        pane.innerHTML =
            row(col(chartCard('Compute Cost per Day (summed)', 'cmp-cost-day'), 'col-lg-7 mt-2')
                + col(chartCard('Query Volume & Cache Hit per Day', 'cmp-volume'), 'col-lg-5 mt-2'))
            + row(col(tableCard('Most Expensive Jobs', 'cmp-expensive')))
            + row(col(tableCard('Slowest Jobs', 'cmp-slowest'), 'col-lg-6')
                + col(tableCard('Most Repeated Jobs', 'cmp-repeated'), 'col-lg-6'))
            + row(col(tableCard('Materialization Candidates', 'cmp-materialize')))
            + row(col(tableCard('Job Timeline Analysis (slot / queue efficiency)', 'cmp-timeline')))
            + row(col(tableCard('Job Failure Analysis', 'cmp-failures')))
            + row(col(tableCard('Cost by Label', 'cmp-labels'), 'col-lg-6')
                + col(tableCard('BI Engine Analysis', 'cmp-biengine'), 'col-lg-6'))
            + row(col(tableCard('Compute Cost per Hour', 'cmp-hourly'), 'col-lg-6')
                + col(tableCard('Compute Cost per Minute', 'cmp-minute'), 'col-lg-6'))
            + row(col(tableCard('Reservation / Slot Usage per Hour', 'cmp-reservation'), 'col-lg-6')
                + col(tableCard('Reservation / Slot Usage per Minute', 'cmp-reservation-min'), 'col-lg-6'))
            + row(col(tableCard('Cheaper under Flat-rate Pricing', 'cmp-flat'), 'col-lg-6')
                + col(tableCard('Cheaper under On-demand Pricing', 'cmp-ondemand'), 'col-lg-6'));

        loadChart('cmp-cost-day', 'cost_per_project', f, function (rows, ctx) {
            var a = aggByDay(rows, 'day', ['total_query_cost']);
            return new Chart(ctx, { type: 'bar', data: { labels: a.days, datasets: [{ label: 'Query cost', data: a.days.map(function (d) { return round2(a.map[d].total_query_cost); }), backgroundColor: PALETTE[0] }] }, options: baseOptions() });
        });
        // Reliable reliability/usage signal: query volume (bars) + cache hit ratio (line).
        // NOTE: error_rate_over_time / total_failing_query_cost are unreliable in this
        // package build (failed == total), so we do NOT chart an error rate here.
        loadChart('cmp-volume', 'cost_per_project', f, function (rows, ctx) {
            var byDay = {};
            rows.forEach(function (r) {
                var d = String(r.day).slice(0, 10);
                if (!byDay[d]) { byDay[d] = { q: 0, ch: 0, n: 0 }; }
                byDay[d].q += r.query_count || 0;
                byDay[d].ch += r.cache_hit_ratio || 0;
                byDay[d].n += 1;
            });
            var days = Object.keys(byDay).sort();
            var opts = baseOptions();
            opts.scales = {
                x: { ticks: { color: axisColor() }, grid: { color: gridColor() } },
                y: { position: 'left', ticks: { color: axisColor() }, grid: { color: gridColor() }, title: { display: true, text: 'Queries', color: axisColor() } },
                y1: { position: 'right', ticks: { color: axisColor() }, grid: { drawOnChartArea: false }, title: { display: true, text: 'Cache hit', color: axisColor() } }
            };
            return new Chart(ctx, {
                data: {
                    labels: days,
                    datasets: [
                        { type: 'bar', label: 'Query count', data: days.map(function (d) { return byDay[d].q; }), backgroundColor: PALETTE[3], yAxisID: 'y' },
                        { type: 'line', label: 'Cache hit ratio', data: days.map(function (d) { return round2(byDay[d].ch / byDay[d].n); }), borderColor: PALETTE[1], backgroundColor: PALETTE[1] + '22', tension: 0.3, pointRadius: 0, borderWidth: 2, yAxisID: 'y1' }
                    ]
                },
                options: opts
            });
        });
        loadTable('cmp-expensive', 'most_expensive_jobs', f);
        loadTable('cmp-slowest', 'slowest_jobs', f);
        loadTable('cmp-repeated', 'most_repeated_jobs', f);
        loadTable('cmp-materialize', 'materialization_candidates', f);
        loadTable('cmp-timeline', 'job_timeline_analysis', f);
        loadTable('cmp-failures', 'job_failure_analysis', f);
        loadTable('cmp-labels', 'cost_by_label', f);
        loadTable('cmp-biengine', 'bi_engine_materialized_view_analysis', f);
        loadTable('cmp-hourly', 'compute_cost_per_hour_view', f);
        loadTable('cmp-minute', 'compute_cost_per_minute_view', f);
        loadTable('cmp-reservation', 'reservation_usage_per_hour', f);
        loadTable('cmp-reservation-min', 'reservation_usage_per_minute', f);
        loadTable('cmp-flat', 'query_with_better_pricing_using_flat_pricing_view', f);
        loadTable('cmp-ondemand', 'query_with_better_pricing_using_on_demand_view', f);
    }

    function renderUsers(f) {
        var pane = document.getElementById('sub-users');
        pane.innerHTML =
            row(col(chartCard('Top 15 Users by Total Query Cost', 'usr-top'), 'col-12 mt-2'))
            + row(col(tableCard('User Spend (daily)', 'usr-table')));

        loadChart('usr-top', 'most_expensive_users', f, function (rows, ctx) {
            var top = sumByKey(rows, 'user_email', 'total_query_cost', 15);
            return new Chart(ctx, {
                type: 'bar',
                data: { labels: top.map(function (t) { return t[0]; }), datasets: [{ label: 'Total query cost', data: top.map(function (t) { return round2(t[1]); }), backgroundColor: PALETTE[0] }] },
                options: baseOptions({ indexAxis: 'y' })
            });
        });
        loadTable('usr-table', 'most_expensive_users', f);
    }

    function renderDbt(f) {
        var pane = document.getElementById('sub-dbt');
        pane.innerHTML =
            row(col(chartCard('dbt Model Cost Trend (daily, summed)', 'dbt-trend'), 'col-12 mt-2'))
            + row(col(tableCard('Most Expensive Models', 'dbt-expensive'), 'col-lg-6')
                + col(tableCard('Most Repeated Models', 'dbt-repeated'), 'col-lg-6'))
            + row(col(tableCard('Model Trends (detail)', 'dbt-trends-tbl')));

        loadChart('dbt-trend', 'dbt_model_trends', f, function (rows, ctx) {
            var a = aggByDay(rows, 'day', ['total_query_cost']);
            return new Chart(ctx, { type: 'line', data: { labels: a.days, datasets: [{ label: 'Model cost', data: a.days.map(function (d) { return round2(a.map[d].total_query_cost); }), borderColor: PALETTE[6], backgroundColor: PALETTE[6] + '22', tension: 0.3, pointRadius: 0, borderWidth: 2, fill: true }] }, options: baseOptions() });
        });
        loadTable('dbt-expensive', 'most_expensive_models', f);
        loadTable('dbt-repeated', 'most_repeated_models', f);
        loadTable('dbt-trends-tbl', 'dbt_model_trends', f);
    }

    function renderStorage(f) {
        var pane = document.getElementById('sub-storage');
        pane.innerHTML =
            row(col(tableCard('Most Expensive Tables', 'st-expensive')))
            + row(col(tableCard('Unused Tables', 'st-unused'), 'col-lg-6')
                + col(tableCard('Write-only Tables', 'st-writeonly'), 'col-lg-6'))
            + row(col(tableCard('Read-heavy Tables', 'st-readheavy'), 'col-lg-6')
                + col(tableCard('Partitions Monitoring', 'st-partitions'), 'col-lg-6'))
            + row(col(tableCard('Datasets with Cost', 'st-datasets')))
            + row(col(tableCard('Write Ingestion Cost per Table', 'st-wicost')))
            + row(col(tableCard('Write Ingestion Errors', 'st-wierrors'), 'col-lg-6')
                + col(tableCard('Storage Billing per Hour', 'st-billing'), 'col-lg-6'))
            + row(col(tableCard('All Tables — Storage & Cost (full inventory)', 'st-allcost')))
            + row(col(tableCard('Tables — Storage, Cost & DDL', 'st-ddl')));

        loadTable('st-expensive', 'most_expensive_tables', f);
        loadTable('st-unused', 'unused_tables', f);
        loadTable('st-writeonly', 'write_only_tables', f);
        loadTable('st-readheavy', 'read_heavy_tables', f);
        loadTable('st-partitions', 'partitions_monitoring', f);
        loadTable('st-datasets', 'dataset_with_cost', f);
        loadTable('st-wicost', 'write_ingestion_cost_per_table', f);
        loadTable('st-wierrors', 'write_ingestion_errors_analysis', f);
        loadTable('st-billing', 'storage_billing_per_hour', f);
        loadTable('st-allcost', 'storage_with_cost', f);
        loadTable('st-ddl', 'table_and_storage_with_cost', f);
    }

    function renderSavings(f) {
        var pane = document.getElementById('sub-savings');
        pane.innerHTML =
            row(col(tableCard('Recommendations', 'sv-recs')))
            + row(col(tableCard('Tables – Potential Savings', 'sv-tables'), 'col-lg-6')
                + col(tableCard('Datasets – Potential Savings', 'sv-datasets'), 'col-lg-6'));

        loadTable('sv-recs', 'recommendations', f);
        loadTable('sv-tables', 'table_with_potential_savings', f);
        loadTable('sv-datasets', 'dataset_with_potential_savings', f);
    }

    function renderConfig(f) {
        var pane = document.getElementById('sub-config');
        pane.innerHTML =
            row(col(tableCard('Package Configuration', 'cfg-pkg'), 'col-lg-6')
                + col(tableCard('Dataset Storage Billing Models', 'cfg-datasets'), 'col-lg-6'))
            + row(col(tableCard('Project Options (effective)', 'cfg-eff')))
            + row(col(tableCard('Project Options', 'cfg-proj'), 'col-lg-6')
                + col(tableCard('Organization Options', 'cfg-org'), 'col-lg-6'))
            + row(col(tableCard('Project Option Changes (audit log)', 'cfg-proj-chg'), 'col-lg-6')
                + col(tableCard('Organization Option Changes (audit log)', 'cfg-org-chg'), 'col-lg-6'));

        loadTable('cfg-pkg', 'dbt_bigquery_monitoring_options', f);
        loadTable('cfg-datasets', 'dataset_options', f);
        loadTable('cfg-eff', 'information_schema_effective_project_options', f);
        loadTable('cfg-proj', 'information_schema_project_options', f);
        loadTable('cfg-org', 'information_schema_organization_options', f);
        loadTable('cfg-proj-chg', 'information_schema_project_options_changes', f);
        loadTable('cfg-org-chg', 'information_schema_organization_options_changes', f);
    }

    function renderRaw(f) {
        var pane = document.getElementById('sub-raw');
        pane.innerHTML =
            '<div class="finops-note">Raw &amp; backing models behind the curated datamarts. Useful for deep ad-hoc analysis; tables are large and capped per query.</div>'
            + row(col(tableCard('Raw Jobs with Cost', 'rw-jobs')))
            + row(col(tableCard('Jobs by Project with Cost', 'rw-jobs-proj')))
            + row(col(tableCard('Jobs from Audit Logs', 'rw-jobs-audit')))
            + row(col(tableCard('Job Costs (incremental)', 'rw-jobs-inc')))
            + row(col(tableCard('Model Costs (incremental)', 'rw-models-inc'), 'col-lg-6')
                + col(tableCard('User Costs (incremental)', 'rw-users-inc'), 'col-lg-6'))
            + row(col(tableCard('Compute Cost per Hour (raw)', 'rw-cph'), 'col-lg-6')
                + col(tableCard('Compute Cost per Minute (raw)', 'rw-cpm'), 'col-lg-6'))
            + row(col(tableCard('Compute Rollup per Hour', 'rw-rph'), 'col-lg-6')
                + col(tableCard('Compute Rollup per Minute', 'rw-rpm'), 'col-lg-6'))
            + row(col(tableCard('Compute Billing per Hour', 'rw-billing'), 'col-lg-6')
                + col(tableCard('Table Reference (incremental)', 'rw-tableref'), 'col-lg-6'))
            + row(col(tableCard('Partitions Monitoring (staging)', 'rw-stgpart')));

        loadTable('rw-jobs', 'jobs_with_cost', f);
        loadTable('rw-jobs-proj', 'jobs_by_project_with_cost', f);
        loadTable('rw-jobs-audit', 'jobs_from_audit_logs', f);
        loadTable('rw-jobs-inc', 'jobs_costs_incremental', f);
        loadTable('rw-models-inc', 'models_costs_incremental', f);
        loadTable('rw-users-inc', 'users_costs_incremental', f);
        loadTable('rw-cph', 'compute_cost_per_hour', f);
        loadTable('rw-cpm', 'compute_cost_per_minute', f);
        loadTable('rw-rph', 'compute_rollup_per_hour', f);
        loadTable('rw-rpm', 'compute_rollup_per_minute', f);
        loadTable('rw-billing', 'compute_billing_per_hour', f);
        loadTable('rw-tableref', 'table_reference_incremental', f);
        loadTable('rw-stgpart', 'stg_partitions_monitoring', f);
    }

    var RENDERERS = {
        overview: renderOverview, compute: renderCompute, users: renderUsers,
        dbt: renderDbt, storage: renderStorage, savings: renderSavings,
        config: renderConfig, raw: renderRaw
    };

    // ---------- navigation ----------
    function showSub(sub) {
        currentSub = sub;
        document.querySelectorAll('.finops-subtab').forEach(function (t) { t.classList.toggle('active', t.dataset.sub === sub); });
        document.querySelectorAll('.finops-subpane').forEach(function (p) { p.classList.toggle('active', p.id === 'sub-' + sub); });
        if (!loaded[sub]) { loaded[sub] = true; RENDERERS[sub](getFilters()); }
    }

    function showPane(paneId) {
        document.querySelectorAll('.stats-tab').forEach(function (t) { t.classList.toggle('active', t.dataset.pane === paneId); });
        document.querySelectorAll('.stats-pane').forEach(function (p) { p.classList.toggle('active', p.id === paneId); });
        if (paneId === 'pane-bq-finops' && !loaded[currentSub]) { showSub(currentSub); }
    }

    function applyFilters() {
        // Invalidate other sub-tabs so they re-fetch on next open; re-render current now.
        Object.keys(loaded).forEach(function (k) { if (k !== currentSub) loaded[k] = false; });
        loaded[currentSub] = true;
        RENDERERS[currentSub](getFilters());
    }

    function resetFilters() {
        ['finops-date-from', 'finops-date-to', 'finops-project', 'finops-user', 'finops-model', 'finops-category']
            .forEach(function (id) { var el = document.getElementById(id); if (el) el.value = ''; });
        Object.keys(loaded).forEach(function (k) { loaded[k] = false; });
        loaded[currentSub] = true;
        RENDERERS[currentSub](getFilters());
    }

    function fillSelect(id, items) {
        var el = document.getElementById(id);
        if (!el || !items) return;
        items.forEach(function (v) {
            var o = document.createElement('option');
            o.value = v; o.textContent = v;
            el.appendChild(o);
        });
    }

    function loadFilterOptions() {
        fetchJSON(API + '/filters').then(function (d) {
            fillSelect('finops-project', d.projects);
            fillSelect('finops-user', d.users);
            fillSelect('finops-model', d.dbt_models);
            fillSelect('finops-category', d.cost_categories);
        }).catch(function () { /* non-fatal: filters just stay as "All" */ });
    }

    // ---------- query view/copy modal ----------
    function ensureModal() {
        if (document.getElementById('finops-modal')) { return; }
        var m = document.createElement('div');
        m.id = 'finops-modal';
        m.className = 'finops-modal';
        m.style.display = 'none';
        m.innerHTML =
            '<div class="finops-modal-box">'
            + '<div class="finops-modal-head"><span>Query</span><div>'
            + '<button type="button" id="finops-modal-copy" class="finops-btn finops-btn-primary"><i class="fa-solid fa-copy"></i> Copy</button> '
            + '<button type="button" id="finops-modal-close" class="finops-btn finops-btn-ghost">Close</button></div></div>'
            + '<pre id="finops-modal-pre" class="finops-modal-pre"></pre></div>';
        document.body.appendChild(m);
        m.addEventListener('click', function (e) { if (e.target === m) { hideModal(); } });
        document.getElementById('finops-modal-close').addEventListener('click', hideModal);
        document.getElementById('finops-modal-copy').addEventListener('click', function () {
            var txt = document.getElementById('finops-modal-pre').textContent;
            var btn = document.getElementById('finops-modal-copy');
            function done() { btn.innerHTML = '<i class="fa-solid fa-check"></i> Copied'; setTimeout(function () { btn.innerHTML = '<i class="fa-solid fa-copy"></i> Copy'; }, 1500); }
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(txt).then(done).catch(function () { fallbackCopy(txt); done(); });
            } else { fallbackCopy(txt); done(); }
        });
    }
    function fallbackCopy(txt) {
        var ta = document.createElement('textarea');
        ta.value = txt; document.body.appendChild(ta); ta.select();
        try { document.execCommand('copy'); } catch (e) { /* noop */ }
        document.body.removeChild(ta);
    }
    function showModal(text) { ensureModal(); document.getElementById('finops-modal-pre').textContent = text; document.getElementById('finops-modal').style.display = 'flex'; }
    function hideModal() { var m = document.getElementById('finops-modal'); if (m) { m.style.display = 'none'; } }

    function init() {
        if (!document.getElementById('pane-bq-finops')) { return; } // audit disabled
        ensureModal();
        // Delegated open-on-click for query cells (tables are rendered dynamically).
        document.addEventListener('click', function (e) {
            if (!e.target.closest) { return; }
            var qcell = e.target.closest('.finops-q');
            if (qcell) {
                var full = qcell.querySelector('.finops-q-full');
                if (full) { showModal(full.textContent); }
            }
        });
        document.addEventListener('keydown', function (e) { if (e.key === 'Escape') { hideModal(); } });
        document.querySelectorAll('.stats-tab').forEach(function (t) {
            t.addEventListener('click', function () { showPane(t.dataset.pane); });
        });
        document.querySelectorAll('.finops-subtab').forEach(function (t) {
            t.addEventListener('click', function () { showSub(t.dataset.sub); });
        });
        var apply = document.getElementById('finops-apply');
        var reset = document.getElementById('finops-reset');
        if (apply) apply.addEventListener('click', applyFilters);
        if (reset) reset.addEventListener('click', resetFilters);
        loadFilterOptions();
    }

    if (document.readyState !== 'loading') { init(); }
    else { document.addEventListener('DOMContentLoaded', init); }
})();
