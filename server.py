#!/usr/bin/env python3
"""
ИдаПроджект + ИдаЛайт Marketing Dashboard — Backend
Realtime data from AmoCRM + Yandex Metrika + Google Sheets
Multi-project support via ?project=main|lite
"""
import json
import time
import os
import csv
import io
import requests
from datetime import datetime, timedelta
from collections import defaultdict
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

# ============================================================
# CACHE (in-memory, per project+endpoint, TTL 3 hours)
# ============================================================
CACHE = {}  # key: (project, endpoint) -> {'data': ..., 'ts': timestamp}
CACHE_TTL = 3 * 3600  # 3 hours in seconds

def cache_get(project, endpoint):
    """Return cached data if fresh, else None."""
    key = (project, endpoint)
    entry = CACHE.get(key)
    if entry and (time.time() - entry['ts']) < CACHE_TTL:
        return entry['data'], entry['ts']
    return None, None

def cache_set(project, endpoint, data):
    """Store data in cache."""
    CACHE[(project, endpoint)] = {'data': data, 'ts': time.time()}

def is_refresh():
    """Check if client requested forced refresh."""
    return request.args.get('refresh') == '1'

# ============================================================
# CONFIG
# ============================================================
CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.json')

# Project definitions
PROJECTS = {
    'main': {
        'name': 'ИдаПроджект',
        'pipeline_id': 8342526,
        'config_amo_key': 'amo',
        'config_metrika_key': 'metrika',
        'config_gsheet_key': 'google_sheet',
        'status_names': {
            67988798: 'Неразобранное',
            67988802: 'Первичный контакт',
            67988806: 'Бриф / Встреча',
            68025742: 'Работы',
            68350434: 'Отправка КП',
            67988810: 'Принимают решение',
            67988814: 'Согласование договора',
            68024722: 'Холд',
        },
        'loss_reasons': {
            18144242: 'Слишком дорого',
            18144246: 'Пропала потребность',
            18144250: 'Не устроили условия',
            18154174: 'Другое',
            18287198: 'Слишком долго',
            18287202: 'Перестали выходить на связь',
            18287206: 'Мы отказали',
            18338566: 'Ида.Лайт (передано)',
            18287230: 'Ида.Чат (передано)',
            22741334: 'Не лид',
        },
        'field_lead_source': 957929,
        'field_comment': 935554,
        'cache_file': 'amo_leads_idaproject.json',
    },
    'lite': {
        'name': 'ИдаЛайт',
        'pipeline_id': 5889679,
        'config_amo_key': 'lite.amo',
        'config_metrika_key': 'lite.metrika',
        'config_gsheet_key': 'lite.google_sheet',
        'status_names': {
            51439024: 'Неразобранное',
            72287470: 'Прямой контакт',
            51439027: 'Валидация',
            51439030: 'Квалификация 20%',
            51439033: 'Встреча 30%',
            51441334: 'Продажа 40%',
            51441337: 'Продажа 50%',
            51441340: 'Решение 80%',
            51441343: 'Оплачено 100%',
            51718570: 'Оплата закончилась',
            55966762: 'Отложенные',
            80147554: 'Отказ с возвратом',
        },
        'won_statuses': {51441343},  # Оплачено 100% = выигранная сделка
        'lost_statuses': {51718570, 80147554},  # Оплата закончилась, Отказ с возвратом
        'loss_reasons': {},  # Will be discovered dynamically
        'field_lead_source': None,  # Will discover from data
        'field_comment': None,
        'cache_file': None,
    },
}

def _get_nested(cfg, dotpath):
    """Get nested config value like 'lite.amo' -> cfg['lite']['amo']"""
    parts = dotpath.split('.')
    val = cfg
    for p in parts:
        val = val.get(p, {})
    return val

def _set_nested(cfg, dotpath, value):
    """Set nested config value"""
    parts = dotpath.split('.')
    val = cfg
    for p in parts[:-1]:
        if p not in val:
            val[p] = {}
        val = val[p]
    val[parts[-1]] = value

def load_config():
    # Try env var first (for Railway/cloud), then file
    env_cfg = os.environ.get('CONFIG_JSON')
    if env_cfg:
        return json.loads(env_cfg)
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            return json.load(f)
    return {}

def save_config(cfg):
    # In cloud env, save to env-based config is not persistent
    # but still save to file if possible
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
    except OSError:
        pass  # Read-only filesystem in cloud

def get_project(req=None):
    """Get project key from request args, default 'main'"""
    if req is None:
        req = request
    return req.args.get('project', 'main')

def get_proj_cfg(project):
    """Get project definition + its config sections"""
    proj = PROJECTS.get(project, PROJECTS['main'])
    cfg = load_config()
    amo = _get_nested(cfg, proj['config_amo_key']) or {}
    metrika = _get_nested(cfg, proj['config_metrika_key']) or {}
    gsheet = _get_nested(cfg, proj['config_gsheet_key']) or {}
    return proj, cfg, amo, metrika, gsheet

# ============================================================
# AMOCRM (multi-project)
# ============================================================
def amo_refresh_token(project='main'):
    proj, cfg, amo, _, _ = get_proj_cfg(project)
    resp = requests.post(f"https://{amo['subdomain']}.amocrm.ru/oauth2/access_token", json={
        'client_id': amo['client_id'],
        'client_secret': amo['client_secret'],
        'grant_type': 'refresh_token',
        'refresh_token': amo['refresh_token'],
        'redirect_uri': 'https://example.com',
    })
    if resp.status_code == 200:
        data = resp.json()
        amo_section = _get_nested(cfg, proj['config_amo_key'])
        amo_section['access_token'] = data['access_token']
        amo_section['refresh_token'] = data['refresh_token']
        amo_section['token_expires'] = time.time() + data.get('expires_in', 86400)
        save_config(cfg)
        return True
    print(f"AMO [{project}] token refresh failed: {resp.status_code} {resp.text}")
    return False

def amo_get_token(project='main'):
    proj, cfg, amo, _, _ = get_proj_cfg(project)
    if amo.get('token_expires', 0) < time.time() + 300:
        amo_refresh_token(project)
        _, cfg, amo, _, _ = get_proj_cfg(project)
    return amo.get('access_token', ''), amo.get('subdomain', '')

def amo_api(endpoint, params=None, project='main'):
    token, subdomain = amo_get_token(project)
    url = f"https://{subdomain}.amocrm.ru/api/v4/{endpoint}"
    resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, params=params or {})
    if resp.status_code == 401:
        amo_refresh_token(project)
        token, subdomain = amo_get_token(project)
        resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, params=params or {})
    if resp.status_code == 204:
        return {'_embedded': {}}
    return resp.json()

def fetch_all_amo_leads(project='main'):
    proj = PROJECTS.get(project, PROJECTS['main'])
    pipeline_id = proj['pipeline_id']
    all_leads = []
    seen_ids = set()
    date_from = int(datetime(2026, 1, 1).timestamp())

    # 1) Leads created in 2026 (filtered by pipeline)
    page = 1
    while True:
        data = amo_api('leads', {
            'limit': 250, 'page': page, 'with': 'loss_reason',
            'filter[created_at][from]': date_from,
            'filter[pipeline_id]': pipeline_id,
        }, project=project)
        leads = data.get('_embedded', {}).get('leads', [])
        for l in leads:
            if l['id'] not in seen_ids:
                all_leads.append(l)
                seen_ids.add(l['id'])
        if not leads or 'next' not in data.get('_links', {}):
            break
        page += 1

    # 2) Leads closed in 2026 (may have been created earlier, filtered by pipeline)
    page = 1
    while True:
        data = amo_api('leads', {
            'limit': 250, 'page': page, 'with': 'loss_reason',
            'filter[closed_at][from]': date_from,
            'filter[pipeline_id]': pipeline_id,
        }, project=project)
        leads = data.get('_embedded', {}).get('leads', [])
        for l in leads:
            if l['id'] not in seen_ids:
                all_leads.append(l)
                seen_ids.add(l['id'])
        if not leads or 'next' not in data.get('_links', {}):
            break
        page += 1

    return all_leads

def fetch_loss_reasons(project='main'):
    """Fetch loss reason names from AmoCRM API"""
    try:
        data = amo_api('leads/loss_reasons', {}, project)
        if data and '_embedded' in data and 'loss_reasons' in data['_embedded']:
            result = {}
            for r in data['_embedded']['loss_reasons']:
                result[r['id']] = r['name']
            return result
    except Exception as e:
        print(f'Error fetching loss reasons for {project}: {e}')
    return {}

def analyze_amo_data(leads, project='main'):
    proj = PROJECTS.get(project, PROJECTS['main'])
    pipeline_id = proj['pipeline_id']
    status_names = proj['status_names']
    loss_reasons_map = proj['loss_reasons']
    # Auto-fetch loss reasons if empty
    if not loss_reasons_map:
        loss_reasons_map = fetch_loss_reasons(project)
        proj['loss_reasons'] = loss_reasons_map
    field_lead_source = proj.get('field_lead_source')
    field_comment = proj.get('field_comment')

    leads = [l for l in leads if l.get('pipeline_id') == pipeline_id]

    monthly = defaultdict(lambda: {
        'leads': 0, 'won': 0, 'lost': 0,
        'total_budget': 0, 'won_budget': 0,
        'by_source': defaultdict(int),
        'loss_reasons': defaultdict(int),
        'has_price': 0, 'has_source': 0, 'has_comment': 0,
        'has_company': 0, 'lost_total': 0, 'has_loss_reason': 0,
    })

    source_totals = defaultdict(lambda: {'leads': 0, 'won': 0, 'budget': 0})
    loss_totals = defaultdict(int)
    active_funnel = defaultdict(int)

    for lead in leads:
        created = datetime.fromtimestamp(lead['created_at'])
        mk = created.strftime('%Y-%m')
        is_2026 = mk >= '2026-01'
        price = lead.get('price', 0) or 0
        status_id = lead['status_id']

        # Count lead in its creation month (only 2026+)
        if is_2026:
            m = monthly[mk]
            m['leads'] += 1
            m['total_budget'] += price
            if price > 0:
                m['has_price'] += 1
            companies = lead.get('_embedded', {}).get('companies', [])
            if companies:
                m['has_company'] += 1

        source = None
        cf = lead.get('custom_fields_values') or []
        for field in cf:
            fid = field['field_id']
            vals = field.get('values', [])
            if not vals:
                continue
            val = vals[0].get('value', '')
            if not val:
                continue
            if field_lead_source and fid == field_lead_source:
                source = val
                if is_2026:
                    m['has_source'] += 1
                    m['by_source'][val] += 1
            elif field_comment and fid == field_comment:
                if is_2026:
                    m['has_comment'] += 1

        won_statuses = proj.get('won_statuses', set())
        lost_statuses = proj.get('lost_statuses', set())
        is_won = status_id == 142 or status_id in won_statuses
        is_lost = status_id == 143 or status_id in lost_statuses

        if is_won:
            # Use closed_at month for won deals (they may have been created much earlier)
            closed_mk = mk
            if lead.get('closed_at'):
                closed_mk = datetime.fromtimestamp(lead['closed_at']).strftime('%Y-%m')
            wm = monthly[closed_mk]
            wm['won'] += 1
            wm['won_budget'] += price
            if source:
                source_totals[source]['won'] += 1
                source_totals[source]['budget'] += price
        elif is_lost:
            # Use closed_at month for lost deals too
            closed_mk = mk
            if lead.get('closed_at'):
                closed_mk = datetime.fromtimestamp(lead['closed_at']).strftime('%Y-%m')
            lm = monthly[closed_mk]
            lm['lost'] += 1
            lm['lost_total'] += 1
            loss_id = lead.get('loss_reason_id')
            if loss_id:
                reason = loss_reasons_map.get(loss_id, f'Причина {loss_id}')
                lm['loss_reasons'][reason] += 1
                loss_totals[reason] += 1
                lm['has_loss_reason'] += 1
        else:  # Active
            stage = status_names.get(status_id, f'Статус {status_id}')
            active_funnel[stage] += 1

        if source:
            source_totals[source]['leads'] += 1

    # Only show 2026+ months with actual activity
    months_sorted = sorted(k for k in monthly.keys()
                           if k >= '2026-01' and (monthly[k]['leads'] > 0 or monthly[k]['won'] > 0 or monthly[k]['lost'] > 0))
    result = {
        'monthly': {},
        'loss_reasons': sorted(loss_totals.items(), key=lambda x: -x[1]),
        'lead_sources': sorted(source_totals.items(), key=lambda x: -x[1]['leads']),
        'active_funnel': sorted(active_funnel.items(),
            key=lambda x: list(status_names.values()).index(x[0]) if x[0] in status_names.values() else 99),
        'totals': {
            'leads': sum(monthly[k]['leads'] for k in months_sorted),
            'won': sum(monthly[k]['won'] for k in months_sorted),
            'lost': sum(monthly[k]['lost'] for k in months_sorted),
            'active': sum(active_funnel.values()),
            'total_budget': sum(monthly[k]['total_budget'] for k in months_sorted),
            'won_budget': sum(monthly[k]['won_budget'] for k in months_sorted),
        }
    }

    for mk in months_sorted:
        m = monthly[mk]
        t = m['leads'] or 1
        lt = m['lost_total'] or 1
        result['monthly'][mk] = {
            'leads': m['leads'],
            'won': m['won'],
            'lost': m['lost'],
            'total_budget': m['total_budget'],
            'won_budget': m['won_budget'],
            'sources': dict(m['by_source']),
            'loss_reasons': dict(m['loss_reasons']),
            'quality': {
                'price_pct': round(m['has_price'] / t * 100),
                'source_pct': round(m['has_source'] / t * 100),
                'comment_pct': round(m['has_comment'] / t * 100),
                'company_pct': round(m['has_company'] / t * 100),
                'loss_reason_pct': round(m['has_loss_reason'] / lt * 100) if m['lost_total'] > 0 else None,
            }
        }

    return result

# ============================================================
# YANDEX METRIKA (multi-project)
# ============================================================
def metrika_api(endpoint, params=None, project='main'):
    proj, cfg, _, metrika, _ = get_proj_cfg(project)
    token = metrika.get('access_token', '')
    if not token:
        return {'error': 'no_token', 'auth_url': get_metrika_auth_url(project)}
    url = f"https://api-metrika.yandex.net/{endpoint}"
    all_params = {'ids': metrika.get('counter_id', ''), 'accuracy': 'full'}
    if params:
        all_params.update(params)
    resp = requests.get(url, headers={'Authorization': f'OAuth {token}'}, params=all_params)
    if resp.status_code == 403:
        return {'error': 'invalid_token', 'auth_url': get_metrika_auth_url(project)}
    return resp.json()

def get_metrika_auth_url(project='main'):
    _, cfg, _, metrika, _ = get_proj_cfg(project)
    return f"https://oauth.yandex.ru/authorize?response_type=code&client_id={metrika.get('client_id', '')}"

def exchange_metrika_code(code, project='main'):
    proj, cfg, _, metrika, _ = get_proj_cfg(project)
    resp = requests.post('https://oauth.yandex.ru/token', data={
        'grant_type': 'authorization_code',
        'code': code,
        'client_id': metrika.get('client_id', ''),
        'client_secret': metrika.get('client_secret', ''),
    })
    if resp.status_code == 200:
        data = resp.json()
        m_section = _get_nested(cfg, proj['config_metrika_key'])
        m_section['access_token'] = data['access_token']
        m_section['refresh_token'] = data.get('refresh_token', '')
        save_config(cfg)
        return True
    return False

# ============================================================
# GOOGLE SHEETS (multi-project)
# ============================================================
def _parse_rub(s):
    if not s:
        return 0
    s = s.replace('\u0440.','').replace('\u20bd','').replace('\xa0','').replace(' ','').replace(',','.').strip()
    try:
        return float(s)
    except:
        return 0

def fetch_google_sheet(project='main'):
    _, cfg, _, _, gsheet = get_proj_cfg(project)
    sid = gsheet.get('sheet_id', '')
    gid = gsheet.get('gid', '0')
    if not sid:
        return {'error': 'No sheet configured'}
    url = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"
    resp = requests.get(url)
    if resp.status_code != 200:
        return {'error': 'Failed to fetch sheet'}
    resp.encoding = 'utf-8'
    rows = list(csv.reader(io.StringIO(resp.text)))

    MM = {'январ':'01','фев':'02','март':'03','апрел':'04','май':'05','июн':'06',
          'июл':'07','август':'08','сентяб':'09','октяб':'10','нояб':'11','декаб':'12'}

    current_month = None
    last_block = ''
    monthly_totals = {}
    # Accumulate sums from individual rows (Итого formulas may be stale)
    month_row_sums = {}
    activities = []

    for row in rows:
        if len(row) < 2:
            continue
        c1 = row[1].strip()
        c1l = c1.lower()
        # Month header? Detect year from the header
        for prefix, num in MM.items():
            if prefix in c1l and ("'" in c1 or "\u2019" in c1):
                # Extract year from header like "Январь'26" or "Июль '25"
                year = '2026'
                for part in c1.replace("\u2019", "'").split("'"):
                    part = part.strip()
                    if part.isdigit() and len(part) == 2:
                        year = f'20{part}'
                        break
                current_month = f'{year}-{num}'
                if current_month not in monthly_totals:
                    monthly_totals[current_month] = {'plan':0,'fact':0,'leads':0,'touches':0}
                    month_row_sums[current_month] = {'plan':0,'fact':0,'leads':0,'touches':0}
                break
        if not current_month:
            continue
        if c1 == 'Блок':
            continue
        if c1 == 'Итого':
            itogo_plan = _parse_rub(row[5] if len(row)>5 else '')
            itogo_fact = _parse_rub(row[6] if len(row)>6 else '')
            itogo_leads = 0
            itogo_touches = 0
            try: itogo_leads = int(row[7].strip()) if len(row)>7 and row[7].strip() else 0
            except: pass
            try: itogo_touches = int(row[8].strip()) if len(row)>8 and row[8].strip() else 0
            except: pass
            # Use MAX of Итого row vs sum of individual rows (formulas may be stale)
            sums = month_row_sums.get(current_month, {})
            monthly_totals[current_month]['plan'] = max(itogo_plan, sums.get('plan', 0))
            monthly_totals[current_month]['fact'] = max(itogo_fact, sums.get('fact', 0))
            monthly_totals[current_month]['leads'] = max(itogo_leads, sums.get('leads', 0))
            monthly_totals[current_month]['touches'] = max(itogo_touches, sums.get('touches', 0))
            continue
        if c1 and c1 not in ('`',):
            last_block = c1
        act = row[3].strip() if len(row)>3 else ''
        what = row[4].strip() if len(row)>4 else ''
        bp = _parse_rub(row[5] if len(row)>5 else '')
        bf = _parse_rub(row[6] if len(row)>6 else '')
        lf = 0
        tc = 0
        cpl = ''
        try: lf = int(row[7].strip()) if len(row)>7 and row[7].strip() else 0
        except: pass
        try: tc = int(row[8].strip()) if len(row)>8 and row[8].strip() else 0
        except: pass
        cpl = row[9].strip() if len(row)>9 else ''
        # Accumulate row-level sums
        if current_month in month_row_sums:
            month_row_sums[current_month]['plan'] += bp
            month_row_sums[current_month]['fact'] += bf
            month_row_sums[current_month]['leads'] += lf
            month_row_sums[current_month]['touches'] += tc
        if not act and not what and bp==0 and bf==0:
            continue
        if act == 'Активность':
            continue
        activities.append({
            'month': current_month, 'block': last_block,
            'activity': act, 'what': what,
            'budget_plan': bp, 'budget_fact': bf,
            'leads': lf, 'touches': tc, 'cpl': cpl,
        })

    return {'monthly_totals': monthly_totals, 'activities': activities, 'raw_rows': len(rows)}

# ============================================================
# ROUTES (all accept ?project=main|lite)
# ============================================================
@app.route('/')
def index():
    return render_template('dashboard.html')

@app.route('/api/amo/cycle')
def api_amo_cycle():
    project = get_project()
    if not is_refresh():
        cached, ts = cache_get(project, 'cycle')
        if cached:
            return jsonify({'ok': True, 'data': cached, 'cached': True})
    proj = PROJECTS.get(project, PROJECTS['main'])
    try:
        cache_name = proj.get('cache_file')
        all_leads = []
        if cache_name:
            cache_file = os.path.join(os.path.dirname(__file__), cache_name)
            if os.path.exists(cache_file):
                with open(cache_file) as f:
                    all_leads = json.load(f)
        if not all_leads:
            all_leads = fetch_all_amo_leads(project)

        won_statuses = proj.get('won_statuses', set()) | {142}
        won = [l for l in all_leads
               if l.get('pipeline_id') == proj['pipeline_id']
               and l.get('status_id') in won_statuses
               and l.get('closed_at') and l.get('created_at')]

        cycles = []
        quarterly = defaultdict(list)
        monthly = defaultdict(list)

        for lead in won:
            created = datetime.fromtimestamp(lead['created_at'])
            closed = datetime.fromtimestamp(lead['closed_at'])
            days = (closed - created).days
            if days < 0:
                continue
            cycles.append(days)
            q = f"{created.year}-Q{(created.month-1)//3+1}"
            quarterly[q].append(days)
            mk = closed.strftime('%Y-%m')
            monthly[mk].append(days)

        if not cycles:
            return jsonify({'ok': True, 'data': {'avg': 0, 'median': 0, 'count': 0, 'quarterly': {}, 'monthly': {}}})

        cycles_sorted = sorted(cycles)
        median = cycles_sorted[len(cycles_sorted)//2]

        q_stats = {}
        for q in sorted(quarterly.keys()):
            vals = quarterly[q]
            s = sorted(vals)
            q_stats[q] = {'avg': round(sum(vals)/len(vals)), 'median': s[len(s)//2], 'count': len(vals)}

        m_stats = {}
        for mk in sorted(monthly.keys()):
            vals = monthly[mk]
            s = sorted(vals)
            m_stats[mk] = {'avg': round(sum(vals)/len(vals)), 'median': s[len(s)//2], 'count': len(vals)}

        result = {
            'avg': round(sum(cycles)/len(cycles)),
            'median': median,
            'count': len(cycles),
            'quarterly': q_stats,
            'monthly': m_stats,
        }
        cache_set(project, 'cycle', result)
        return jsonify({'ok': True, 'data': result, 'cached': False})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/amo/data')
def api_amo_data():
    project = get_project()
    if not is_refresh():
        cached, ts = cache_get(project, 'amo')
        if cached:
            return jsonify({'ok': True, 'data': cached, 'fetched_at': datetime.fromtimestamp(ts).isoformat(), 'cached': True})
    try:
        leads = fetch_all_amo_leads(project)
        result = analyze_amo_data(leads, project)
        cache_set(project, 'amo', result)
        return jsonify({'ok': True, 'data': result, 'fetched_at': datetime.now().isoformat(), 'cached': False})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

@app.route('/api/metrika/auth')
def api_metrika_auth():
    project = get_project()
    code = request.args.get('code', '')
    if code:
        ok = exchange_metrika_code(code, project)
        return jsonify({'ok': ok})
    return jsonify({'auth_url': get_metrika_auth_url(project)})

@app.route('/api/metrika/traffic')
def api_metrika_traffic():
    project = get_project()
    if not is_refresh():
        cached, ts = cache_get(project, 'metrika_traffic')
        if cached:
            return jsonify(cached)
    date_from = request.args.get('from', '2026-01-01')
    date_to = request.args.get('to', datetime.now().strftime('%Y-%m-%d'))
    data = metrika_api('stat/v1/data/bytime', {
        'metrics': 'ym:s:visits,ym:s:users',
        'date1': date_from,
        'date2': date_to,
        'group': 'month',
    }, project=project)
    cache_set(project, 'metrika_traffic', data)
    return jsonify(data)

@app.route('/api/metrika/sources')
def api_metrika_sources():
    project = get_project()
    if not is_refresh():
        cached, ts = cache_get(project, 'metrika_sources')
        if cached:
            return jsonify(cached)
    date_from = request.args.get('from', '2026-01-01')
    date_to = request.args.get('to', datetime.now().strftime('%Y-%m-%d'))
    data = metrika_api('stat/v1/data/bytime', {
        'metrics': 'ym:s:visits',
        'dimensions': 'ym:s:trafficSource',
        'date1': date_from,
        'date2': date_to,
        'group': 'month',
    }, project=project)
    cache_set(project, 'metrika_sources', data)
    return jsonify(data)

@app.route('/api/metrika/organic')
def api_metrika_organic():
    project = get_project()
    if not is_refresh():
        cached, ts = cache_get(project, 'metrika_organic')
        if cached:
            return jsonify(cached)
    date_from = request.args.get('from', '2026-01-01')
    date_to = request.args.get('to', datetime.now().strftime('%Y-%m-%d'))
    data = metrika_api('stat/v1/data/bytime', {
        'metrics': 'ym:s:visits',
        'date1': date_from,
        'date2': date_to,
        'group': 'month',
        'filters': "ym:s:trafficSource=='organic'",
    }, project=project)
    cache_set(project, 'metrika_organic', data)
    return jsonify(data)

@app.route('/api/metrika/summary')
def api_metrika_summary():
    project = get_project()
    if not is_refresh():
        cached, ts = cache_get(project, 'metrika_summary')
        if cached:
            return jsonify(cached)
    date_from = request.args.get('from', '2026-01-01')
    date_to = request.args.get('to', datetime.now().strftime('%Y-%m-%d'))
    data = metrika_api('stat/v1/data', {
        'metrics': 'ym:s:visits,ym:s:users,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds',
        'dimensions': 'ym:s:trafficSource',
        'date1': date_from,
        'date2': date_to,
    }, project=project)
    cache_set(project, 'metrika_summary', data)
    return jsonify(data)

@app.route('/api/gsheet/data')
def api_gsheet_data():
    project = get_project()
    if not is_refresh():
        cached, ts = cache_get(project, 'gsheet')
        if cached:
            return jsonify({'ok': True, 'data': cached, 'cached': True})
    try:
        data = fetch_google_sheet(project)
        cache_set(project, 'gsheet', data)
        return jsonify({'ok': True, 'data': data, 'cached': False})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500

def fetch_amo_leads_for_year(year, project='main'):
    """Fetch all leads created in a given year for the project's main pipeline."""
    proj = PROJECTS.get(project, PROJECTS['main'])
    pipeline_id = proj['pipeline_id']
    all_leads = []
    seen_ids = set()
    date_from = int(datetime(year, 1, 1).timestamp())
    date_to = int(datetime(year, 12, 31, 23, 59, 59).timestamp())

    page = 1
    while True:
        data = amo_api('leads', {
            'limit': 250, 'page': page, 'with': 'loss_reason',
            'filter[created_at][from]': date_from,
            'filter[created_at][to]': date_to,
            'filter[pipeline_id]': pipeline_id,
        }, project=project)
        leads = data.get('_embedded', {}).get('leads', [])
        for l in leads:
            if l['id'] not in seen_ids:
                all_leads.append(l)
                seen_ids.add(l['id'])
        if not leads or 'next' not in data.get('_links', {}):
            break
        page += 1

    return all_leads


@app.route('/api/amo/yoy')
def api_amo_yoy():
    project = get_project()
    if not is_refresh():
        cached, ts = cache_get(project, 'yoy')
        if cached:
            return jsonify({'ok': True, 'data': cached, 'cached': True})
    try:
        proj = PROJECTS.get(project, PROJECTS['main'])
        won_statuses = proj.get('won_statuses', set()) | {142}

        leads_2025 = fetch_amo_leads_for_year(2025, project)
        leads_2026 = fetch_amo_leads_for_year(2026, project)

        months = {f'{m:02d}': {
            'leads_2025': 0, 'leads_2026': 0,
            'won_2025': 0, 'won_2026': 0,
            'budget_2025': 0, 'budget_2026': 0,
        } for m in range(1, 13)}

        for lead in leads_2025:
            created = datetime.fromtimestamp(lead['created_at'])
            mk = f'{created.month:02d}'
            months[mk]['leads_2025'] += 1
            if lead.get('status_id') in won_statuses:
                closed_ts = lead.get('closed_at') or lead['created_at']
                closed = datetime.fromtimestamp(closed_ts)
                if closed.year == 2025:
                    wmk = f'{closed.month:02d}'
                    months[wmk]['won_2025'] += 1
                    months[wmk]['budget_2025'] += (lead.get('price', 0) or 0)

        for lead in leads_2026:
            created = datetime.fromtimestamp(lead['created_at'])
            mk = f'{created.month:02d}'
            months[mk]['leads_2026'] += 1
            if lead.get('status_id') in won_statuses:
                closed_ts = lead.get('closed_at') or lead['created_at']
                closed = datetime.fromtimestamp(closed_ts)
                if closed.year == 2026:
                    wmk = f'{closed.month:02d}'
                    months[wmk]['won_2026'] += 1
                    months[wmk]['budget_2026'] += (lead.get('price', 0) or 0)

        result = {'months': months}
        cache_set(project, 'yoy', result)
        return jsonify({'ok': True, 'data': result, 'cached': False})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# Pipeline stage definitions (ordered by sort) for funnel analysis
PIPELINE_STAGES = {
    'main': [
        {'id': 67988798, 'name': 'Неразобранное', 'sort': 10},
        {'id': 67988802, 'name': 'Первичный контакт', 'sort': 20},
        {'id': 67988806, 'name': 'Бриф / Встреча', 'sort': 30},
        {'id': 68025742, 'name': 'Работы', 'sort': 40},
        {'id': 68350434, 'name': 'Отправка КП', 'sort': 50},
        {'id': 67988810, 'name': 'Принимают решение', 'sort': 60},
        {'id': 67988814, 'name': 'Согласование договора', 'sort': 70},
        {'id': 68024722, 'name': 'Холд', 'sort': 80},
    ],
    'lite': [
        {'id': 51439024, 'name': 'Неразобранное', 'sort': 10},
        {'id': 72287470, 'name': 'Прямой контакт', 'sort': 20},
        {'id': 51439027, 'name': 'Валидация', 'sort': 30},
        {'id': 51439030, 'name': 'Квалификация 20%', 'sort': 40},
        {'id': 51439033, 'name': 'Встреча 30%', 'sort': 50},
        {'id': 51441334, 'name': 'Продажа 40%', 'sort': 60},
        {'id': 51441337, 'name': 'Продажа 50%', 'sort': 70},
        {'id': 51441340, 'name': 'Решение 80%', 'sort': 80},
        {'id': 51441343, 'name': 'Оплачено 100%', 'sort': 90},
        {'id': 51718570, 'name': 'Оплата закончилась', 'sort': 100},
        {'id': 55966762, 'name': 'Отложенные', 'sort': 110},
        {'id': 80147554, 'name': 'Отказ с возвратом', 'sort': 120},
    ],
}

# Key conversion stage mappings per project
KEY_CONV_STAGES = {
    'main': {
        'contact': 67988802,   # Первичный контакт
        'meeting': 67988806,   # Бриф / Встреча
        'deal': 68025742,      # Работы
    },
    'lite': {
        'contact': 72287470,   # Прямой контакт
        'meeting': 51439033,   # Встреча 30%
        'deal': 51441334,      # Продажа 40%
    },
}


@app.route('/api/amo/funnel')
def api_amo_funnel():
    project = get_project()
    if not is_refresh():
        cached, ts = cache_get(project, 'funnel')
        if cached:
            return jsonify({'ok': True, 'data': cached, 'cached': True})
    try:
        proj = PROJECTS.get(project, PROJECTS['main'])
        pipeline_id = proj['pipeline_id']
        stages_def = PIPELINE_STAGES.get(project, PIPELINE_STAGES['main'])
        key_conv = KEY_CONV_STAGES.get(project, KEY_CONV_STAGES['main'])
        won_statuses = proj.get('won_statuses', set()) | {142}
        lost_statuses = proj.get('lost_statuses', set()) | {143}

        # Build stage sort order lookup: status_id -> sort value
        stage_sort = {s['id']: s['sort'] for s in stages_def}
        # Won is beyond all stages
        max_sort = max(s['sort'] for s in stages_def) + 10
        for ws in won_statuses:
            stage_sort[ws] = max_sort

        # Fetch leads (reuse existing function)
        leads = fetch_all_amo_leads(project)
        leads = [l for l in leads if l.get('pipeline_id') == pipeline_id]

        # Count leads at or past each stage
        stage_counts = {s['id']: 0 for s in stages_def}
        won_count = 0
        loss_by_stage = defaultdict(int)

        for lead in leads:
            status_id = lead.get('status_id')
            is_won = status_id in won_statuses
            is_lost = status_id in lost_statuses

            if is_won:
                won_count += 1
                # Won leads have passed through all stages
                for s in stages_def:
                    stage_counts[s['id']] += 1
            elif is_lost:
                # For lost leads, determine last active stage by status_id
                # If status_id is 143 (generic lost), check pipeline_id match
                lead_sort = stage_sort.get(status_id, 0)
                if lead_sort == 0:
                    # Lost lead with unknown stage — count at first stage only
                    lead_sort = stages_def[0]['sort'] if stages_def else 0
                # Count at stages up to the lead's sort position
                for s in stages_def:
                    if s['sort'] <= lead_sort:
                        stage_counts[s['id']] += 1
                # Attribute loss to the furthest stage reached
                furthest = None
                for s in stages_def:
                    if s['sort'] <= lead_sort:
                        furthest = s['name']
                if furthest:
                    loss_by_stage[furthest] += 1
            else:
                # Active lead — count at stages up to current
                lead_sort = stage_sort.get(status_id, 0)
                for s in stages_def:
                    if s['sort'] <= lead_sort:
                        stage_counts[s['id']] += 1

        # Build stages result (skip Неразобранное for cleaner funnel)
        total_in_funnel = stage_counts[stages_def[0]['id']] if stages_def else 0
        stages_result = []
        for s in stages_def:
            count = stage_counts[s['id']]
            pct = round(count / total_in_funnel * 100, 1) if total_in_funnel > 0 else 0
            stages_result.append({
                'name': s['name'],
                'count': count,
                'pct': pct,
                'loss': loss_by_stage.get(s['name'], 0),
            })
        # Add Won stage
        won_pct = round(won_count / total_in_funnel * 100, 1) if total_in_funnel > 0 else 0
        stages_result.append({
            'name': 'Успешно реализовано',
            'count': won_count,
            'pct': won_pct,
            'loss': 0,
        })

        # Key conversions
        contact_count = stage_counts.get(key_conv['contact'], 0)
        meeting_count = stage_counts.get(key_conv['meeting'], 0)
        deal_count = stage_counts.get(key_conv['deal'], 0)

        key_conversions = {
            'contact_to_meeting': round(meeting_count / contact_count * 100, 1) if contact_count > 0 else 0,
            'meeting_to_deal': round(deal_count / meeting_count * 100, 1) if meeting_count > 0 else 0,
            'deal_to_won': round(won_count / deal_count * 100, 1) if deal_count > 0 else 0,
        }

        # Loss reasons from existing data
        loss_reasons_map = proj['loss_reasons']
        if not loss_reasons_map:
            loss_reasons_map = fetch_loss_reasons(project)
            proj['loss_reasons'] = loss_reasons_map
        loss_reasons_agg = defaultdict(int)
        for lead in leads:
            if lead.get('status_id') in lost_statuses:
                rid = lead.get('loss_reason_id')
                if rid:
                    reason = loss_reasons_map.get(rid, f'Причина {rid}')
                    loss_reasons_agg[reason] += 1

        result = {
            'stages': stages_result,
            'key_conversions': key_conversions,
            'loss_by_stage': dict(loss_by_stage),
            'loss_reasons': sorted(loss_reasons_agg.items(), key=lambda x: -x[1]),
        }
        cache_set(project, 'funnel', result)
        return jsonify({'ok': True, 'data': result, 'cached': False})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/api/erz/data')
def api_erz_data():
    data_file = os.path.join(os.path.dirname(__file__), 'data', 'erz_top100.json')
    if os.path.exists(data_file):
        with open(data_file) as f:
            return jsonify({'ok': True, 'data': json.load(f)})
    return jsonify({'ok': False, 'error': 'No data file'})


@app.route('/api/status')
def api_status():
    project = get_project()
    proj, cfg, amo, metrika, _ = get_proj_cfg(project)
    return jsonify({
        'project': project,
        'project_name': proj['name'],
        'amo_connected': bool(amo.get('access_token')),
        'metrika_connected': bool(metrika.get('access_token')),
        'metrika_auth_url': get_metrika_auth_url(project) if not metrika.get('access_token') else None,
    })

if __name__ == '__main__':
    if not os.path.exists(CONFIG_FILE):
        save_config({})
    print("Dashboard server starting at http://localhost:5050")
    print("   Open http://localhost:5050 in your browser")
    app.run(host='0.0.0.0', port=5050, debug=True, threaded=True)
