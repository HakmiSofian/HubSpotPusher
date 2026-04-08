#!/usr/bin/env python3
"""
Application Web - Import Contacts HubSpot / PostgreSQL
=======================================================
Interface Streamlit pour executer les 3 etapes :
1. Transformer le fichier Excel
2. Pousser vers PostgreSQL
3. Pousser vers HubSpot (contacts, liste, taches)

Lancement : streamlit run app.py
"""

import os, sys, json, time, logging, re, locale
import streamlit as st
import pandas as pd
import yaml
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date, timedelta
from io import BytesIO

# ─── Config ───────────────────────────────────────────────────────────────────

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'config.yaml')

@st.cache_resource
def _load_yaml():
    with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def load_config():
    import copy
    config = copy.deepcopy(_load_yaml())

    # Surcharger les credentials avec les secrets Streamlit Cloud si disponibles
    # (permet de ne pas mettre les mots de passe dans config.yaml sur GitHub)
    try:
        secrets = st.secrets
        if "hubspot" in secrets:
            config['hubspot']['api_key'] = secrets['hubspot']['api_key']
        if "postgresql" in secrets:
            for key in ('host', 'port', 'database', 'user', 'password'):
                if key in secrets['postgresql']:
                    config['postgresql'][key] = secrets['postgresql'][key]
    except Exception:
        pass  # Pas de secrets Streamlit -> on utilise config.yaml

    return config

def get_config():
    return load_config()

# ─── Logger ───────────────────────────────────────────────────────────────────

def setup_logger(config, filename):
    log_dir = config.get('logs', {}).get('directory', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_name = re.sub(r'[^\w\-.]', '_', filename)
    log_file = os.path.join(log_dir, f"{timestamp}_{safe_name}.log")

    logger = logging.getLogger(f'import_{timestamp}')
    logger.setLevel(logging.INFO)
    # Fichier
    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    logger.addHandler(fh)
    return logger, log_file

# ─── Helpers ──────────────────────────────────────────────────────────────────

FRENCH_MONTHS = {
    'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
    'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
    'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12',
    'fevrier': '02', 'aout': '08',
}

def parse_french_date(text):
    """Convertit 'jeudi 23 avril 2026' en '23-04-2026'."""
    if pd.isna(text) or str(text).strip() == '':
        return text
    text = str(text).strip().lower()
    for month_name, month_num in FRENCH_MONTHS.items():
        if month_name in text:
            parts = text.split()
            day = None
            year = None
            for p in parts:
                if p.isdigit() and len(p) <= 2:
                    day = p.zfill(2)
                elif p.isdigit() and len(p) == 4:
                    year = p
            if day and year:
                return f"{day}-{month_num}-{year}"
    return text

def safe_str(val):
    if val is None or str(val).strip() in ('', 'nan', 'None'):
        return None
    return str(val).strip()

def clean(val):
    if val is None or str(val).strip() in ('', 'nan', 'None'):
        return ''
    return str(val).strip()

def appointment_to_timestamp(date_str):
    try:
        dt = datetime.strptime(str(date_str).strip(), '%d-%m-%Y')
        dt = (dt - timedelta(days=2)).replace(hour=8, minute=0, second=0, microsecond=0)
        return int(dt.timestamp() * 1000)
    except Exception:
        dt = (datetime.now() + timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
        return int(dt.timestamp() * 1000)

def build_props(row, config):
    mapping = config.get('hubspot_mapping', {})
    number_props = set(config.get('hubspot_number_props', []))
    max_num = 9007199254740992
    props = {}
    for col, hs in mapping.items():
        val = row.get(col)
        if val is None or str(val).strip() in ('', 'nan', 'None'):
            continue
        val_str = str(val).strip()
        if hs in number_props:
            try:
                if abs(int(float(val_str))) > max_num:
                    continue
            except (ValueError, TypeError):
                continue
        props[hs] = val_str
    return props

def create_session(config):
    s = requests.Session()
    s.headers.update({
        'Authorization': f'Bearer {config["hubspot"]["api_key"]}',
        'Content-Type': 'application/json',
    })
    adapter = requests.adapters.HTTPAdapter(pool_connections=5, pool_maxsize=5)
    s.mount('https://', adapter)
    return s

def batch_request(session, url, payload, config, max_retries=3):
    pause = config.get('batch', {}).get('rate_limit_pause', 11)
    for attempt in range(max_retries):
        resp = session.post(url, json=payload, timeout=30)
        if resp.status_code == 429:
            retry_after = int(resp.headers.get('Retry-After', pause))
            time.sleep(retry_after)
            continue
        return resp
    return resp

# ─── VALIDATION FICHIER EXCEL ────────────────────────────────────────────────

REQUIRED_COLUMNS = [
    'WorkOrderId', 'WorkOrderExternalReference', 'CustomerName',
    'Street', 'HouseNumber', 'ZipCode', 'City',
    'Language', 'AppointmentDate', 'Login', 'Password',
]

IMPORTANT_COLUMNS = [
    'E EAN Number', 'PostBox', 'Appointment Window',
    'Login Url', 'GRD', 'CreationDate',
]

DATE_PATTERN = re.compile(
    r'(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)?\s*'
    r'\d{1,2}\s+'
    r'(janvier|février|fevrier|mars|avril|mai|juin|juillet|août|aout|'
    r'septembre|octobre|novembre|décembre|decembre)\s+'
    r'\d{4}',
    re.IGNORECASE
)

def validate_excel(df):
    """
    Valide le fichier Excel.
    Retourne (is_valid, errors, warnings, infos)
    - errors   : bloquants, empechent l'import
    - warnings : non bloquants, signalent des donnees manquantes
    - infos    : statistiques utiles
    """
    errors = []
    warnings = []
    infos = []

    # 1. Colonnes obligatoires manquantes
    missing_required = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing_required:
        errors.append(f"Colonnes obligatoires manquantes : **{', '.join(missing_required)}**")

    # 2. Colonnes importantes absentes (non bloquant)
    missing_important = [c for c in IMPORTANT_COLUMNS if c not in df.columns]
    if missing_important:
        warnings.append(f"Colonnes optionnelles absentes : {', '.join(missing_important)}")

    # 3. Fichier vide
    if len(df) == 0:
        errors.append("Le fichier est vide (0 lignes de donnees).")
        return False, errors, warnings, infos

    # 4. Colonne Login : obligatoire et unique (cle HubSpot)
    if 'Login' in df.columns:
        null_login = df['Login'].isnull().sum() + (df['Login'].astype(str).str.strip() == '').sum()
        if null_login > 0:
            errors.append(f"Colonne **Login** : {null_login} valeur(s) vide(s). Login est obligatoire pour HubSpot.")
        dup_login = df['Login'].astype(str).str.strip().duplicated(keep=False)
        dup_count = dup_login.sum()
        if dup_count > 0:
            dup_vals = df.loc[dup_login, 'Login'].unique()[:5].tolist()
            errors.append(f"Colonne **Login** : {dup_count} doublons detectes (ex: {', '.join(str(v) for v in dup_vals)}). Chaque contact doit avoir un Login unique.")

    # 5. AppointmentDate : format date francaise
    if 'AppointmentDate' in df.columns:
        non_empty = df['AppointmentDate'].dropna().astype(str).str.strip()
        non_empty = non_empty[non_empty != '']
        if len(non_empty) == 0:
            errors.append("Colonne **AppointmentDate** entièrement vide.")
        else:
            bad_dates = non_empty[~non_empty.str.lower().apply(lambda v: bool(DATE_PATTERN.search(v)))]
            if len(bad_dates) > 0:
                exemples = bad_dates.head(3).tolist()
                warnings.append(
                    f"Colonne **AppointmentDate** : {len(bad_dates)} date(s) au format non reconnu "
                    f"(ex: `{'`, `'.join(exemples)}`). "
                    f"Format attendu : `jeudi 23 avril 2026`."
                )

    # 6. CustomerName vide
    if 'CustomerName' in df.columns:
        null_name = df['CustomerName'].isnull().sum() + (df['CustomerName'].astype(str).str.strip() == '').sum()
        if null_name > 0:
            warnings.append(f"Colonne **CustomerName** : {null_name} valeur(s) vide(s).")

    # 7. Lignes entièrement vides
    empty_rows = df.isnull().all(axis=1).sum()
    if empty_rows > 0:
        warnings.append(f"{empty_rows} ligne(s) entièrement vide(s) dans le fichier.")

    # 8. Stats informatives
    infos.append(f"{len(df)} lignes au total")
    if 'Login' in df.columns:
        unique_logins = df['Login'].astype(str).str.strip().nunique()
        infos.append(f"{unique_logins} logins uniques")
    if 'AppointmentDate' in df.columns:
        unique_dates = df['AppointmentDate'].dropna().astype(str).str.strip()
        unique_dates = unique_dates[unique_dates != ''].nunique()
        infos.append(f"{unique_dates} date(s) de RDV distincte(s)")
    if 'City' in df.columns:
        unique_cities = df['City'].dropna().nunique()
        infos.append(f"{unique_cities} ville(s) distincte(s)")

    is_valid = len(errors) == 0
    return is_valid, errors, warnings, infos


# ─── DETECTION DOUBLONS HUBSPOT ──────────────────────────────────────────────

def detect_hubspot_duplicates(df, config):
    """
    Cherche les contacts existants dans HubSpot par CustomerName (firstname).
    Retourne un dict: {customer_name: [list of hubspot contact ids]}
    """
    session = create_session(config)
    customer_names = df['CustomerName'].dropna().astype(str).str.strip()
    customer_names = customer_names[customer_names != ''].unique().tolist()

    duplicates = {}  # name -> [{'id': ..., 'firstname': ..., 'lastname': ...}]

    for name in customer_names:
        try:
            resp = session.post(
                'https://api.hubapi.com/crm/v3/objects/contacts/search',
                json={
                    'filterGroups': [{
                        'filters': [{
                            'propertyName': 'firstname',
                            'operator': 'EQ',
                            'value': name
                        }]
                    }],
                    'properties': ['firstname', 'lastname', 'login'],
                    'limit': 10
                },
                timeout=15
            )
            if resp.status_code == 200:
                results = resp.json().get('results', [])
                if results:
                    duplicates[name] = [{
                        'id': r['id'],
                        'firstname': r.get('properties', {}).get('firstname', ''),
                        'lastname': r.get('properties', {}).get('lastname', ''),
                        'login': r.get('properties', {}).get('login', ''),
                    } for r in results]
            elif resp.status_code == 429:
                time.sleep(2)
        except Exception:
            pass

    return duplicates


# ─── ROLLBACK (SUPPRESSION IMPORT) ──────────────────────────────────────────

def rollback_hubspot(contact_ids, list_id, config, logger, progress_callback=None):
    """
    Supprime les contacts et la liste creee sur HubSpot.
    Les taches associees aux contacts sont supprimees automatiquement par HubSpot.
    """
    session = create_session(config)
    base_url = 'https://api.hubapi.com/crm/v3/objects'
    deleted_contacts = 0
    errors = []

    # Supprimer la liste
    if list_id and list_id not in ('', 'None', 'N/A'):
        try:
            resp = session.delete(f'https://api.hubapi.com/crm/v3/lists/{list_id}', timeout=15)
            if resp.status_code in (200, 204):
                logger.info(f"Rollback: liste {list_id} supprimee")
            else:
                logger.warning(f"Rollback: erreur suppression liste {list_id}: {resp.status_code}")
        except Exception as e:
            logger.warning(f"Rollback: erreur liste: {e}")

    # Supprimer les contacts par batch de 100
    total = len(contact_ids)
    for i in range(0, total, 100):
        batch = contact_ids[i:i + 100]
        inputs = [{'id': cid} for cid in batch]
        try:
            resp = session.post(
                f'{base_url}/contacts/batch/archive',
                json={'inputs': inputs},
                timeout=30
            )
            if resp.status_code in (200, 204):
                deleted_contacts += len(batch)
            elif resp.status_code == 429:
                time.sleep(11)
                resp = session.post(
                    f'{base_url}/contacts/batch/archive',
                    json={'inputs': inputs}, timeout=30
                )
                if resp.status_code in (200, 204):
                    deleted_contacts += len(batch)
            else:
                errors.append(f"Batch archive {i//100+1}: {resp.status_code}")
                logger.warning(f"Rollback contacts batch {i//100+1}: {resp.status_code}")
        except Exception as e:
            errors.append(str(e))

        if progress_callback:
            progress_callback(min((i + 100) / total, 1.0), f"Suppression: {deleted_contacts}/{total}")

    logger.info(f"Rollback: {deleted_contacts}/{total} contacts supprimes")
    return deleted_contacts, errors


def rollback_postgresql(import_date, config, logger):
    """Supprime les lignes inserees dans PostgreSQL pour une date d'import donnee."""
    pg = config['postgresql']
    try:
        conn = psycopg2.connect(
            host=pg['host'], port=int(pg['port']),
            dbname=pg['database'], user=pg['user'], password=pg['password']
        )
        cur = conn.cursor()
        cur.execute(f"DELETE FROM {pg['table']} WHERE import_date = %s", (import_date,))
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Rollback PostgreSQL: {deleted} lignes supprimees (import_date={import_date})")
        return deleted
    except Exception as e:
        logger.error(f"Rollback PostgreSQL erreur: {e}")
        return 0


# ─── STEP 1 : Transformer Excel ──────────────────────────────────────────────

def step1_transform(df, logger):
    logger.info(f"Step 1 : {len(df)} lignes a transformer")

    # Convertir AppointmentDate
    if 'AppointmentDate' in df.columns:
        df['AppointmentDate'] = df['AppointmentDate'].apply(parse_french_date)
        logger.info("AppointmentDate converti en dd-mm-yyyy")

    # Calculer Nom
    df['Nom'] = df.apply(
        lambda r: f"{clean(r.get('WorkOrderId'))} - {clean(r.get('WorkOrderExternalReference'))}",
        axis=1
    )
    logger.info("Colonne 'Nom' calculee")

    # Calculer Adresse postale
    df['Adresse postale'] = df.apply(
        lambda r: (
            clean(r.get('Street')) + ' ' +
            clean(r.get('HouseNumber')) + ' ' +
            clean(r.get('PostBox')) + ', ' +
            clean(r.get('ZipCode')) + ' ' +
            clean(r.get('City'))
        ).strip(),
        axis=1
    )
    logger.info("Colonne 'Adresse postale' calculee")
    logger.info(f"Step 1 termine : {len(df)} lignes")
    return df

# ─── STEP 2 : Push PostgreSQL ────────────────────────────────────────────────

def step2_postgresql(df, config, logger, progress_callback=None):
    pg = config['postgresql']
    logger.info(f"Step 2 : connexion a {pg['host']}:{pg['port']}/{pg['database']}")

    conn = psycopg2.connect(
        host=pg['host'], port=int(pg['port']),
        dbname=pg['database'], user=pg['user'], password=pg['password']
    )
    cur = conn.cursor()
    today = date.today()

    # Convertir colonnes en text si necessaire
    pg_cols = config.get('postgresql_columns', [])
    for col_def in pg_cols:
        pg_name = col_def['pg']
        try:
            cur.execute(f'ALTER TABLE {pg["table"]} ALTER COLUMN "{pg_name}" TYPE text USING "{pg_name}"::text')
            conn.commit()
        except Exception:
            conn.rollback()

    # Preparer les lignes
    rows = []
    for _, row in df.iterrows():
        vals = [safe_str(row.get(col_def['excel'])) for col_def in pg_cols]
        vals.append(today)
        rows.append(tuple(vals))

    col_names = ', '.join([f'"{c["pg"]}"' for c in pg_cols] + ['import_date'])

    if progress_callback:
        progress_callback(0.5, f"Insertion de {len(rows)} lignes...")

    execute_values(
        cur,
        f'INSERT INTO {pg["table"]} ({col_names}) VALUES %s',
        rows,
        page_size=1000
    )
    conn.commit()
    cur.close()
    conn.close()

    logger.info(f"Step 2 termine : {len(rows)} lignes inserees (import_date={today})")
    if progress_callback:
        progress_callback(1.0, f"{len(rows)} lignes inserees")
    return len(rows)

# ─── STEP 3 : Push HubSpot ───────────────────────────────────────────────────

def step3_hubspot(df, config, logger, list_name, progress_callback=None, task_owners=None):
    session = create_session(config)
    base_url = 'https://api.hubapi.com/crm/v3/objects'
    batch_size = config.get('batch', {}).get('contacts_size', 100)
    if task_owners is None:
        task_owners = config.get('task_owners', [])
    results = {'contacts': 0, 'tasks': 0, 'list_id': None, 'errors': []}

    # ── 3.1 Contacts (batch create) ──
    total = len(df)
    contacts_done = {}  # row_idx -> contact_id
    login_to_idx = {}

    for batch_start in range(0, total, batch_size):
        batch_df = df.iloc[batch_start:batch_start + batch_size]
        inputs = []
        batch_logins = {}

        for idx, row in batch_df.iterrows():
            props = build_props(row, config)
            inputs.append({'properties': props})
            login_val = props.get('login', '')
            batch_logins[login_val] = idx

        resp = batch_request(session, f'{base_url}/contacts/batch/create', {'inputs': inputs}, config)

        if resp.status_code in (200, 201):
            for result in resp.json().get('results', []):
                contact_id = str(result['id'])
                result_login = result.get('properties', {}).get('login', '')
                if result_login in batch_logins:
                    contacts_done[str(batch_logins[result_login])] = contact_id
                    results['contacts'] += 1
        elif resp.status_code == 207:
            for result in resp.json().get('results', []):
                contact_id = str(result['id'])
                result_login = result.get('properties', {}).get('login', '')
                if result_login in batch_logins:
                    contacts_done[str(batch_logins[result_login])] = contact_id
                    results['contacts'] += 1
            errs = resp.json().get('errors', [])
            for e in errs:
                results['errors'].append(f"Contact batch: {str(e)[:100]}")
                logger.warning(f"Contact batch err: {str(e)[:150]}")
        else:
            err_msg = f"Batch contacts {batch_start//batch_size + 1}: {resp.status_code}"
            results['errors'].append(err_msg)
            logger.error(f"{err_msg} - {resp.text[:200]}")

        pct = min((batch_start + batch_size) / total * 0.4, 0.4)  # 0-40%
        if progress_callback:
            progress_callback(pct, f"Contacts: {results['contacts']}/{total}")

    logger.info(f"Contacts: {results['contacts']} crees")

    # ── 3.2 Liste statique ──
    if progress_callback:
        progress_callback(0.45, "Creation de la liste...")

    resp = session.post(
        'https://api.hubapi.com/crm/v3/lists',
        json={'name': list_name, 'objectTypeId': '0-1', 'processingType': 'MANUAL'},
        timeout=15
    )
    if resp.status_code in (200, 201):
        data = resp.json()
        if 'list' in data and isinstance(data['list'], dict):
            results['list_id'] = str(data['list'].get('listId') or data['list'].get('id') or '')
        else:
            results['list_id'] = str(data.get('listId') or data.get('id') or '')
        logger.info(f"Liste creee: {list_name} (ID: {results['list_id']})")
    else:
        logger.error(f"Erreur liste: {resp.status_code} - {resp.text[:200]}")
        results['errors'].append(f"Liste: {resp.status_code}")

    # ── 3.3 Ajout membres ──
    list_id = results.get('list_id', '')
    if list_id and list_id not in ('', 'None') and contacts_done:
        if progress_callback:
            progress_callback(0.55, "Ajout des contacts a la liste...")

        contact_ids = [str(cid) for cid in contacts_done.values()]
        list_batch = config.get('batch', {}).get('list_members_size', 250)
        added = 0

        for i in range(0, len(contact_ids), list_batch):
            batch = contact_ids[i:i + list_batch]
            resp = session.put(
                f'https://api.hubapi.com/crm/v3/lists/{list_id}/memberships/add',
                json=batch,
                timeout=30
            )
            if resp.status_code in (200, 204):
                added += len(batch)
            elif resp.status_code == 429:
                time.sleep(config.get('batch', {}).get('rate_limit_pause', 11))
                resp = session.put(
                    f'https://api.hubapi.com/crm/v3/lists/{list_id}/memberships/add',
                    json=batch, timeout=30
                )
                if resp.status_code in (200, 204):
                    added += len(batch)
            else:
                logger.warning(f"Ajout liste batch {i//list_batch+1}: {resp.status_code}")

        logger.info(f"Liste: {added} contacts ajoutes")

    # ── 3.4 Taches (batch create, round-robin) ──
    if progress_callback:
        progress_callback(0.6, "Creation des taches...")

    task_batch_size = config.get('batch', {}).get('tasks_size', 100)
    nb_owners = len(task_owners)
    owner_counter = 0

    # Preparer les jobs
    jobs = []
    for idx, row in df.iterrows():
        if str(idx) not in contacts_done:
            continue
        contact_id = contacts_done[str(idx)]
        appt_date = str(row.get('AppointmentDate', '')).strip()
        owner = task_owners[owner_counter % nb_owners] if nb_owners > 0 else {'id': config.get('default_owner_id', '')}
        jobs.append((idx, contact_id, appt_date, owner['id']))
        owner_counter += 1

    task_success = 0
    for batch_start in range(0, len(jobs), task_batch_size):
        batch = jobs[batch_start:batch_start + task_batch_size]
        inputs = []
        for idx, contact_id, appt_date, owner_id in batch:
            titre = f"RAPPEL RDV {appt_date}"
            due_ts = appointment_to_timestamp(appt_date)
            inputs.append({
                'properties': {
                    'hs_task_subject': titre,
                    'hs_task_body': '',
                    'hs_task_status': 'NOT_STARTED',
                    'hs_task_type': 'CALL',
                    'hs_timestamp': str(due_ts),
                    'hubspot_owner_id': owner_id,
                },
                'associations': [{
                    'to': {'id': contact_id},
                    'types': [{'associationCategory': 'HUBSPOT_DEFINED', 'associationTypeId': 204}]
                }]
            })

        resp = batch_request(session, f'{base_url}/tasks/batch/create', {'inputs': inputs}, config)
        if resp.status_code in (200, 201):
            task_success += len(resp.json().get('results', []))
        elif resp.status_code == 207:
            task_success += len(resp.json().get('results', []))
            for e in resp.json().get('errors', [])[:3]:
                logger.warning(f"Task batch err: {str(e)[:100]}")
        else:
            logger.error(f"Task batch {batch_start//task_batch_size+1}: {resp.status_code} - {resp.text[:150]}")

        pct = 0.6 + min((batch_start + task_batch_size) / max(len(jobs), 1) * 0.4, 0.4)
        if progress_callback:
            progress_callback(pct, f"Taches: {task_success}/{len(jobs)}")

    results['tasks'] = task_success
    results['contact_ids'] = contacts_done
    logger.info(f"Taches: {task_success} creees ({nb_owners} agents)")

    if progress_callback:
        progress_callback(1.0, "Termine !")

    return results

# ─── Interface Streamlit ──────────────────────────────────────────────────────

def main():
    st.set_page_config(page_title="Import HubSpot / PostgreSQL", page_icon="📊", layout="wide")

    st.title("📊 Import Contacts - HubSpot & PostgreSQL")
    st.caption("Transformez un fichier Excel, poussez les donnees vers PostgreSQL et HubSpot en quelques clics.")

    # Charger config
    try:
        config = get_config()
    except Exception as e:
        st.error(f"Erreur lecture config.yaml : {e}")
        st.stop()

    # Sidebar : config
    with st.sidebar:
        st.header("Configuration")
        st.markdown(f"**HubSpot** : `...{config['hubspot']['api_key'][-8:]}`")
        st.markdown(f"**PostgreSQL** : `{config['postgresql']['host'][:25]}...`")
        st.divider()
        st.markdown("Modifiez `config.yaml` pour changer les parametres.")

        # Checkboxes pour selectionner les agents
        st.subheader("Agents (repartition taches)")
        all_owners = config.get('task_owners', []) + config.get('excluded_owners', [])
        active_ids = {o['id'] for o in config.get('task_owners', [])}

        selected_owners = []
        for owner in all_owners:
            checked = st.checkbox(
                f"{owner['name']}",
                value=(owner['id'] in active_ids),
                key=f"owner_{owner['id']}"
            )
            if checked:
                selected_owners.append(owner)

        st.caption(f"{len(selected_owners)} agent(s) selectionne(s)")

    # Upload fichier
    uploaded = st.file_uploader(
        "Deposez le fichier Excel du client (RESA / ORES)",
        type=['xlsx', 'xls', 'csv'],
        help="Le fichier sera transforme, puis pousse vers PostgreSQL et HubSpot."
    )

    if not uploaded:
        st.info("Deposez un fichier Excel pour commencer.")
        st.stop()

    # Lire le fichier
    filename = uploaded.name
    ext = os.path.splitext(filename)[1].lower()
    if ext == '.csv':
        df = pd.read_csv(uploaded, dtype=str)
    else:
        df = pd.read_excel(uploaded, dtype=str)
    df.columns = df.columns.str.strip()

    st.success(f"**{filename}** charge — {len(df)} lignes, {len(df.columns)} colonnes")

    # ── Validation du fichier ──
    is_valid, val_errors, val_warnings, val_infos = validate_excel(df)

    # Statistiques
    if val_infos:
        cols_info = st.columns(len(val_infos))
        for i, info in enumerate(val_infos):
            parts = info.split(' ', 1)
            cols_info[i].metric(parts[1] if len(parts) > 1 else info, parts[0])

    # Erreurs bloquantes
    if val_errors:
        st.error("**Le fichier contient des erreurs bloquantes. Corrigez-les avant de continuer.**")
        for err in val_errors:
            st.error(f"❌ {err}")

    # Avertissements non bloquants
    if val_warnings:
        with st.expander(f"⚠️ {len(val_warnings)} avertissement(s) — non bloquant(s)", expanded=True):
            for w in val_warnings:
                st.warning(f"⚠️ {w}")

    # Apercu des données
    with st.expander("Apercu des donnees brutes", expanded=False):
        st.dataframe(df.head(10), width='stretch')

    # Bloquer si erreurs
    if not is_valid:
        st.info("Corrigez le fichier Excel puis re-deposez-le.")
        st.stop()

    st.success("✅ Fichier valide — vous pouvez lancer l'import.")

    # Selection des etapes
    st.subheader("Etapes a executer")
    col1, col2, col3 = st.columns(3)
    with col1:
        do_step1 = st.checkbox("1. Transformer Excel", value=True)
    with col2:
        do_step2 = st.checkbox("2. Push PostgreSQL", value=True)
    with col3:
        do_step3 = st.checkbox("3. Push HubSpot", value=True)

    # ── Detection doublons HubSpot ──
    if do_step3:
        st.subheader("Verification des doublons HubSpot")
        if st.button("Verifier les doublons (par CustomerName)", key="check_dup"):
            with st.spinner("Recherche des doublons dans HubSpot..."):
                duplicates = detect_hubspot_duplicates(df, config)
            if duplicates:
                st.warning(f"**{len(duplicates)} CustomerName(s) deja present(s) dans HubSpot.**")
                dup_data = []
                for name, contacts in duplicates.items():
                    for c in contacts:
                        dup_data.append({
                            'CustomerName': name,
                            'HubSpot ID': c['id'],
                            'Firstname (HubSpot)': c['firstname'],
                            'Lastname (HubSpot)': c['lastname'],
                            'Login (HubSpot)': c['login'],
                        })
                st.session_state['duplicates'] = dup_data
                st.dataframe(pd.DataFrame(dup_data), width='stretch')

                st.session_state['dup_action'] = st.radio(
                    "Que faire avec les doublons ?",
                    ["Creer quand meme (doublons possibles)", "Ignorer les doublons (ne pas les re-creer)"],
                    key="dup_radio"
                )
            else:
                st.success("Aucun doublon detecte — tous les CustomerName sont nouveaux.")
                st.session_state['dup_action'] = "Creer quand meme (doublons possibles)"
                st.session_state['duplicates'] = []

    # ── Preview / Resume avant import ──
    st.subheader("Resume avant import")
    list_name = os.path.splitext(filename)[0]

    preview_cols = st.columns(2)
    with preview_cols[0]:
        st.markdown("**Actions prevues :**")
        if do_step1:
            st.markdown(f"- Transformation Excel ({len(df)} lignes)")
        if do_step2:
            st.markdown(f"- Push PostgreSQL → `{config['postgresql']['table']}`")
        if do_step3:
            st.markdown(f"- Push HubSpot : {len(df)} contacts + {len(df)} taches")
            st.markdown(f"- Liste statique : `{list_name}`")

    with preview_cols[1]:
        if do_step3:
            st.markdown("**Repartition des taches :**")
            nb_owners = len(selected_owners)
            if nb_owners > 0:
                per_agent = len(df) // nb_owners
                reste = len(df) % nb_owners
                for i, o in enumerate(selected_owners):
                    count = per_agent + (1 if i < reste else 0)
                    st.markdown(f"- {o['name']} : ~{count} tache(s)")
            else:
                st.warning("Aucun agent selectionne !")

    # ── Bouton Confirmer et Lancer ──
    st.divider()
    if not st.checkbox("J'ai verifie le resume ci-dessus et je confirme le lancement", key="confirm_check"):
        st.stop()

    if st.button("Confirmer et lancer l'import", type="primary", width='stretch'):
        logger, log_file = setup_logger(config, filename)
        logger.info(f"=== Debut import : {filename} ({len(df)} lignes) ===")
        start_time = time.time()

        # Identifier les doublons a ignorer
        skip_names = set()
        if do_step3 and st.session_state.get('dup_action', '').startswith('Ignorer'):
            dup_data = st.session_state.get('duplicates', [])
            skip_names = {d['CustomerName'] for d in dup_data}
            if skip_names:
                orig_len = len(df)
                df = df[~df['CustomerName'].astype(str).str.strip().isin(skip_names)]
                logger.info(f"Doublons ignores: {orig_len - len(df)} lignes exclues ({len(skip_names)} noms)")
                st.info(f"{orig_len - len(df)} doublon(s) ignore(s). Import de {len(df)} lignes.")

        # Step 1
        if do_step1:
            with st.status("Etape 1 : Transformation Excel...", expanded=True) as status:
                try:
                    df = step1_transform(df, logger)
                    status.update(label=f"Etape 1 terminee ({len(df)} lignes)", state="complete")

                    st.dataframe(df[['Nom', 'Adresse postale', 'AppointmentDate']].head(5), width='stretch')

                    buffer = BytesIO()
                    df.to_excel(buffer, index=False, engine='openpyxl')
                    buffer.seek(0)
                    cleaned_name = os.path.splitext(filename)[0] + '_cleaned.xlsx'
                    st.download_button(
                        label=f"Telecharger {cleaned_name}",
                        data=buffer,
                        file_name=cleaned_name,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                except Exception as e:
                    status.update(label=f"Erreur Step 1 : {e}", state="error")
                    logger.error(f"Step 1 erreur: {e}")
                    st.stop()

        # Step 2
        if do_step2:
            with st.status("Etape 2 : Push PostgreSQL...", expanded=True) as status:
                progress2 = st.progress(0)
                msg2 = st.empty()
                try:
                    def cb2(pct, msg):
                        progress2.progress(min(pct, 1.0))
                        msg2.text(msg)

                    nb = step2_postgresql(df, config, logger, cb2)
                    status.update(label=f"Etape 2 terminee ({nb} lignes inserees)", state="complete")
                except Exception as e:
                    status.update(label=f"Erreur Step 2 : {e}", state="error")
                    logger.error(f"Step 2 erreur: {e}")

        # Step 3
        if do_step3:
            with st.status("Etape 3 : Push HubSpot...", expanded=True) as status:
                progress3 = st.progress(0)
                msg3 = st.empty()
                try:
                    def cb3(pct, msg):
                        progress3.progress(min(pct, 1.0))
                        msg3.text(msg)

                    res = step3_hubspot(df, config, logger, list_name, cb3, task_owners=selected_owners)

                    status.update(
                        label=f"Etape 3 terminee ({res['contacts']} contacts, {res['tasks']} taches)",
                        state="complete"
                    )

                    # Stocker les resultats pour rollback
                    st.session_state['last_import'] = {
                        'contact_ids': list(res.get('contact_ids', {}).values()) if isinstance(res.get('contact_ids'), dict) else [],
                        'list_id': res.get('list_id', ''),
                        'import_date': str(date.today()),
                        'contacts_count': res['contacts'],
                        'tasks_count': res['tasks'],
                        'filename': filename,
                    }

                    col_a, col_b, col_c = st.columns(3)
                    col_a.metric("Contacts", res['contacts'])
                    col_b.metric("Taches", res['tasks'])
                    col_c.metric("Liste", res.get('list_id', 'N/A'))

                    if res['errors']:
                        with st.expander(f"{len(res['errors'])} erreur(s)"):
                            for err in res['errors'][:20]:
                                st.warning(err)
                except Exception as e:
                    status.update(label=f"Erreur Step 3 : {e}", state="error")
                    logger.error(f"Step 3 erreur: {e}")

        # Resume final
        elapsed = time.time() - start_time
        logger.info(f"=== Import termine en {elapsed:.1f}s ===")

        st.divider()
        st.success(f"Import termine en **{elapsed:.1f} secondes** !")
        st.caption(f"Log sauvegarde : `{log_file}`")

    # ── ROLLBACK : Annuler le dernier import ──
    if 'last_import' in st.session_state and st.session_state['last_import']:
        last = st.session_state['last_import']
        st.divider()
        st.subheader("Annuler le dernier import")
        st.markdown(
            f"Dernier import : **{last['filename']}** — "
            f"{last['contacts_count']} contacts, {last['tasks_count']} taches, "
            f"liste `{last.get('list_id', 'N/A')}`"
        )
        st.warning("Cette action est irreversible. Les contacts, taches associees et la liste seront supprimes de HubSpot. Les lignes PostgreSQL du jour seront aussi supprimees.")

        if st.button("Annuler cet import (ROLLBACK)", type="secondary", key="rollback_btn"):
            logger, log_file = setup_logger(config, f"ROLLBACK_{last['filename']}")
            logger.info(f"=== ROLLBACK demande pour {last['filename']} ===")

            with st.status("Rollback en cours...", expanded=True) as status:
                # Rollback HubSpot
                contact_ids = last.get('contact_ids', [])
                if contact_ids:
                    progress_rb = st.progress(0)
                    msg_rb = st.empty()
                    def cb_rb(pct, msg):
                        progress_rb.progress(min(pct, 1.0))
                        msg_rb.text(msg)
                    deleted, rb_errors = rollback_hubspot(contact_ids, last.get('list_id', ''), config, logger, cb_rb)
                    st.markdown(f"HubSpot : {deleted} contacts supprimes")
                else:
                    st.markdown("HubSpot : aucun contact_id enregistre, suppression manuelle necessaire.")

                # Rollback PostgreSQL
                import_date = last.get('import_date', '')
                if import_date:
                    pg_deleted = rollback_postgresql(import_date, config, logger)
                    st.markdown(f"PostgreSQL : {pg_deleted} lignes supprimees (import_date={import_date})")

                status.update(label="Rollback termine", state="complete")

            st.session_state['last_import'] = None
            logger.info("=== ROLLBACK termine ===")

if __name__ == '__main__':
    main()
