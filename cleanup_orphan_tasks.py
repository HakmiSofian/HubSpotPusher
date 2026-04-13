#!/usr/bin/env python3
"""
Nettoyage des taches orphelines HubSpot
========================================
Trouve et supprime toutes les taches qui ne sont associees a aucun contact.

Fonctionnement :
1. Recupere toutes les taches par pages de 100 (API Search)
2. Pour chaque batch de 100 taches, verifie les associations contact (API Batch Associations)
3. Identifie les taches sans contact → orphelines
4. Affiche un resume et demande confirmation
5. Supprime les orphelines par batch de 100 (API Batch Archive)

Usage : python cleanup_orphan_tasks.py
        python cleanup_orphan_tasks.py --dry-run    (simulation, pas de suppression)
"""

import sys
import time
import yaml
import requests

# ─── Config ──────────────────────────────────────────────────────────────────

with open('config.yaml', 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

API_KEY = config['hubspot']['api_key']
HEADERS = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json',
}
BASE = 'https://api.hubapi.com'
DRY_RUN = '--dry-run' in sys.argv

session = requests.Session()
session.headers.update(HEADERS)


def api_call(method, url, json=None, params=None, max_retries=5):
    """Appel API avec gestion automatique du rate limit."""
    for attempt in range(max_retries):
        resp = getattr(session, method)(url, json=json, params=params, timeout=30)
        if resp.status_code == 429:
            pause = int(resp.headers.get('Retry-After', 10))
            print(f"  Rate limit — pause {pause}s...")
            time.sleep(pause)
            continue
        return resp
    return resp


# ─── Etape 1 : Recuperer toutes les taches ──────────────────────────────────

print("=" * 60)
print("NETTOYAGE TACHES ORPHELINES HUBSPOT")
print("=" * 60)
if DRY_RUN:
    print(">>> MODE SIMULATION (--dry-run) — aucune suppression <<<\n")

print("\n[1/3] Recuperation de toutes les taches...")

all_task_ids = []
after = 0
page = 0

while True:
    payload = {
        'limit': 100,
        'properties': ['hs_task_subject'],
        'after': after,
    }
    resp = api_call('post', f'{BASE}/crm/v3/objects/tasks/search', json=payload)

    if resp.status_code != 200:
        print(f"Erreur API Search: {resp.status_code} - {resp.text[:200]}")
        break

    data = resp.json()
    results = data.get('results', [])

    for task in results:
        all_task_ids.append(task['id'])

    page += 1
    total = data.get('total', '?')
    print(f"  Page {page} — {len(all_task_ids)}/{total} taches chargees", end='\r')

    paging = data.get('paging', {}).get('next', {})
    next_after = paging.get('after')
    if not next_after:
        break
    after = int(next_after)

    # HubSpot search API limite a 10 000 resultats — on doit paginer autrement
    if len(all_task_ids) >= 10000 and int(next_after) >= 10000:
        print(f"\n  ⚠ Limite API Search atteinte (10 000). Passage au scan par date...")
        break

print(f"\n  → {len(all_task_ids)} taches recuperees")

# Si > 10 000, on doit scanner par tranches de dates
if len(all_task_ids) >= 10000:
    print("\n  Scan etendu par tranches de dates (createdate)...")
    all_task_ids = []
    # Trouver la plus ancienne tache
    resp = api_call('post', f'{BASE}/crm/v3/objects/tasks/search', json={
        'limit': 1,
        'properties': ['hs_createdate'],
        'sorts': [{'propertyName': 'hs_createdate', 'direction': 'ASCENDING'}]
    })
    if resp.status_code == 200 and resp.json().get('results'):
        oldest = resp.json()['results'][0]['properties']['hs_createdate']
        print(f"  Plus ancienne tache : {oldest}")

    # Scanner par tranches de 1 jour
    from datetime import datetime, timedelta

    # Commencer depuis 2024-01-01 pour couvrir tout
    start_date = datetime(2024, 1, 1)
    end_date = datetime.now() + timedelta(days=1)
    current = start_date

    while current < end_date:
        next_day = current + timedelta(days=1)
        start_ms = str(int(current.timestamp() * 1000))
        end_ms = str(int(next_day.timestamp() * 1000))

        after = 0
        day_count = 0
        while True:
            payload = {
                'limit': 100,
                'properties': ['hs_task_subject'],
                'filterGroups': [{
                    'filters': [
                        {'propertyName': 'hs_createdate', 'operator': 'GTE', 'value': start_ms},
                        {'propertyName': 'hs_createdate', 'operator': 'LT', 'value': end_ms},
                    ]
                }],
                'after': after,
            }
            resp = api_call('post', f'{BASE}/crm/v3/objects/tasks/search', json=payload)
            if resp.status_code != 200:
                break

            data = resp.json()
            results = data.get('results', [])
            for task in results:
                all_task_ids.append(task['id'])
            day_count += len(results)

            paging = data.get('paging', {}).get('next', {})
            next_after = paging.get('after')
            if not next_after or int(next_after) >= 10000:
                break
            after = int(next_after)

        if day_count > 0:
            print(f"  {current.strftime('%Y-%m-%d')} : {day_count} taches (total: {len(all_task_ids)})")

        current = next_day

    print(f"\n  → {len(all_task_ids)} taches recuperees au total")


# ─── Etape 2 : Verifier les associations ────────────────────────────────────

print(f"\n[2/3] Verification des associations (batch de 100)...")

orphan_ids = []
associated_count = 0
BATCH = 100

for i in range(0, len(all_task_ids), BATCH):
    batch = all_task_ids[i:i + BATCH]
    inputs = [{'id': str(tid)} for tid in batch]

    resp = api_call('post',
        f'{BASE}/crm/v4/associations/tasks/contacts/batch/read',
        json={'inputs': inputs}
    )

    if resp.status_code == 200:
        results = resp.json().get('results', [])
        # IDs ayant au moins une association contact
        associated_ids = set()
        for r in results:
            from_id = str(r.get('from', {}).get('id', ''))
            associations = r.get('to', [])
            if associations and len(associations) > 0:
                associated_ids.add(from_id)

        for tid in batch:
            if str(tid) in associated_ids:
                associated_count += 1
            else:
                orphan_ids.append(str(tid))
    else:
        # En cas d'erreur, considerer comme orphelin par securite? Non — on skip
        print(f"\n  ⚠ Erreur batch associations {i//BATCH+1}: {resp.status_code}")

    checked = min(i + BATCH, len(all_task_ids))
    print(f"  Verifie : {checked}/{len(all_task_ids)} — {len(orphan_ids)} orpheline(s)", end='\r')

print(f"\n\n  Resume :")
print(f"    Taches verifiees  : {len(all_task_ids)}")
print(f"    Avec contact      : {associated_count}")
print(f"    ORPHELINES        : {len(orphan_ids)}")


# ─── Etape 3 : Supprimer les orphelines ─────────────────────────────────────

if len(orphan_ids) == 0:
    print("\n✅ Aucune tache orpheline trouvee !")
    sys.exit(0)

print(f"\n[3/3] Suppression de {len(orphan_ids)} tache(s) orpheline(s)...")

if DRY_RUN:
    print("  >>> DRY RUN — aucune suppression effectuee <<<")
    print(f"  IDs a supprimer (10 premiers) : {orphan_ids[:10]}")
    sys.exit(0)

# Demander confirmation
print(f"\n⚠  ATTENTION : {len(orphan_ids)} taches vont etre SUPPRIMEES definitivement.")
confirm = input("Confirmer ? (oui/non) : ").strip().lower()

if confirm not in ('oui', 'o', 'yes', 'y'):
    print("Annule.")
    sys.exit(0)

deleted = 0
for i in range(0, len(orphan_ids), BATCH):
    batch = orphan_ids[i:i + BATCH]
    inputs = [{'id': tid} for tid in batch]

    resp = api_call('post',
        f'{BASE}/crm/v3/objects/tasks/batch/archive',
        json={'inputs': inputs}
    )

    if resp.status_code in (200, 204):
        deleted += len(batch)
    else:
        print(f"\n  ⚠ Erreur suppression batch {i//BATCH+1}: {resp.status_code} - {resp.text[:100]}")

    print(f"  Supprime : {deleted}/{len(orphan_ids)}", end='\r')

print(f"\n\n✅ Termine : {deleted} tache(s) orpheline(s) supprimee(s).")
