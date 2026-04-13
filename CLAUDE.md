# CLAUDE.md — Contexte projet HubSpot Pusher

## Vue d'ensemble

Application Streamlit (app.py) pour Kartu Group qui automatise l'import de contacts clients RESA/ORES vers HubSpot CRM et PostgreSQL (AWS RDS). Déployée sur Streamlit Community Cloud.

Propriétaire : Sofian Hakmi (sofian.hakmi@kartu-group.com)
Repo GitHub : https://github.com/HakmiSofian/HubSpotPusher

## Architecture

Tout le code principal est dans **app.py** (fichier unique ~1000+ lignes).
Config externalisée dans **config.yaml** (credentials, mapping, owners, batch sizes).
Scripts CLI standalone aussi disponibles (step1, step2, step3) mais l'app Streamlit est le livrable principal.

### 3 étapes automatisées :
1. **Transformation Excel** : dates françaises ("jeudi 23 avril 2026" → "23-04-2026"), calcul colonnes Nom et Adresse postale
2. **Push PostgreSQL** : bulk insert via execute_values dans public.dataformails (AWS RDS)
3. **Push HubSpot** : batch/create contacts (100/req), liste statique, tâches CALL "RAPPEL RDV" avec round-robin sur agents sélectionnés

### Fonctionnalités :
- Validation Excel (colonnes obligatoires, doublons Login, format dates)
- Détection doublons HubSpot par CustomerName (firstname) via API Search batch par groupes de 5
- Preview/résumé avant import avec confirmation obligatoire
- Checkboxes sidebar pour sélection dynamique des agents (12 actifs, 6 exclus par défaut)
- Rollback complet (suppression contacts+tâches+liste HubSpot + lignes PostgreSQL)
- Nettoyage tâches orphelines (scan par jour + associations v4 batch)
- Logs horodatés par exécution

## Décisions techniques et erreurs passées (IMPORTANT)

### HubSpot API
- **batch/upsert ne marche PAS** : la propriété "login" n'est pas marquée comme unique dans HubSpot. On utilise **batch/create** à la place.
- **batch/create retourne les résultats dans un ORDRE DIFFÉRENT** des inputs. Il faut matcher par la valeur de la propriété "login" : `login_to_idx[result_login] = row_idx`. NE JAMAIS assumer que result[0] correspond à input[0].
- **Réponse liste imbriquée** : la création de liste retourne `{"list": {"listId": "87"}}` PAS `{"listId": "87"}`. Toujours vérifier `data["list"]["listId"]` d'abord.
- **Ajout membres liste** : l'API attend un JSON array brut `["id1","id2"]` PAS `{"recordIdsToAdd": [...]}`.
- **Rate limit** : 100 req/10s. Pause de 11s sur HTTP 429. Retry-After header.
- **Propriétés number** : valeurs > 2^53 (JS MAX_SAFE_INTEGER) doivent être ignorées.
- **adresse_postale** : propriété custom supprimée manuellement. Remappée vers "address" (Street Address built-in).
- La base HubSpot contient 70 000+ contacts et 170 000+ tâches. Ne JAMAIS fetch tous les contacts pour vérifier les doublons — utiliser l'API Search par batch de 5 filterGroups.

### PostgreSQL
- **Toutes les colonnes sont de type TEXT** : on fait ALTER TABLE ... TYPE text sur chaque colonne avant insert car des colonnes comme WorkOrderId, Login contiennent du texte mixte (ex: "TEST1", "000110056762_0010") qui casse les bigint.
- Bulk insert avec **execute_values** (psycopg2.extras), page_size=1000.
- Toutes les valeurs passées via safe_str (jamais safe_int).

### Streamlit
- **use_container_width=True** est déprécié → remplacé par **width='stretch'**
- **@st.cache_data** sur la config empêchait les secrets Streamlit Cloud de s'appliquer → changé en **@st.cache_resource** pour le YAML + deepcopy pour les secrets.
- Config.yaml est sur GitHub AVEC les vrais credentials (pas dans .gitignore). Les secrets Streamlit Cloud surchargent si présents.

### Git / Déploiement
- OneDrive crée des problèmes de lock files (.git/index.lock, HEAD.lock). Solution : `Remove-Item .git\index.lock -Force` depuis PowerShell.
- Streamlit Cloud a ajouté automatiquement .devcontainer/devcontainer.json.
- requirements.txt DOIT être dans le repo (psycopg2-binary, pyyaml, etc.).

## Structure des fichiers

```
app.py                        # App Streamlit principale (tout-en-un)
config.yaml                   # Config avec credentials réels
config.example.yaml           # Template sans credentials
requirements.txt              # Dépendances Python
cleanup_orphan_tasks.py       # Script CLI nettoyage tâches orphelines
step1_modifier_excel.py       # Script CLI transformation Excel
step2_push_postgresql.py      # Script CLI push PostgreSQL (bulk)
step3_push_hubspot_fast.py    # Script CLI push HubSpot (batch API)
README.md                     # Documentation GitHub
CLAUDE.md                     # Ce fichier (contexte pour Claude Code)
```

## Config.yaml — structure clé

```yaml
hubspot:
  api_key: "pat-eu1-..."        # Private App token

postgresql:
  host: "...rds.amazonaws.com"
  table: "public.dataformails"

hubspot_mapping:                 # Excel col → HubSpot property
  CustomerName: "firstname"
  Nom: "lastname"
  Login: "login"
  AppointmentDate: "jobtitle"
  ...

task_owners:                     # 12 agents actifs (round-robin)
  - { id: "9460622", name: "Anas BELLAL" }
  ...

excluded_owners:                 # 6 agents exclus par défaut
  - { id: "30134213", name: "Aziz Assafi" }
  ...
```

## Mapping HubSpot important

| Excel | HubSpot | Notes |
|---|---|---|
| CustomerName | firstname | Prénom client, utilisé pour détection doublons |
| Nom (calculé) | lastname | = WorkOrderId - WorkOrderExternalReference |
| Login | login | Clé unique pour matcher batch results |
| AppointmentDate | jobtitle | Date RDV stockée dans intitulé de poste |
| Adresse postale (calculé) | address | Street + HouseNumber + PostBox, ZipCode City |

## Agents (owners) HubSpot

12 actifs : Anas BELLAL, Omraam Bankanguila, Yassine Bounouh, Nali Likibi, Sublime Dzassouka, Mbarek Hemmam, Mehdi Karroum, Yassine Chraibi, Sabrine Meddeb, HAROLD AKOLI, Saad Hajir, Achraf ENNAJAH

6 exclus par défaut : Aziz Assafi, Sofian Hakmi, Oussama Essakhi, Kawtar EL BAZ, Mouad FIALI, Ilham MARGHALA

La sélection se fait dynamiquement via checkboxes dans la sidebar Streamlit.
