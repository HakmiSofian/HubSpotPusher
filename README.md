# HubSpot Pusher

Application web Streamlit pour automatiser l'import de contacts clients (RESA / ORES) vers HubSpot CRM et PostgreSQL.

## Fonctionnalités

**3 étapes automatisées :**

1. **Transformation Excel** — Convertit les dates françaises (`jeudi 23 avril 2026` → `23-04-2026`), calcule les colonnes `Nom` et `Adresse postale`.
2. **Push PostgreSQL** — Insertion bulk dans la table `public.dataformails` (AWS RDS).
3. **Push HubSpot** — Création batch des contacts, liste statique, et tâches CALL (`RAPPEL RDV`) avec répartition round-robin entre les agents.

**Fonctionnalités avancées :**

- Validation du fichier Excel avant import (colonnes manquantes, doublons Login, format dates)
- Détection des doublons HubSpot par `CustomerName` avant création
- Preview / résumé détaillé avec confirmation obligatoire avant lancement
- Sélection dynamique des agents via checkboxes dans la sidebar
- Rollback complet : suppression des contacts, tâches, liste HubSpot et lignes PostgreSQL
- Logs horodatés pour chaque exécution

## Prérequis

- Python 3.9+
- Un compte HubSpot avec une [Private App](https://developers.hubspot.com/docs/api/private-apps) (scopes : `crm.objects.contacts.write`, `crm.objects.contacts.read`, `crm.lists.write`, `crm.objects.tasks.write`)
- Une base PostgreSQL accessible

## Installation locale

```bash
git clone https://github.com/HakmiSofian/HubSpotPusher.git
cd HubSpotPusher
pip install -r requirements.txt
```

Configurez vos credentials dans `config.yaml` :

```yaml
hubspot:
  api_key: "pat-eu1-votre-cle-ici"

postgresql:
  host: "votre-host.rds.amazonaws.com"
  port: 5432
  database: "postgres"
  user: "votre_user"
  password: "votre_password"
  table: "public.dataformails"
```

Lancez l'application :

```bash
streamlit run app.py
```

## Déploiement Streamlit Cloud

1. Forkez ou poussez le repo sur GitHub.
2. Allez sur [share.streamlit.io](https://share.streamlit.io) et déployez `app.py` depuis votre repo.
3. Dans **Settings → Secrets**, ajoutez vos credentials au format TOML :

```toml
[hubspot]
api_key = "pat-eu1-votre-cle-ici"

[postgresql]
host = "votre-host.rds.amazonaws.com"
port = "5432"
database = "postgres"
user = "votre_user"
password = "votre_password"
```

Les secrets Streamlit Cloud surchargent automatiquement les valeurs de `config.yaml`.

## Structure du projet

```
├── app.py                        # Application Streamlit (interface + logique)
├── config.yaml                   # Configuration (credentials, mapping, agents)
├── config.example.yaml           # Template de config sans credentials
├── requirements.txt              # Dépendances Python
├── step1_modifier_excel.py       # Script CLI — transformation Excel
├── step2_push_postgresql.py      # Script CLI — push PostgreSQL
├── step3_push_hubspot_fast.py    # Script CLI — push HubSpot (batch API)
└── logs/                         # Logs d'exécution (auto-généré)
```

## Format du fichier Excel attendu

Le fichier uploadé doit contenir au minimum ces colonnes :

| Colonne | Description |
|---|---|
| `WorkOrderId` | Identifiant du bon de travail |
| `WorkOrderExternalReference` | Référence externe |
| `CustomerName` | Prénom du client |
| `Street`, `HouseNumber`, `ZipCode`, `City` | Adresse |
| `Language` | Langue du client |
| `AppointmentDate` | Date de RDV (format français : `jeudi 23 avril 2026`) |
| `Login` | Identifiant unique du contact |
| `Password` | Mot de passe client |

Colonnes optionnelles : `E EAN Number`, `PostBox`, `Appointment Window`, `Login Url`, `GRD`, `CreationDate`.

## Configuration des agents

Dans `config.yaml`, la section `task_owners` définit les agents qui reçoivent les tâches. La section `excluded_owners` liste ceux qui sont exclus par défaut. Dans l'interface, chaque agent a une checkbox pour l'inclure ou l'exclure dynamiquement avant chaque import.

## Licence

Usage interne — Kartu Group.
