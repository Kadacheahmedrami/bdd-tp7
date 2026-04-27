# TP7 – Fragmentation BDD Distribuée
## 2CS ESTIN 2025/2026 — MongoDB & PyMongo

### Prérequis
- Docker Desktop installé et lancé
- Python 3.8+ avec pymongo : `pip install pymongo`

---

## Étapes (3 commandes seulement)

### 1. Lancer MongoDB avec Docker
```bash
docker-compose up -d
```
Attend ~5 secondes que MongoDB démarre, puis :

### 2. Peupler la base globale
```bash
python setup_hopital_db.py
```

### 3. Exécuter toutes les solutions du TP
```bash
python tp7_solutions.py
```

---

## Ce que fait chaque fichier

| Fichier | Rôle |
|---|---|
| `docker-compose.yml` | Lance MongoDB (port 27017) + Mongo Express (port 8081) |
| `setup_hopital_db.py` | Crée la base `hopital_global` avec toutes les données |
| `tp7_solutions.py` | Répond aux questions 1, 2, 3 et 4 |

## Mongo Express — Interface web

Mongo Express est une interface graphique pour explorer vos collections MongoDB dans le navigateur.

- URL : **http://localhost:8081**
- Pas de login requis (authentification désactivée)
- Permet de parcourir les bases `hopital_global` et `hopital_frags`, voir les documents, et vérifier les fragments créés

---

## Résumé des fragments créés (Question 2)

### Fragmentation horizontale
| Collection | Fragment | Site |
|---|---|---|
| hopital | hopital_nord | Site Nord |
| hopital | hopital_centre | Site Centre |
| hopital | hopital_sud | Site Sud |
| hopital | hopital_all_* | Répliqué sur tous les sites (contrainte e) |
| service | service_nord/centre/sud | Idem région |
| patient | patient_nord/centre/sud | Idem région |
| consultation | consultation_nord/centre/sud | Idem région patient |
| infirmier | infirmier_nord, infirmier_sud | Sites Nord et Sud |

### Fragmentation verticale
| Collection | Fragment | Attributs | Site |
|---|---|---|---|
| medecin | medecin_civil | idMed, nom, adr, tel | Centre |
| medecin | medecin_pro | idMed, specialite | Tous |
| infirmier (Centre) | infirmier_centre_civil | idInf, nom, adr, tel | Centre |
| infirmier (Centre) | infirmier_centre_pro | idInf, idHop, idService, salaire, anciennete | Centre |

---

## Arrêter MongoDB
```bash
docker-compose down
```