"""
TP - Fragmentation d'une Base de Données Distribuée
2CS – ESTIN – 2025/2026  |  MongoDB & PyMongo

Solutions complètes : Questions 1, 2, 3, 4
Connexion : MongoDB localhost:27017  (docker-compose up -d)
"""

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db     = client["hopital_global"]   # base globale
db_f   = client["hopital_frags"]    # base des fragments


# ══════════════════════════════════════════════════════════════════════════════
# QUESTION 1 – Requête globale
# Trouver nom + spécialité des médecins ayant fait ≥1 consultation
# motif='urgence' pour des patients hospitalisés en région Nord.
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("QUESTION 1 — Requête globale")
print("=" * 60)

pipeline_q1 = [
    # 1) Jointure consultation → patient
    {"$lookup": {
        "from": "patient",
        "localField": "idPat",
        "foreignField": "idPat",
        "as": "pat"
    }},
    {"$unwind": "$pat"},
    # 2) Jointure patient → hôpital (pour connaître la région)
    {"$lookup": {
        "from": "hopital",
        "localField": "pat.idHop",
        "foreignField": "idHop",
        "as": "hop"
    }},
    {"$unwind": "$hop"},
    # 3) Filtres : urgence ET région Nord
    {"$match": {"motif": "urgence", "hop.region": "Nord"}},
    # 4) Jointure → médecin
    {"$lookup": {
        "from": "medecin",
        "localField": "idMed",
        "foreignField": "idMed",
        "as": "med"
    }},
    {"$unwind": "$med"},
    # 5) Dédoublonnage par médecin
    {"$group": {
        "_id": "$idMed",
        "nom":       {"$first": "$med.nom"},
        "specialite":{"$first": "$med.specialite"}
    }},
    {"$sort": {"nom": 1}}
]

resultats_q1 = list(db["consultation"].aggregate(pipeline_q1))
print(f"Résultat ({len(resultats_q1)} médecin(s)) :")
for r in resultats_q1:
    print(f"  {r['nom']} — {r['specialite']}")


# ══════════════════════════════════════════════════════════════════════════════
# QUESTION 2 – Fragmentation
#
# Stratégie appliquée :
#
#  hopital      → Fragmentation HORIZONTALE par région
#                 + RÉPLICATION sur les 3 sites (contrainte e)
#                 → hopital_nord, hopital_centre, hopital_sud (+ copies complètes)
#
#  service      → Fragmentation HORIZONTALE dérivée de hopital (par idHop)
#                 → service_nord, service_centre, service_sud
#
#  patient      → Fragmentation HORIZONTALE dérivée de hopital (par idHop)
#                 → patient_nord, patient_centre, patient_sud
#
#  infirmier    → Fragmentation HORIZONTALE dérivée de hopital (par idHop)
#                 → infirmier_nord, infirmier_sud
#                 + Fragmentation VERTICALE pour le site Centre (contrainte d)
#                   infirmier_centre_civil  (idInf, nom, adr, tel)
#                   infirmier_centre_pro    (idInf, idHop, idService, salaire, anciennete)
#
#  medecin      → Fragmentation VERTICALE (contrainte d : état civil au Centre)
#                 → medecin_civil   (idMed, nom, adr, tel)     → Site Centre
#                 → medecin_pro     (idMed, specialite)        → tous sites
#
#  consultation → Fragmentation HORIZONTALE dérivée du patient (par région)
#                 → consultation_nord, consultation_centre, consultation_sud
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("QUESTION 2 — Création des fragments")
print("=" * 60)

# ── Récupérer les idHop par région ──────────────────────────────────────────
def hopitaux_region(region):
    return [h["idHop"] for h in db["hopital"].find({"region": region}, {"idHop": 1})]

hops_nord   = hopitaux_region("Nord")
hops_centre = hopitaux_region("Centre")
hops_sud    = hopitaux_region("Sud")

# ── Nettoyage des fragments précédents ──────────────────────────────────────
for col in db_f.list_collection_names():
    db_f[col].drop()

# ── Helper : copier une requête vers un fragment ─────────────────────────────
def creer_fragment(src_col, filtre, projection, dest_col):
    docs = list(db[src_col].find(filtre, projection))
    if docs:
        db_f[dest_col].insert_many(docs)
    print(f"  {dest_col:<35} : {len(docs)} docs")
    return len(docs)


print("\n— hopital (horizontal + réplication totale) —")
creer_fragment("hopital", {"region": "Nord"},   {"_id": 0}, "hopital_nord")
creer_fragment("hopital", {"region": "Centre"}, {"_id": 0}, "hopital_centre")
creer_fragment("hopital", {"region": "Sud"},    {"_id": 0}, "hopital_sud")
# Réplication complète sur chaque site (contrainte e)
for region in ["nord", "centre", "sud"]:
    docs = list(db["hopital"].find({}, {"_id": 0}))
    db_f[f"hopital_all_{region}"].insert_many(docs)
    print(f"  hopital_all_{region:<26} : {len(docs)} docs (répliqué)")

print("\n— service (horizontal dérivé de hopital) —")
creer_fragment("service", {"idHop": {"$in": hops_nord}},   {"_id": 0}, "service_nord")
creer_fragment("service", {"idHop": {"$in": hops_centre}}, {"_id": 0}, "service_centre")
creer_fragment("service", {"idHop": {"$in": hops_sud}},    {"_id": 0}, "service_sud")

print("\n— patient (horizontal dérivé de hopital) —")
creer_fragment("patient", {"idHop": {"$in": hops_nord}},   {"_id": 0}, "patient_nord")
creer_fragment("patient", {"idHop": {"$in": hops_centre}}, {"_id": 0}, "patient_centre")
creer_fragment("patient", {"idHop": {"$in": hops_sud}},    {"_id": 0}, "patient_sud")

print("\n— infirmier (horizontal Nord/Sud + vertical Centre) —")
creer_fragment("infirmier", {"idHop": {"$in": hops_nord}}, {"_id": 0}, "infirmier_nord")
creer_fragment("infirmier", {"idHop": {"$in": hops_sud}},  {"_id": 0}, "infirmier_sud")
# Fragmentation verticale pour le Centre
creer_fragment("infirmier", {"idHop": {"$in": hops_centre}},
               {"_id": 0, "idInf": 1, "nom": 1, "adr": 1, "tel": 1},
               "infirmier_centre_civil")
creer_fragment("infirmier", {"idHop": {"$in": hops_centre}},
               {"_id": 0, "idInf": 1, "idHop": 1, "idService": 1, "salaire": 1, "anciennete": 1},
               "infirmier_centre_pro")

print("\n— medecin (vertical : civil au Centre, pro partout) —")
creer_fragment("medecin", {}, {"_id": 0, "idMed": 1, "nom": 1, "adr": 1, "tel": 1},
               "medecin_civil")
creer_fragment("medecin", {}, {"_id": 0, "idMed": 1, "specialite": 1},
               "medecin_pro")

print("\n— consultation (horizontal dérivé patient/région) —")
pats_nord   = [p["idPat"] for p in db_f["patient_nord"].find({},   {"idPat": 1})]
pats_centre = [p["idPat"] for p in db_f["patient_centre"].find({}, {"idPat": 1})]
pats_sud    = [p["idPat"] for p in db_f["patient_sud"].find({},    {"idPat": 1})]
creer_fragment("consultation", {"idPat": {"$in": pats_nord}},   {"_id": 0}, "consultation_nord")
creer_fragment("consultation", {"idPat": {"$in": pats_centre}}, {"_id": 0}, "consultation_centre")
creer_fragment("consultation", {"idPat": {"$in": pats_sud}},    {"_id": 0}, "consultation_sud")


# ══════════════════════════════════════════════════════════════════════════════
# QUESTION 3 – Reconstruction
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("QUESTION 3 — Reconstruction")
print("=" * 60)

# ── 3a. Reconstruction de patient ───────────────────────────────────────────
print("\n3a. Reconstruction de patient")
patient_reconstruit = (
    list(db_f["patient_nord"].find({}, {"_id": 0})) +
    list(db_f["patient_centre"].find({}, {"_id": 0})) +
    list(db_f["patient_sud"].find({}, {"_id": 0}))
)
nb_global   = db["patient"].count_documents({})
nb_reconst  = len(patient_reconstruit)
print(f"  Global   : {nb_global} docs")
print(f"  Reconstruit : {nb_reconst} docs")
print(f"  ✅ Cohérent" if nb_global == nb_reconst else f"  ❌ Incohérent !")

# ── 3b. Reconstruction de medecin (jointure vertical) ───────────────────────
print("\n3b. Reconstruction de medecin")
civil = {d["idMed"]: d for d in db_f["medecin_civil"].find({}, {"_id": 0})}
pro   = {d["idMed"]: d for d in db_f["medecin_pro"].find({},   {"_id": 0})}
medecin_reconstruit = []
for idMed, c in civil.items():
    if idMed in pro:
        doc = {**c, **pro[idMed]}
        medecin_reconstruit.append(doc)
nb_global  = db["medecin"].count_documents({})
nb_reconst = len(medecin_reconstruit)
print(f"  Global   : {nb_global} docs")
print(f"  Reconstruit : {nb_reconst} docs")
print(f"  ✅ Cohérent" if nb_global == nb_reconst else f"  ❌ Incohérent !")

# ── 3c. Reconstruction de infirmier ─────────────────────────────────────────
print("\n3c. Reconstruction de infirmier")

# Nord et Sud : fragments complets
inf_nord = list(db_f["infirmier_nord"].find({}, {"_id": 0}))
inf_sud  = list(db_f["infirmier_sud"].find({},  {"_id": 0}))

# Centre : jointure des deux fragments verticaux
civil_c = {d["idInf"]: d for d in db_f["infirmier_centre_civil"].find({}, {"_id": 0})}
pro_c   = {d["idInf"]: d for d in db_f["infirmier_centre_pro"].find({},   {"_id": 0})}
inf_centre = []
for idInf, c in civil_c.items():
    if idInf in pro_c:
        inf_centre.append({**c, **pro_c[idInf]})

infirmier_reconstruit = inf_nord + inf_centre + inf_sud
nb_global  = db["infirmier"].count_documents({})
nb_reconst = len(infirmier_reconstruit)
print(f"  Global   : {nb_global} docs")
print(f"  Reconstruit : {nb_reconst} docs")
print(f"  ✅ Cohérent" if nb_global == nb_reconst else f"  ❌ Incohérent !")


# ══════════════════════════════════════════════════════════════════════════════
# QUESTION 4 – Requête distribuée
# Même résultat que Q1, mais uniquement sur les fragments.
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("QUESTION 4 — Requête distribuée (sans collections globales)")
print("=" * 60)

# Étape 1 : Récupérer les idPat des patients hospitalisés en région Nord
#           → fragment patient_nord suffit (idHop des hops_nord)
pats_nord_ids = [p["idPat"] for p in db_f["patient_nord"].find({}, {"idPat": 1, "_id": 0})]

# Étape 2 : Filtrer les consultations d'urgence concernant ces patients
#           → fragment consultation_nord
consults_urgence = list(db_f["consultation_nord"].find(
    {"motif": "urgence", "idPat": {"$in": pats_nord_ids}},
    {"idMed": 1, "_id": 0}
))
ids_med = list({c["idMed"] for c in consults_urgence})

# Étape 3 : Récupérer nom + spécialité des médecins concernés
#           → jointure medecin_civil + medecin_pro (fragments verticaux)
civil_map = {d["idMed"]: d["nom"]       for d in db_f["medecin_civil"].find({"idMed": {"$in": ids_med}}, {"_id": 0})}
pro_map   = {d["idMed"]: d["specialite"] for d in db_f["medecin_pro"].find({"idMed":  {"$in": ids_med}}, {"_id": 0})}

resultats_q4 = sorted(
    [{"nom": civil_map[m], "specialite": pro_map[m]} for m in ids_med if m in civil_map and m in pro_map],
    key=lambda x: x["nom"]
)

print(f"Résultat distribué ({len(resultats_q4)} médecin(s)) :")
for r in resultats_q4:
    print(f"  {r['nom']} — {r['specialite']}")

# Vérification
noms_q1 = sorted([r["nom"] for r in resultats_q1])
noms_q4 = sorted([r["nom"] for r in resultats_q4])
print(f"\n  Q1 : {noms_q1}")
print(f"  Q4 : {noms_q4}")
print(f"  {'✅ Résultats identiques' if noms_q1 == noms_q4 else '❌ Résultats différents !'}")

client.close()