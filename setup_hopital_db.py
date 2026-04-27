from pymongo import MongoClient
from datetime import datetime

client = MongoClient("mongodb://localhost:27017")
db = client["hopital_global"]

for col in ["hopital", "service", "medecin", "patient", "infirmier", "consultation"]:
    db[col].drop()
print("Collections nettoyées.\n")

hopitaux = [
    {"idHop": "H01", "nom": "CHU Mustapha",        "ville": "Alger",       "region": "Nord",   "directeur": "M001"},
    {"idHop": "H02", "nom": "Hôpital Bab El Oued", "ville": "Alger",       "region": "Nord",   "directeur": "M002"},
    {"idHop": "H03", "nom": "CHU Benbadis",         "ville": "Constantine", "region": "Nord",   "directeur": "M003"},
    {"idHop": "H04", "nom": "CHU Frantz Fanon",     "ville": "Blida",       "region": "Centre", "directeur": "M004"},
    {"idHop": "H05", "nom": "Hôpital Kouba",        "ville": "Alger",       "region": "Centre", "directeur": "M005"},
    {"idHop": "H06", "nom": "EPH Médéa",            "ville": "Médéa",       "region": "Centre", "directeur": "M006"},
    {"idHop": "H07", "nom": "CHU Oran",             "ville": "Oran",        "region": "Sud",    "directeur": "M007"},
    {"idHop": "H08", "nom": "EPH Béchar",           "ville": "Béchar",      "region": "Sud",    "directeur": "M008"},
    {"idHop": "H09", "nom": "CHU Annaba",           "ville": "Annaba",      "region": "Nord",   "directeur": "M003"},
]
db["hopital"].insert_many(hopitaux)
print(f"Hopital      : {db['hopital'].count_documents({})} docs")

services = [
    {"idHop": "H01", "idService": "S01", "nomService": "Urgences",        "etage": 0, "capacite": 30},
    {"idHop": "H01", "idService": "S02", "nomService": "Cardiologie",     "etage": 2, "capacite": 20},
    {"idHop": "H01", "idService": "S03", "nomService": "Pédiatrie",       "etage": 3, "capacite": 25},
    {"idHop": "H02", "idService": "S01", "nomService": "Urgences",        "etage": 0, "capacite": 20},
    {"idHop": "H02", "idService": "S02", "nomService": "Neurologie",      "etage": 1, "capacite": 15},
    {"idHop": "H03", "idService": "S01", "nomService": "Urgences",        "etage": 0, "capacite": 25},
    {"idHop": "H03", "idService": "S02", "nomService": "Oncologie",       "etage": 4, "capacite": 18},
    {"idHop": "H04", "idService": "S01", "nomService": "Urgences",        "etage": 0, "capacite": 22},
    {"idHop": "H04", "idService": "S02", "nomService": "Chirurgie",       "etage": 2, "capacite": 16},
    {"idHop": "H05", "idService": "S01", "nomService": "Médecine interne","etage": 1, "capacite": 20},
    {"idHop": "H06", "idService": "S01", "nomService": "Urgences",        "etage": 0, "capacite": 12},
    {"idHop": "H07", "idService": "S01", "nomService": "Urgences",        "etage": 0, "capacite": 28},
    {"idHop": "H07", "idService": "S02", "nomService": "Cardiologie",     "etage": 3, "capacite": 20},
    {"idHop": "H08", "idService": "S01", "nomService": "Urgences",        "etage": 0, "capacite": 10},
    {"idHop": "H09", "idService": "S01", "nomService": "Traumatologie",   "etage": 1, "capacite": 20},
]
db["service"].insert_many(services)
print(f"Service      : {db['service'].count_documents({})} docs")

medecins = [
    {"idMed": "M001", "nom": "Dr. Benali Ahmed",   "adr": "12 rue des Roses, Alger",         "tel": "0550112233", "specialite": "Cardiologie"},
    {"idMed": "M002", "nom": "Dr. Khelil Fatima",  "adr": "5 cité des Pins, Alger",          "tel": "0661223344", "specialite": "Urgences"},
    {"idMed": "M003", "nom": "Dr. Meziane Karim",  "adr": "8 bd de la Liberté, Constantine", "tel": "0770334455", "specialite": "Oncologie"},
    {"idMed": "M004", "nom": "Dr. Hadj Nadia",     "adr": "22 rue Didouche, Blida",          "tel": "0550445566", "specialite": "Chirurgie"},
    {"idMed": "M005", "nom": "Dr. Oukil Samir",    "adr": "3 allée des Orangers, Alger",     "tel": "0661556677", "specialite": "Neurologie"},
    {"idMed": "M006", "nom": "Dr. Bouras Leila",   "adr": "17 rue Ben M'hidi, Médéa",        "tel": "0770667788", "specialite": "Pédiatrie"},
    {"idMed": "M007", "nom": "Dr. Ferhat Mourad",  "adr": "9 bd des Génies, Oran",           "tel": "0550778899", "specialite": "Cardiologie"},
    {"idMed": "M008", "nom": "Dr. Rahmani Sonia",  "adr": "4 rue de la Paix, Béchar",        "tel": "0661889900", "specialite": "Médecine générale"},
    {"idMed": "M009", "nom": "Dr. Amrani Yacine",  "adr": "6 cité Garidi, Alger",            "tel": "0770990011", "specialite": "Urgences"},
    {"idMed": "M010", "nom": "Dr. Tlemcani Rima",  "adr": "11 rue Pasteur, Annaba",          "tel": "0550001122", "specialite": "Traumatologie"},
]
db["medecin"].insert_many(medecins)
print(f"Medecin      : {db['medecin'].count_documents({})} docs")

patients = [
    {"idPat": "P001", "idHop": "H01", "nom": "Benaissa Moussa", "adr": "14 rue Didouche, Alger",         "tel": "0550111222", "dateNaissance": datetime(1985, 3, 12)},
    {"idPat": "P002", "idHop": "H01", "nom": "Chergui Amina",   "adr": "6 bd Zighoud, Alger",            "tel": "0661222333", "dateNaissance": datetime(1992, 7, 5)},
    {"idPat": "P003", "idHop": "H02", "nom": "Derbal Hichem",   "adr": "3 cité Climat, Alger",           "tel": "0770333444", "dateNaissance": datetime(1978, 11, 20)},
    {"idPat": "P004", "idHop": "H03", "nom": "Ferdjani Warda",  "adr": "18 rue Salah Bey, Constantine",  "tel": "0550444555", "dateNaissance": datetime(2001, 1, 30)},
    {"idPat": "P005", "idHop": "H03", "nom": "Guettaf Tarek",   "adr": "9 rue des Martyrs, Constantine", "tel": "0661555666", "dateNaissance": datetime(1965, 9, 14)},
    {"idPat": "P006", "idHop": "H04", "nom": "Hamdi Rania",     "adr": "5 cité Soummam, Blida",          "tel": "0770666777", "dateNaissance": datetime(1990, 4, 22)},
    {"idPat": "P007", "idHop": "H05", "nom": "Issad Anis",      "adr": "7 rue Ibn Khaldoun, Alger",      "tel": "0550777888", "dateNaissance": datetime(1999, 12, 3)},
    {"idPat": "P008", "idHop": "H07", "nom": "Kaddour Nora",    "adr": "12 bd de l'ALN, Oran",           "tel": "0661888999", "dateNaissance": datetime(1975, 6, 18)},
    {"idPat": "P009", "idHop": "H07", "nom": "Lalaoui Sofiane", "adr": "2 rue Larbi Ben M'hidi, Oran",   "tel": "0770999000", "dateNaissance": datetime(1988, 8, 9)},
    {"idPat": "P010", "idHop": "H08", "nom": "Mekki Dalila",    "adr": "33 rue de la Gare, Béchar",      "tel": "0550000111", "dateNaissance": datetime(2003, 2, 28)},
    {"idPat": "P011", "idHop": "H09", "nom": "Nacer Bilal",     "adr": "10 cité El Bouni, Annaba",       "tel": "0661101202", "dateNaissance": datetime(1982, 5, 7)},
    {"idPat": "P012", "idHop": "H01", "nom": "Ouali Meriem",    "adr": "1 rue Hassiba, Alger",           "tel": "0770202303", "dateNaissance": datetime(1995, 10, 15)},
]
db["patient"].insert_many(patients)
print(f"Patient      : {db['patient'].count_documents({})} docs")

infirmiers = [
    {"idInf": "I001", "idHop": "H01", "idService": "S01", "nom": "Bouzid Sara",     "adr": "4 rue Abane, Alger",           "tel": "0550121212", "salaire": 55000, "anciennete": 5},
    {"idInf": "I002", "idHop": "H01", "idService": "S02", "nom": "Chalabi Omar",    "adr": "8 cité Diar, Alger",           "tel": "0661232323", "salaire": 60000, "anciennete": 8},
    {"idInf": "I003", "idHop": "H02", "idService": "S01", "nom": "Dif Lynda",       "adr": "2 rue Hassane, Alger",         "tel": "0770343434", "salaire": 52000, "anciennete": 3},
    {"idInf": "I004", "idHop": "H03", "idService": "S01", "nom": "Elouahed Ryad",   "adr": "5 cité Enmouchah, Constantine","tel": "0550454545", "salaire": 54000, "anciennete": 6},
    {"idInf": "I005", "idHop": "H04", "idService": "S01", "nom": "Faiza Rouabah",   "adr": "11 rue Trik, Blida",           "tel": "0661565656", "salaire": 58000, "anciennete": 7},
    {"idInf": "I006", "idHop": "H05", "idService": "S01", "nom": "Ghedjghoudj Ali", "adr": "9 bd Mokhtar, Alger",          "tel": "0770676767", "salaire": 57000, "anciennete": 4},
    {"idInf": "I007", "idHop": "H07", "idService": "S01", "nom": "Haddad Imane",    "adr": "6 rue Ahmed Zabana, Oran",     "tel": "0550787878", "salaire": 53000, "anciennete": 2},
    {"idInf": "I008", "idHop": "H07", "idService": "S02", "nom": "Idrissi Mehdi",   "adr": "14 cité USTO, Oran",           "tel": "0661898989", "salaire": 61000, "anciennete": 9},
    {"idInf": "I009", "idHop": "H08", "idService": "S01", "nom": "Jilali Hakima",   "adr": "7 rue de la Victoire, Béchar", "tel": "0770909090", "salaire": 50000, "anciennete": 1},
    {"idInf": "I010", "idHop": "H09", "idService": "S01", "nom": "Kaci Samia",      "adr": "3 bd Zighoud, Annaba",         "tel": "0550010101", "salaire": 56000, "anciennete": 5},
]
db["infirmier"].insert_many(infirmiers)
print(f"Infirmier    : {db['infirmier'].count_documents({})} docs")

consultations = [
    {"idMed": "M002", "idPat": "P001", "dateConsult": datetime(2025, 1, 10), "idService": "S01", "motif": "urgence",   "diagnostic": "Fracture du poignet"},
    {"idMed": "M009", "idPat": "P002", "dateConsult": datetime(2025, 1, 15), "idService": "S01", "motif": "urgence",   "diagnostic": "Crise d'asthme sévère"},
    {"idMed": "M002", "idPat": "P003", "dateConsult": datetime(2025, 2,  5), "idService": "S01", "motif": "urgence",   "diagnostic": "Appendicite aiguë"},
    {"idMed": "M003", "idPat": "P004", "dateConsult": datetime(2025, 2, 20), "idService": "S01", "motif": "urgence",   "diagnostic": "Traumatisme crânien"},
    {"idMed": "M001", "idPat": "P001", "dateConsult": datetime(2025, 3,  1), "idService": "S02", "motif": "controle",  "diagnostic": "Arythmie stable"},
    {"idMed": "M005", "idPat": "P003", "dateConsult": datetime(2025, 3, 10), "idService": "S02", "motif": "suivi",     "diagnostic": "Migraine chronique"},
    {"idMed": "M010", "idPat": "P011", "dateConsult": datetime(2025, 3, 12), "idService": "S01", "motif": "urgence",   "diagnostic": "Entorse sévère"},
    {"idMed": "M004", "idPat": "P006", "dateConsult": datetime(2025, 1, 22), "idService": "S02", "motif": "operation", "diagnostic": "Appendicectomie"},
    {"idMed": "M006", "idPat": "P007", "dateConsult": datetime(2025, 2, 14), "idService": "S01", "motif": "suivi",     "diagnostic": "Bronchite récidivante"},
    {"idMed": "M007", "idPat": "P008", "dateConsult": datetime(2025, 1, 30), "idService": "S01", "motif": "urgence",   "diagnostic": "Infarctus du myocarde"},
    {"idMed": "M008", "idPat": "P010", "dateConsult": datetime(2025, 2,  8), "idService": "S01", "motif": "urgence",   "diagnostic": "Déshydratation sévère"},
    {"idMed": "M001", "idPat": "P009", "dateConsult": datetime(2025, 3,  5), "idService": "S02", "motif": "controle",  "diagnostic": "Hypertension artérielle"},
    {"idMed": "M009", "idPat": "P006", "dateConsult": datetime(2025, 3, 18), "idService": "S01", "motif": "urgence",   "diagnostic": "Choc anaphylactique"},
    {"idMed": "M005", "idPat": "P012", "dateConsult": datetime(2025, 4,  2), "idService": "S03", "motif": "suivi",     "diagnostic": "Épilepsie contrôlée"},
]
db["consultation"].insert_many(consultations)
print(f"Consultation : {db['consultation'].count_documents({})} docs")

print("\n✅ Base hopital_global prête.")
client.close()
