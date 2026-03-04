import pandas as pd
import numpy as np
import os

os.makedirs('data', exist_ok=True)

# 1. D_Patients (100 rader)
patients = pd.DataFrame({
    'patient_id': [f'P{i}' for i in range(1, 101)],
    'name': [f'Patient {i}' for i in range(1, 101)],
    'age': np.random.randint(0, 95, 100),
    'city': np.random.choice(['Stockholm', 'Göteborg', 'Malmö', 'Uppsala'], 100)
})
patients.loc[0:5, 'city'] = None # Skapa lite smuts (NULLs)
patients.to_csv('data/raw_patients.csv', index=False)

# 2. D_Doctors (20 rader - räcker för lookup)
doctors = pd.DataFrame({
    'doctor_id': [f'D{i}' for i in range(1, 21)],
    'doctor_name': [f'Dr. {name}' for name in ['Hansson', 'Sjöberg', 'Ek', 'Lind', 'Hurtig']*4],
    'specialty': np.random.choice(['Kirurgi', 'Kardiologi', 'Allmän', 'Pediatrik'], 20)
}).to_csv('data/raw_doctors.csv', index=False)

# 3. D_Clinics
clinics = pd.DataFrame({
    'clinic_id': ['C1', 'C2', 'C3', 'C4'],
    'clinic_name': ['Centralen', 'Nordsjukhuset', 'Sydkliniken', 'Västakuten'],
    'region': ['Region Stockholm', 'Region Norr', 'Region Syd', 'Region Väst']
}).to_csv('data/raw_clinics.csv', index=False)

# 4. D_Diagnosis (ICD-10 förenklat)
diagnosis = pd.DataFrame({
    'diag_code': ['J00', 'I10', 'E11', 'M54', 'Z00'],
    'description': ['Förkylning', 'Högt blodtryck', 'Diabetes', 'Ryggvärk', 'Allmän kontroll']
}).to_csv('data/raw_diagnosis.csv', index=False)

# 5. D_Date (Enkel tidstabell)
dates = pd.date_range(start='2025-01-01', periods=200)
date_dim = pd.DataFrame({
    'date_key': dates.strftime('%Y%m%d'),
    'full_date': dates,
    'is_weekend': dates.weekday >= 5
}).to_csv('data/raw_date.csv', index=False)

# 6. F_Visits (500 rader - Faktatabellen)
visits = pd.DataFrame({
    'visit_id': [f'V{i}' for i in range(1, 501)],
    'patient_id': [f'P{np.random.randint(1, 101)}' for _ in range(500)],
    'doctor_id': [f'D{np.random.randint(1, 21)}' for _ in range(500)],
    'clinic_id': [f'C{np.random.randint(1, 5)}' for _ in range(500)],
    'diag_code': np.random.choice(['J00', 'I10', 'E11', 'M54', 'Z00'], 500),
    'visit_date_key': np.random.choice(dates.strftime('%Y%m%d'), 500),
    'amount': np.random.uniform(200, 5000, 500).round(2)
})
visits.loc[10:20, 'amount'] = -99 # Skapa smuts (felaktiga priser)
visits.to_csv('data/raw_visits.csv', index=False)

print("Check! 6 filer skapade i /data. Nu kan du börja bygga din pipeline.")