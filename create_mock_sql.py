import duckdb

def setup_diagnosis_lookup():
    # Detta skapar en helt ny databas-fil som agerar källsystem
    conn = duckdb.connect("external_source.db")
    
    # Skapa tabellen med svenska diagnos-förklaringar
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sql_diagnosis_lookup AS 
        SELECT * FROM (VALUES 
            ('A10', 'Typ 2-diabetes', 'Kronisk sjukdom som påverkar kroppens förmåga att reglera blodsocker.'),
            ('B20', 'Hjärtsvikt', 'Tillstånd där hjärtat inte kan pumpa tillräckligt med blod till kroppen.'),
            ('C30', 'Lunginflammation', 'Infektion i lungorna orsakad av bakterier eller virus.'),
            ('D40', 'Högt blodtryck', 'Ett tillstånd där blodets tryck mot kärlvärggarna är för högt.'),
            ('E50', 'Astma', 'Inflammation i luftvägarna som gör det svårt att andas.'),
            ('F60', 'Ryggskott', 'Plötslig och kraftig smärta i ländryggen.'),
            ('G70', 'Migrän', 'Återkommande attacker av intensiv huvudvärk.'),
            ('H80', 'Gikt', 'Ledinflammation orsakad av urinsyrakristaller.'),
            ('I90', 'Bältros', 'En smärtsam virusinfektion orsakad av samma virus som vattkoppor.'),
            ('J00', 'Förkylning', 'En viral infektion i övre luftvägarna.')
        ) t(diag_code, diagnos_namn, beskrivning)
    """)
    
    conn.close()
    print("✅ Filen 'external_source.db' har skapats med diagnos-data!")

if __name__ == "__main__":
    setup_diagnosis_lookup()