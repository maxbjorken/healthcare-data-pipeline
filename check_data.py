import duckdb

con = duckdb.connect('healthcare.db')
query = """
SELECT 
    visit_id, 
    TYPEOF(visit_id) as type,
    COUNT(*) as antal
FROM Stage_Visits 
WHERE visit_id IS NULL 
   OR visit_id = 'NULL' 
   OR visit_id = ''
GROUP BY 1, 2;
"""
print(con.execute(query).df())