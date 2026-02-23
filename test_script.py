import duckdb
con = duckdb.connect('healthcare.db')
# dbt skapar tabeller/vyer i "main"-schemat om inget annat sagts
df = con.execute("SELECT * FROM my_first_model").pl() 
print(df)