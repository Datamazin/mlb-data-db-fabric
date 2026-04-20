from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env", override=True)
from src.connections import get_warehouse_conn

# Connection 1: create view
conn1 = get_warehouse_conn()
conn1.autocommit = True
cur1 = conn1.cursor()
cur1.execute("SELECT DB_NAME() AS db, @@SERVERNAME AS srv")
row = cur1.fetchone()
print(f"DB: {row[0]}, Server: {row[1]}")
cur1.execute("DROP VIEW IF EXISTS gold.test_view;")
cur1.execute("CREATE VIEW gold.test_view AS SELECT 1 AS n;")
print("View created on conn1")
conn1.close()
print("conn1 closed")

# Connection 2: check if view persists
conn2 = get_warehouse_conn()
conn2.autocommit = True
cur2 = conn2.cursor()
cur2.execute("SELECT DB_NAME() AS db")
print(f"conn2 DB: {cur2.fetchone()[0]}")
cur2.execute("SELECT name FROM sys.objects WHERE schema_id = SCHEMA_ID('gold') AND type = 'V'")
views = cur2.fetchall()
print("Views in fresh connection:", [r[0] for r in views])
cur2.execute("DROP VIEW IF EXISTS gold.test_view;")
conn2.close()
