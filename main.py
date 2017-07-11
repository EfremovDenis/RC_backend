import DB_worker
import sqlite3

connection = sqlite3.connect("../measured_data.db")
DB_worker.db_init(connection=connection, empty_db=True)