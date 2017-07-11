import DB_worker
import sqlite3

connection = sqlite3.connect("../measured_data.db")
DB_worker.db_init(connection=connection, empty_db=True)
DB_worker.new_tag(connection=connection, name="My new sensor", from_bit=2, bit_len=5, sensor_id=1)
