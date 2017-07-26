import DB_worker
import sqlite3
from datetime import datetime

connection = sqlite3.connect("../measured_data.db")
print DB_worker.select_tag(connection=connection, tag_id=4)
