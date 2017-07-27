import sqlite3
from datetime import datetime

from Db_worker import Tag_initializer
from Db_worker import DB_worker

connection = sqlite3.connect("../measured_data.db")
#Tag_initializer.init(connection=connection)
for i in range(0, 100):
    DB_worker.new_sample(connection=connection, tag_id=1, batch_id=2, time_stamp=datetime.now(), value=i)
    DB_worker.new_sample(connection=connection, tag_id=2, batch_id=2, time_stamp=datetime.now(), value=i-200)
