import sqlite3
from datetime import datetime

from Db_worker import Tag_initializer
from Db_worker import DB_worker

connection = sqlite3.connect("../measured_data.db")
DB_worker.db_init(connection=connection, empty_db=True)
Tag_initializer.init(connection=connection)
DB_worker.new_batch(connection=connection, start_date=datetime.now(), description="Some desc.")
DB_worker.new_batch(connection=connection, start_date=datetime.now(), description="The second")
for i in range(0, 100):
    DB_worker.new_sample(connection=connection, tag_id=1, batch_id=2, time_stamp=i, value=i)
    DB_worker.new_sample(connection=connection, tag_id=2, batch_id=2, time_stamp=i, value=i-200)
for i in range(0, 10):
    DB_worker.new_sample(connection=connection, tag_id=1, batch_id=1, time_stamp=i, value=i)
    DB_worker.new_sample(connection=connection, tag_id=2, batch_id=1, time_stamp=i, value=i-200)

#print DB_worker.select_samples_under_batch(connection=connection, batch_id=2)
