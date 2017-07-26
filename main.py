import sqlite3

import Tag_initializer

connection = sqlite3.connect("../measured_data.db")
Tag_initializer.init(connection=connection)
