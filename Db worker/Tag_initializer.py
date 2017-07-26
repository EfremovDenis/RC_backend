# TODO: add all sensors as tags to init function

import DB_worker


def init(connection):
    """
    Initializes an empty database and add all measured tags.
    Args:
        connection: connection object to the database
    """
    DB_worker.db_init(connection=connection, empty_db=True)

    DB_worker.new_tag(connection=connection, name='Velocity', sensor_id=1, from_bit=1, bit_len=8)
    DB_worker.new_tag(connection=connection, name='Acceleration', sensor_id=2, from_bit=9, bit_len=8)

    return
