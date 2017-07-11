def db_init(connection, empty_db=False):
    """
    Initializes the database. Creates all tables.
    Args:
        connection: connection object to the database
        empty_db: if True - drops all tables in the database
    """

    cursor = connection.cursor()

    if empty_db:
        drop_db(connection)

    create_table_tag = """
        CREATE TABLE IF NOT EXISTS tag (
        id INTEGER PRIMARY KEY,
        name VARCHAR(30) NOT NULL,
        from_bit INTEGER NOT NULL,
        bit_len INTEGER NOT NULL,
        sensor_id INTEGER NOT NULL UNIQUE);"""
    cursor.execute(create_table_tag)

    print('Table Tag successfully created')

    create_table_batch = """
        CREATE TABLE IF NOT EXISTS batch (
        id INTEGER PRIMARY KEY,
        start_date DATETIME NOT NULL,
        stop_date DATETIME,
        description VARCHAR(150));"""
    cursor.execute(create_table_batch)

    print('Table Batch successfully created')

    create_table_sample = """
        CREATE TABLE IF NOT EXISTS sample (
        id INTEGER PRIMARY KEY,
        tag_id INTEGER NOT NULL,
        batch_id INTEGER NOT NULL,
        time_stamp DATETIME NOT NULL,
        value REAL,
        FOREIGN KEY(tag_id) REFERENCES tag(id),
        FOREIGN KEY(batch_id) REFERENCES batch(id));"""
    cursor.execute(create_table_sample)

    print('Table Sample successfully created')

    connection.commit()
    return


def drop_db(connection):
    """
    Drops all tables in the database.
    Args:
        connection: connection object to the database
    """
    cursor = connection.cursor()

    cursor.execute("""DROP TABLE IF EXISTS sample;""")
    cursor.execute("""DROP TABLE IF EXISTS batch;""")
    cursor.execute("""DROP TABLE IF EXISTS tag;""")

    print('Database was successfully dropped')
    return


# TODO: check if bit segment does not matching on already existing bit segments
# TODO: check the uniqueness of id_protocol
def new_tag(connection, name, from_bit, bit_len, sensor_id):
    """
    Creates a new tag in the database. In case of bad input raises ValueError
    Args:
        connection: connection object to the database
        name: name of a new tag (not null, string, max_len=30)
        from_bit: non-existing order in connection protocol (via Bluetooth) (not null, int)
        bit_len: size of the value in bits (not null, int)
        sensor_id: the id of the sensor in the connection protocol
    """
    if name is None:
        raise ValueError('Name can not be null!')

    if len(name) > 30:
        raise ValueError('Max length of name is 30!')

    if from_bit is None:
        raise ValueError('From_bit can not be null!')

    if not isinstance(from_bit, int):
        raise ValueError('From_bit must be int!')

    if from_bit <= 0:
        raise ValueError('From_bit must be grater than 0!')

    # check if bit segment does not matching on already existing bit segments

    if bit_len is None:
        raise ValueError('Bit_len can not be null!')

    if not isinstance(bit_len, int):
        raise ValueError('Bit_len must be int!')

    if bit_len <= 0:
        raise ValueError('Bit_len must be grater than 0!')

    # check the uniqueness of sensor_id

    cursor = connection.cursor()

    format_str = """
    INSERT INTO tag (id, name, from_bit, bit_len, sensor_id)
    VALUES (NULL, "{name}", "{from_bit}", "{bit_len}", "{sensor_id}");"""

    insert_tag = format_str.format(name=name, from_bit=from_bit, bit_len=bit_len, sensor_id=sensor_id)

    cursor.execute(insert_tag)

    connection.commit()
    print('New tag [{0}] with id_protocol [1] was successfully created'.format(name, sensor_id))
    return


# TODO: implement
def new_batch(connection, start_date, description):
    """
    Creates a new batch in the database.
    Args:
        connection: connection object to the database
        start_date: start time of a new batch (not null)
        description: optional description of the batch (string, max_len=150)
    """
    return


# TODO: implement
def close_batch(connection, batch_id, stop_date):
    """
    Closes the existing batch, if it's not closed yet.
    Args:
        connection: connection object to the database
        batch_id: the existing batch's id (not null)
        stop_date: stop time of the batch (not null)
    """
    return


# TODO: implement
def new_sample(connection, tag_id, batch_id, timestamp, value):
    """
    Creates a new sample in the database.
    Args:
        connection: connection object to the database
        tag_id: the existing tag's id (int, not null)
        batch_id: the existing batch's id (int, not null)
        timestamp: measuring time (not null)
        value: measured value (real, not null)
    """
    return


# TODO: implement
def delete_tag(connection, tag_id):
    """
    Deletes the existing tag and all related measured samples.
    Args:
        connection: connection object to the database
        tag_id: the existing tag's id (int, not null)
    """
    return


# TODO: implement
def delete_batch(connection, batch_id):
    """
    Deletes the existing batch and all related measured samples.
    Args:
        connection: connection object to the database
        batch_id: the existing batch's id (int, not null)
    """
    return


def delete_all_batches(connection):
    """
    Deletes all existing batches and all measured samples.
    Args:
        connection: connection object to the database
    """
    cursor = connection.cursor()

    cursor.execute("""DROP TABLE IF EXISTS sample;""")
    cursor.execute("""DROP TABLE IF EXISTS batch;""")

    connection.commit()
    print('DateBase was successfully dropped')
    return
