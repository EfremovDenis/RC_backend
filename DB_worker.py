import datetime
# TODO: GLOBAL TRY CATCH IN ALL FUNCTIONS
# TODO: WORK WITH STRUCTURED RETURN IN SELECT FUNCTIONS ???


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

    print('Table Tag was successfully created')

    create_table_batch = """
        CREATE TABLE IF NOT EXISTS batch (
        id INTEGER PRIMARY KEY,
        start_date DATETIME NOT NULL,
        stop_date DATETIME,
        description VARCHAR(150));"""
    cursor.execute(create_table_batch)

    print('Table Batch was successfully created')

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

    print('Table Sample was successfully created')

    print('Database was successfully created')

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
    Creates a new tag in the database. In case of wrong input raises ValueError
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


def new_batch(connection, start_date, description):
    """
    Creates a new batch in the database.
    Args:
        connection: connection object to the database
        start_date: start time of a new batch (datetime, not null)
        description: optional description of the batch (string, max_len=150)
    Returns:
        id of the created batch.
    """
    if start_date is None:
        raise ValueError('Start_date can not be null!')

    if type(start_date) is datetime:
        raise ValueError('Start_date must have datetime type!')

    if len(description) > 150:
        raise ValueError('Description\'s max length is 150!')

    cursor = connection.cursor()

    format_str = """
        INSERT INTO batch (id, start_date, description)
        VALUES (NULL, "{start_date}", "{description}");"""

    insert_batch = format_str.format(start_date=start_date, description=description)

    cursor.execute(insert_batch)

    connection.commit()

    # select id of the created batch

    format_str = """
            SELECT id FROM batch
            WHERE start_date = "{start_date}";"""

    select_created_batch = format_str.format(start_date=start_date)

    cursor.execute(select_created_batch)

    id_created_batch = cursor.fetchall()

    print('New batch with ID [{0}] was successfully created'.format(id_created_batch[0][0]))

    return id_created_batch[0][0]


# TODO: add checks
def close_batch(connection, batch_id, stop_date):
    """
    Closes the existing batch, if it's not closed yet.
    Args:
        connection: connection object to the database
        batch_id: the existing batch's id (not null)
        stop_date: stop time of the batch (not null)
    """
    # TODO: some checks

    cursor = connection.cursor()
    format_str = """
        UPDATE batch
        SET stop_date = "{stop_date}"
        WHERE id = "{id}";"""

    update_batch = format_str.format(stop_date=stop_date, id=batch_id)

    cursor.execute(update_batch)

    connection.commit()

    print('The batch with ID [{0}] was successfully stopped'.format(batch_id))

    return


# TODO: some checks
def new_sample(connection, tag_id, batch_id, time_stamp, value):
    """
    Creates a new sample in the database.
    Args:
        connection: connection object to the database
        tag_id: the existing tag's id (int, not null)
        batch_id: the existing batch's id (int, not null)
        time_stamp: measuring time (not null)
        value: measured value (real, not null)
    """

    cursor = connection.cursor()

    format_str = """
        INSERT INTO sample (id, tag_id, batch_id, time_stamp, value)
        VALUES (NULL, "{tag_id}", "{batch_id}", "{time_stamp}", "{value}");"""

    insert_sample = format_str.format(tag_id=tag_id, batch_id=batch_id, time_stamp=time_stamp, value=value)

    cursor.execute(insert_sample)

    connection.commit()

    print('The sample under Tag [{0}] and Batch [{1}] was successfully created'.format(tag_id, batch_id))

    return


# TODO: some checks
def delete_tag(connection, tag_id):
    """
    Deletes the existing tag and all related measured samples.
    Args:
        connection: connection object to the database
        tag_id: the existing tag's id (int, not null)
    """
    delete_all_samples_under_tag(connection, tag_id)

    cursor = connection.cursor()

    format_str = """
        DELETE FROM tag
        WHERE id = "{tag_id}";"""

    delete_tag_where = format_str.format(tag_id=tag_id)

    cursor.execute(delete_tag_where)

    print ('The tag with ID [{0}] was successfully deleted'.format(tag_id))
    return


# TODO: some checks
def delete_batch(connection, batch_id):
    """
    Deletes the existing batch and all related measured samples.
    Args:
        connection: connection object to the database
        batch_id: the existing batch's id (int, not null)
    """

    # TODO: some checks

    delete_all_samples_under_batch(connection=connection, batch_id=batch_id)

    cursor = connection.cursor()
    format_str = """
        DELETE FROM batch
        WHERE id = "{id}";"""

    delete_batch_where_id = format_str.format(id=batch_id)

    cursor.execute(delete_batch_where_id)

    connection.commit()

    print('The batch with ID [{0}] was successfully deleted'.format(batch_id))

    return


# TODO: some checks
def delete_all_samples_under_batch(connection, batch_id):
    """
    Deletes all samples under one batch.
    Args:
        connection: connection object to the database
        batch_id: the database id of the selected batch
    """

    # TODO: some checks

    cursor = connection.cursor()
    format_str = """
        DELETE FROM sample
        WHERE batch_id = "{batch_id}";"""

    delete_samples_where_batch_id = format_str.format(batch_id=batch_id)

    cursor.execute(delete_samples_where_batch_id)

    affected_rows = cursor.rowcount

    connection.commit()

    print('All samples under batch with ID [{0}] was successfully deleted. Number of affected rows: {1}'
          .format(batch_id, affected_rows))

    return


# TODO: some checks
def delete_all_samples_under_tag(connection, tag_id):
    """
    Deletes all samples under one tag.
    Args:
        connection: connection object to the database
        tag_id: the database id of the selected tag
    """

    # TODO: some checks

    cursor = connection.cursor()
    format_str = """
        DELETE FROM sample
        WHERE tag_id = "{tag_id}";"""

    delete_samples_where_tag_id = format_str.format(tag_id=tag_id)

    cursor.execute(delete_samples_where_tag_id)

    affected_rows = cursor.rowcount

    connection.commit()

    print('All samples under tag with ID [{0}] was successfully deleted. Number of affected rows: {1}'
          .format(tag_id, affected_rows))

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


# TODO: some checks
def select_samples_under_batch(connection, batch_id):
    """
    Selects all samples under one batch.
    Args:
        connection: connection object to the database
        batch_id: the database id of the selected batch
    Returns:
        all samples under the wanted batch id in format (id, tag_id, batch_id, time_stamp, value)
    """
    cursor = connection.cursor()

    format_str = """SELECT * FROM sample
        WHERE batch_id = "{batch_id}"
        ORDER BY tag_id, time_stamp;"""

    select_sample = format_str.format(batch_id=batch_id)

    all_sample_under_batch = []
    for row in cursor.execute(select_sample):
        all_sample_under_batch.append(row)

    connection.commit()

    return all_sample_under_batch


def select_all_batches(connection):
    """
    Selects all batches in the database.
    Args:
        connection: connection object to the database
    Returns:
        List of all batches, which are in format ((id, start_date, stop_date, description)
    """
    cursor = connection.cursor()

    format_str = """SELECT * FROM batch;"""

    all_batches = []
    for row in cursor.execute(format_str):
        all_batches.append(row)

    connection.commit()

    return all_batches


def select_all_tags(connection):
    """
    Selects all tags in the database.
    Args:
        connection: connection object to the database
    Returns:
        List of all tags, which are in format (id, name, from_bit, bit_len, sensor_id)
    """
    cursor = connection.cursor()

    format_str = """SELECT * FROM tag;"""

    all_tags = []
    for row in cursor.execute(format_str):
        all_tags.append(row)

    connection.commit()

    return all_tags


# TODO: some checks
def select_tag(connection, sensor_id):
    """
    Selects tag with the defined sensor id.
    Args:
        connection: connection object to the database
        sensor_id: wanted sensor_id
    Returns:
        id of the searched tag.
    """
    cursor = connection.cursor()

    format_str = """
        SELECT id FROM tag
        WHERE sensor_id = "{sensor_id}";"""

    select_tag_where = format_str.format(sensor_id=sensor_id)

    cursor.execute(select_tag_where)

    list_tag_id = cursor.fetchall()

    # check if list is empty
    if not list_tag_id:
        raise TypeError('There is no tag with sensor_id = {0}'.format(sensor_id))
    else:
        tag_id = list_tag_id[0][0]

    return tag_id
