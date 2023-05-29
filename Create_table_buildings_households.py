import psycopg2

def create_and_populate_buildings():
    connection = psycopg2.connect(host="localhost", database="mydb", user="myuser", password="mypassword")
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS buildings (id SERIAL PRIMARY KEY, building_number INTEGER)")

    buildings_data = [(1901 + i,) for i in range(11)]

    cursor.executemany("INSERT INTO buildings (building_number) VALUES (%s)", buildings_data)
    connection.commit()
    cursor.close()
    connection.close()

def create_and_populate_households():
    connection = psycopg2.connect(host="localhost", database="mydb", user="myuser", password="mypassword")
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE IF NOT EXISTS households (id SERIAL PRIMARY KEY, building_id INTEGER REFERENCES buildings(id), floor INTEGER, unit INTEGER)")

    households_data = []
    for building_id in range(1, 12):
        if building_id != 11:
            units = 4
            floors = 20
        else:
            units = 2
            floors = 20

        for floor in range(1, floors + 1):
            for unit in range(1, units + 1):
                households_data.append((building_id, floor, unit))

    cursor.executemany("INSERT INTO households (building_id, floor, unit) VALUES (%s, %s, %s)", households_data)
    connection.commit()
    cursor.close()
    connection.close()

def create_table_energy_usage():
    connection = psycopg2.connect(host="localhost", database="mydb", user="myuser", password="mypassword")
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS energy_usage (
            id SERIAL PRIMARY KEY,
            household_id INTEGER,
            energy_type VARCHAR(10),
            usage_date DATE,
            amount DOUBLE PRECISION,
            emission DOUBLE PRECISION
    )
    """
    )
    connection.commit()
    cursor.close()

def create_table_energy_usage_2():
    connection = psycopg2.connect(host="localhost", database="mydb", user="myuser", password="mypassword")
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS energy_usage_2 (
            id SERIAL PRIMARY KEY,
            household_id INTEGER,
            energy_type VARCHAR(10),
            usage_date DATE,
            amount DOUBLE PRECISION,
            emission DOUBLE PRECISION
    )
    """
    )
    connection.commit()
    cursor.close()

create_and_populate_buildings()
create_and_populate_households()
create_table_energy_usage()
create_table_energy_usage_2() # 테스트 테이블
