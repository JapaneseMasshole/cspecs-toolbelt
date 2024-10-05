import sqlite3

def execute_schema():
    connection = sqlite3.connect('tickcapturejobs.db')
    cursor = connection.cursor()

    with open('schema.sql', 'r') as schema_file:
        schema_sql = schema_file.read()
        cursor.executescript(schema_sql)

    connection.commit()
    connection.close()

def main():
    execute_schema()

if __name__ == '__main__':
    main()