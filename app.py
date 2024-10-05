import sqlite3


def create_table():
    connection = sqlite3.connect('data.db')
    cursor = connection.cursor()
    cursor.execute('CREATE TABLE users (id int, username text, password text)')
    connection.commit()
    connection.close()


def main():
    create_table();

if __name__ == '__main__':
    main()