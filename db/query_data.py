import sqlite3

def query_data():
    connection = sqlite3.connect('tickcapturejobs.db')
    cursor = connection.cursor()

    # Query data from jobs table
    cursor.execute('SELECT * FROM jobs')
    jobs = cursor.fetchall()
    print("Jobs Table:")
    for job in jobs:
        print(job)

    # Query data from instruments table
    cursor.execute('SELECT * FROM instruments')
    instruments = cursor.fetchall()
    print("\nInstruments Table:")
    for instrument in instruments:
        print(instrument)

    # Query data from fields table
    cursor.execute('SELECT * FROM fields')
    fields = cursor.fetchall()
    print("\nFields Table:")
    for field in fields:
        print(field)

    connection.close()

def main():
    query_data()

if __name__ == '__main__':
    main()