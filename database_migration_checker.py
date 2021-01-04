import docker
import psycopg2
import time
import csv

# Thrown when initialization fails.
class InitError(Exception):
    pass

# Thrown when PSQL connection fails.
class PSQLConnectionError(Exception):
    pass

# Returns db containers
def start_docker_containers(old_db_port_no, new_db_port_no):
    print("Initializing docker environment.")
    try:
        env_client = docker.from_env()
    except Exception:
        print("Docker initialization failed.")
        raise InitError

    # Start containers
    print("Creating container for old database.")
    try:
        old_db = env_client.containers.create(image="guaranteedrate/homework-pre-migration:1607545060-a7085621", publish_all_ports=True, ports={5432:old_db_port_no})
        old_db.start()
    except Exception:
        print("Old database setup failed.")
        old_db.stop()
        env_client.containers.prune()
        raise InitError
    print("Old database container running successfully on port " + str(old_db_port_no) + ".")

    print("Creating container for new database.")
    try:
        new_db = env_client.containers.create(image="guaranteedrate/homework-post-migration:1607545060-a7085621", publish_all_ports=True, ports={5432:new_db_port_no})
        new_db.start()
    except Exception:
        old_db.stop()
        new_db.stop()
        env_client.containers.prune()
        print("New database setup failed.")
        raise InitError
    print("New database container running successfully on port " + str(new_db_port_no) + ".\n")

    return env_client, old_db, new_db

# Returns connection and cursor
def connect_db(db, usr, pswd, prt_no):
    print("Connecting to", db, "database.")
    # Connect to PSQL server
    retry = 1
    connection = None
    while retry <= 30:
        try:
            print("Connecting to server | attempt ", retry)
            connection = psycopg2.connect(
                    host="localhost",
                    database=db,
                    user=usr,
                    password=pswd,
                    port=prt_no
                )

            print("Connection created successfully.")

            # create a cursor
            cursor = connection.cursor()
            break

        except psycopg2.DatabaseError as err:
            time.sleep(1)
            retry += 1
    
    if retry > 30:
        print("Failed to connect to psql server.")
        raise PSQLConnectionError

    print("Successfully connected to " + db + " database on port " + str(prt_no) + ".\n")
    return connection, cursor

# Returns list of tuples containing new db entries
def find_new_entries(old_db_cursor, new_db_cursor):
    # execute a statement
    print("Finding new entries.")
    old_db_cursor.execute("SELECT row_number() over(), * FROM accounts ORDER BY id ASC")
    new_db_cursor.execute("SELECT row_number() over(), * FROM accounts ORDER BY id ASC")
    
    old_db_id_list = old_db_cursor.fetchall()
    time.sleep(1)
    new_db_id_list = new_db_cursor.fetchall()
    time.sleep(1)
    new_entries_list = []
    offset = 0

    for i in range(len(new_db_id_list)):
        new_db_id = new_db_id_list[i]
        for j in range(offset, len(old_db_id_list)):
            old_db_id = old_db_id_list[j]
            if new_db_id[1] <= old_db_id[1]:
                if new_db_id[1] < old_db_id[1]:
                    new_entries_list.append(new_db_id)
                offset = j
                break
        else:
            new_entries_list.append(new_db_id)

    print(len(new_entries_list), "new employees found.\n")
    return new_entries_list

# Returns list of tuples containing missing db entries
def find_missing_entries(old_db_cursor, new_db_cursor):
    print("Finding missing entries.")
    # execute a statement
    old_db_cursor.execute("SELECT row_number() over(), * FROM accounts ORDER BY id ASC")
    new_db_cursor.execute("SELECT row_number() over(), * FROM accounts ORDER BY id ASC")
    
    old_db_id_list = old_db_cursor.fetchall()
    time.sleep(1)
    new_db_id_list = new_db_cursor.fetchall()
    time.sleep(1)
    missing_entries_list = []
    offset = 0

    for i in range(len(old_db_id_list)):
        old_db_id = old_db_id_list[i]
        for j in range(offset, len(new_db_id_list)):
            new_db_id = new_db_id_list[j]
            if old_db_id[1] <= new_db_id[1]:
                if old_db_id[1] < new_db_id[1]:
                    missing_entries_list.append(old_db_id)
                offset = j
                break
        else:
            missing_entries_list.append(old_db_id)

    print(len(missing_entries_list), "missing employees discovered.\n")
    return missing_entries_list

# Returns list of tuples containing corrupted db entries
def find_corrupted_entries(old_db_cursor, new_db_cursor):
    print("Finding corrupted entries.")
    # execute a statement
    old_db_cursor.execute("SELECT row_number() over(), * FROM accounts ORDER BY id ASC")
    new_db_cursor.execute("SELECT row_number() over(), * FROM accounts ORDER BY id ASC")
    
    old_db_id_list = old_db_cursor.fetchall()
    time.sleep(1)
    new_db_id_list = new_db_cursor.fetchall()
    time.sleep(1)
    corrupted_entries_list = []
    offset = 0

    for i in range(len(new_db_id_list)):
        new_db_id = new_db_id_list[i]
        for j in range(offset, len(old_db_id_list)):
            old_db_id = old_db_id_list[j]
            if new_db_id[1] <= old_db_id[1]:
                if new_db_id[1] == old_db_id[1] and (new_db_id[2] != old_db_id[2] or new_db_id[3] != old_db_id[3]):
                    corrupted_entries_list.append(old_db_id + new_db_id)
                offset = j
                break

    print(len(corrupted_entries_list), "Corrupted entries found.\n")
    return corrupted_entries_list

def write_report(new_entries, missing_entries, corrupted_entries):
    # Create new CSV report
        print("Writing report to database_migration_report.csv.")
        with open('database_migration_report.csv', "w+", newline="") as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(["Database Migration Report"])
            csv_writer.writerow([""])
            csv_writer.writerow(["New Entries"])
            csv_writer.writerow(["Row (New)", "ID", "Name", "Email", "Favorite Flavor"])
            csv_writer.writerows(new_entries)
            csv_writer.writerow([""])
            csv_writer.writerow(["Missing Entries"])
            csv_writer.writerow(["Row (Old)", "ID", "Name", "Email"])
            csv_writer.writerows(missing_entries)
            csv_writer.writerow([""])
            csv_writer.writerow(["Corrupted Entries"])
            csv_writer.writerow(["Row (Old)", "ID", "Name", "Email", "Row (New)", "ID", "Name", "Email", "Favorite Flavor"])
            csv_writer.writerows(corrupted_entries)
        print("Report completed.\n")

# Main function
def main():
    print("-----------------------------------------")
    print("-- Starting Database Migration Checker --")
    print("-----------------------------------------\n")
    try:
        old_db_port_no = 5432
        new_db_port_no = 5433
        env_client, old_db_container, new_db_container = start_docker_containers(old_db_port_no, new_db_port_no)

        # Connect to PSQL server
        old_db_connection, old_db_cursor = connect_db(db="old", usr="old", pswd="hehehe", prt_no=old_db_port_no)
        new_db_connection, new_db_cursor = connect_db(db="new", usr="new", pswd="hahaha", prt_no=new_db_port_no)

        # Find entries worth noting
        new_entries = find_new_entries(old_db_cursor, new_db_cursor)
        missing_entries = find_missing_entries(old_db_cursor, new_db_cursor)
        corrupted_entries = find_corrupted_entries(old_db_cursor, new_db_cursor)

        write_report(new_entries, missing_entries, corrupted_entries)

        old_db_connection.close()
        print("old_db connection closed.")
        new_db_connection.close()
        print("new_db connection closed.")

    except InitError:
        print("!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!FATAL ERROR OCCURRED!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!")
        print("Check if the ports " + str(old_db_port_no) + " and " + str(new_db_port_no) + " are currently in use.")
        print("Exiting.")
        return
    except Exception:
        print("!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!FATAL ERROR OCCURRED!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!")
        print("Exiting.")
    else:
         print("Report generation successful.")

    # Docker cleanup
    old_db_container.stop()
    new_db_container.stop()
    env_client.containers.prune()
   

if __name__ == "__main__":
    main()