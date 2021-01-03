import docker
import psycopg2
import time
from macropy.case_classes import macros, enum

# Test if containers are already running or not
# Handle running containers gracefully by clean starts
'''
class MigrationError(Exception):
    def __init__(self, code):
        self.code = code

class DockerError(migrationError):
    @enum
    class DockerErrorCode:
        InitFailure
    
    errorMessages = {initFailure: "Docker initialization failure."}
'''
class InitError(Exception):
    pass

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

            # execute a statement
            #cursor.execute('SELECT id FROM accounts ORDER BY id')
            
            # display the PostgreSQL database server version
            #db_version = cursor.fetchmany(10)
            #print(db_version)
            break

        except psycopg2.DatabaseError as err:
            time.sleep(1)
            retry += 1
    
    if retry > 30:
        print("Failed to connect to psql server.")
        raise PSQLConnectionError

    print("Successfully connected to " + db + " database on port " + str(prt_no) + ".\n")
    return connection, cursor

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

        old_db_connection.close()
        print("old_db connection closed.")
        new_db_connection.close()
        print("new_db connection closed.")

        # Docker cleanup
        old_db_container.stop()
        new_db_container.stop()
        env_client.containers.prune()
    except InitError:
        print("Exiting.")
        return
    except PSQLConnectionError:
        old_db_container.stop()
        new_db_container.stop()
        env_client.containers.prune()
        return
    except Exception:
        old_db_container.stop()
        new_db_container.stop()
        env_client.containers.prune()
        print("Exiting.")
        return
    
    print("Report generation successful.")

if __name__ == "__main__":
    main()