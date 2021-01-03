import docker
from docker import APIClient
import psycopg2
import time

# Test if containers are already running or not
# Handle running containers gracefully by clean starts

def main():
    # Initialize Docker
    client = docker.from_env()
    api_client = docker.APIClient()

    # Start containers
    old_db = client.containers.create(image="guaranteedrate/homework-pre-migration:1607545060-a7085621", publish_all_ports=True, ports={5432:5432})
    new_db = client.containers.create(image="guaranteedrate/homework-post-migration:1607545060-a7085621", publish_all_ports=True, ports={5432:5433})
    old_db.start()
    #time.sleep(10)
    new_db.start()

    old_db_metadata = api_client.inspect_container(old_db.id)
    print(old_db_metadata["NetworkSettings"]["IPAddress"])

    # Show containers
    print(client.containers.list())
    
    # Connect to PSQL server
    retry = 1
    connection = None
    while retry <= 30:
        try:
            print("Connecting to server | attempt ", retry)
            connection = psycopg2.connect(
                    host="localhost",
                    database="old",
                    user="old",
                    password="hehehe",
                    port=5432
                )

            print("Connection created")

            # create a cursor
            cur = connection.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT * FROM accounts')
            
            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            db_version = cur.fetchone()
            print(db_version)
            break

        except psycopg2.DatabaseError as err:
            time.sleep(1)
            retry += 1
    connection.close()
    print("Connection closed")
    
    # Connect to PSQL server
    retry = 1
    connection = None
    while retry <= 30:
        try:
            print("Connecting to server | attempt ", retry)
            connection = psycopg2.connect(
                    host="localhost",
                    database="new",
                    user="new",
                    password="hahaha",
                    port=5433
                )

            print("Connection created")

            # create a cursor
            cur = connection.cursor()

            # execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT * FROM accounts')
            
            # display the PostgreSQL database server version
            db_version = cur.fetchone()
            print(db_version)
            break

        except psycopg2.DatabaseError as err:
            time.sleep(1)
            retry += 1
    connection.close()
    print("Connection closed")

    # Stop containers
    old_db.stop()
    new_db.stop()
    client.containers.prune()

    pass

if __name__ == "__main__":
    main()