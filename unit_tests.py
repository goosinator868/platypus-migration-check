import unittest
import docker
import psycopg2
import time
import csv
import database_migration_checker

class TestStartDockerContainers(unittest.TestCase):
    def test_basic_environment(self):
        # Check that normal case creates containers without failures
        docker_list = None
        try:
            # Create & run containers
            docker_list = database_migration_checker.start_docker_containers(5432, 5433)

            # Ensure list is populated
            self.assertIsNotNone(docker_list)
            self.assertEqual(3, len(docker_list))
            for value in docker_list:
                self.assertIsNotNone(value)
            # Check that values are real
            self.assertIsNotNone(docker_list[0].version)
            self.assertIsNotNone(docker_list[0].containers.get(docker_list[1].id))
            self.assertIsNotNone(docker_list[0].containers.get(docker_list[2].id))
            
            docker_list[1].stop()
            docker_list[2].stop()
            docker_list[0].containers.prune()

        except docker.errors.NotFound:
            self.fail("Container does not exist!")
        except Exception:
            # Container creation failed entirely
            self.fail("Container creation failed!")

    def test_cleanup_on_failure(self):
        # Checking that containers are closed upon failure
        existing_container_count = 0
        try:
            client = docker.from_env()
            existing_container_count = len(client.containers.list())
            self.assertRaises(database_migration_checker.InitError, database_migration_checker.start_docker_containers(5432, 5432))
        except database_migration_checker.InitError:
            pass
        except Exception:
            self.fail("Container creation failed!")
        
        try:
            self.assertEqual(existing_container_count, len(client.containers.list()))
        except Exception:
            self.fail("Container access failed!")

class TestConnectDB(unittest.TestCase):
    def test_basic_environment(self):
        # Test that normal case returns real cursors without failures
        try:
            env_client, old_db_container, new_db_container = database_migration_checker.start_docker_containers(5432, 5433)
            old_db_connection, old_db_cursor = database_migration_checker.connect_db(db="old", usr="old", pswd="hehehe", prt_no=5432)
            new_db_connection, new_db_cursor = database_migration_checker.connect_db(db="new", usr="new", pswd="hahaha", prt_no=5433)

            self.assertIsNotNone(old_db_connection)
            self.assertIsNotNone(new_db_connection)
            self.assertIsNotNone(old_db_cursor)
            self.assertIsNotNone(new_db_cursor)
            # Execute commands on cursors. If not possible, will throw exception.
            old_db_cursor.execute("SELECT * FROM accounts")
            new_db_cursor.execute("SELECT * FROM accounts")

            old_db_connection.close()
            new_db_connection.close()
            old_db_container.stop()
            new_db_container.stop()
            env_client.containers.prune()

        except:
            self.fail("Connection to db failed unexpectedly!")

    def test_cleanup_on_bad_data(self):
        # Confirm proper error is thrown upon failure
        # WARNING: This one takes a while
        old_db_connection = None
        old_db_cursor = None
        try:
            env_client, old_db_container, new_db_container = database_migration_checker.start_docker_containers(5432, 5433)
            old_db_connection, old_db_cursor = database_migration_checker.connect_db(db="old", usr="foo", pswd="hehehe", prt_no=5432)
        except database_migration_checker.PSQLConnectionError:
            pass
        except:
            self.fail("Connection to db failed unexpectedly!")
        else:
            self.fail("Connection is supposed to fail.")
        
        try:
            old_db_connection, old_db_cursor = database_migration_checker.connect_db(db=5, usr="old", pswd="hehehe", prt_no=5432)
        except database_migration_checker.PSQLConnectionError:
            pass
        except:
            self.fail("Connection to db failed unexpectedly!")
        else:
            self.fail("Connection is supposed to fail.")
        
        old_db_container.stop()
        new_db_container.stop()
        env_client.containers.prune()

        self.assertIsNone(old_db_connection)
        self.assertIsNone(old_db_cursor)

class TestFindNewEntries(unittest.TestCase):
    pass

class TestFindMissingEntries(unittest.TestCase):
    pass

class TestFindCorruptEntries(unittest.TestCase):
    pass

if __name__ == "__main__":
    unittest.main()
