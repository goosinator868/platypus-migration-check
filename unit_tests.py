import unittest
import docker
import psycopg2
import time
import csv
import database_migration_checker
import database_samples

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

    @unittest.skip("takes a long time")
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

class TestSortAndSelectEntries(unittest.TestCase):
    def test_basic_environment(self):
        # Unfortunately, this cannot be extensively tested for correctness without another database. Manually compared changes using DBeaver.
        # Check for lack of failure when connecting
        try:
            old_db_port_no = 5432
            new_db_port_no = 5433
            env_client, old_db_container, new_db_container = database_migration_checker.start_docker_containers(old_db_port_no, new_db_port_no)

            # Connect to PSQL server
            old_db_connection, old_db_cursor = database_migration_checker.connect_db(db="old", usr="old", pswd="hehehe", prt_no=old_db_port_no)
            new_db_connection, new_db_cursor = database_migration_checker.connect_db(db="new", usr="new", pswd="hahaha", prt_no=new_db_port_no)

            old_db_id_list, new_db_id_list = database_migration_checker.sort_and_select_entries(old_db_cursor, new_db_cursor)

        except:
            self.fail("Something failed unexpectedly when testing for sort_and_select_entries().")
        
        if old_db_connection != None:
            old_db_connection.close()
        if new_db_connection != None:
            new_db_connection.close()
        if old_db_container != None:
            old_db_container.stop()
        if new_db_container != None:
            new_db_container.stop()
        env_client.containers.prune()
    
    def test_cleanup_on_bad_data(self):
        try:
            old_db_port_no = 5432
            new_db_port_no = 5433
            env_client, old_db_container, new_db_container = database_migration_checker.start_docker_containers(old_db_port_no, new_db_port_no)

            # Connect to PSQL server
            old_db_connection, old_db_cursor = database_migration_checker.connect_db(db="old", usr="old", pswd="hehehe", prt_no=old_db_port_no)
            new_db_connection, new_db_cursor = database_migration_checker.connect_db(db="new", usr="new", pswd="hahaha", prt_no=new_db_port_no)

            self.assertRaises(database_migration_checker.SortError, database_migration_checker.sort_and_select_entries(0, new_db_cursor))
        except database_migration_checker.SortError:
            pass
        except:
            self.fail("Something failed unexpectedly when testing for sort_and_select_entries().")
        
        try:
            self.assertRaises(database_migration_checker.SortError, database_migration_checker.sort_and_select_entries(old_db_cursor, 0))
        except database_migration_checker.SortError:
            pass
        except:
            self.fail("Something failed unexpectedly when testing for sort_and_select_entries().")
        
        if old_db_connection != None:
            old_db_connection.close()
        if new_db_connection != None:
            new_db_connection.close()
        if old_db_container != None:
            old_db_container.stop()
        if new_db_container != None:
            new_db_container.stop()
        env_client.containers.prune()

class TestFindNewEntries(unittest.TestCase):
    def test_basic_environment(self):
        try:
            # Test individual successes and failures
            same_data = database_migration_checker.find_new_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0)
            print(same_data)
            self.assertListEqual(same_data, [])

            # Test id above and below actual value
            diff_id = database_migration_checker.find_new_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_id_0)
            self.assertListEqual(diff_id, database_samples.new_database_entry_0_diff_id_0)
            diff_id = None
            diff_id = database_migration_checker.find_new_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_id_1)
            self.assertListEqual(diff_id, database_samples.new_database_entry_0_diff_id_1)

            # Test name above and below actual value
            diff_name = database_migration_checker.find_new_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_name_0)
            self.assertListEqual(diff_name, [])
            diff_name = None
            diff_name = database_migration_checker.find_new_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_name_1)
            self.assertListEqual(diff_name, [])
            diff_name = None
            diff_name = database_migration_checker.find_new_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_name_2)
            self.assertListEqual(diff_name, [])

            # Test email above and below actual value
            diff_email = database_migration_checker.find_new_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_email_0)
            self.assertListEqual(diff_email, [])
            diff_email = None
            diff_email = database_migration_checker.find_new_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_email_1)
            self.assertListEqual(diff_email, [])
            diff_email = None
            diff_email = database_migration_checker.find_new_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_email_2)
            self.assertListEqual(diff_email, [])

            # Test larger scale results
            new_entries = database_migration_checker.find_new_entries(database_samples.old_database_sample, database_samples.new_database_sample)
            self.assertListEqual(new_entries, database_samples.new_entries_sol)

        except:
            self.fail("Unexpected failure looking for new entries!")
    
    def test_cleanup_on_bad_data(self):
        try:
            self.assertRaises(database_migration_checker.EntryError, database_migration_checker.find_new_entries(database_samples.old_database_sample, 0))
        except database_migration_checker.EntryError:
            pass
        except:
            self.fail("Unexpected failure looking for new entries!")
        
        try:
            self.assertRaises(database_migration_checker.EntryError, database_migration_checker.find_new_entries(0, database_samples.new_database_sample))
        except database_migration_checker.EntryError:
            pass
        except:
            self.fail("Unexpected failure looking for new entries!")
        # Assumed tables are given with the same number of columns with the same naming conventions in that order. See README for more info on assumed guidelines.

class TestFindMissingEntries(unittest.TestCase):
    def test_basic_environment(self):
        try:
            # Test individual successes and failures
            same_data = database_migration_checker.find_missing_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0)
            print(same_data)
            self.assertListEqual(same_data, [])

            # Test id above and below actual value
            diff_id = database_migration_checker.find_missing_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_id_0)
            self.assertListEqual(diff_id, database_samples.old_database_entry_0)
            diff_id = None
            diff_id = database_migration_checker.find_missing_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_id_1)
            self.assertListEqual(diff_id, database_samples.old_database_entry_0)

            # Test name above and below actual value
            diff_name = database_migration_checker.find_missing_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_name_0)
            self.assertListEqual(diff_name, [])
            diff_name = None
            diff_name = database_migration_checker.find_missing_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_name_1)
            self.assertListEqual(diff_name, [])
            diff_name = None
            diff_name = database_migration_checker.find_missing_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_name_2)
            self.assertListEqual(diff_name, [])

            # Test email above and below actual value
            diff_email = database_migration_checker.find_missing_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_email_0)
            self.assertListEqual(diff_email, [])
            diff_email = None
            diff_email = database_migration_checker.find_missing_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_email_1)
            self.assertListEqual(diff_email, [])
            diff_email = None
            diff_email = database_migration_checker.find_missing_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_email_2)
            self.assertListEqual(diff_email, [])

            # Test larger scale results
            new_entries = database_migration_checker.find_missing_entries(database_samples.old_database_sample, database_samples.new_database_sample)
            self.assertListEqual(new_entries, database_samples.missing_entries_sol)

        except:
            self.fail("Unexpected failure looking for new entries!")
    
    def test_cleanup_on_bad_data(self):
        try:
            self.assertRaises(database_migration_checker.EntryError, database_migration_checker.find_missing_entries(database_samples.old_database_sample, 0))
        except database_migration_checker.EntryError:
            pass
        except:
            self.fail("Unexpected failure looking for missing entries!")
        
        try:
            self.assertRaises(database_migration_checker.EntryError, database_migration_checker.find_missing_entries(0, database_samples.new_database_sample))
        except database_migration_checker.EntryError:
            pass
        except:
            self.fail("Unexpected failure looking for missing entries!")
        # Assumed tables are given with the same number of columns with the same naming conventions in that order. See README for more info on assumed guidelines.

class TestFindCorruptedEntries(unittest.TestCase):
    def test_basic_environment(self):
        try:
            # Test individual successes and failures
            same_data = database_migration_checker.find_corrupted_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0)
            print(same_data)
            self.assertListEqual(same_data, [])

            # Test id above and below actual value
            diff_id = database_migration_checker.find_corrupted_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_id_0)
            self.assertListEqual(diff_id, [])
            diff_id = None
            diff_id = database_migration_checker.find_corrupted_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_id_1)
            self.assertListEqual(diff_id, [])

            # Test name above and below actual value
            diff_name = database_migration_checker.find_corrupted_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_name_0)
            self.assertListEqual(diff_name, [database_samples.old_database_entry_0[0] + database_samples.new_database_entry_0_diff_name_0[0]])
            diff_name = None
            diff_name = database_migration_checker.find_corrupted_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_name_1)
            self.assertListEqual(diff_name, [database_samples.old_database_entry_0[0] + database_samples.new_database_entry_0_diff_name_1[0]])
            diff_name = None
            diff_name = database_migration_checker.find_corrupted_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_name_2)
            self.assertListEqual(diff_name, [database_samples.old_database_entry_0[0] + database_samples.new_database_entry_0_diff_name_2[0]])

            # Test email above and below actual value
            diff_email = database_migration_checker.find_corrupted_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_email_0)
            self.assertListEqual(diff_email, [database_samples.old_database_entry_0[0] + database_samples.new_database_entry_0_diff_email_0[0]])
            diff_email = None
            diff_email = database_migration_checker.find_corrupted_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_email_1)
            self.assertListEqual(diff_email, [database_samples.old_database_entry_0[0] + database_samples.new_database_entry_0_diff_email_1[0]])
            diff_email = None
            diff_email = database_migration_checker.find_corrupted_entries(database_samples.old_database_entry_0, database_samples.new_database_entry_0_diff_email_2)
            self.assertListEqual(diff_email, [database_samples.old_database_entry_0[0] + database_samples.new_database_entry_0_diff_email_2[0]])

            # Test larger scale results
            new_entries = database_migration_checker.find_corrupted_entries(database_samples.old_database_sample, database_samples.new_database_sample)
            self.assertListEqual(new_entries, database_samples.corrupted_entries_sol)

        except:
            self.fail("Unexpected failure looking for new entries!")
    
    def test_cleanup_on_bad_data(self):
        try:
            self.assertRaises(database_migration_checker.EntryError, database_migration_checker.find_corrupted_entries(database_samples.old_database_sample, 0))
        except database_migration_checker.EntryError:
            pass
        except:
            self.fail("Unexpected failure looking for corrupted entries!")

        try:
            self.assertRaises(database_migration_checker.EntryError, database_migration_checker.find_corrupted_entries(0, database_samples.new_database_sample))
        except database_migration_checker.EntryError:
            pass
        except:
            self.fail("Unexpected failure looking for corrupted entries!")
        # Assumed tables are given with the same number of columns with the same naming conventions in that order. See README for more info on assumed guidelines.

class TestWriteReport(unittest.TestCase):
    pass

if __name__ == "__main__":
    unittest.main()
