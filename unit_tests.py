import unittest
import docker
import psycopg2
import time
import csv
import database_migration_checker

class TestStartDockerContainers(unittest.TestCase):
    def test_basic_environment(self):
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
            pass
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

class TestFindNewEntries(unittest.TestCase):
    pass

class TestFindMissingEntries(unittest.TestCase):
    pass

class TestFindCorruptEntries(unittest.TestCase):
    pass

if __name__ == "__main__":
    unittest.main()
