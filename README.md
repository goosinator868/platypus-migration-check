# platypus-migration-check
## Introduction
Hi there! Thank you for taking the time to review my code submission.
This code is supposed to create a report of the discrepancies between the two given databases.
Make sure that you have the requirements installed before running the code.

## Requirements
[Python 2.7](https://www.python.org/download/releases/2.7/)
[Docker](https://www.docker.com/products/docker-desktop)
[psycopg2](https://pypi.org/project/psycopg2/#files)

## Instructions
1. Download repos to computer.
2. Navigate to the repos' directory in the terminal shell.
3. Run 'python database_migration_checker.py' in the terminal.
4. (Optional) To view tests, run "python unit_tests.py'.
The generated report can be found in the same repos directory.

## Assumed Guidelines
### Old Database Restrictions
The old database set being used is only accessed through the Docker image found at guaranteedrate/homework-pre-migration:1607545060-a7085621.
The old database's Docker image is accessible through port 5432 and can be mapped to the local machine's port 5433.
The table being examined is named "accounts".
All old table entries have unique ids (primary key).
The old table contains only the following columns in the following order:
- id
- name
- email

### New Database Restrictions
The new database set being used is only accessed through the Docker image found at guaranteedrate/homework-post-migration:1607545060-a7085621.
The new database's Docker image is accessible through port 5432 and can be mapped to the local machine's port 5433.
The table being examined is named "accounts".
All new table entries have unique ids (primary key).
The new table contains online the following columns in the following order:
- id
- name
- email
- favorite_flavor