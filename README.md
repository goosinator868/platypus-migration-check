# platypus-migration-check
## Requirements
[Docker](https://www.docker.com/products/docker-desktop)
[psycopg2](https://pypi.org/project/psycopg2/#files)

## Assumed Guidelines
### Old Database Restrictions
The old database set being used is only accessed through the Docker image found at guaranteedrate/homework-pre-migration:1607545060-a7085621.
The old database's Docker image is accessible through port 5432 and can be mapped to the local machine's port 5433.
All old database entries have unique ids (primary key).
The old database contains only the following columns:
- id
- name
- address

### New Database Restrictions
The new database set being used is only accessed through the Docker image found at guaranteedrate/homework-post-migration:1607545060-a7085621.
The new database's Docker image is accessible through port 5432 and can be mapped to the local machine's port 5433.
All new database entries have unique ids (primary key).
The new database contains online the following columns:
- id
- name
- address
- flavor