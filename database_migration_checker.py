import docker

# Test if containers are already running or not
# Handle running containers gracefully by clean starts

def main():
    # Initialize Docker
    client = docker.from_env()

    # Start containers
    old_db = client.containers.create(image="guaranteedrate/homework-pre-migration:1607545060-a7085621", publish_all_ports=True, ports={5432:5432})
    old_db.start()

    # Show containers
    print(client.containers.list())
    old_db.stop()
    client.containers.prune()

    pass

if __name__ == "__main__":
    main()