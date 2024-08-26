# Basics

Documentation of docker command
```bash
docker --help
docker <command> --help
```

Showing active containers
```bash
docker container ls <flags>
# Or 
docker ps <flags> #older versions

# You can add the flag -a to check all runned containers
```

Running a container
```bash
docker run <image>
# Interactive mode
docker run -it <image>
# Detached mode
docker run -d <image>
# Auto remove when stopped
docker run --rm <image>
```

Exposing container port
```bash
docker run -p <access port>:<container port> <image>
# Ex. Exposing Nginx port 80 to port 3000
docker run -d -p 3000:80 nginx
```

Stoping containers
```bash
docker stop <container>
```

Start already created container
```bash
docker start <container>
# You can run a container in interactive mode
docker start -i <container>
```

Defining container names
```bash
docker run --name <name> <image>
```

Verifying   container log
```bash
docker logs <container>
```

Removing containers
```bash
docker rm <container>
```

Removing unused containers, images and networks
```bash
docker system prune
```

# Images

Building an image
```bash
docker build -t <name> <Dockerfile dir>
```

Listing local images
```bash
docker image ls
```

Downloading external images
```bash
docker pull <image>
```

Renaming images
```bash
docker tag <image ID> <name>:<tag>
```

Removing images
```bash
docker rmi <image>
```

# Container Advanced Features

Copying files from container
```bash
docker cp <container file> <ouput dir>
# Ex
docker cp node_app:/app/app.js ~/Downloads
```

Container processing monitor
```bash
docker top <container>
```

Inspecting a container
```bash
docker inspect <container>
```

Verify processing of all running container
```bash
docker stats
```

# Volumes

Listing volumes
```bash
docker volume ls
```

Creating an anonymous volume
```bash
docker run -v <dir>
```

Creating named volume
```bash
docker run -v <name>:<dir>
# Manual creation
docker volume create <name>
```

Creating binding mount
```bash
docker run -v <machine dir>:<container dir>
```

Inspecting volumes
```bash
docker volume inspect <name>
```
Removing volume
```bash
docker volume rm <name>
```

Removing unused volumes
```bash
docker volume prune
```

Using volume as read only
```bash
docker run -v <volume>:<dir>:ro
```

# Network

### Choosing the Right Network Driver

- **Bridge**: Use for simple container-to-container communication on the same host.
- **Host**: Container-Host connection, use for performance-critical applications where network latency is a concern.
- **Overlay**: Use for distributed applications that require communication across multiple Docker hosts.
- **Macvlan**: Use for containers that need direct access to the physical network with unique MAC addresses.
- **None**: Use for isolated containers that do not need any network access.
- **Plugins**: Third Party drivers maded for specific uses.

### Commands

Listing networks
```bash
docker network ls
```

Creating networks
```bash
# Creates a bridge network by default
docker network create <network name>
# You can use -d tag to determine driver
docker network create -d bridge default-network
docker network create -d macvlan example-network
```

Indicate network to be used by a container
```bash
docker run --network <network>
```

Connect a container to a network
```bash
docker network connect <network> <container>
```

Disconnect a container from a network
```bash
docker network disconnect <network> <container>
```

Inspecting a network
```bash
docker inspect <network>
```

Removing networks
```bash
docker network rm <network>
```

Removing unused networks
```bash
docker network prune
```

# YAML

### Basic Structure

1. **Scalars**: YAML represents basic data types like strings, numbers, booleans, and null.

```yaml
string: "Hello, YAML"
number: 123
float: 12.34
boolean: true
null_value: null
```

2. **Collections**: YAML supports sequences (lists) and mappings (dictionaries).

#### Sequences (Lists)

```yaml
fruits:
  - Apple
  - Orange
  - Banana
```

You can also write lists inline:

```yaml
fruits: [Apple, Orange, Banana]
```

#### Mappings (Dictionaries)

```yaml
person:
  name: John Doe
  age: 30
  email: john.doe@example.com
```

You can also write dictionaries inline:

```yaml
person: {name: John Doe, age: 30, email: john.doe@example.com}
```

### Nesting

YAML supports nesting of sequences and mappings.

```yaml
persons:
  - name: John Doe
    age: 30
    email: john.doe@example.com
  - name: Jane Smith
    age: 25
    email: jane.smith@example.com
```

### Comments

Comments in YAML start with `#`.

```yaml
# This is a comment
key: value  # This is an inline comment
```

### Multi-line Strings

YAML supports multi-line strings in two styles: block and folded.

#### Block Scalars (literal style, using `|`)

Preserves newlines:

```yaml
address: |
  123 Main Street
  Anytown, USA
```

#### Folded Scalars (using `>`)

Folds newlines into spaces:

```yaml
description: >
  This is a folded
  scalar that will be
  turned into a single
  line.
```

### Anchors and Aliases

YAML supports reuse of nodes using anchors (`&`) and aliases (`*`).

```yaml
default: &default
  name: Default Name
  age: 0

person1:
  <<: *default
  name: John Doe

person2:
  <<: *default
  name: Jane Smith
  age: 25
```