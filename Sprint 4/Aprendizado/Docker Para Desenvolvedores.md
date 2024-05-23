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
