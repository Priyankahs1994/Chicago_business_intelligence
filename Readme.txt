Follow the steps below to build Docker/Conatiner images for Go App and Postgres

Make sure before you build and deploy to add your API KEY for Google Geocoder in the main.go


1. Install Golang
2. From the command/terminal window, execute the command: 
	go mod init main
3. From the command/terminal window, execute the command: 
	go mod tidy
4. From the directory the has the Go source code, execute the command: 
	go run main.go
5. Start docker Daemon by opening the docker application.
	Download it from "https://docs.docker.com/desktop/windows/install/" for windows and
	"https://docs.docker.com/desktop/mac/install/" for Mac.
6. Copy the shared Dockerfile and docker-compose.yml to folder with source code.

7. execute "docker-compose up" command from the source directory

8. RUN "docker container ls" command and check for chicago_business_intelligence and postgres_container containers.

9. Open pgadmin and click on add server. Add server with hostname as localhost and Port as 5432 and password as root.





//////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////
Misc commands for building, testing, and deployment


- Setup your Go module
go mod init main
go mod tidy

-Stop all containers:
docker stop $(docker ps -a -q)


-Remove all containers:
docker rm $(docker ps -a -q)


- Remove all images:
docker rmi $(docker images -a -q)


- Create your Docker/Conatiner images
docker-compose up --build

- Use the following Commands for inspection and testing of your App deployment


- Check the containers and their assigned IPs in your network
docker network inspect cbi_backend


- List availabel networks
docker network ls


- List available containers
docker container ls


- connect to ubuntu  docker container interactive tty
docker run -it ubuntu bash

	
- install curl in a container from tty
apt-get update; apt-get install curl

- Sanity test your connection to the outside world, from inside the container, get HTTP headers from Google
curl -Is www.google.com ss