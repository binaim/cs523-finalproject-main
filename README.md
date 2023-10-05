To start this project:

- Make sure to have `Docker` & `Maven` installed on your system
- Run `./start-all.sh`
  this will stop if there are running containers from the docker-compose configuration and the nit will start all the containers all at once.
  If there are no running containers or no images have been created previously the docker-compose command is executed through the start-all.sh shell file will download the applicable images for the applications and it will then build images of the independent applications (jars) and run them as containers in the same network.
- Navigate to http://localhost:8050/
