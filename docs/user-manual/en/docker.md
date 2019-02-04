# Artemis on Docker

Artemis provide support to build CentOS and Ubuntu images of the broker, allowing to reuse 
an existing broker instance *ie from a previous image run* or just creating a fresh new one.

## Building a CentOS image
From within the folder with both `Dockerfile-centos` file and `assets` folder:
```
$ docker build -f Dockerfile-centos -t artemis-centos .
```
> **Note:**
>`-t artemis-centos` is just a tag name for the purpose of this guide
## Running a CentOS image
The image just created in the previous step allows both stateless or stateful runs.
The stateless run is achieved by:
```
$ docker run --rm -it -p 61616:61616 -p 8161:8161 artemis-centos 
```
While a stateful run with:
```
docker run -it -p 61616:61616 -p 8161:8161 -v <broker folder on host>:/var/lib/artemis-instance artemis-centos 
```
where `<broker folder on host>` is a folder where the broker instance is supposed to 
be saved and reused on each run.


