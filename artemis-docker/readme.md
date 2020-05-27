# Docker Image Example

This is an example on how you could create your own Docker Image For Apache ActiveMQ Artemis based on CentOS or Debian.
# Preparing

Use the script ./prepare-docker.sh as it will copy the docker files under the binary distribution.

```
$ ./prepare-docker.sh $ARTEMIS_HOME
```

# Building

Go to `$ARTEMIS_HOME` where you prepared the binary with Docker files.

## For Debian

From within the `$ARTEMIS_HOME` folder:
```
$ docker build -f ./docker/Dockerfile-debian -t artemis-debian .
```

## For CentOS

From within the `$ARTEMIS_HOME` folder:
```
$ docker build -f ./docker/Dockerfile-centos -t artemis-centos .
```

**Note:**
`-t artemis-debian`,`-t artemis-centos` are just tag names for the purpose of this guide


# Environment Variables

Environment variables determine the options sent to `artemis create` on first execution of the Docker
container. The available options are: 

**`ARTEMIS_USER`**

The administrator username. The default is `artemis`.

**`ARTEMIS_PASSWORD`**

The administrator password. The default is `artemis`.

**`ANONYMOUS_LOGIN`**

Set to `true` to allow anonymous logins. The default is `false`.

**`EXTRA_ARGS`**

Additional arguments sent to the `artemis create` command. The default is `--http-host 0.0.0.0 --relax-jolokia`.
Setting this value will override the default. See the documentation on `artemis create` for available options.

**Final broker creation command:**

The combination of the above environment variables results in the `docker-run.sh` script calling
the following command to create the broker instance the first time the Docker container runs:

    ${ARTEMIS_HOME}/bin/artemis create --user ${ARTEMIS_USER} --password ${ARTEMIS_PASSWORD} --silent ${LOGIN_OPTION} ${EXTRA_ARGS}

Note: `LOGIN_OPTION` is either `--allow-anonymous` or `--require-login` depending on the value of `ANONYMOUS_LOGIN`.

# Mapping point

- `/var/lib/artemis-instance`

It's possible to map a folder as the instance broker.
This will hold the configuration and the data of the running broker. This is useful for when you want the data persisted outside of a container.


# Lifecycle of the execution

A broker instance will be created during the execution of the instance. If you pass a mapped folder for `/var/lib/artemis-instance` an image will be created or reused depending on the contents of the folder.



## Running a CentOS image

The image just created in the previous step allows both stateless or stateful runs.
The stateless run is achieved by:
```
$ docker run --rm -it -p 61616:61616 -p 8161:8161 artemis-centos 
```
The image will also support mapped folders and mapped ports. To run the image with the instance persisted on the host:
```
docker run -it -p 61616:61616 -p 8161:8161 -v <broker folder on host>:/var/lib/artemis-instance artemis-centos 
```
where `<broker folder on host>` is a folder where the broker instance is supposed to 
be saved and reused on each run.
