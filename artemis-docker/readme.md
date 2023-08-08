# Docker Image Example

This is an example on how you could create your own Docker Image For Apache
ActiveMQ Artemis based on CentOS or Ubuntu (Eclipse Temurin JDK images).

# Preparing

You need a set of activemq binary distribution files to build the Docker Image.
These can be your local distribution files, in which case you need to build the project first, 
or they can be pulled automatically from the official ActiveMQ release repository.

## Using a Local Binary Distribution
If you want to use a local binary distribution, build the project from the root of the ActiveMQ source tree using maven.
```
mvn install -DskipTests=true
```
Following the build, the distribution files will be in your local distribution directory.
```
artemis-distribution/target/apache-artemis-2.17.0-SNAPSHOT-bin/apache-artemis-2.17.0-SNAPSHOT
```
In the artemis-docker directory, run the script ./prepare-docker.sh to copy the docker files into your
local binary distribution.

Below is shown the command to prepare the build of the Docker Image starting
from the local distribution (from the source codes of ActiveMQ Artemis)

```
# Prepare for build the Docker Image from the local distribution. Replace the 
# {local-distribution-directory} with your directory.
$ ./prepare-docker.sh --from-local-dist --local-dist-path {local-distribution-directory}
```

The output of the previous command is shown below.

```
$ ./prepare-docker.sh --from-local-dist --local-dist-path ../artemis-distribution/target/apache-artemis-2.17.0-SNAPSHOT-bin/apache-artemis-2.17.0-SNAPSHOT

Using ../artemis-distribution/target/apache-artemis-2.17.0-SNAPSHOT-bin/apache-artemis-2.17.0-SNAPSHOT
Cleaning up ../artemis-distribution/target/apache-artemis-2.17.0-SNAPSHOT-bin/apache-artemis-2.17.0-SNAPSHOT/docker

Well done! Now you can continue with building the Docker image:

  # Go to ../artemis-distribution/target/apache-artemis-2.17.0-SNAPSHOT-bin/apache-artemis-2.17.0-SNAPSHOT
  $ cd ../artemis-distribution/target/apache-artemis-2.17.0-SNAPSHOT-bin/apache-artemis-2.17.0-SNAPSHOT

  # For CentOS with full JDK 11
  $ docker build -f ./docker/Dockerfile-centos7-11 -t artemis-centos .

  # For Ubuntu with full JDK 11
  $ docker build -f ./docker/Dockerfile-ubuntu-11 -t artemis-ubuntu .

  # For Ubuntu with just JRE 11
  $ docker build -f ./docker/Dockerfile-ubuntu-11-jre -t artemis-ubuntu-jre .

  # For Alpine with full JDK 17
  $ docker build -f ./docker/Dockerfile-alpine-17 -t artemis-alpine .

  # For Alpine with just JRE 11
  $ docker build -f ./docker/Dockerfile-alpine-11-jre -t artemis-alpine-jre .

  # For Ubuntu on Linux ARMv7/ARM64 with full JDK
  $ docker buildx build --platform linux/arm64,linux/arm/v7 --push -t {your-repository}/apache-artemis:2.17.0-SNAPSHOT -f ./docker/Dockerfile-ubuntu-11 .

Note: -t artemis-centos and -t artemis-ubuntu are just tag names for the purpose of this guide

For more info see readme.md.
```

## Using the Official ActiveMQ Binary Release
The command to prepare the build of the Docker Image starting from the official
release of ActiveMQ Artemis is shown below

```
# Prepare for build the Docker Image from the release version. Replace the
# {release-version} with the version that you want 
$ ./prepare-docker.sh --from-release --artemis-version {release-version}
```

The output of the previous command is shown below.

```
$ ./prepare-docker.sh --from-release --artemis-version 2.16.0
Creating _TMP_/artemis/2.16.0
Downloading apache-artemis-2.16.0-bin.tar.gz from https://downloads.apache.org/activemq/activemq-artemis/2.16.0/...
################################################################################################################################################################################################################################ 100,0%
Expanding _TMP_/artemis/2.16.0/apache-artemis-2.16.0-bin.tar.gz...
Removing _TMP_/artemis/2.16.0/apache-artemis-2.16.0-bin.tar.gz...

Well done! Now you can continue with building the Docker image:

  # Go to _TMP_/artemis/2.16.0
  $ cd _TMP_/artemis/2.16.0

  # For CentOS with full JDK
  $ docker build -f ./docker/Dockerfile-centos7-11 -t artemis-centos .

  # For Ubuntu with full JDK
  $ docker build -f ./docker/Dockerfile-ubuntu-11 -t artemis-ubuntu .

  # For Ubuntu with just JRE
  $ docker build -f ./docker/Dockerfile-ubuntu-11-jre -t artemis-ubuntu .

  # For Ubuntu on Linux ARMv7/ARM64 with full JDK
  $ docker buildx build --platform linux/arm64,linux/arm/v7 --push -t {your-repository}/apache-artemis:2.16.0 -f ./docker/Dockerfile-ubuntu-11 .

Note: -t artemis-centos and -t artemis-ubuntu are just tag names for the purpose of this guide

For more info read the readme.md
```

# Building

Go to `$ARTEMIS_DIST` where you prepared the binary with Docker files.

## For CentOS

From within the `$ARTEMIS_DIST` folder:
```
$ docker build -f ./docker/Dockerfile-centos7-11 -t artemis-centos .
```

## For Ubuntu

From within the `$ARTEMIS_DIST` folder:
```
$ docker build -f ./docker/Dockerfile-ubuntu-11 -t artemis-ubuntu .
```

## Smaller Ubuntu image with just JRE
From within the `$ARTEMIS_DIST` folder:
```
$ docker build -f ./docker/Dockerfile-ubuntu-11-jre -t artemis-ubuntu .
```

## For Ubuntu (Build for linux ARMv7/ARM64)
```
$ docker buildx build --platform linux/arm64,linux/arm/v7 --push -t {your-repository}/apache-artemis:2.17.0-SNAPSHOT -f ./docker/Dockerfile-ubuntu-11 .
```

**Note:**
`-t artemis-centos` and `-t artemis-ubuntu` are just tag names for the purpose of this guide


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

# Overriding files in etc folder

You can use customized configuration for the artemis instance by replacing the files residing in `etc` folder with the custom ones, eg. `broker.xml` or `artemis.profile`. Put the replacement files inside a folder and map it as a volume to:
- `/var/lib/artemis-instance/etc-override`

The contents of `etc-override` folder will be copied over to etc folder after the instance creation. Therefore, the image will always start with user-supplied configuration.

It you are mapping the whole `var/lib/artemis-instance` to an outside folder for persistence, you can place an `etc-override` folder inside the mapped one, its contents will again be copied over etc folder after creating the instance.

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
