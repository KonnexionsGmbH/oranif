# Testing based on a Docker Container and a Sample Database 

## 1. Introduction

The simplest solution for providing a suitable test environment is surely a Docker image with an empty database and the installation of a sample database schemas based on it.

Testing in oranif then requires the following steps: 

- Installing the [Docker Toolbox](https://docs.docker.com/toolbox/) software.
- Creating a Docker virtual machine.
- Downloading a Docker image with an empty database.
- Creating a Docker container from the Docker image.
- Load the database sample schemas from Oracle into the Docker container.

The following description is based on the Docker image `konnexionsgmbh/db_12_2`  (Oracle Database 12c Release 2) and  the related [sample schemas](https://github.com/oracle/db-sample-schemas/releases/tag/v12.2.0.1).
In the following examples it is also assumed that the password for the `SYS` schema is `oracle` and the IP address of the Docker container is `192.168.99.122`. 

## 2. Installing Docker Toolbox and Docker Virtual Machine

Installing the [Docker Toolbox](https://docs.docker.com/toolbox/) is preferable to installing [Docker](https://www.docker.com/), otherwise the VirtualBox can no longer be used for other purposes - quote from the Docker documentation:

> After Hyper-V is enabled, VirtualBox no longer works, but any VirtualBox VM images remain. VirtualBox VMs created with docker-machine (including the default one typically created during Toolbox install) no longer start.

To run a Docker container, you:

- create a new (or start an existing) Docker virtual machine
- use the docker client to create, load, and manage containers

Once you create a machine, you can reuse it as often as you like. 
Like any VirtualBox VM, it maintains its configuration between uses.

#### Processing steps:

- Download from [here](https://github.com/docker/toolbox/releases) and install the [Docker Toolbox](https://docs.docker.com/toolbox/toolbox_install_windows/) software.

- Create a suitable Docker virtual machine from a PowerShell based command window: with the following command: 

    ```docker-machine create -d virtualbox --virtualbox-disk-size "24000" --virtualbox-memory "3072" default```

- With the command `docker-machine ls` the state of the Docker virtual machine can be queried:

![docker-machine ls](https://i.imgur.com/LWp9KaA.png)

- It is a good practice always to terminate the Docker virtual machine when it is no longer needed:

    ```docker-machine stop```

- The start of `Docker Quickstart Terminal` always starts the Docker virtual machine as well. To start or restart the Docker virtual machine manually use the command:

    ```docker-machine start```

## 3. Downloading a Docker Image with an Empty Database

The prebuilt Docker image [konnexionsgmbh/db_12_2](https://cloud.docker.com/u/konnexionsgmbh/repository/docker/konnexionsgmbh/db_12_2) can be used with Oracle Database 12c Release 2.
You only have to download the docker image with the command `docker pull konnexionsgmbh/db_12_2` (best with the  `Docker Quickstart Terminal` if the docker demon is not running in the background yet).

#### Processing steps:

- Start the `Docker Quickstart Terminal` or alternatively start the Docker virtual machine in a PowerShell based command window:

    ```docker-machine start```

- Download the Docker image with the following command:

    ```docker pull konnexionsgmbh/db_12_2```

![docker pull](https://i.imgur.com/DHxNg6c.png)

- The result can be checked with the command: 

    ```docker images```

![docker images](https://i.imgur.com/ZAHA63R.png)

## 4. Creation of a Docker Container

The following assumes that the docker image [konnexionsgmbh/db_12_2](https://cloud.docker.com/u/konnexionsgmbh/repository/docker/konnexionsgmbh/db_12_2) from [dockerhub](https://hub.docker.com/) is used. 

#### Processing steps:

- Start the `Docker Quickstart Terminal` or alternatively start the Docker virtual machine in a PowerShell based command window:

    ```docker-machine start```

- Create the Docker container with the following command - if necessary, the database password for SYS etc. may be adjusted using parameter `ORACLE_PWD`:

    ```docker run --name oranif_db -p 1521:1521/tcp -e ORACLE_PWD=oracle konnexionsgmbh/db_12_2```

- In another PowerShell based command window, the result can be checked with the command: 

    ```docker ps -a```

![docker_ps_a](https://i.imgur.com/sMjo39S.png)

- The Docker container`oranif_db` is then to be terminated with the command:  

    ```docker stop oranif_db```

- For any further processing, the Docker container`db_12_2` can then be started again in the background with the command:

    ```docker start oranif_db```

- Conversely, it is always very advisable to stop the Docker container`db_12_2` after processing so that the Oracle database it contains is not destroyed: 

    ```docker stop oranif_db```

- If for any reason you need the IP address of the container, just execute the following command: 

    ```docker-machine ip default```

The created docker container oranif_db now contains an Oracle database version 12c release 2 with the pluggable container orclpdb1. 
The database schema SYS is assigned the password oracle by default. 
A possible entry in the file tnsnames.ora looks like this: 

```
ORCLCDB =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.99.122)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = orclcdb)
    )
  )

ORCLPDB1 =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.99.122)(PORT = 1521))
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = orclpdb1)
    )
  )
```

## 5. Creating the Sample Database Schemas

in this section it is assumed that the Docker virtual machine and the Docker container `db_12_2` are both already running:

- Start the Docker virtual machine via `Docker Quickstart Terminal` or manually with the command: `docker-machine start`.
- Start the Docker container `db_12_2` with the command: `docker start db_12_2`.

Caution: The `create_schemas.sql` script recreates all affected schemas and deletes them if they already exist.

#### Processing steps:

- Download the [oranif repository](https://github.com/K2InformaticsGmbH/oranif).

- Switch to the oranif repository directory `..\oranif\test\install\oracle\db-sample-schemas-12.2.0.1`.

- Start `sqlplus` and execute the script `mksample.sql`, for example as follows: 

    ```sqlplus sys/oracle@192.168.99.122:1521/orclpdb1 as sysdba @mksample oracle oracle oracle oracle oracle oracle oracle oracle users temp c:\tmp\log\ 192.168.99.122:1521/orclpdb1``` 
 
After executing the script, the necessary database schemas should be created. 
By default all schemas are created with the password `oracle`.


