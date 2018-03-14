# Deploying Hillview

The Hillview service architecture is shown in the following figure:

![System architecture](../docs/system-architecture.png)

This documentation describes how the Hillview service can be deployed.
These tools require `ansible` to be installed on the host running the
following commands, as well as the servers you are trying to deploy
to.  Ansible must be able to run commands on the servers.

*Please note that Hillview allows arbitrary access to files on the
worker nodes from the client application.  The worker nodes should be
deployed within a restricted secure environment.*

Before you run these commands, make sure you've built both `platform`
and `web` projects.  The deployment scripts are in the `deployment`
folder:

```
$: cd deployment
```

## Service configuration

### Service parameters

The fixed configuration of the Hillview service is obtained from the
file `config.yaml`.  Currently this file stores the service port for
the Hillview service and the Java heap size.

Here is an example config.yaml file:

```
---
# Hillview service parameters
  # Network port where the servers listen for requests
  backend_port: 3569
  # Java heap size for Hillview service
  # Can be overridden for each machine
  default_heap_size: "5g"
  # Folder where the hillview service is installed on remote machines
  service_folder: "~"
  # Version of Apache Tomcat to deploy
  tomcat_version: "9.0.4"
  # Tomcat installation folder name
  tomcat: "apache-tomcat-{{ tomcat_version }}"
```

### Service machines

The cluster where the Hillview service is deployed is described by a
file containing the list of machines where the service should be
deployed.  Here is an example of such a file:

```
[web]
192.168.1.2

[backends]
192.168.1.3
192.168.1.4 heap_size=10g
```

The `web` group describes the front-end web server.

The `backends` group lists all machines running the `hillview` service.
Individual machines heap sizes can be specified by setting the machine's `heap_size` variable.

### Service permissions

In this file we assume that the Hillview service runs under a user
account named `hillview`.  No special privileges are needed to run the
Hillview service.

## Deploying the service

In the following commands we assume that the file describing the list
of machines is named `hosts`.

You can verify that ansible is able to authorized commands on these servers:

```
$: ansible all -a "ls" -i hosts -u hillview
```

To install Java on all servers (only needed once):

```
$: ansible-playbook install-java.yaml -i hosts -u hillview
```

The following command installs the software on the machines:

```
$: ansible-playbook prepare.yaml -i hosts -u hillview
```

The service is started by running the following command:

```
$: ansible-playbook start.yaml -i hosts -u hillview -v
```

To verify if the services are up and running, open
`http://<ip-address-of-web-node>:8080` in your web browser.

To stop the services you can run:

```
$: ansible-playbook stop.yaml -i hosts -u hillview -v
```
