# Deploying Hillview

We are deprecating Ansible for Hillview's management; the deployment
is now done using just: ![Python scripts](../README.md#3-deploying-the-hillview-service-on-a-cluster).
This folder contains some left-over ansible scripts for managing
software installation on the cluster; they will be eventually
deprecated.

* install-java.yaml: installs Java 8 on the machines specified in the
  configuration file (see below)

* demo-data-cleaner.yaml: creates a flights dataset with only 15
  columns from the full dataset on all machines in the cluster (the
  full dataset must be already installed)

### Cluster configuration file

The cluster where the Hillview service is deployed is described by a
file containing the list of machines where the service should be
deployed.  Here is an example of such a file:

```
[web]
192.168.1.2

[backends]
192.168.1.3
192.168.1.4
```

The `web` group describes the front-end web server.

The `backends` group lists all machines running the `hillview`
service.  Individual machines heap sizes can be specified by setting
the machine's `heap_size` variable.
