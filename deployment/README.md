# Deploying Hillview

We are deprecating Ansible for Hillview's management; the deployment
is now done using just: ![Python scripts](../README.md#3-deploying-the-hillview-service-on-a-cluster).
This folder contains a left-over ansible script for installing
java on the cluster.

## Cluster configuration file

The cluster where the Hillview service is deployed is described by a `hosts`
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
service.  The script is invoked using:

`ansible-playbook -i hosts -u user install-java.yaml`
