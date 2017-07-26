## Deploying Hillview

The following steps require `ansible` to be installed on the host running the following
commands, as well as the servers you are trying to deploy to. Ansible must be able
to run commands on the servers. Before you run these commands, make sure you've built
both `platform` and `web` projects. We assume there is a user named 'hillview' on the
servers in the following commands.

The servers also require `java` to be installed.

```
$: cd deployment
```

The fixed configuration of the Hillview service is obtained from the
file `config.yaml`.  Currently this file only stores the service port.

A deployment of the service is described by a file named `hosts`;
create this file and populate it with the hostnames or IP addresses of
the servers. You need two groups of servers: 1) a `web` group with one
server, and 2) a `backends` group with all the backend servers. For
instance:


```
[web]
192.168.1.2

[backends]
192.168.1.3
192.168.1.4
```

Verify that ansible is able to run commands on these servers:

```
$: ansible all -a "ls" -i hosts -u hillview
```

Next, run the following command to prepare both servers:

```
$: ansible-playbook prepare.yaml -i hosts -u hillview
```

Next, start the services with:

```
$: ansible-playbook start.yaml -i hosts -u hillview
```

To verify if the services are up and running, login to `http://<ip-address-of-web-node>:3569`.

Stop all services with:

```
$: ansible-playbook stop.yaml -i hosts -u hillview
```
