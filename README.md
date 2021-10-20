OU Library Queue
====

This is a Cybercom task queue for handling replication from the DigiLab NAS.

### Deployment
This is deployed via ansible using the 
[OULibraries.celery-workerq](https://github.com/OULibraries/ansible-role-celery-worker)
role and offline playbooks for deploying to production and test environments. 

### Testing
#### Automated testing using Tox
* `conda install -c conda-forge tox tox-conda`
* From the root directory of this repository run `tox` to initiate tests.
* Adjust [tox.ini](../tox.ini) file as needed to modify environments and dependencies.
* See the [Tox documentation](https://tox.readthedocs.io/en/latest/) for more details. 