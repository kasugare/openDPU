#!/bin/bash
rm src/*.pyc src/**/*.pyc src/**/**/*.pyc src/**/**/**/*.pyc src/**/**/**/**/*.pyc src/**/**/**/**/**/*.pyc src/**/**/**/**/**/**/*.pyc

# Staging
scp -r VERSION ./src nilm@10.0.1.113:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.114:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.115:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.116:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.117:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.118:~/nilm_cluster/

scp -r ./client/python/src/*.py nilm@10.0.1.113:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.114:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.115:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.116:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.117:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.118:~/nilm_cluster/client/python/src/
