#!/bin/bash
rm src/*.pyc src/**/*.pyc src/**/**/*.pyc src/**/**/**/*.pyc src/**/**/**/**/*.pyc src/**/**/**/**/**/*.pyc src/**/**/**/**/**/**/*.pyc

scp -r VERSION ./src nilm@10.0.1.11:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.12:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.13:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.14:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.15:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.16:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.17:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.18:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.101:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.102:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.103:~/nilm_cluster/
#scp -r VERSION ./src nilm@10.0.1.104:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.105:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.106:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.107:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.108:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.109:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.110:~/nilm_cluster/
scp -r VERSION ./src nilm@10.0.1.111:~/nilm_cluster/
#scp -r VERSION ./src nilm@10.0.1.112:~/nilm_cluster/

scp -r ./client/python/src/*.py nilm@10.0.1.11:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.12:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.13:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.14:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.15:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.16:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.17:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.18:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.101:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.102:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.103:~/nilm_cluster/client/python/src/
#scp -r ./client/python/src/*.py nilm@10.0.1.104:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.105:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.106:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.107:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.108:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.109:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.110:~/nilm_cluster/client/python/src/
scp -r ./client/python/src/*.py nilm@10.0.1.111:~/nilm_cluster/client/python/src/
#scp -r ./client/python/src/*.py nilm@10.0.1.112:~/nilm_cluster/client/python/src/
