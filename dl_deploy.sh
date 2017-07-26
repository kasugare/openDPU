#!/bin/bash
rm src/*.pyc src/**/*.pyc src/**/**/*.pyc src/**/**/**/*.pyc src/**/**/**/**/*.pyc src/**/**/**/**/**/*.pyc src/**/**/**/**/**/**/*.pyc
rm client/*.pyc client/**/*pyc client/**/**/*.pyc

scp -r src nilm@10.0.1.31:~/nilm_cluster/
scp -r src nilm@10.0.1.32:~/nilm_cluster/
scp -r src nilm@10.0.1.33:~/nilm_cluster/
scp -r src nilm@10.0.1.34:~/nilm_cluster/
scp -r ./client/cluster/src/*.py nilm@10.0.1.31:~/nilm_cluster/client/cluster/src/
scp -r ./client/cluster/src/*.py nilm@10.0.1.32:~/nilm_cluster/client/cluster/src/
scp -r ./client/cluster/src/*.py nilm@10.0.1.33:~/nilm_cluster/client/cluster/src/
scp -r ./client/cluster/src/*.py nilm@10.0.1.34:~/nilm_cluster/client/cluster/src/
scp -r ./client/dl_stub/* nilm@10.0.1.31:~/nilm_cluster/client/dl_stub/
scp -r ./client/dl_stub/* nilm@10.0.1.32:~/nilm_cluster/client/dl_stub/
scp -r ./client/dl_stub/* nilm@10.0.1.33:~/nilm_cluster/client/dl_stub/
scp -r ./client/dl_stub/* nilm@10.0.1.34:~/nilm_cluster/client/dl_stub/
