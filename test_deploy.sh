#!/bin/bash
rm src/*.pyc src/**/*.pyc src/**/**/*.pyc src/**/**/**/*.pyc src/**/**/**/**/*.pyc src/**/**/**/**/**/*.pyc src/**/**/**/**/**/**/*.pyc

# Staging
scp -r VERSION ./src nilm@10.0.1.10:~/nilm_cluster/

scp -r ./client nilm@10.0.1.10:~/nilm_cluster/
