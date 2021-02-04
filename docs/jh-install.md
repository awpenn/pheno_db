# Install on TLJH (for dev only)

## Docker CE: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04
    ```
    sudo apt install apt-transport-https ca-certificates curl software-properties-common

    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu bionic stable"

    sudo apt update

    apt-cache policy docker-ce

    sudo apt install docker-ce
    ```

## install tljh: https://tljh.jupyter.org/en/latest/contributing/dev-setup.html
    ```
    git clone https://github.com/jupyterhub/the-littlest-jupyterhub.git
    
    docker build -t tljh-systemd . -f integration-tests/Dockerfile 

    docker run \
    --privileged \
    --detach \
    --name=tljh-dev \
    --publish 12000:80 \
    --mount type=bind,source=$(pwd),target=/srv/src \
    tljh-systemd

    docker exec -it tljh-dev /bin/bash

    python3 /srv/src/bootstrap/bootstrap.py --admin admin:password
    ```
- now it's running at https://your.url:12000

## setup pheno_db app
- clone repo and install depos
```
git clone https://github.com/awpenn/pheno_db.git

cd pheno_db

bash setup.sh
```
- config .env
```
cp .env-template .env

vi .env

--fill out the connection vars--
```

( still need to test out the last part, got env set up but couldnt connect to db.  Think it was because was a droplet trying to reach a protected IP (the db) )