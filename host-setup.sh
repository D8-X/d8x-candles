#!/bin/bash

# Minimal script to install required dependencies on host

if [[ $EUID != 0 ]]; then
    echo "run this script as root!"
    return 1
fi


# Update repos
apt-get update -y 

# Remove any outdated packages
for pkg in docker.io docker-doc docker-compose podman-docker containerd runc; do apt-get remove $pkg -y; done

apt-get install ca-certificates curl gnupg -y 

# Get docker apt repo key
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg


# Add docker repo to sources
echo \
  "deb [arch="$(dpkg --print-architecture)" signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  "$(. /etc/os-release && echo "$VERSION_CODENAME")" stable" | \
  tee /etc/apt/sources.list.d/docker.list > /dev/null


# Install docker
apt-get update -y
apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y

# Enable on startup and start
systemctl enable docker
systemctl restart docker



