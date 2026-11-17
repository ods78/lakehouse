ARG JUPYTERHUB_VERSION=5.2.1
FROM jupyterhub/jupyterhub:${JUPYTERHUB_VERSION}

# DockerSpawner launches single-user notebooks as sibling containers
RUN pip install --no-cache-dir dockerspawner
