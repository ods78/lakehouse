"""JupyterHub with DockerSpawner — single-user notebooks on the lakehouse network."""
import os

c = get_config()  # noqa: F821

c.JupyterHub.spawner_class = "dockerspawner.DockerSpawner"
c.DockerSpawner.image = os.environ.get("JUPYTER_SINGLEUSER_IMAGE", "quay.io/jupyter/scipy-notebook:latest")
c.DockerSpawner.network_name = "lakehouse"
c.DockerSpawner.use_internal_ip = True
c.DockerSpawner.extra_host_config = {"auto_remove": True}
c.DockerSpawner.notebook_dir = "/home/jovyan"
# Shared scratch space across users/notebooks (named volume)
c.DockerSpawner.volumes = {"jupyter-shared": "/home/jovyan/work"}
c.DockerSpawner.environment = {
    "MLFLOW_TRACKING_URI": "http://mlflow:5000",
    "TRINO_HOST": "trino",
    "TRINO_PORT": "8080",
    "STARROCKS_HOST": "starrocks-fe",
    "STARROCKS_PORT": "9030",
}

# Auth: simple allow-list (swap to Keycloak OAuth for SSO — see docs/runbook.md)
c.JupyterHub.authenticator_class = "dummy"
allowed = {u.strip() for u in os.environ.get("JUPYTERHUB_ALLOWED_USERS", "admin").split(",") if u.strip()}
c.Authenticator.allowed_users = allowed
c.Authenticator.admin_users = allowed

# Hub networking inside the compose network
c.JupyterHub.hub_ip = "0.0.0.0"
c.JupyterHub.hub_connect_ip = "jupyterhub"
c.JupyterHub.db_url = "sqlite:////srv/jupyterhub/jupyterhub.sqlite"

c.JupyterHub.allow_named_servers = False
