import yaml
from yaml.loader import SafeLoader

# Open the file and load the file
with open("./infra/config/db/env_config.yaml") as f:
    db_config = yaml.load(f, Loader=SafeLoader)
