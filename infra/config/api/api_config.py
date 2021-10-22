import yaml
from yaml.loader import SafeLoader

# Open the file and load the file
with open("./infra/config/api/api_config.yaml") as f:
    api_config = yaml.load(f, Loader=SafeLoader)
