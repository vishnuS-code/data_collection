import configparser

CONFIG_PATH = "/home/kniti/projects/knit-i/config/coreconfig.ini"

class Config:
    def __init__(self, path=CONFIG_PATH):
        self.config = configparser.ConfigParser()
        self.config.optionxform = str  # preserve case
        try:
            self.config.read(path)
        except Exception as e:
            print(f"Error reading config: {e}")

    def get(self, section, key, fallback=None):
        try:
            return self.config.get(section, key, fallback=fallback)
        except Exception:
            return fallback


# Create a global config object
config = Config()
