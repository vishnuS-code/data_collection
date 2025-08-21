import configparser

CONFIG_PATH = "/home/kniti/projects/knit-i/config/coreconfig.ini"

class Config:
    def __init__(self, path=CONFIG_PATH):
        self.config = configparser.ConfigParser()
        self.config.optionxform = str  # preserve case
        try:
            with open(path, "r") as f:
                lines = f.readlines()

            core_lines = []
            core_started = False
            for line in lines:
                stripped = line.strip()
                if stripped.startswith("[Core]"):
                    core_started = True
                    core_lines.append(line)
                elif stripped.startswith("[") and core_started:
                    break
                elif core_started and stripped and not stripped.startswith(("#", ";")):
                    core_lines.append(line)

            if core_lines:
                self.config.read_string("".join(core_lines))
        except Exception as e:
            print(f"Error reading config: {e}")

    def get(self, section, key, fallback=None):
        return self.config.get(section, key, fallback=fallback)


config = Config()
