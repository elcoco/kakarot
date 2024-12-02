import json
import yaml
import os, sys
import logging
from pprint import pprint
from pathlib import Path
from typing import Any, Optional
import copy

from types import SimpleNamespace

logger = logging.getLogger('mona')

class ConfigException(Exception): pass

class NestedNamespace(SimpleNamespace):
    def __init__(self, dictionary: dict[str,Any], **kwargs):
        super().__init__(**kwargs)
        self.__dict__['_config'] = dictionary
        self.update(self._config)

    def update(self, d: dict[str,Any]):
        for key, value in d.items():
            if isinstance(value, dict):
                super().__setattr__(key, NestedNamespace(value))
            else:
                super().__setattr__(key, value)

    def __setattr__(self, key: str, value: Any):
        self.__dict__['_config'][key] = value
        self.update(self._config)

    def get(self, k: str, default: Any):
        if k in self.keys():
            return super().__getattribute__(k)
        return default

    def keys(self):
        return self._config.keys()

    def values(self):
        objs = []
        for a in dir(super()):
            if not a.startswith("__") and not a == '_config':
                objs.append(super().__getattribute__(a))

        return objs

    def items(self):
        items = []
        for a in dir(super()):
            if not a.startswith("__") and not a == '_config':
                items.append([a, super().__getattribute__(a)])

        return items

    def __iter__(self):
        objs = []
        for a in dir(super()):
            if not a.startswith("__") and not a == '_config':
                objs.append(super().__getattribute__(a))

        return iter(objs)


class Config(NestedNamespace):
    def __init__(self, path: Optional[Path]=None, **kwargs):
        super().__init__({}, **kwargs)

        # we can't directly assign because that would trigger __setattr__
        if path:
            self.__dict__['_config_path'] = path
        else:
            self.__dict__['_config_path'] = (Path.home() / '.config' / Path(__file__).name).with_suffix('') / 'config.yaml'

    def set_path(self, path: Path):
        self.__dict__['_config_path'] = path

    def configfile_exists(self):
        return self._config_path.is_file()

    def dict_deep_merge(self, d1: dict[str,Any], d2: dict[str,Any]):
        """ deep merge two dicts """
        #dm = d1.copy()
        dm = copy.deepcopy(d1)
        for k,v in d2.items():
            if k in dm.keys() and type(v) == dict:
                dm[k] = self.dict_deep_merge(dm[k], d2[k])
            elif k in dm.keys() and type(v) == list:
                dm[k] += v
            else:
                dm[k] = v
        return dm

    def load(self, path: Optional[Path]=None, merge: bool=True):
        if path == None:
            path = self._config_path

        try:
            with open(path, 'r') as configfile:
                cfg = yaml.safe_load(configfile)

                if not cfg:
                    return

                if merge:
                    self.__dict__['_config'] = self.dict_deep_merge(self._config, cfg)
                else:
                    self.__dict__['_config'] = copy.deepcopy(cfg)

                # update attributes
                self.update(self._config)

                logger.debug(f"Loaded config file, path={path}")
            return True
        except yaml.YAMLError as e:
            raise ConfigException(f"Failed to load YAML in config file: {path}\n{e}")
        except FileNotFoundError as e:
            raise ConfigException(f"Config file doesn't exist: {path}\n{e}")

    def load_dict(self, d: dict[str,Any], merge: bool=True):
        if merge:
            self.__dict__['_config'] = self.dict_deep_merge(self._config, d)
        else:
            self.__dict__['_config'] = d

        # update attributes
        self.update(self._config)

        logger.debug(f"Loaded from dict")

    def write(self, path: Optional[Path]=None, commented: Optional[str]=None):
        if path == None:
            path = self._config_path

        if not path.parent.is_dir():
            path.parent.mkdir()
            logger.info(f"Created directory: {path}")

        with open(path, 'w') as outfile:
            try:
                yaml.dump(self._config, outfile, default_flow_style=False)
                logger.info(f"Wrote config to: {path}")
            except yaml.YAMLError as e:
                raise ConfigException(f"Failed to write YAML in config file: {path}, message={e}")

        # comment the config file that was just written by libYAML
        if commented:
            lines = [f"#{x}" for x in path.read_text().split('\n')]
            path.write_text('\n'.join(lines))
