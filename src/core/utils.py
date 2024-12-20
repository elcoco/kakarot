import logging
from datetime import datetime, timezone

logger = logging.getLogger('kakarot')

CATEGORY_LEN = 10

def colorize(string: str, color: str) -> str:
    colors = {}
    colors['black']    = '\033[0;30m' # ]  neovim formatting fix
    colors['bblack']   = '\033[1;30m' # ]
    colors['red']      = '\033[0;31m' # ]
    colors['bred']     = '\033[1;31m' # ]
    colors['green']    = '\033[0;32m' # ]
    colors['bgreen']   = '\033[1;32m' # ]
    colors['yellow']   = '\033[0;33m' # ]
    colors['byellow']  = '\033[1;33m' # ]
    colors['blue']     = '\033[0;34m' # ]
    colors['bblue']    = '\033[1;34m' # ]
    colors['magenta']  = '\033[0;35m' # ]
    colors['bmagenta'] = '\033[1;35m' # ]
    colors['cyan']     = '\033[0;36m' # ]
    colors['bcyan']    = '\033[1;36m' # ]
    colors['white']    = '\033[0;37m' # ]
    colors['bwhite']   = '\033[1;37m' # ]
    colors['reset']    = '\033[0m' # ]
    colors['default']    = '\033[0m' # ]
    return colors[color] + str(string) + colors["reset"]

def info(category: str, action: str, msg: str):
    """ Display pretty messages """
    category = f"{category:{CATEGORY_LEN}}"
    action = f"{action:{CATEGORY_LEN}}"

    category = colorize(category, 'blue')
    action = colorize(action, 'magenta')
    #msg = colorize(msg, 'default')
    dt = colorize(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "green")
    logger.info(f"{category} {action} {msg}")

def error(category: str, action: str, msg: str):
    """ Display pretty messages """
    category = f"{category:{CATEGORY_LEN}}"
    action = f"{action:{CATEGORY_LEN}}"

    category = colorize(category, 'red')
    action = colorize(action, 'magenta')
    #msg = colorize(msg, 'default')
    dt = colorize(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "green")
    logger.error(f"{category} {action} {msg}")

def debug(category: str, action: str, msg: str):
    """ Display pretty messages """
    category = colorize(category, 'green')
    action = colorize(action, 'magenta')
    #msg = colorize(msg, 'default')
    dt = colorize(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "green")
    logger.debug(f"{category} {action} {msg}")

def id_to_str(uuid: int, ip: str, port: int, keyspace: int):
    uuid_fmt = f"{uuid:5}"
    #uuid_fmt = f"{uuid:{int(keyspace/4)}x}"   # to hex
    port_fmt = f"{port:5}"
    uuid_fmt = colorize(uuid_fmt, "bgreen")
    port_fmt = colorize(port_fmt, "bblue")
    ip_fmt   = colorize(ip, "magenta")
    at = colorize("@", "bblue")
    colon = colorize(":", "bgreen")
    return f"{uuid_fmt}{at}{ip_fmt}{colon}{port_fmt}"
