"""MTG inventory station: database, ManaPool sync, intake, picking, pricing and audit tools.

Run the command-line tools from the repository root, e.g. `python -m station.repricer run`.
"""

import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(ROOT, "config")
SET_BOX_MAP_CSV = os.path.join(CONFIG_DIR, "set_box_map.csv")
