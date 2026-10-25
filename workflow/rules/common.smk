"""What every source shares. Included FIRST, before any source rules file.

Adding a source means three things, and nothing in the Snakefile beyond one
`include:`:

  1. a config block of its own, `worldbank:` next to `comtrade:`
  2. a `plan(cfg, run_id)` in its `<source>_common.py`, returning at least
     `{'params', 'stem', 'out', 'formats', 'csv_sep', 'targets'}`
  3. a `rules/<source>.smk` that calls it and does `TARGETS += ...`

Division of labour across the three kinds of file:

  .py        logic. Importable, therefore testable by a `_demo()`.
  .smk       wiring. Paths and rules. Not importable, so keep it thin.
  Snakefile  assembly. Include order and the default target.
"""

import sys
from datetime import datetime
from pathlib import Path

# Makes the modules in workflow/scripts/ importable by the rules files and by
# the `script:` files. MUST come before any `from <module> import ...` in an
# included file, which is why this file is included first.
sys.path.insert(0, str(Path(workflow.basedir) / "scripts"))

# Run identifier, SHARED by every source: one invocation, one run id.
# Omitted, it is the current timestamp, so every invocation produces new files
# and nothing is ever overwritten.
#   snakemake                                   -> new run
#   snakemake --config run_id=20261024T143000   -> targets an existing run, so
#                                                  reproducible, and the
#                                                  downstream rules are skipped
#
# Read at the TOP level of the config, not inside a source block. That is
# deliberate and it is why `--config run_id=...` works at all: --config only
# ever writes top-level keys (see CLAUDE.md). It is the one override meant to
# be used from the command line; everything else is changed by editing
# config/config.yaml.
RUN_ID = str(config.get("run_id") or datetime.now().strftime("%Y%m%dT%H%M%S"))

# Filled by each source's rules file, consumed by `rule all` in the Snakefile.
TARGETS = []
