# coding: utf-8

"""
Configuration of the h4l analysis.
"""

import law
import order as od
from scinum import Number

from columnflow.config_util import (
    get_root_processes_from_campaign, add_shift_aliases, add_category, verify_config_processes,
)
from columnflow.columnar_util import EMPTY_FLOAT, ColumnCollection, skip_column
from columnflow.util import DotDict, maybe_import

ak = maybe_import("awkward")


#
# the main analysis object
#

analysis_h4l = ana = od.Analysis(
    name="analysis_h4l",
    id=1,
)

# analysis-global versions
# (see cfg.x.versions below for more info)
ana.x.versions = {}

# files of bash sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
ana.x.bash_sandboxes = ["$CF_BASE/sandboxes/cf.sh"]
default_sandbox = law.Sandbox.new(law.config.get("analysis", "default_columnar_sandbox"))
if default_sandbox.sandbox_type == "bash" and default_sandbox.name not in ana.x.bash_sandboxes:
    ana.x.bash_sandboxes.append(default_sandbox.name)

# files of cmssw sandboxes that might be required by remote tasks
# (used in cf.HTCondorWorkflow)
ana.x.cmssw_sandboxes = [
    "$CF_BASE/sandboxes/cmssw_default.sh",
]

# config groups for conveniently looping over certain configs
# (used in wrapper_factory)
ana.x.config_groups = {}

# named function hooks that can modify store_parts of task outputs if needed
ana.x.store_parts_modifiers = {}

# histogramming hooks, invoked before creating plots when --hist-hook parameter set
ana.x.hist_hooks = {}


#
# setup configs
#

# an example config is setup below, based on cms NanoAOD v9 for Run2 2017, focussing on
# ttbar and single top MCs, plus single muon data
# update this config or add additional ones to accomodate the needs of your analysis

from cmsdb.campaigns.run2_2017_nano_v9 import campaign_run2_2017_nano_v9

from h4l.config.config_das import add_das_config

add_das_config(
    analysis=analysis_h4l,
    campaign=campaign_run2_2017_nano_v9.copy(),
    config_name=campaign_run2_2017_nano_v9.name,
    config_id=1
)

add_das_config(
    analysis=analysis_h4l,
    campaign=campaign_run2_2017_nano_v9.copy(),
    config_name=f"{campaign_run2_2017_nano_v9.name}_limited",
    config_id=2,
    limit_dataset_files=2
)
