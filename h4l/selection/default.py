from operator import and_
from functools import reduce
from collections import defaultdict
from typing import Tuple

from columnflow.util import maybe_import

from columnflow.selection.stats import increment_stats
from columnflow.selection import Selector, SelectionResult, selector
from columnflow.selection.cms.met_filters import met_filters
from columnflow.selection.cms.json_filter import json_filter
from columnflow.selection.cms.jets import jet_veto_map

from columnflow.production.categories import category_ids
from columnflow.production.util import attach_coffea_behavior
from columnflow.production.cms.mc_weight import mc_weight
from columnflow.production.processes import process_ids

from h4l.selection.lepton import electron_selection, muon_selection
from h4l.selection.trigger import trigger_selection

np = maybe_import("numpy")
ak = maybe_import("awkward")


@selector(
    uses={
        "event",
        category_ids,
        attach_coffea_behavior, json_filter, mc_weight,
        electron_selection, muon_selection,
        trigger_selection,
        increment_stats, process_ids
    },
    produces={
        category_ids,
        attach_coffea_behavior, json_filter, mc_weight,
        electron_selection, muon_selection,
        trigger_selection,
        increment_stats, process_ids,
    },
    # sandbox=dev_sandbox("bash::$CF_BASE/sandboxes/venv_columnar.sh"),
    exposed=True,
)
def default(
    self: Selector,
    events: ak.Array,
    stats: defaultdict,
    **kwargs,
) -> tuple[ak.Array, SelectionResult]:
    # ensure coffea behaviors are loaded
    events = self[attach_coffea_behavior](events, **kwargs)
    events = self[category_ids](events, **kwargs)

    # add corrected mc weights
    if self.dataset_inst.is_mc:
        events = self[mc_weight](events, **kwargs)
    # TODO: Add weights

    # initialize `SelectionResult` object
    results = SelectionResult()

    # filter bad data events according to golden lumi mask
    if self.dataset_inst.is_data:
        events, json_filter_results = self[json_filter](events, **kwargs)
        results += json_filter_results

    # run trigger selection
    events, trigger_results = self[trigger_selection](events, call_force=True, **kwargs)
    results += trigger_results

    # run electron selection
    events, ele_results = self[electron_selection](events, call_force=True, **kwargs)
    results += ele_results

    # run muon selection
    events, muon_results = self[muon_selection](events, call_force=True, **kwargs)
    results += muon_results

    # get indices of selected leptons
    ele_idx = results.objects.Electron.Electron
    muon_idx = results.objects.Muon.Muon

    # count selected leptons
    n_ele = ak.num(events.Electron[ele_idx], axis=1)
    n_muon = ak.num(events.Muon[muon_idx], axis=1)

    # select events with at least four selected leptons
    results.steps["four_leptons"] = (n_ele + n_muon) >= 4

    # post selection build process IDs
    events = self[process_ids](events, **kwargs)

    # final event selection mask is AND of all selection steps
    results.event = reduce(and_, results.steps.values())
    results.event = ak.fill_none(results.event, False)

    weight_map = {
      "num_events": Ellipsis,
      "num_events_selected": results.event,
    }
    group_map = {}
    if self.dataset_inst.is_mc:
      weight_map = {
          **weight_map,
          # mc weight for all events
          "sum_mc_weight": (events.mc_weight, Ellipsis),
          "sum_mc_weight_selected": (events.mc_weight, results.event),
      }
      group_map = {
          # per process
          "process": {
              "values": events.process_id,
               "mask_fn": (lambda v: events.process_id == v),
          },
      }

    events, results = self[increment_stats](
        events,
        results,
        stats,
        weight_map=weight_map,
        group_map=group_map,
        **kwargs,
    )

    return events, results
