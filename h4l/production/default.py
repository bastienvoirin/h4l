# coding: utf-8

"""
Column production methods related to higher-level features.
"""

import functools

from columnflow.production import Producer, producer
from columnflow.production.categories import category_ids
from columnflow.production.normalization import normalization_weights
from columnflow.production.util import attach_coffea_behavior
from columnflow.production.cms.seeds import deterministic_seeds
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column
# TODO #
# Task 1.1
# Add electrons and muons SFs
# Start by uncommenting these lines and
# add electron_weights, muon_weights below to uses and produces:
from columnflow.production.cms.electron import electron_weights
from columnflow.production.cms.muon import muon_weights


from h4l.production.invariant_mass import four_lep_invariant_mass

ak = maybe_import("awkward")
coffea = maybe_import("coffea")
np = maybe_import("numpy")
maybe_import("coffea.nanoevents.methods.nanoaod")

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@producer(
    uses={
        attach_coffea_behavior,
        deterministic_seeds,
        category_ids, normalization_weights,
        four_lep_invariant_mass,
        electron_weights,
        muon_weights,
        "process_id",
    },
    produces={
        attach_coffea_behavior,
        deterministic_seeds,
        category_ids, normalization_weights,
        four_lep_invariant_mass,
        electron_weights,
        muon_weights,
        "process_id"
    }
)
def default(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # Build categories
    events = self[category_ids](events, **kwargs)

    # deterministic seeds
    events = self[deterministic_seeds](events, **kwargs)

    events = self[four_lep_invariant_mass](events, **kwargs)

    if self.dataset_inst.is_mc:
        # normalization weights
        events = self[normalization_weights](events, **kwargs)

        # TODO #
        # Task 1.1
        # Add electrons and muons SFs
        # Hint: the json in columnflow contain maps from pT > 15 GeV
        # Hint: call 
        # events = self[electron_weights](events_high_pt, **kwargs)
        # events = self[muon_weights](events_muon_high_pt, **kwargs)
        # but first you need to define events_high_pt and events_muon_high_pt
        # as those events where only leptons with pT > 15 GeV are saved
        # Hint: use the ak.with_field() method
        # Hint: the following gives you a skeleton on top of which to build
        el_high_pt_mask = events.Electron.pt > 15 # define mask for electrons
        events_el_high_pt = ak.with_field(events, events.Electron[el_high_pt_mask], "Electron")
        events_el_high_pt = self[electron_weights](events_el_high_pt, **kwargs)
        for postfix in ["", "_up", "_down"]:
            colname = f"{electron_weights.weight_name}{postfix}"
            if hasattr(events_el_high_pt, colname):
                sf_ele = getattr(events_el_high_pt, colname)
                events = set_ak_column(events, colname, sf_ele, value_type=np.float32)
        # Now repeat for muons
        mu_high_pt_mask = events.Muon.pt > 15 # define mask for electrons
        events_mu_high_pt = ak.with_field(events, events.Muon[mu_high_pt_mask], "Muon")
        events_mu_high_pt = self[muon_weights](events_mu_high_pt, **kwargs)
        for postfix in ["", "_up", "_down"]:
           colname = f"{muon_weights.weight_name}{postfix}"
           if hasattr(events_mu_high_pt, colname):
               sf_muo = getattr(events_mu_high_pt, colname)
               events = set_ak_column(events, colname, sf_muo, value_type=np.float32)
        # Apply the electron and muon SFs
        events = self[electron_weights](events_el_high_pt, **kwargs)
        events = self[muon_weights](events_mu_high_pt, **kwargs)

    return events

