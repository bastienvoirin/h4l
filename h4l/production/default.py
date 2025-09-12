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
        "process_id",
    },
    produces={
        attach_coffea_behavior,
        deterministic_seeds,
        category_ids, normalization_weights,
        four_lep_invariant_mass,
        "process_id"
    }
)
def default(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    # Build categories
    events = self[category_ids](events, **kwargs)

    # deterministic seeds
    events = self[deterministic_seeds](events, **kwargs)

    if self.dataset_inst.is_mc:
        # normalization weights
        events = self[normalization_weights](events, **kwargs)

    events = self[four_lep_invariant_mass](events, **kwargs)

    return events

