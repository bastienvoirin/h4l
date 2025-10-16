# coding: utf-8

import functools
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, set_ak_column
from columnflow.production.util import attach_coffea_behavior

# Task 3.
# Produce variables for ZZ, Z1, Z2
# Hint: import the following
# First you need to define build_4sf in util.py
from h4l.util import build_2e2mu, build_4sf

np = maybe_import("numpy")
ak = maybe_import("awkward")

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)


@producer(
    uses=(
        {
            f"{field}.{var}"
            for field in ["Electron", "Muon"]
            for var in ["pt", "mass", "eta", "phi", "charge"]
        } | {
            attach_coffea_behavior,
        }
    ),
    produces={
        "m4l", "mz1", "mz2", "mzz",
    },
)
def four_lep_invariant_mass(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
    """
    Construct four-lepton invariant mass given the Electron and Muon arrays.
    """

    # attach coffea behavior for four-vector arithmetic
    events = self[attach_coffea_behavior](
        events,
        collections=["Electron", "Muon"],
        **kwargs,
    )

    # four-vector sum of first four elements of each
    # lepton collection (possibly fewer)
    dielectron = events.Electron[:, :4].sum(axis=1)
    dimuon = events.Muon[:, :4].sum(axis=1)

    # sum the results to form the four-lepton four-vector
    # TODO # 
    # Task 3.
    # Produce variables for ZZ, Z1, Z2
    # Hint: drop the fourlep and do the correct definition
    # Hint: Build 2e2mu, 4e, 4mu separately and then
    # Hint: zz_inclusive = ak.concatenate([zz_2e2mu, zz_4e, zz_4mu], axis=1)
    # Hint: ak.firsts(zz_inclusive.zz.mass) could be useful
    electrons_plus = events.Electron[events.Electron.charge > 0]
    electrons_minus = events.Electron[events.Electron.charge < 0]
    muons_plus = events.Muon[events.Muon.charge > 0]
    muons_minus = events.Muon[events.Muon.charge < 0]

    # Build Z1, Z2 and ZZ candidates for 4e, 2e2mu, 4mu channels
    mix_z1, mix_z2, mix_zz = ak.unzip(build_2e2mu(muons_plus, muons_minus, electrons_plus, electrons_minus))
    ele_z1, ele_z2, ele_zz = ak.unzip(build_4sf(electrons_plus, electrons_minus))
    muo_z1, muo_z2, muo_zz = ak.unzip(build_4sf(muons_plus, muons_minus))

    # Build Z1, Z2, ZZ candidates
    z1 = ak.concatenate([mix_z1, ele_z1, muo_z1], axis=1)
    z2 = ak.concatenate([mix_z2, ele_z2, muo_z2], axis=1)
    zz = ak.concatenate([mix_zz, ele_zz, muo_zz], axis=1)

    events = set_ak_column_f32(events, "mz1", ak.firsts(z1.mass))
    events = set_ak_column_f32(events, "mz2", ak.firsts(z2.mass))
    events = set_ak_column_f32(events, "mzz", ak.firsts(zz.mass))

    fourlep = dielectron + dimuon

    # total number of leptons per event
    n_leptons = (
        ak.num(events.Electron, axis=1) +
        ak.num(events.Muon, axis=1)
    )

    # four-lepton mass, taking into account only events with at least four leptons,
    # and otherwise substituting a predefined EMPTY_FLOAT value
    fourlep_mass = ak.where(
        n_leptons >= 4,
        fourlep.mass,
        EMPTY_FLOAT,
    )

    # write out the resulting mass to the `events` array,
    events = set_ak_column_f32(
        events,
        "m4l",
        fourlep_mass,
    )

    # return the events
    return events
