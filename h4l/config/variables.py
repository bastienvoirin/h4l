# coding: utf-8

"""
Definition of variables.
"""

import order as od

from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT

np = maybe_import("numpy")
ak = maybe_import("awkward")


def add_variables(config: od.Config) -> None:
    """
    Adds all variables to a *config* that are present after `ReduceEvents`
    without calling any producer
    """

    # (the "event", "run" and "lumi" variables are required for some cutflow plotting task,
    # and also correspond to the minimal set of columns that coffea's nano scheme requires)
    config.add_variable(
      name="event",
      expression="event",
      binning=(1, 0.0, 1.0e9),
      x_title="Event number",
      discrete_x=False,
    )
    config.add_variable(
      name="run",
      expression="run",
      binning=(1, 100000.0, 500000.0),
      x_title="Run number",
      discrete_x=True,
    )
    config.add_variable(
      name="lumi",
      expression="luminosityBlock",
      binning=(1, 0.0, 5000.0),
      x_title="Luminosity block",
      discrete_x=True,
    )
    config.add_variable(
      name="category_ids",
      expression="category_ids",
      binning=(20, 0, 100000),
      unit="",
      x_title="Event category",
    )

    #
    # Object properties
    #

    config.add_variable(
        name="n_jet",
        expression="n_jet",
        binning=(11, -0.5, 10.5),
        x_title="Number of jets",
        discrete_x=True,
    )
    config.add_variable(
        name="jets_pt",
        expression="Jet.pt",
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"$p_{T} of all jets$",
    )
    config.add_variable(
        name="muon_pt",
        expression="Muon.pt",
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"$p_{T} of all \mu$",
    )
    config.add_variable(
        name="jet1_pt",
        expression="Jet.pt[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"Jet 1 $p_{T}$",
    )
    config.add_variable(
        name="jet1_eta",
        expression="Jet.eta[:,0]",
        null_value=EMPTY_FLOAT,
        binning=(30, -3.0, 3.0),
        x_title=r"Jet 1 $\eta$",
    )
    config.add_variable(
        name="m4l",
        null_value=EMPTY_FLOAT,
        binning=(100, 0, 200.0),
        unit="GeV",
        x_title=r"$m_{4\ell}$",
    )
    config.add_variable(
        name="electron_pt",
        expression="Electron.pt",
        binning=(40, 0.0, 400.0),
        unit="GeV",
        x_title=r"$p_{T} of all e$",
    )
    config.add_variable(
        name="electron_bdt",
        expression="Electron.mvaFall17V2Iso",
        binning=(40, 0.0, 1.0),
        x_title=r"Electron ID BDT",
    )

    # TODO #
    # Task 3.
    # Add the following observables:
    # 1. Number of electrons in event
    config.add_variable(
        name="n_electron",
        expression=lambda events: ak.num(events.Electron["pt"], axis=1),
        aux={"inputs": {"Electron.pt"}},
        binning=(9, -0.5, 9.5),
        x_title="Number of electrons",
        discrete_x=True,
    )
    # 2. Number of muons in event
    config.add_variable(
        name="n_muon",
        expression=lambda events: ak.num(events.Muon["pt"], axis=1),
        aux={"inputs": {"Muon.pt"}},
        binning=(9, -0.5, 9.5),
        x_title="Number of muons",
        discrete_x=True,
    )
    # 3. Mass of 4l system in 118, 130 GeV
    config.add_variable(
        name="m4l_zoomed",
        expression="m4l",
        null_value = EMPTY_FLOAT,
        binning=(50.0, 100.0, 150.0),
        unit="GeV",
        x_title=r"$m_{4\ell}$ (zoomed)",
    )
    # (define a new observable called m4l_zoomed)
    # 4. Mass of Z1 boson
    config.add_variable(
        name="mz1",
        expression="mz1",
        null_value=EMPTY_FLOAT,
        binning=(100, 0.0, 200.0),
        unit="GeV",
        x_title=r"$m_{Z_1}$",
    )
    # 5. Mass of Z2 boson
    config.add_variable(
        name="mz2",
        expression="mz2",
        null_value=EMPTY_FLOAT,
        binning=(100, 0.0, 200.0),
        unit="GeV",
        x_title=r"$m_{Z_2}$",
    )
    # Hint: for some of these you can defined them directly here
    # Hint: some others need some modification/extension of production/default.py
    config.add_variable(
        name="mzz",
        expression="mzz",
        null_value=EMPTY_FLOAT,
        binning=(100, 50.0, 250.0),
        unit="GeV",
        x_title=r"$m_{ZZ}$",
    )
    config.add_variable(
        name="mzz_zoomed",
        expression="mzz",
        null_value = EMPTY_FLOAT,
        binning=(50.0, 100.0, 150.0),
        unit="GeV",
        x_title=r"$m_{ZZ}$ (zoomed)",
    )