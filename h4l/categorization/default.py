# coding: utf-8

"""
H4L Categorization methods.
"""

from columnflow.categorization import Categorizer, categorizer
from columnflow.util import maybe_import

ak = maybe_import("awkward")


#
# categorizer functions used by categories definitions
#

@categorizer(uses={"event"})
def catid_incl(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
      # fully inclusive selection
      return events, ak.ones_like(events.event) == 1

@categorizer(uses={"event"}, call_force=True)
def catid_4e(self: Categorizer, events: ak.Array, **kwargs) -> tuple[ak.Array, ak.Array]:
      mask = (ak.num(events.Electron, axis=-1) == 4) & (ak.num(events.Muon, axis=-1) == 0)
      return events, mask
