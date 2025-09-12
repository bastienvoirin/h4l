# coding: utf-8

"""
Helpful utils.
"""

from __future__ import annotations

__all__ = ["IF_NANO_V9", "IF_NANO_V10"]

import re
import itertools
import time
from typing import Any, Hashable, Iterable, Callable
from functools import wraps, reduce, partial
import tracemalloc

import law

from columnflow.types import Any
from columnflow.columnar_util import ArrayFunction, deferred_column, get_ak_routes
from columnflow.util import maybe_import

np = maybe_import("numpy")
ak = maybe_import("awkward")
coffea = maybe_import("coffea")

_logger = law.logger.get_logger(__name__)


def masked_sorted_indices(mask: ak.Array, sort_var: ak.Array, ascending: bool = False) -> ak.Array:
  """
  Helper function to obtain the correct indices of an object mask
  """
  indices = ak.argsort(sort_var, axis=-1, ascending=ascending)
  return indices[mask[indices]]


def call_once_on_config(func=None, *, include_hash=False):
  """
  Parametrized decorator to ensure that function *func* is only called once for the config *config*.
  Can be used with or without parentheses.
  """
  if func is None:
    # If func is None, it means the decorator was called with arguments.
    def wrapper(f):
      return call_once_on_config(f, include_hash=include_hash)
    return wrapper

  @wraps(func)
  def inner(config, *args, **kwargs):
    tag = f"{func.__name__}_called"
    if include_hash:
      tag += f"_{func.__hash__()}"

    if config.has_tag(tag):
      return

    config.add_tag(tag)
    return func(config, *args, **kwargs)

  return inner

@deferred_column
def IF_NANO_V9(self, func: ArrayFunction) -> Any | set[Any]:
    return self.get() if func.config_inst.campaign.x.version == 9 else None


@deferred_column
def IF_NANO_V10(self, func: ArrayFunction) -> Any | set[Any]:
    return self.get() if func.config_inst.campaign.x.version >= 10 else None
