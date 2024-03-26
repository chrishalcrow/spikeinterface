from __future__ import annotations
import numpy as np
import warnings

from .template import Templates
from .sparsity import _sparsity_doc
from .sortinganalyzer import SortingAnalyzer


# TODO make this function a non private function
def _get_dense_templates_array(one_object, return_scaled=True):
    if isinstance(one_object, Templates):
        templates_array = one_object.get_dense_templates()
    elif isinstance(one_object, SortingAnalyzer):
        ext = one_object.get_extension("templates")
        if ext is not None:
            templates_array = ext.data["average"]
            assert (
                return_scaled == ext.params["return_scaled"]
            ), f"templates have been extracted with return_scaled={not return_scaled} you cannot get then with return_scaled={return_scaled}"
        else:
            ext = one_object.get_extension("fast_templates")
            assert (
                return_scaled == ext.params["return_scaled"]
            ), f"fast_templates have been extracted with return_scaled={not return_scaled} you cannot get then with return_scaled={return_scaled}"
            if ext is not None:
                templates_array = ext.data["average"]
            else:
                raise ValueError("SortingAnalyzer need extension 'templates' or 'fast_templates' to be computed")
    else:
        raise ValueError("Input should be Templates or SortingAnalyzer or SortingAnalyzer")

    return templates_array


def _get_nbefore(one_object):
    if isinstance(one_object, Templates):
        return one_object.nbefore
    elif isinstance(one_object, SortingAnalyzer):
        ext = one_object.get_extension("templates")
        if ext is not None:
            return ext.nbefore
        ext = one_object.get_extension("fast_templates")
        if ext is not None:
            return ext.nbefore
        raise ValueError("SortingAnalyzer need extension 'templates' or 'fast_templates' to be computed")
    else:
        raise ValueError("Input should be Templates or SortingAnalyzer or SortingAnalyzer")


def get_template_amplitudes(
    templates_or_sorting_analyzer,
    peak_sign: "neg" | "pos" | "both" = "neg",
    mode: "extremum" | "at_index" = "extremum",
    return_scaled: bool = True,
):
    """
    Get amplitude per channel for each unit.

    Parameters
    ----------
    templates_or_sorting_analyzer: Templates | SortingAnalyzer
        A Templates or a SortingAnalyzer object
    peak_sign: "neg" | "pos" | "both", default: "neg"
        Sign of the template to compute best channels
    mode: "extremum" | "at_index", default: "extremum"
        "extremum":  max or min
        "at_index": take value at spike index
    return_scaled: bool, default True
        The amplitude is scaled or not.

    Returns
    -------
    peak_values: dict
        Dictionary with unit ids as keys and template amplitudes as values
    """
    assert peak_sign in ("both", "neg", "pos"), "'peak_sign' must be 'both', 'neg', or 'pos'"
    assert mode in ("extremum", "at_index"), "'mode' must be 'extremum' or 'at_index'"

    unit_ids = templates_or_sorting_analyzer.unit_ids

    templates_array = _get_dense_templates_array(templates_or_sorting_analyzer, return_scaled=return_scaled)

    if mode == "extremum":
        if peak_sign == "both":
            values = np.max(np.abs(templates_array), axis=1)
        elif peak_sign == "neg":
            values = -np.min(templates_array, axis=1)
        elif peak_sign == "pos":
            values = np.max(templates_array, axis=1)
    elif mode == "at_index":
        before = _get_nbefore(templates_or_sorting_analyzer)
        if peak_sign == "both":
            values = np.abs(templates_array[:,before, :])
        elif peak_sign == "neg":
            values = -templates_array[:,before, :]
        elif peak_sign == "pos":
            values = templates_array[:,before, :]

    return dict(zip(unit_ids, values))



def get_template_extremum_channel(
    templates_or_sorting_analyzer,
    peak_sign: "neg" | "pos" | "both" = "neg",
    mode: "extremum" | "at_index" = "extremum",
    outputs: "id" | "index" = "id",
):
    """
    Compute the channel with the extremum peak for each unit.

    Parameters
    ----------
    templates_or_sorting_analyzer: Templates | SortingAnalyzer
        A Templates or a SortingAnalyzer object
    peak_sign: "neg" | "pos" | "both", default: "neg"
        Sign of the template to compute best channels
    mode: "extremum" | "at_index", default: "extremum"
        "extremum":  max or min
        "at_index": take value at spike index
    outputs: "id" | "index", default: "id"
        * "id": channel id
        * "index": channel index

    Returns
    -------
    extremum_channels: dict
        Dictionary with unit ids as keys and extremum channels (id or index based on "outputs")
        as values
    """
    assert peak_sign in ("both", "neg", "pos")
    assert mode in ("extremum", "at_index")
    assert outputs in ("id", "index")

    unit_ids = templates_or_sorting_analyzer.unit_ids
    channel_ids = templates_or_sorting_analyzer.channel_ids

    peak_values = get_template_amplitudes(templates_or_sorting_analyzer, peak_sign=peak_sign, mode=mode)
    extremum_channels_id = {}
    extremum_channels_index = {}
    for unit_id in unit_ids:
        max_ind = np.argmax(peak_values[unit_id])
        extremum_channels_id[unit_id] = channel_ids[max_ind]
        extremum_channels_index[unit_id] = max_ind

    if outputs == "id":
        return extremum_channels_id
    elif outputs == "index":
        return extremum_channels_index


def get_template_extremum_channel_peak_shift(templates_or_sorting_analyzer, peak_sign: "neg" | "pos" | "both" = "neg"):
    """
    In some situations spike sorters could return a spike index with a small shift related to the waveform peak.
    This function estimates and return these alignment shifts for the mean template.
    This function is internally used by `compute_spike_amplitudes()` to accurately retrieve the spike amplitudes.

    Parameters
    ----------
    templates_or_sorting_analyzer: Templates | SortingAnalyzer
        A Templates or a SortingAnalyzer object
    peak_sign: "neg" | "pos" | "both", default: "neg"
        Sign of the template to compute best channels

    Returns
    -------
    shifts: dict
        Dictionary with unit ids as keys and shifts as values
    """
    unit_ids = templates_or_sorting_analyzer.unit_ids
    channel_ids = templates_or_sorting_analyzer.channel_ids
    nbefore = _get_nbefore(templates_or_sorting_analyzer)

    extremum_channels_ids = get_template_extremum_channel(templates_or_sorting_analyzer, peak_sign=peak_sign)

    shifts = {}

    templates_array = _get_dense_templates_array(templates_or_sorting_analyzer)

    for unit_ind, unit_id in enumerate(unit_ids):
        template = templates_array[unit_ind, :, :]

        chan_id = extremum_channels_ids[unit_id]
        chan_ind = list(channel_ids).index(chan_id)

        if peak_sign == "both":
            peak_pos = np.argmax(np.abs(template[:, chan_ind]))
        elif peak_sign == "neg":
            peak_pos = np.argmin(template[:, chan_ind])
        elif peak_sign == "pos":
            peak_pos = np.argmax(template[:, chan_ind])
        shift = peak_pos - nbefore
        shifts[unit_id] = shift

    return shifts


def get_template_extremum_amplitude(
    templates_or_sorting_analyzer,
    peak_sign: "neg" | "pos" | "both" = "neg",
    mode: "extremum" | "at_index" = "at_index",
):
    """
    Computes amplitudes on the best channel.

    Parameters
    ----------
    templates_or_sorting_analyzer: Templates | SortingAnalyzer
        A Templates or a SortingAnalyzer object
    peak_sign:  "neg" | "pos" | "both"
        Sign of the template to compute best channels
    mode: "extremum" | "at_index", default: "at_index"
        Where the amplitude is computed
        "extremum":  max or min
        "at_index": take value at spike index

    Returns
    -------
    amplitudes: dict
        Dictionary with unit ids as keys and amplitudes as values
    """
    assert peak_sign in ("both", "neg", "pos"), "'peak_sign' must be  'neg' or 'pos' or 'both'"
    assert mode in ("extremum", "at_index"), "'mode' must be 'extremum' or 'at_index'"
    unit_ids = templates_or_sorting_analyzer.unit_ids
    channel_ids = templates_or_sorting_analyzer.channel_ids

    extremum_channels_ids = get_template_extremum_channel(templates_or_sorting_analyzer, peak_sign=peak_sign, mode=mode)

    extremum_amplitudes = get_template_amplitudes(templates_or_sorting_analyzer, peak_sign=peak_sign, mode=mode)

    unit_amplitudes = {}
    for unit_id in unit_ids:
        channel_id = extremum_channels_ids[unit_id]
        best_channel = list(channel_ids).index(channel_id)
        unit_amplitudes[unit_id] = extremum_amplitudes[unit_id][best_channel]

    return unit_amplitudes
