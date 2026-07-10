import numpy as np

from spikeinterface.core.node_pipeline import (
    find_parent_of_type,
    WaveformsNode,
)


from spikeinterface.sortingcomponents.peak_selection import select_peaks
from spikeinterface.sortingcomponents.peak_detection import detect_peaks

from spikeinterface.core import BaseRecording

from spikeinterface.core.core_tools import ms_to_samples
from spikeinterface.core.waveform_tools import extract_waveforms_to_single_buffer

from spikeinterface.postprocessing.unit_locations import dtype_localize_by_method

from .base import LocalizeBase


class DartSortLocalize(LocalizeBase):
    """Localize peaks using DartSort blah blah"""

    name = "dartsort_localize"
    params_doc = """
    model:
    """

    def __init__(
        self,
        recording,
        parents,
        localization_model=None,
        localization_model_path=None,
        return_output=True,
        radius_um=75.0,
        feature="ptp",
    ):
        import torch

        LocalizeBase.__init__(self, recording, return_output=return_output, parents=parents, radius_um=radius_um)
        assert localization_model is not None or localization_model_path is not None

        assert not ((localization_model_path is not None) and (localization_model is not None))

        if localization_model_path is not None:
            self.localization_model = torch.load(localization_model_path)
        else:
            self.localization_model = localization_model

        self.localizer_dtype = next(self.localization_model.parameters()).dtype
        self._dtype = np.dtype(dtype_localize_by_method["dartsort_localization"])

        # Find waveform extractor in the parents
        waveform_extractor = find_parent_of_type(self.parents, WaveformsNode)
        if waveform_extractor is None:
            raise TypeError(f"{self.name} should have a single {WaveformsNode.__name__} in its parents")

        self.nbefore = waveform_extractor.nbefore
        self._kwargs.update(dict(feature=feature))

    def compute(self, traces, peaks, waveforms):

        from torch import from_numpy

        channels = peaks["channel_index"]

        waveforms_torch = from_numpy(waveforms)

        if waveforms_torch.dtype != self.localizer_dtype:
            waveforms_torch = waveforms_torch.to(self.localizer_dtype)

        results_2d = self.localization_model.transform(
            waveforms=waveforms_torch,
            channels=channels,
        )

        peak_locations = results_2d["point_source_localizations"].numpy()

        return peak_locations


def fit_dartsort_localizer_from_recording(
    recording: BaseRecording,
    peaks,
    n_peaks=10_000,
    radius_um=50.0,
    ms_before=1,
    ms_after=2,
):
    """ """

    selected_peaks = select_peaks(peaks, method="uniform", n_peaks=n_peaks)

    num_channels = recording.get_num_channels()

    unit_ids = np.arange(num_channels, dtype="int64")

    channel_locations = recording.get_channel_locations()
    distances = np.linalg.norm(channel_locations[:, np.newaxis] - channel_locations[np.newaxis, :], axis=2)

    mask = np.zeros((num_channels, num_channels), dtype="bool")
    distances = np.linalg.norm(channel_locations[:, np.newaxis] - channel_locations[np.newaxis, :], axis=2)
    for channel_index, _ in enumerate(channel_locations):
        (chan_inds,) = np.nonzero(distances[channel_index, :] <= radius_um)
        mask[channel_index, chan_inds] = True

    spikes = np.zeros(
        selected_peaks.size, dtype=[("sample_index", "int64"), ("unit_index", "int64"), ("segment_index", "int64")]
    )
    spikes["sample_index"] = selected_peaks["sample_index"]
    spikes["unit_index"] = selected_peaks["channel_index"]
    spikes["segment_index"] = selected_peaks["segment_index"]

    nbefore = ms_to_samples(ms_before, recording.sampling_frequency)
    nafter = ms_to_samples(ms_after, recording.sampling_frequency)

    all_wfs = extract_waveforms_to_single_buffer(
        recording,
        spikes,
        unit_ids,
        nbefore,
        nafter,
        mode="shared_memory",
        return_in_uV=False,
        dtype="float32",
        sparsity_mask=mask,
        copy=True,
        verbose=False,
        job_name="extract_waveforms",
    )

    neighboring_channels = [
        recording.ids_to_indices(recording.channel_ids[one_channel_mask]) for one_channel_mask in mask
    ]
    peak_channel_indices = selected_peaks["channel_index"]

    localizer = fit_dartsort_localizer_from_waveforms(
        waveforms=all_wfs,
        channel_locations=channel_locations,
        neighboring_channels=neighboring_channels,
        ms_before=ms_before,
        ms_after=ms_after,
        peak_channel_indices=peak_channel_indices,
        radius_um=radius_um,
        recording=recording,
    )

    return localizer


def fit_dartsort_localizer_from_waveforms(
    waveforms, channel_locations, neighboring_channels, ms_before, ms_after, peak_channel_indices, radius_um, recording
):
    """ """

    from dartsort.transform.amortized_localization import AmortizedLocalization
    from dartsort.util.internal_config import ComputationConfig
    from dartsort.util.internal_config import WaveformConfig
    import torch

    computation_cfg = ComputationConfig()

    waveform_cfg = WaveformConfig(ms_before=ms_before, ms_after=ms_after)
    amortized_localization = AmortizedLocalization(
        channel_index=neighboring_channels, geom=channel_locations, waveform_cfg=waveform_cfg, radius=radius_um
    )
    amortized_localization.fit(
        recording=recording,
        waveforms=torch.from_numpy(waveforms),
        computation_cfg=computation_cfg,
        channels=torch.from_numpy(peak_channel_indices),
    )

    return amortized_localization
