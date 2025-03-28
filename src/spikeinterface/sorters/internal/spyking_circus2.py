from __future__ import annotations

from .si_based import ComponentsBasedSorter

import shutil
import numpy as np

from spikeinterface.core import NumpySorting
from spikeinterface.core.job_tools import fix_job_kwargs, split_job_kwargs
from spikeinterface.core.recording_tools import get_noise_levels
from spikeinterface.core.template import Templates
from spikeinterface.core.waveform_tools import estimate_templates
from spikeinterface.preprocessing import common_reference, whiten, bandpass_filter, correct_motion
from spikeinterface.sortingcomponents.tools import (
    cache_preprocessing,
    get_prototype_and_waveforms_from_recording,
    get_shuffled_recording_slices,
)
from spikeinterface.core.basesorting import minimum_spike_dtype
from spikeinterface.core.sparsity import compute_sparsity


class Spykingcircus2Sorter(ComponentsBasedSorter):
    sorter_name = "spykingcircus2"

    _default_params = {
        "general": {"ms_before": 2, "ms_after": 2, "radius_um": 75},
        "sparsity": {"method": "snr", "amplitude_mode": "peak_to_peak", "threshold": 1},
        "filtering": {"freq_min": 150, "freq_max": 7000, "ftype": "bessel", "filter_order": 2, "margin_ms": 10},
        "whitening": {"mode": "local", "regularize": False},
        "detection": {"peak_sign": "neg", "detect_threshold": 5},
        "selection": {
            "method": "uniform",
            "n_peaks_per_channel": 5000,
            "min_n_peaks": 100000,
            "select_per_channel": False,
            "seed": 42,
        },
        "apply_motion_correction": True,
        "motion_correction": {"preset": "dredge_fast"},
        "merging": {"max_distance_um": 50},
        "clustering": {"legacy": True},
        "matching": {"method": "circus-omp-svd"},
        "apply_preprocessing": True,
        "matched_filtering": True,
        "cache_preprocessing": {"mode": "memory", "memory_limit": 0.5, "delete_cache": True},
        "multi_units_only": False,
        "job_kwargs": {"n_jobs": 0.5},
        "seed": 42,
        "debug": False,
    }

    handle_multi_segment = True

    _params_description = {
        "general": "A dictionary to describe how templates should be computed. User can define ms_before and ms_after (in ms) \
                                        and also the radius_um used to be considered during clustering",
        "sparsity": "A dictionary to be passed to all the calls to sparsify the templates",
        "filtering": "A dictionary for the high_pass filter to be used during preprocessing",
        "whitening": "A dictionary for the whitening option to be used during preprocessing",
        "detection": "A dictionary for the peak detection node (locally_exclusive)",
        "selection": "A dictionary for the peak selection node. Default is to use smart_sampling_amplitudes, with a minimum of 20000 peaks\
                                         and 5000 peaks per electrode on average.",
        "clustering": "A dictionary to be provided to the clustering method. By default, random_projections is used, but if legacy is set to\
                            True, one other clustering called circus will be used, similar to the one used in Spyking Circus 1",
        "matching": "A dictionary to specify the matching engine used to recover spikes. The method default is circus-omp-svd, but other engines\
                                          can be used",
        "merging": "A dictionary to specify the final merging param to group cells after template matching (auto_merge_units)",
        "motion_correction": "A dictionary to be provided if motion correction has to be performed (dense probe only)",
        "apply_preprocessing": "Boolean to specify whether circus 2 should preprocess the recording or not. If yes, then high_pass filtering + common\
                                                    median reference + whitening",
        "apply_motion_correction": "Boolean to specify whether circus 2 should apply motion correction to the recording or not",
        "matched_filtering": "Boolean to specify whether circus 2 should detect peaks via matched filtering (slightly slower)",
        "cache_preprocessing": "How to cache the preprocessed recording. Mode can be memory, file, zarr, with extra arguments. In case of memory (default), \
                         memory_limit will control how much RAM can be used. In case of folder or zarr, delete_cache controls if cache is cleaned after sorting",
        "multi_units_only": "Boolean to get only multi units activity (i.e. one template per electrode)",
        "job_kwargs": "A dictionary to specify how many jobs and which parameters they should used",
        "seed": "An int to control how chunks are shuffled while detecting peaks",
        "debug": "Boolean to specify if internal data structures made during the sorting should be kept for debugging",
    }

    sorter_description = """Spyking Circus 2 is a rewriting of Spyking Circus, within the SpikeInterface framework
    It uses a more conservative clustering algorithm (compared to Spyking Circus), which is less prone to hallucinate units and/or find noise.
    In addition, it also uses a full Orthogonal Matching Pursuit engine to reconstruct the traces, leading to more spikes
    being discovered. The code is much faster and memory efficient, inheriting from all the preprocessing possibilities of spikeinterface"""

    @classmethod
    def get_sorter_version(cls):
        return "2.0"

    @classmethod
    def _run_from_folder(cls, sorter_output_folder, params, verbose):
        try:
            import hdbscan

            HAVE_HDBSCAN = True
        except:
            HAVE_HDBSCAN = False

        assert HAVE_HDBSCAN, "spykingcircus2 needs hdbscan to be installed"

        try:
            import torch
        except ImportError:
            HAVE_TORCH = False
            print("spykingcircus2 could benefit from using torch. Consider installing it")

        # this is importanted only on demand because numba import are too heavy
        from spikeinterface.sortingcomponents.peak_detection import detect_peaks
        from spikeinterface.sortingcomponents.peak_selection import select_peaks
        from spikeinterface.sortingcomponents.clustering import find_cluster_from_peaks
        from spikeinterface.sortingcomponents.matching import find_spikes_from_templates
        from spikeinterface.sortingcomponents.tools import remove_empty_templates
        from spikeinterface.sortingcomponents.tools import check_probe_for_drift_correction

        job_kwargs = fix_job_kwargs(params["job_kwargs"])
        job_kwargs.update({"progress_bar": verbose})

        recording = cls.load_recording_from_folder(sorter_output_folder.parent, with_warnings=False)

        sampling_frequency = recording.get_sampling_frequency()
        num_channels = recording.get_num_channels()
        ms_before = params["general"].get("ms_before", 2)
        ms_after = params["general"].get("ms_after", 2)
        radius_um = params["general"].get("radius_um", 75)
        peak_sign = params["detection"].get("peak_sign", "neg")
        exclude_sweep_ms = params["detection"].get("exclude_sweep_ms", max(ms_before, ms_after))

        ## First, we are filtering the data
        filtering_params = params["filtering"].copy()
        if params["apply_preprocessing"]:
            if verbose:
                print("Preprocessing the recording (bandpass filtering + CMR + whitening)")
            recording_f = bandpass_filter(recording, **filtering_params, dtype="float32")
            if num_channels > 1:
                recording_f = common_reference(recording_f)
        else:
            if verbose:
                print("Skipping preprocessing (whitening only)")
            recording_f = recording
            recording_f.annotate(is_filtered=True)

        valid_geometry = check_probe_for_drift_correction(recording_f)
        if params["apply_motion_correction"]:
            if not valid_geometry:
                if verbose:
                    print("Geometry of the probe does not allow 1D drift correction")
                motion_folder = None
            else:
                if verbose:
                    print("Motion correction activated (probe geometry compatible)")
                motion_folder = sorter_output_folder / "motion"
                params["motion_correction"].update({"folder": motion_folder})
                recording_f = correct_motion(recording_f, **params["motion_correction"], **job_kwargs)
        else:
            motion_folder = None

        ## We need to whiten before the template matching step, to boost the results
        # TODO add , regularize=True chen ready
        whitening_kwargs = params["whitening"].copy()
        whitening_kwargs["dtype"] = "float32"
        whitening_kwargs["regularize"] = whitening_kwargs.get("regularize", False)
        if num_channels == 1:
            whitening_kwargs["regularize"] = False
        if whitening_kwargs["regularize"]:
            whitening_kwargs["regularize_kwargs"] = {"method": "LedoitWolf"}

        recording_w = whiten(recording_f, **whitening_kwargs)
        noise_levels = get_noise_levels(recording_w, return_scaled=False, **job_kwargs)

        if recording_w.check_serializability("json"):
            recording_w.dump(sorter_output_folder / "preprocessed_recording.json", relative_to=None)
        elif recording_w.check_serializability("pickle"):
            recording_w.dump(sorter_output_folder / "preprocessed_recording.pickle", relative_to=None)

        recording_w = cache_preprocessing(recording_w, **job_kwargs, **params["cache_preprocessing"])

        ## Then, we are detecting peaks with a locally_exclusive method
        detection_params = params["detection"].copy()
        selection_params = params["selection"].copy()
        detection_params["radius_um"] = radius_um
        detection_params["exclude_sweep_ms"] = exclude_sweep_ms
        detection_params["noise_levels"] = noise_levels

        fs = recording_w.get_sampling_frequency()
        nbefore = int(ms_before * fs / 1000.0)
        nafter = int(ms_after * fs / 1000.0)

        skip_peaks = not params["multi_units_only"] and selection_params.get("method", "uniform") == "uniform"
        max_n_peaks = selection_params["n_peaks_per_channel"] * num_channels
        n_peaks = max(selection_params["min_n_peaks"], max_n_peaks)

        if params["debug"]:
            clustering_folder = sorter_output_folder / "clustering"
            clustering_folder.mkdir(parents=True, exist_ok=True)
            np.save(clustering_folder / "noise_levels.npy", noise_levels)

        if params["matched_filtering"]:
            prototype, waveforms, _ = get_prototype_and_waveforms_from_recording(
                recording_w,
                n_peaks=10000,
                ms_before=ms_before,
                ms_after=ms_after,
                seed=params["seed"],
                **detection_params,
                **job_kwargs,
            )
            detection_params["prototype"] = prototype
            detection_params["ms_before"] = ms_before
            if params["debug"]:
                np.save(clustering_folder / "waveforms.npy", waveforms)
                np.save(clustering_folder / "prototype.npy", prototype)
            if skip_peaks:
                detection_params["skip_after_n_peaks"] = n_peaks
                detection_params["recording_slices"] = get_shuffled_recording_slices(
                    recording_w, seed=params["seed"], **job_kwargs
                )
            peaks = detect_peaks(recording_w, "matched_filtering", **detection_params, **job_kwargs)
        else:
            waveforms = None
            if skip_peaks:
                detection_params["skip_after_n_peaks"] = n_peaks
                detection_params["recording_slices"] = get_shuffled_recording_slices(
                    recording_w, seed=params["seed"], **job_kwargs
                )
            peaks = detect_peaks(recording_w, "locally_exclusive", **detection_params, **job_kwargs)

        if not skip_peaks and verbose:
            print("Found %d peaks in total" % len(peaks))

        if params["multi_units_only"]:
            sorting = NumpySorting.from_peaks(peaks, sampling_frequency, unit_ids=recording_w.unit_ids)
        else:
            ## We subselect a subset of all the peaks, by making the distributions os SNRs over all
            ## channels as flat as possible
            selection_params = params["selection"]
            selection_params["n_peaks"] = n_peaks
            selection_params.update({"noise_levels": noise_levels})
            selected_peaks = select_peaks(peaks, **selection_params)

            if verbose:
                print("Kept %d peaks for clustering" % len(selected_peaks))

            ## We launch a clustering (using hdbscan) relying on positions and features extracted on
            ## the fly from the snippets
            clustering_params = params["clustering"].copy()
            clustering_params["waveforms"] = {}
            sparsity_kwargs = params["sparsity"].copy()
            if "peak_sign" not in sparsity_kwargs:
                sparsity_kwargs["peak_sign"] = peak_sign

            clustering_params["sparsity"] = sparsity_kwargs
            clustering_params["radius_um"] = radius_um
            clustering_params["waveforms"]["ms_before"] = ms_before
            clustering_params["waveforms"]["ms_after"] = ms_after
            clustering_params["few_waveforms"] = waveforms
            clustering_params["noise_levels"] = noise_levels
            clustering_params["ms_before"] = ms_before
            clustering_params["ms_after"] = ms_after
            clustering_params["verbose"] = verbose
            clustering_params["tmp_folder"] = sorter_output_folder / "clustering"
            clustering_params["debug"] = params["debug"]
            clustering_params["noise_threshold"] = detection_params.get("detect_threshold", 4)

            legacy = clustering_params.get("legacy", True)

            if legacy:
                clustering_method = "circus"
            else:
                clustering_method = "random_projections"

            labels, peak_labels = find_cluster_from_peaks(
                recording_w, selected_peaks, method=clustering_method, method_kwargs=clustering_params, **job_kwargs
            )

            ## We get the labels for our peaks
            mask = peak_labels > -1

            labeled_peaks = np.zeros(np.sum(mask), dtype=minimum_spike_dtype)
            labeled_peaks["sample_index"] = selected_peaks[mask]["sample_index"]
            labeled_peaks["segment_index"] = selected_peaks[mask]["segment_index"]
            for count, l in enumerate(labels):
                sub_mask = peak_labels[mask] == l
                labeled_peaks["unit_index"][sub_mask] = count
            unit_ids = np.arange(len(np.unique(labeled_peaks["unit_index"])))
            sorting = NumpySorting(labeled_peaks, sampling_frequency, unit_ids=unit_ids)

            if params["debug"]:
                np.save(clustering_folder / "peak_labels", peak_labels)
                np.save(clustering_folder / "labels", labels)
                np.save(clustering_folder / "peaks", selected_peaks)

            templates_array = estimate_templates(
                recording_w, labeled_peaks, unit_ids, nbefore, nafter, return_scaled=False, job_name=None, **job_kwargs
            )

            templates = Templates(
                templates_array=templates_array,
                sampling_frequency=sampling_frequency,
                nbefore=nbefore,
                sparsity_mask=None,
                channel_ids=recording_w.channel_ids,
                unit_ids=unit_ids,
                probe=recording_w.get_probe(),
                is_scaled=False,
            )

            sparsity = compute_sparsity(templates, noise_levels, **sparsity_kwargs)
            templates = templates.to_sparse(sparsity)
            templates = remove_empty_templates(templates)

            if params["debug"]:
                templates.to_zarr(folder_path=clustering_folder / "templates")
                sorting = sorting.save(folder=clustering_folder / "sorting")

            ## We launch a OMP matching pursuit by full convolution of the templates and the raw traces
            matching_method = params["matching"].pop("method")
            matching_params = params["matching"].copy()
            matching_params["templates"] = templates

            if matching_method is not None:
                spikes = find_spikes_from_templates(
                    recording_w, matching_method, method_kwargs=matching_params, **job_kwargs
                )

                if params["debug"]:
                    fitting_folder = sorter_output_folder / "fitting"
                    fitting_folder.mkdir(parents=True, exist_ok=True)
                    np.save(fitting_folder / "spikes", spikes)

                if verbose:
                    print("Found %d spikes" % len(spikes))

                ## And this is it! We have a spyking circus
                sorting = np.zeros(spikes.size, dtype=minimum_spike_dtype)
                sorting["sample_index"] = spikes["sample_index"]
                sorting["unit_index"] = spikes["cluster_index"]
                sorting["segment_index"] = spikes["segment_index"]
                sorting = NumpySorting(sorting, sampling_frequency, unit_ids)

        sorting_folder = sorter_output_folder / "sorting"
        if sorting_folder.exists():
            shutil.rmtree(sorting_folder)

        merging_params = params["merging"].copy()
        if params["debug"]:
            merging_params["debug_folder"] = sorter_output_folder / "merging"

        if len(merging_params) > 0:
            if params["motion_correction"] and motion_folder is not None:
                from spikeinterface.preprocessing.motion import load_motion_info

                motion_info = load_motion_info(motion_folder)
                motion = motion_info["motion"]
                max_motion = max(
                    np.max(np.abs(motion.displacement[seg_index])) for seg_index in range(len(motion.displacement))
                )
                max_distance_um = merging_params.get("max_distance_um", 50)
                merging_params["max_distance_um"] = max(max_distance_um, 2 * max_motion)

            if params["debug"]:
                curation_folder = sorter_output_folder / "curation"
                if curation_folder.exists():
                    shutil.rmtree(curation_folder)
                sorting.save(folder=curation_folder)
                # np.save(fitting_folder / "amplitudes", guessed_amplitudes)

            sorting = final_cleaning_circus(recording_w, sorting, templates, **merging_params, **job_kwargs)

            if verbose:
                print(f"Kept {len(sorting.unit_ids)} units after final merging")

        folder_to_delete = None
        cache_mode = params["cache_preprocessing"].get("mode", "memory")
        delete_cache = params["cache_preprocessing"].get("delete_cache", True)

        if cache_mode in ["folder", "zarr"] and delete_cache:
            folder_to_delete = recording_w._kwargs["folder_path"]

        del recording_w
        if folder_to_delete is not None:
            shutil.rmtree(folder_to_delete)

        sorting = sorting.save(folder=sorting_folder)

        return sorting


def final_cleaning_circus(
    recording,
    sorting,
    templates,
    similarity_kwargs={"method": "l2", "support": "union", "max_lag_ms": 0.1},
    sparsity_overlap=0.5,
    censor_ms=3.0,
    max_distance_um=50,
    template_diff_thresh=np.arange(0.05, 0.5, 0.05),
    debug_folder=None,
    **job_kwargs,
):

    from spikeinterface.sortingcomponents.tools import create_sorting_analyzer_with_existing_templates
    from spikeinterface.curation.auto_merge import auto_merge_units

    # First we compute the needed extensions
    analyzer = create_sorting_analyzer_with_existing_templates(sorting, recording, templates)
    analyzer.compute("unit_locations", method="monopolar_triangulation")
    analyzer.compute("template_similarity", **similarity_kwargs)

    if debug_folder is not None:
        analyzer.save_as(format="binary_folder", folder=debug_folder)

    presets = ["x_contaminations"] * len(template_diff_thresh)
    steps_params = [
        {"template_similarity": {"template_diff_thresh": i}, "unit_locations": {"max_distance_um": max_distance_um}}
        for i in template_diff_thresh
    ]
    final_sa = auto_merge_units(
        analyzer,
        presets=presets,
        steps_params=steps_params,
        recursive=True,
        censor_ms=censor_ms,
        sparsity_overlap=sparsity_overlap,
        **job_kwargs,
    )
    return final_sa.sorting
