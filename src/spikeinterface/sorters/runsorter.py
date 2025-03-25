from __future__ import annotations

import stat
import subprocess
import sys

from .sorterlist import sorter_dict


import shutil
import os
from pathlib import Path
import json
import pickle
import platform
from warnings import warn
from typing import Optional, Union

from spikeinterface import DEV_MODE
import spikeinterface

from spikeinterface import __version__ as si_version


from spikeinterface.core import BaseRecording, NumpySorting, load
from spikeinterface.core.core_tools import check_json, is_editable_mode
from .sorterlist import sorter_dict
from .utils import (
    SpikeSortingError,
    has_nvidia,
    has_docker,
    has_docker_python,
    has_singularity,
    has_spython,
    has_docker_nvidia_installed,
    get_nvidia_docker_dependecies,
)
from .container_tools import (
    find_recording_folders,
    path_to_unix,
    windows_extractor_dict_to_unix,
    ContainerClient,
    install_package_in_container,
)

REGISTRY = "spikeinterface"

SORTER_DOCKER_MAP = dict(
    combinato="combinato",
    herdingspikes="herdingspikes",
    kilosort4="kilosort4",
    klusta="klusta",
    mountainsort4="mountainsort4",
    mountainsort5="mountainsort5",
    pykilosort="pykilosort",
    spykingcircus="spyking-circus",
    spykingcircus2="spyking-circus2",
    tridesclous="tridesclous",
    yass="yass",
    # Matlab compiled sorters:
    hdsort="hdsort-compiled",
    ironclust="ironclust-compiled",
    kilosort="kilosort-compiled",
    kilosort2="kilosort2-compiled",
    kilosort2_5="kilosort2_5-compiled",
    kilosort3="kilosort3-compiled",
    waveclus="waveclus-compiled",
    waveclus_snippets="waveclus-compiled",
)

SORTER_DOCKER_MAP = {k: f"{REGISTRY}/{v}-base" for k, v in SORTER_DOCKER_MAP.items()}


def run_sorter(
    sorter_name: str,
    recording: BaseRecording,
    folder: Optional[str] = None,
    remove_existing_folder: bool = False,
    delete_output_folder: bool = False,
    verbose: bool = False,
    raise_error: bool = True,
    docker_image: Optional[Union[bool, str]] = False,
    singularity_image: Optional[Union[bool, str]] = False,
    delete_container_files: bool = True,
    with_output: bool = True,
    output_folder: None = None,
    engine="loop",
    engine_kwargs={},
    **sorter_params,
):
    # Docstrings generated in `Doc and docstring` section below
    """
    Function to run a sorter on a `recording` or dict of `recordings`

    {}
    {}
    {}
    {}

    {}
    """

    if output_folder is not None and folder is None:
        deprecation_msg = (
            "`output_folder` is deprecated and will be removed in version 0.103.0 Please use folder instead"
        )
        folder = output_folder
        warn(deprecation_msg, category=DeprecationWarning, stacklevel=2)

    if folder is None:
        folder = sorter_name + "_output"

    common_kwargs = dict(
        sorter_name=sorter_name,
        recording=recording,
        folder=folder,
        remove_existing_folder=remove_existing_folder,
        delete_output_folder=delete_output_folder,
        verbose=verbose,
        raise_error=raise_error,
        with_output=with_output,
        docker_image=docker_image,
        singularity_image=singularity_image,
        delete_container_files=delete_container_files**sorter_params,
    )

    if isinstance(recording, BaseRecording):
        return run_one_sorter(recording, **common_kwargs)
    elif isinstance(recording, dict):
        return _run_dict_of_sortings(
            dict_of_recordings=recording, engine=engine, engine_kwargs=engine_kwargs, **common_kwargs
        )
    else:
        raise TypeError("The `run_sorter` recording argument only accepts recording or a dict of recordings.")


def _run_dict_of_sortings(
    sorter_name: str,
    dict_of_recordings: dict,
    engine="loop",
    engine_kwargs={},
    folder: Optional[str] = None,
    remove_existing_folder: bool = False,
    delete_output_folder: bool = False,
    verbose: bool = False,
    raise_error: bool = True,
    docker_image: Optional[Union[bool, str]] = False,
    singularity_image: Optional[Union[bool, str]] = False,
    delete_container_files: bool = True,
    with_output: bool = True,
    **sorter_params,
):
    # Docstrings generated in `Doc and docstring` section below
    """

    Parameters
    ----------
    dict_of_recording : dict
        A dictionary of `RecordingExtractors`, grouped by their key.
    {}
    {}
    {}

    Returns
    -------
    dict | None
        Dict of sorting objects `BaseSorter`s (if `with_output` is True) or None (if `with_output` is False)
    """

    working_folder = Path(folder).absolute()

    split_by_property = dict_of_recordings.values[0].get_annotation("split_by_property")
    if split_by_property is not None:
        grouping_property = grouping_property
    else:
        grouping_property = "group"

    info_file = folder / f"spikeinterface_info.json"
    info = dict(
        version=spikeinterface.__version__,
        dev_mode=spikeinterface.DEV_MODE,
        object="Group",
        group_of="BaseSorting",
        grouping_property=grouping_property,
        keys=list(dict_of_recordings.keys()),
    )
    with open(info_file, mode="w") as f:
        json.dump(check_json(info), f, indent=4)

    job_list = []
    for k, rec in dict_of_recordings.items():
        job = dict(
            sorter_name=sorter_name,
            recording=rec,
            folder=working_folder / str(k),
            verbose=verbose,
            docker_image=docker_image,
            singularity_image=singularity_image,
            delete_container_files=delete_container_files,
            raise_error=raise_error,
            delete_output_folder=delete_output_folder,
            remove_existing_folder=remove_existing_folder,
            **sorter_params,
        )
        job_list.append(job)

    sorting_list = run_sorter_jobs(job_list, engine=engine, engine_kwargs=engine_kwargs, return_output=with_output)
    return sorting_list


def run_one_sorter(
    sorter_name: str,
    recording: BaseRecording,
    folder: Optional[str] = None,
    remove_existing_folder: bool = False,
    delete_output_folder: bool = False,
    verbose: bool = False,
    raise_error: bool = True,
    docker_image: Optional[Union[bool, str]] = False,
    singularity_image: Optional[Union[bool, str]] = False,
    delete_container_files: bool = True,
    with_output: bool = True,
    **sorter_params,
):
    # Docstrings generated in `Doc and docstring` section below
    """
    Function to run one sorter, either locally or using Docker or Singularity.

    {}
    {}

    {}

    Examples
    --------
    >>> sorting = run_sorter("tridesclous", recording)
    """

    common_kwargs = dict(
        sorter_name=sorter_name,
        recording=recording,
        folder=folder,
        remove_existing_folder=remove_existing_folder,
        delete_output_folder=delete_output_folder,
        verbose=verbose,
        raise_error=raise_error,
        with_output=with_output,
        **sorter_params,
    )

    if docker_image or singularity_image:
        common_kwargs.update(dict(delete_container_files=delete_container_files))
        if docker_image:
            mode = "docker"
            assert not singularity_image
            if isinstance(docker_image, bool):
                container_image = None
            else:
                container_image = docker_image

            if not has_docker():
                raise RuntimeError(
                    "Docker is not installed. Install docker on this machine to run sorting with docker."
                )

            if not has_docker_python():
                raise RuntimeError("The python `docker` package must be installed. Install with `pip install docker`")

        else:
            mode = "singularity"
            assert not docker_image
            if isinstance(singularity_image, bool):
                container_image = None
            else:
                container_image = singularity_image

            if not has_singularity():
                raise RuntimeError(
                    "Singularity is not installed. Install singularity "
                    "on this machine to run sorting with singularity."
                )

            if not has_spython():
                raise RuntimeError(
                    "The python `spython` package must be installed to "
                    "run singularity. Install with `pip install spython`"
                )

        return run_sorter_container(
            container_image=container_image,
            mode=mode,
            **common_kwargs,
        )

    return run_sorter_local(**common_kwargs)


def run_sorter_local(
    sorter_name,
    recording,
    folder,
    remove_existing_folder=True,
    delete_output_folder=False,
    verbose=False,
    raise_error=True,
    with_output=True,
    output_folder=None,
    **sorter_params,
):
    # Docstrings generated in `Doc and docstring` section below
    """
    Runs a sorter locally.

    {}
    {}

    {}
    """
    if isinstance(recording, list):
        raise Exception("If you want to run several sorters/recordings use run_sorter_jobs(...)")

    if output_folder is not None and folder is None:
        deprecation_msg = (
            "`output_folder` is deprecated and will be removed in version 0.103.0 Please use folder instead"
        )
        folder = output_folder
        warn(deprecation_msg, category=DeprecationWarning, stacklevel=2)

    SorterClass = sorter_dict[sorter_name]

    # only classmethod call not instance (stateless at instance level but state is in folder)
    folder = SorterClass.initialize_folder(recording, folder, verbose, remove_existing_folder)
    SorterClass.set_params_to_folder(recording, folder, sorter_params, verbose)
    # This writes parameters and recording to binary and could ideally happen in the host
    SorterClass.setup_recording(recording, folder, verbose=verbose)
    # This NEEDS to happen in the docker because of dependencies
    SorterClass.run_from_folder(folder, raise_error, verbose)
    if with_output:
        sorting = SorterClass.get_result_from_folder(folder, register_recording=True, sorting_info=True)
    else:
        sorting = None
    sorter_output_folder = folder / "sorter_output"
    if delete_output_folder:
        if with_output and sorting is not None:
            # if we delete the folder the sorting can have a data reference to deleted file/folder: we need a copy
            sorting_info = sorting.sorting_info
            sorting = NumpySorting.from_sorting(sorting, with_metadata=True, copy_spike_vector=True)
            sorting.set_sorting_info(
                recording_dict=sorting_info["recording"],
                params_dict=sorting_info["params"],
                log_dict=sorting_info["log"],
            )
        shutil.rmtree(sorter_output_folder)

    return sorting


def run_sorter_container(
    sorter_name: str,
    recording: BaseRecording,
    mode: str,
    # todo 0.103: make folder non-optional
    folder: Optional[str] = None,
    container_image: Optional[str] = None,
    remove_existing_folder: bool = True,
    delete_output_folder: bool = False,
    verbose: bool = False,
    raise_error: bool = True,
    with_output: bool = True,
    delete_container_files: bool = True,
    extra_requirements=None,
    installation_mode="auto",
    spikeinterface_version=None,
    spikeinterface_folder_source=None,
    output_folder: None = None,
    **sorter_params,
):
    # Docstrings generated in `Doc and docstring` section below
    """
    Runs a sorter in a container.

    {}
    {}
    {}
    extra_requirements : list, default: None
        List of extra requirements to install in the container
    installation_mode : "auto" | "pypi" | "github" | "folder" | "dev" | "no-install", default: "auto"
        How spikeinterface is installed in the container:
          * "auto" : if host installation is a pip release then use "github" with tag
                    if host installation is DEV_MODE=True then use "dev"
          * "pypi" : use pypi with pip install spikeinterface
          * "github" : use github with `pip install git+https`
          * "folder" : mount a folder in container and install from this one.
                      So the version in the container is a different spikeinterface version from host, useful for
                      cross checks
          * "dev" : same as "folder", but the folder is the spikeinterface.__file__ to ensure same version as host
          * "no-install" : do not install spikeinterface in the container because it is already installed
    spikeinterface_version : str, default: None
        The spikeinterface version to install in the container. If None, the current version is used
    spikeinterface_folder_source : Path or None, default: None
        In case of installation_mode="folder", the spikeinterface folder source to use to install in the container

    {}
    """

    assert installation_mode in ("auto", "pypi", "github", "folder", "dev", "no-install")

    if output_folder is not None and folder is None:
        deprecation_msg = (
            "`output_folder` is deprecated and will be removed in version 0.103.0 Please use folder instead"
        )
        folder = output_folder
        warn(deprecation_msg, category=DeprecationWarning, stacklevel=2)
    assert folder is not None, "Must provide a `folder`"

    spikeinterface_version = spikeinterface_version or si_version

    if extra_requirements is None:
        extra_requirements = []

    # common code for docker and singularity

    if container_image is None:
        if sorter_name in SORTER_DOCKER_MAP:
            container_image = SORTER_DOCKER_MAP[sorter_name]
        else:
            raise ValueError(f"sorter {sorter_name} not in SORTER_DOCKER_MAP. Please specify a container_image.")

    SorterClass = sorter_dict[sorter_name]
    folder = Path(folder).absolute().resolve()
    parent_folder = folder.parent.absolute().resolve()
    parent_folder.mkdir(parents=True, exist_ok=True)

    # find input folder of recording for folder bind
    rec_dict = recording.to_dict(recursive=True)
    recording_input_folders = find_recording_folders(rec_dict)

    if platform.system() == "Windows":
        rec_dict = windows_extractor_dict_to_unix(rec_dict)

    # create 3 files for communication with container
    # recording dict inside
    if recording.check_serializability("json"):
        (parent_folder / "in_container_recording.json").write_text(
            json.dumps(check_json(rec_dict), indent=4), encoding="utf8"
        )
    elif recording.check_serializability("pickle"):
        (parent_folder / "in_container_recording.pickle").write_bytes(pickle.dumps(rec_dict))
    else:
        raise RuntimeError("To use run_sorter with a container the recording must be serializable")

    # need to share specific parameters
    (parent_folder / "in_container_params.json").write_text(
        json.dumps(check_json(sorter_params), indent=4), encoding="utf8"
    )

    in_container_sorting_folder = folder / "in_container_sorting"

    # if in Windows, skip C:
    parent_folder_unix = path_to_unix(parent_folder)
    output_folder_unix = path_to_unix(folder)
    recording_input_folders_unix = [path_to_unix(rf) for rf in recording_input_folders]
    in_container_sorting_folder_unix = path_to_unix(in_container_sorting_folder)

    # the py script
    py_script = f"""
import json
from pathlib import Path
from spikeinterface import load
from spikeinterface.sorters import run_sorter_local

if __name__ == '__main__':
    # this __name__ protection help in some case with multiprocessing (for instance HS2)
    # load recording in container
    json_rec = Path('{parent_folder_unix}/in_container_recording.json')
    pickle_rec = Path('{parent_folder_unix}/in_container_recording.pickle')
    if json_rec.exists():
        recording = load(json_rec)
    else:
        recording = load(pickle_rec)

    # load params in container
    with open('{parent_folder_unix}/in_container_params.json', encoding='utf8', mode='r') as f:
        sorter_params = json.load(f)

    # run in container
    output_folder = '{output_folder_unix}'
    sorting = run_sorter_local(
        '{sorter_name}', recording, output_folder=output_folder,
        remove_existing_folder={remove_existing_folder}, delete_output_folder=False,
        verbose={verbose}, raise_error={raise_error}, with_output=True, **sorter_params
    )
    sorting.save(folder='{in_container_sorting_folder_unix}')
"""
    (parent_folder / "in_container_sorter_script.py").write_text(py_script, encoding="utf8")

    volumes = {}
    for recording_folder, recording_folder_unix in zip(recording_input_folders, recording_input_folders_unix):
        # handle duplicates
        if str(recording_folder) not in volumes:
            volumes[str(recording_folder)] = {"bind": str(recording_folder_unix), "mode": "ro"}
    volumes[str(parent_folder)] = {"bind": str(parent_folder_unix), "mode": "rw"}

    host_folder_source = None
    if installation_mode == "auto":
        if DEV_MODE:
            if is_editable_mode():
                installation_mode = "dev"
            else:
                installation_mode = "github"
        else:
            installation_mode = "github"
        if verbose:
            print(f"installation_mode='auto' switching to installation_mode: '{installation_mode}'")

    if installation_mode == "folder":
        assert (
            spikeinterface_folder_source is not None
        ), "for installation_mode='folder', spikeinterface_folder_source must be provided"
        host_folder_source = Path(spikeinterface_folder_source)

    if installation_mode == "dev":
        host_folder_source = Path(spikeinterface.__file__).parents[2]

    if host_folder_source is not None:
        host_folder_source = host_folder_source.resolve()
        # this bind is read only  and will be copy later
        container_folder_source_ro = "/spikeinterface"
        volumes[str(host_folder_source)] = {"bind": container_folder_source_ro, "mode": "ro"}

    extra_kwargs = {}

    use_gpu = SorterClass.use_gpu(sorter_params)
    gpu_capability = SorterClass.gpu_capability
    if use_gpu:
        if gpu_capability == "nvidia-required":
            assert has_nvidia(), "The container requires a NVIDIA GPU capability, but it is not available"
            extra_kwargs["container_requires_gpu"] = True

            if platform.system() == "Linux" and not has_docker_nvidia_installed():
                warn(
                    f"nvidia-required but none of \n{get_nvidia_docker_dependecies()}\n were found. "
                    f"This may result in an error being raised during sorting. Try "
                    "installing `nvidia-container-toolkit`, including setting the "
                    "configuration steps, if running into errors."
                )

        elif gpu_capability == "nvidia-optional":
            if has_nvidia():
                extra_kwargs["container_requires_gpu"] = True
            else:
                if verbose:
                    print(
                        f"{SorterClass.sorter_name} supports GPU, but no GPU is available.\n"
                        f"Running the sorter without GPU"
                    )
        else:
            # TODO: make opencl machanism
            raise NotImplementedError("Only nvidia support is available")

    # Creating python user base folder
    py_user_base_unix = None
    if mode == "singularity":
        py_user_base_folder = parent_folder / "in_container_python_base"
        py_user_base_folder.mkdir(parents=True, exist_ok=True)
        py_user_base_unix = path_to_unix(py_user_base_folder)

    container_client = ContainerClient(mode, container_image, volumes, py_user_base_unix, extra_kwargs)
    if verbose:
        print("Starting container")
    container_client.start()

    if installation_mode == "no-install":
        need_si_install = False
    else:
        cmd_1 = ["python", "-c", "import spikeinterface; print(spikeinterface.__version__)"]
        cmd_2 = ["python", "-c", "from spikeinterface.sorters import run_sorter_local"]
        res_output = ""
        for cmd in [cmd_1, cmd_2]:
            res_output += str(container_client.run_command(cmd))
        need_si_install = "ModuleNotFoundError" in res_output

    if need_si_install:
        # update pip in container
        cmd = f"pip install --user --upgrade pip"
        res_output = container_client.run_command(cmd)

        if installation_mode == "pypi":
            install_package_in_container(
                container_client,
                "spikeinterface",
                installation_mode="pypi",
                extra="[full]",
                version=spikeinterface_version,
                verbose=verbose,
            )

        elif installation_mode == "github":
            if DEV_MODE:
                install_package_in_container(
                    container_client,
                    "spikeinterface",
                    installation_mode="github",
                    github_url="https://github.com/SpikeInterface/spikeinterface",
                    extra="[full]",
                    tag="main",
                    verbose=verbose,
                )
            else:
                install_package_in_container(
                    container_client,
                    "spikeinterface",
                    installation_mode="github",
                    github_url="https://github.com/SpikeInterface/spikeinterface",
                    extra="[full]",
                    version=spikeinterface_version,
                    verbose=verbose,
                )
        elif host_folder_source is not None:
            # this is "dev" + "folder"
            install_package_in_container(
                container_client,
                "spikeinterface",
                installation_mode="folder",
                extra="[full]",
                container_folder_source=container_folder_source_ro,
                verbose=verbose,
            )

        if installation_mode == "dev":
            # also install neo from github
            # cmd = "pip install --user --upgrade --no-input https://github.com/NeuralEnsemble/python-neo/archive/master.zip"
            # res_output = container_client.run_command(cmd)
            install_package_in_container(
                container_client,
                "neo",
                installation_mode="github",
                github_url="https://github.com/NeuralEnsemble/python-neo",
                tag="master",
            )

    if hasattr(recording, "extra_requirements"):
        extra_requirements.extend(recording.extra_requirements)

    # install additional required dependencies
    if extra_requirements:
        # if verbose:
        #     print(f"Installing extra requirements: {extra_requirements}")
        # cmd = f"pip install --user --upgrade --no-input {' '.join(extra_requirements)}"
        res_output = container_client.run_command(cmd)
        for package_name in extra_requirements:
            install_package_in_container(container_client, package_name, installation_mode="pypi", verbose=verbose)

    # run sorter on folder
    if verbose:
        print(f"Running {sorter_name} sorter inside {container_image}")

    # this do not work with singularity:
    # cmd = 'python "{}"'.format(parent_folder/'in_container_sorter_script.py')
    # this approach is better
    in_container_script_path_unix = (Path(parent_folder_unix) / "in_container_sorter_script.py").as_posix()
    cmd = ["python", f"{in_container_script_path_unix}"]
    res_output = container_client.run_command(cmd)
    run_sorter_output = res_output

    # chown folder to user uid
    if platform.system() != "Windows":
        uid = os.getuid()
        # this do not work with singularity:
        # cmd = f'chown {uid}:{uid} -R "{output_folder}"'
        # this approach is better
        cmd = ["chown", f"{uid}:{uid}", "-R", f"{folder}"]
        res_output = container_client.run_command(cmd)
    else:
        # not needed for Windows
        pass

    if verbose:
        print("Stopping container")
    container_client.stop()

    # clean useless files
    if delete_container_files:
        if (parent_folder / "in_container_recording.json").exists():
            os.remove(parent_folder / "in_container_recording.json")
        if (parent_folder / "in_container_recording.pickle").exists():
            os.remove(parent_folder / "in_container_recording.pickle")
        os.remove(parent_folder / "in_container_params.json")
        os.remove(parent_folder / "in_container_sorter_script.py")
        if mode == "singularity":
            shutil.rmtree(py_user_base_folder, ignore_errors=True)

    # check error
    folder = Path(folder)
    log_file = folder / "spikeinterface_log.json"
    if not log_file.is_file():
        run_error = True
    else:
        with log_file.open("r", encoding="utf8") as f:
            log = json.load(f)
        run_error = bool(log["error"])

    sorting = None
    if run_error:
        if raise_error:
            raise SpikeSortingError(f"Spike sorting in {mode} failed with the following error:\n{run_sorter_output}")
    else:
        if with_output:
            try:
                sorting = SorterClass.get_result_from_folder(folder)
            except Exception as e:
                try:
                    sorting = load(in_container_sorting_folder)
                except FileNotFoundError:
                    SpikeSortingError(f"Spike sorting in {mode} failed with the following error:\n{run_sorter_output}")

    sorter_output_folder = folder / "sorter_output"
    if delete_output_folder:
        shutil.rmtree(sorter_output_folder)

    return sorting


def read_sorter_folder(folder, register_recording=True, sorting_info=True, raise_error=True):
    """
    Load a sorting object from a spike sorting output folder.
    The 'folder' must contain a valid 'spikeinterface_log.json' file


    Parameters
    ----------
    folder : Pth or str
        The sorter folder
    register_recording : bool, default: True
        Attach recording (when json or pickle) to the sorting
    sorting_info : bool, default: True
        Attach sorting info to the sorting
    raise_error : bool, detault: True
        Raise an error if the spike sorting failed
    """
    folder = Path(folder)
    log_file = folder / "spikeinterface_log.json"

    if not log_file.is_file():
        raise Exception(f"This folder {folder} does not have spikeinterface_log.json")

    with log_file.open("r", encoding="utf8") as f:
        log = json.load(f)

    run_error = bool(log["error"])
    if run_error:
        if raise_error:
            raise SpikeSortingError(f"Spike sorting failed for {folder}")
        else:
            return

    sorter_name = log["sorter_name"]
    SorterClass = sorter_dict[sorter_name]
    sorting = SorterClass.get_result_from_folder(
        folder, register_recording=register_recording, sorting_info=sorting_info
    )
    return sorting


_default_engine_kwargs = dict(
    loop=dict(),
    joblib=dict(n_jobs=-1, backend="loky"),
    processpoolexecutor=dict(max_workers=2, mp_context=None),
    dask=dict(client=None),
    slurm=dict(tmp_script_folder=None, cpus_per_task=1, mem="1G"),
)

_implemented_engine = list(_default_engine_kwargs.keys())


def run_sorter_jobs(job_list, engine="loop", engine_kwargs={}, return_output=False):
    # Docstrings generated in `Doc and docstring` section below
    """
    Run several :py:func:`run_sorter()` sequentially or in parallel given a list of jobs.

    For **engine="loop"** this is equivalent to:

    ..code::

        for job in job_list:
            run_sorter(**job)

    The following engines block the I/O:
      * "loop"
      * "joblib"
      * "multiprocessing"
      * "dask"

    The following engines are *asynchronous*:
      * "slurm"

    Where *blocking* means that this function is blocking until the results are returned.
    This is in opposition to *asynchronous*, where the function returns `None` almost immediately (aka non-blocking),
    but the results must be retrieved by hand when jobs are finished. No mechanisim is provided here to be know
    when jobs are finish.
    In this *asynchronous* case, the :py:func:`~spikeinterface.sorters.read_sorter_folder()` helps to retrieve individual results.


    Parameters
    ----------
    job_list : list of dict
        A list a dict that are propagated to run_sorter(...)
    {}
    return_output : bool, dfault False
        Return a sortings or None.
        This also overwrite kwargs in  in run_sorter(with_sorting=True/False)

    {}
    """

    assert engine in _implemented_engine, f"engine must be in {_implemented_engine}"

    engine_kwargs_ = dict()
    engine_kwargs_.update(_default_engine_kwargs[engine])
    engine_kwargs_.update(engine_kwargs)
    engine_kwargs = engine_kwargs_

    if return_output:
        assert engine in (
            "loop",
            "joblib",
            "processpoolexecutor",
        ), "Only 'loop', 'joblib', and 'processpoolexecutor' support return_output=True."
        out = []
        for kwargs in job_list:
            kwargs["with_output"] = True
    else:
        out = None
        for kwargs in job_list:
            kwargs["with_output"] = False

    if engine == "loop":
        # simple loop in main process
        for kwargs in job_list:
            sorting = run_sorter(**kwargs)
            if return_output:
                out.append(sorting)

    elif engine == "joblib":
        from joblib import Parallel, delayed

        n_jobs = engine_kwargs["n_jobs"]
        backend = engine_kwargs["backend"]
        sortings = Parallel(n_jobs=n_jobs, backend=backend)(delayed(run_sorter)(**kwargs) for kwargs in job_list)
        if return_output:
            out.extend(sortings)

    elif engine == "processpoolexecutor":
        from concurrent.futures import ProcessPoolExecutor

        max_workers = engine_kwargs["max_workers"]
        mp_context = engine_kwargs["mp_context"]

        with ProcessPoolExecutor(max_workers=max_workers, mp_context=mp_context) as executor:
            futures = []
            for kwargs in job_list:
                res = executor.submit(run_sorter, **kwargs)
                futures.append(res)
            for futur in futures:
                sorting = futur.result()
                if return_output:
                    out.append(sorting)

    elif engine == "dask":
        client = engine_kwargs["client"]
        assert client is not None, "For dask engine you have to provide : client = dask.distributed.Client(...)"

        tasks = []
        for kwargs in job_list:
            task = client.submit(run_sorter, **kwargs)
            tasks.append(task)

        for task in tasks:
            task.result()

    elif engine == "slurm":
        # generate python script for slurm
        tmp_script_folder = engine_kwargs["tmp_script_folder"]
        if tmp_script_folder is None:
            tmp_script_folder = tempfile.mkdtemp(prefix="spikeinterface_slurm_")
        tmp_script_folder = Path(tmp_script_folder)
        cpus_per_task = engine_kwargs["cpus_per_task"]
        mem = engine_kwargs["mem"]

        tmp_script_folder.mkdir(exist_ok=True, parents=True)

        for i, kwargs in enumerate(job_list):
            script_name = tmp_script_folder / f"si_script_{i}.py"
            with open(script_name, "w") as f:
                kwargs_txt = ""
                for k, v in kwargs.items():
                    kwargs_txt += "    "
                    if k == "recording":
                        # put None temporally
                        kwargs_txt += "recording=None"
                    else:
                        if isinstance(v, str):
                            kwargs_txt += f'{k}="{v}"'
                        elif isinstance(v, Path):
                            kwargs_txt += f'{k}="{str(v.absolute())}"'
                        else:
                            kwargs_txt += f"{k}={v}"
                    kwargs_txt += ",\n"

                # recording_dict = task_args[1]
                recording_dict = kwargs["recording"].to_dict()
                slurm_script = _slurm_script.format(
                    python=sys.executable, recording_dict=recording_dict, kwargs_txt=kwargs_txt
                )
                f.write(slurm_script)
                os.fchmod(f.fileno(), mode=stat.S_IRWXU)

            subprocess.Popen(["sbatch", str(script_name.absolute()), f"-cpus-per-task={cpus_per_task}", f"-mem={mem}"])

    return out


_slurm_script = """#! {python}
from numpy import array
from spikeinterface import load
from spikeinterface.sorters import run_sorter

rec_dict = {recording_dict}

kwargs = dict(
{kwargs_txt}
)
kwargs['recording'] = load(rec_dict)

run_sorter(**kwargs)
"""

#########################################
# Doc and docstring section
#########################################


_recording_param_doc = """Parameters
    ----------
    recording : RecordingExtractor
        The recording extractor to be spike sorted"""

_common_param_doc = """sorter_name : str
        The sorter name
    folder : str or Path
        Path to output folder
    remove_existing_folder : bool
        If True and folder exists then delete.
    delete_output_folder : bool, default: False
        If True, output folder is deleted
    verbose : bool, default: False
        If True, output is verbose
    raise_error : bool, default: True
        If True, an error is raised if spike sorting fails
        If False, the process continues and the error is logged in the log file.
    with_output : bool, default: True
        If True, the output Sorting is returned as a Sorting
    **sorter_params : keyword args
        Spike sorter specific arguments (they can be retrieved with `get_default_sorter_params(sorter_name_or_class)`)"""

_container_param_doc = """docker_image : bool or str, default: False
        If True, pull the default docker container for the sorter and run the sorter in that container using docker.
        Use a str to specify a non-default container. If that container is not local it will be pulled from docker hub.
        If False, the sorter is run locally
    singularity_image : bool or str, default: False
        If True, pull the default docker container for the sorter and run the sorter in that container using
        singularity. Use a str to specify a non-default container. If that container is not local it will be pulled
        from Docker Hub. If False, the sorter is run locally
    delete_container_files : bool, default: True
        If True, the container temporary files are deleted after the sorting is done"""

_engine_param_doc = """engine : str "loop", "joblib", "dask", "slurm"
        The engine to run the list.
        * "loop" : a simple loop. This engine is
    engine_kwargs : dict"""

_output_doc = """Returns
    -------
    BaseSorting | None
        The spike sorted data (if `with_output` is True) or None (if `with_output` is False)"""


run_sorter.__doc__ = run_sorter.__doc__.format(
    _recording_param_doc, _common_param_doc, _container_param_doc, _engine_param_doc, _output_doc
)
_run_dict_of_sortings.__doc__ = _run_dict_of_sortings.__doc__.format(
    _common_param_doc, _container_param_doc, _engine_param_doc
)

run_one_sorter.__doc__ = run_one_sorter.__doc__.format(
    _recording_param_doc, _common_param_doc, _container_param_doc, _output_doc
)
run_sorter_local.__doc__ = run_sorter_local.__doc__.format(_recording_param_doc, _common_param_doc, _output_doc)
run_sorter_container.__doc__ = run_sorter_container.__doc__.format(
    _recording_param_doc, _common_param_doc, _container_param_doc, _output_doc
)
run_sorter_jobs.__doc__ = run_sorter_jobs.__doc__.format(_engine_param_doc, _output_doc)
