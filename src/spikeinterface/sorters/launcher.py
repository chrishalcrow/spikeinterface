"""
Utils functions to launch several sorter on several recording in parallel or not.
"""

from __future__ import annotations


from pathlib import Path
import shutil
import numpy as np
import tempfile
import os
import stat
import subprocess
import sys
import warnings

from spikeinterface.core import aggregate_units

from .sorterlist import sorter_dict
from .runsorter import run_sorter
from .basesorter import is_log_ok

_default_engine_kwargs = dict(
    loop=dict(),
    joblib=dict(n_jobs=-1, backend="loky"),
    processpoolexecutor=dict(max_workers=2, mp_context=None),
    dask=dict(client=None),
    slurm=dict(tmp_script_folder=None, cpus_per_task=1, mem="1G"),
)


_implemented_engine = list(_default_engine_kwargs.keys())


def run_sorter_jobs(job_list, engine="loop", engine_kwargs={}, return_output=False):
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
    engine : str "loop", "joblib", "dask", "slurm"
        The engine to run the list.
        * "loop" : a simple loop. This engine is
    engine_kwargs : dict

    return_output : bool, dfault False
        Return a sortings or None.
        This also overwrite kwargs in  in run_sorter(with_sorting=True/False)

    Returns
    -------
    sortings : None or list of sorting
        With engine="loop" or "joblib" you can optional get directly the list of sorting result if return_output=True.
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
