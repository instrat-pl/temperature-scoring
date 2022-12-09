from pathlib import Path
import inspect


def project_dir(*path):
    root = (
        Path(inspect.getframeinfo(inspect.currentframe()).filename).resolve().parents[2]
    )
    return Path(root, *path)


def data_dir(*path):
    return project_dir("data", *path)


def plots_dir(*path):
    return project_dir("plots", *path)
