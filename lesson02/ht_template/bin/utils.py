import os
import shutil


def recreate_empty_dir(path: str) -> None:
    """It deletes folder and it again from scratch."""
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path)