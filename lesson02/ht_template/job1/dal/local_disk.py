import json
import os
from typing import List, Dict, Any

from lesson02.ht_template.bin.utils import recreate_empty_dir


def save_to_disk(json_content: List[Dict[str, Any]], path: str, filename: str) -> None:
    """It writes JSON content into file."""
    recreate_empty_dir(path)
    path_to_file = os.path.join(path, filename)
    data = json_content
    # write data to the file
    with open(path_to_file, 'w+') as f:
        json.dump(data, f)
