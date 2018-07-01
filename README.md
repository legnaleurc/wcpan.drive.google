# wcpan.drive.google

Asynchronous Google Drive API.

## Example Usage

```python
import os.path as op

from wcpan.drive.google import Drive


async def api_demo():
    path = op.expanduser('~/.cache/wcpan/drive/google')

    async with Drive(path) as drive:
        node = await drive.get_node_by_path('/path/to/drive/file')
        ok = await drive.download_file(node, '/path/to/local')
```

## Command Line Usage

```sh
python3 -m wcpan.drive.google -h
```
