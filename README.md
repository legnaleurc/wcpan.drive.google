# wcpan.drive.google

`FileService` extension for wcpan.drive which provides Google Drive support.

Please use `wcpan.drive.google.create_service` to create the file.

## Requirement

Need a `client_secret.json` file which can be downloaded from
Google Developer Console.

## Example

```python
from functools import partial

from wcpan.drive.core import create_drive
from wcpan.drive.google import create_service


async def main():
    # Your API credential.
    client_secret = "/path/to/client_secret.json"
    # Stores access token and refresh token.
    oauth_token = "/path/to/oauth_token.json"

    create_file_service = partial(
        create_service,
        client_secret=client_secret,
        oauth_token=oauth_token,
    )

    async with create_drive(
        file=create_file_service,
        snapshot=...,
    ) as drive:
        ...
```
