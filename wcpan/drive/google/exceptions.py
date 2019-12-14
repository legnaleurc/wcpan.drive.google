from wcpan.drive.core.exceptions import DriveError


class AuthenticationError(DriveError):
    pass


class CredentialFileError(DriveError):

    @staticmethod
    def _get_dummy_config_format() -> str:
        import json
        dict_ = {
            'installed': {
                'redirect_uris': [
                    '__URI__',
                ],
                'client_id': '__ID__',
                'client_secret': '__SECRET__',
            },
        }
        return json.dumps(dict_, ensure_ascii=False, indent=2)

    def __init__(self) -> None:
        super().__init__(self, (
            'invalid config file format, correct example:\n'
            f'{CredentialFileError._get_dummy_config_format()}'
        ))


class TokenFileError(DriveError):
    pass


class NetworkError(DriveError):
    pass
