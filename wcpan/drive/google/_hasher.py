from collections.abc import Buffer
from typing import override, Protocol, Self
import hashlib

from wcpan.drive.core.types import Hasher


class BuiltinHasher(Protocol):
    def update(self, data: Buffer, /) -> None:
        ...

    def hexdigest(self) -> str:
        ...

    def digest(self) -> bytes:
        ...

    def copy(self) -> Self:
        ...


async def create_hasher():
    return Md5Hasher(hashlib.md5())


class Md5Hasher(Hasher):
    def __init__(self, hasher: BuiltinHasher):
        self._hasher = hasher

    @override
    async def update(self, data: bytes) -> None:
        self._hasher.update(data)

    @override
    async def hexdigest(self) -> str:
        return self._hasher.hexdigest()

    @override
    async def digest(self) -> bytes:
        return self._hasher.digest()

    @override
    async def copy(self) -> Self:
        hasher = self._hasher.copy()
        return self.__class__(hasher)
