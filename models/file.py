import io


class ChunkStream(io.RawIOBase):
    """File-like object that reads sequentially from a chunk iterator."""

    def __init__(self, chunk_iter):
        super().__init__()
        self._chunks = iter(chunk_iter)
        self._buffer = b""

    def readable(self):
        return True

    def readinto(self, b):
        try:
            while len(self._buffer) == 0:
                self._buffer = next(self._chunks)
        except StopIteration:
            return 0  # EOF

        n = min(len(b), len(self._buffer))
        b[:n] = self._buffer[:n]
        self._buffer = self._buffer[n:]
        return n
