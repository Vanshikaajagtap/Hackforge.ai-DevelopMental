"""Poll-tail a growing file: byte offset + partial-line buffer + checkpoint resume + rotation/truncation.

No inotify: readline-style polling every `poll_seconds` is portable and plenty fast for this scale.
"""
from __future__ import annotations

import asyncio
import os
from typing import Awaitable, Callable, NamedTuple

LineSink = Callable[[str], Awaitable[None]]


class Checkpoint(NamedTuple):
    inode: int
    offset: int


class Tailer:
    def __init__(
        self,
        path: str,
        start_at: str = "end",
        poll_seconds: float = 0.2,
        resume: Checkpoint | None = None,
    ) -> None:
        if start_at not in {"end", "checkpoint"}:
            raise ValueError("start_at must be 'end' or 'checkpoint'")
        self.path = path
        self.start_at = start_at
        self.poll_seconds = poll_seconds
        self.resume = resume
        self.rotations = 0          # times we re-read from 0 after rotation/truncation
        self.resumed_from_checkpoint = False
        self._fh = None
        self._inode = 0
        self._buffer = b""
        self._first_open = True
        self._existed_at_start: bool | None = None    # decided once, on the very first poll

    # ---- state exposed for checkpoints / health -------------------------------------------------
    @property
    def inode(self) -> int | None:
        return self._inode if self._fh is not None else None

    @property
    def offset(self) -> int:
        """Byte offset of the first unparsed byte (what a checkpoint should store)."""
        return 0 if self._fh is None else self._fh.tell() - len(self._buffer)

    @property
    def file_ok(self) -> bool:
        return os.path.exists(self.path)

    # ---- opening ---------------------------------------------------------------------------------
    def _open(self) -> bool:
        if self._existed_at_start is None:
            self._existed_at_start = os.path.exists(self.path)
        try:
            fh = open(self.path, "rb")
        except FileNotFoundError:
            return False
        self._fh = fh
        self._inode = os.fstat(fh.fileno()).st_ino
        self._buffer = b""
        if self._first_open:
            size = os.fstat(fh.fileno()).st_size
            if self.resume and self.resume.inode == self._inode and 0 <= self.resume.offset <= size:
                fh.seek(self.resume.offset)                 # restart recovery: no replay
                self.resumed_from_checkpoint = True
            elif self.start_at == "end" and self._existed_at_start:
                fh.seek(0, os.SEEK_END)                     # don't replay old junk on a first run
            # else: read from byte 0 (checkpoint mode without a checkpoint, or a file created after start)
        self._first_open = False
        return True

    def _drain(self) -> list[str]:
        lines: list[str] = []
        while True:
            chunk = self._fh.read(65536)
            if not chunk:
                return lines
            self._buffer += chunk
            *complete, self._buffer = self._buffer.split(b"\n")   # the tail fragment waits for its "\n"
            for raw in complete:
                text = raw.decode("utf-8", errors="replace").rstrip("\r")
                if text.strip():
                    lines.append(text)

    def read_available(self) -> list[str]:
        """Return every complete line appended since the last call (sync; used by run() and tests)."""
        if self._fh is None and not self._open():
            return []
        lines = self._drain()                               # finish the current handle first
        try:
            st = os.stat(self.path)
        except FileNotFoundError:
            return lines                                    # rotated away, replacement not created yet
        cur = os.fstat(self._fh.fileno())
        rotated = st.st_ino != 0 and cur.st_ino != 0 and st.st_ino != cur.st_ino
        truncated = not rotated and st.st_size < self._fh.tell()
        if rotated:
            self.close()
            if self._open():                                # new file: start at 0
                self.rotations += 1
                lines += self._drain()
        elif truncated:
            self._fh.seek(0)
            self._buffer = b""
            self.rotations += 1
            lines += self._drain()
        return lines

    async def run(self, sink: LineSink) -> None:
        try:
            while True:
                try:
                    lines = self.read_available()
                except OSError:                              # transient (permissions, share violation): retry
                    lines = []
                for i, line in enumerate(lines):
                    await sink(line)
                    if i % 500 == 499:
                        await asyncio.sleep(0)               # a big backlog must not starve the event loop
                if not lines:
                    await asyncio.sleep(self.poll_seconds)
        finally:
            self.close()

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
