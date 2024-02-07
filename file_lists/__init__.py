import sys
from collections.abc import MutableSequence
from typing import overload, Iterable

import h5py
import tempfile
import os
import numpy as np


class FileFloatList(MutableSequence[float]):

    def __init__(self, copy_from: list[float] = None, initial_size: int = 100, increment: int = 100):

        self._size      = 0
        self._increment = increment
        self._temp      = tempfile.NamedTemporaryFile(dir=os.path.curdir)
        self._file      = h5py.File(name=self._temp, mode="a")

        if copy_from is None:
            self._data  = self._file.create_dataset(name="list", shape=(initial_size,), maxshape=(None,), dtype=float, chunks=True)
        else:
            self._data    = self._file.create_dataset("list", (len(copy_from),), maxshape=(None,), dtype=float, chunks=True)
            self._data[:] = copy_from
            self._size    = len(copy_from)


    def __del__(self):

        try:
            self._file.close()
        except:
            ...

        try:
            self._temp.close()
        except:
            ...

    def copy(self):

        return FileFloatList(copy_from=self)


    def append(self, __object):

        self._ensure_space(self._size + 1)
        self._data[self._size] = __object
        self._size += 1

    def _ensure_space(self, size: int = -1):

        if size > self._data.size:
            self._data.resize((max(size, self._data.size + self._increment),))


    def extend(self, __iterable):

        sz = len(__iterable)
        self._ensure_space(self._size + sz)
        self._data[(self._size):(self._size + sz)] = __iterable
        self._size += sz


    def pop(self, __index=-1):

        if __index < 0:
            __index = self._size + __index

        item = self[__index]

        if __index == -1:
            self._move_chunk(start=self._size, spaces=-1)
        else:
            self._move_chunk(start=__index+1, spaces=-1)

        return item

    def _move_chunk(self, start: int, spaces: int):

        if spaces == 0:
            return

        new_size = self._size + spaces

        self._ensure_space(new_size)

        if spaces > 0:

            for i in range(self._size - 1, start - 1, -1):

                self._data[i + spaces] = self._data[i]
                self._data[i]          = 0.0

        else:

            for i in range(start, self._size, +1):

                self._data[i + spaces] = self._data[i]
                self._data[i]          = 0.0

        self._size = new_size


    def index(self, __value, __start=0, __stop=sys.maxsize):

        for i in range(__start, min(self._size, __stop)):
            if self[i] == __value:
                return i

        return -1


    def count(self, __value):

        count = 0

        for thing in self:

            if (thing == __value):
                count += 1

        return count


    def insert(self, __index, __object):

        self._move_chunk(__index, +1)
        self[__index] = __object



    def remove(self, __value):

        for i in range(0, self._size):
            if self[i] == __value:
                self._move_chunk(i + 1, -1)


    @overload
    def __getitem__(self, index: slice) -> MutableSequence[float]:
        ...

    @overload
    def __getitem__(self, index: int) -> float:
        ...

    def __getitem__(self, index: int|slice):

        if type(index) is slice:

            start = 0
            stop  = 0

            if index.start is None:
                start = 0
            else:
                start = min(index.start, self._size)


            if index.stop is None:
                stop = self._size
            else:
                stop = min(index.stop, self._size)

            return self._data[start:stop:index.step].tolist()

        elif type(index) is int:

            if (index < 0 or index >= self._size):
                raise IndexError()

            return self._data[index]

        else:
            raise ValueError()


    @overload
    def __setitem__(self, index: int, value: float):
        ...

    @overload
    def __setitem__(self, index: slice, value: Iterable[float]) -> None:
        ...

    def __setitem__(self, index: int|slice, value: int|Iterable[float]):
        self._data[index] = value


    @overload
    def __delitem__(self, index: int) -> None:
        ...

    @overload
    def __delitem__(self, index: slice) -> None:
        ...

    def __delitem__(self, index):

        if type(index) is int:

            if (index < 0 or index >= self._size):
                raise IndexError()

            self._move_chunk(index + 1, -1)

        elif type(index) is slice:

            start = 0
            stop  = 0

            if index.start is None:
                start = 0
            else:
                start = min(index.start, self._size)

            if index.stop is None:
                stop = self._size
            else:
                stop = min(index.stop, self._size)

            self._move_chunk(stop, start - stop)

        else:
            raise ValueError()


    def __len__(self):
        return self._size

class FileNDList(MutableSequence[np.ndarray]):

    def __init__(self, dtype = float):

        self._size  = 0
        self._temp  = tempfile.NamedTemporaryFile(dir=os.path.curdir)
        self._file  = h5py.File(name=self._temp, mode="a")


    def __del__(self):

        try:
            self._file.close()
        except:
            ...

        try:
            self._temp.close()
        except:
            ...


    def copy(self):
        return []


    def append(self, __object: np.ndarray):
        data    = self._file.create_dataset(name="%d" % self._size, shape=__object.shape, dtype=__object.dtype, chunks=True)
        data[:] = __object[:]
        self._size += 1

    def extend(self, __iterable):
        for x in __iterable:
            self.append(x)


    def pop(self, __index=-1):

        if __index == -1:
            __index = self._size - 1

        item = self[__index]

        del self[__index]

        return item


    def index(self, __value, __start=0, __stop=sys.maxsize):

        for i in range(__start, min(self._size, __stop)):
            if self[i] == __value:
                return i

        return -1


    def count(self, __value):

        count = 0

        for thing in self:

            if (thing == __value):
                count += 1

        return count


    def _move_chunk(self, start: int, spaces: int):

        if spaces == 0:
            return

        new_size = self._size + spaces

        if spaces > 0:

            for i in range(self._size - 1, start - 1, -1):

                new_name = "%d" % (i + spaces)

                if new_name in self._file:
                    del self._file[new_name]

                data    = self._file.create_dataset(name=new_name, shape=self[i].shape, dtype=self[i].dtype, chunks=True)
                data[:] = self[i][:]

                del self._file["%d" % i]

        else:

            for i in range(start, self._size, +1):

                new_name = "%d" % (i + spaces)

                if new_name in self._file:
                    del self._file[new_name]

                data    = self._file.create_dataset(name=new_name, shape=self[i].shape, dtype=self[i].dtype, chunks=True)
                data[:] = self[i][:]

                del self._file["%d" % i]

        self._size = new_size

    def insert(self, __index, __object):

        self._move_chunk(__index, +1)
        self[__index] = __object



    def remove(self, __value):

        for i in range(0, self._size):
            if self[i] == __value:
                self._move_chunk(i + 1, -1)

    def __getitem__(self, index):

        if type(index) is slice:

            start = 0
            stop  = 0
            step  = 1
            ret   = []

            if index.start is None:
                start = 0
            else:
                start = min(index.start, self._size)

            if index.stop is None:
                stop = self._size
            else:
                stop = min(index.stop, self._size)

            if index.step is None:
                step = 1
            else:
                step = index.step


            for i in range(start, stop, step):
                ret.append(self[i])

            return ret

        else:

            key = "%d" % index

            if key in self._file:
                return np.asarray(self._file[key])
            else:
                raise IndexError()

    def __setitem__(self, index: int|slice, value: np.ndarray):

        if (key < 0 or key >= self._size):
            raise IndexError()

        key = "%d" % index

        if key in self._file:
            del self._file[key]

        data    = self._file.create_dataset(name="%d" % index, shape=value.shape, type=value.dtype, chunks=True)
        data[:] = value[:]


    def __delitem__(self, index):

        if type(index) is int:

            if (index < 0 or index >= self._size):
                raise IndexError()

            self._move_chunk(index + 1, -1)

        elif type(index) is slice:
            self._move_chunk(index.stop, index.start - index.stop)

        else:
            raise ValueError()


    def __len__(self):
        return self._size
