#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2019 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import pathlib
import os
import shutil
import functools
import tarfile
import io

class BaseFixup:
    def __init__(self, python_path):
        thunk='''\
#!/usr/bin/env bash
x=$(readlink -f "$0")
b=$(basename "$x")
d=$(dirname "$x")
PYTHONPATH="$d":$PYTHONPATH PATH="{}":$PATH exec -a "$0" "$d"/libexec/"$b.bin" "$@"
'''
        self.thunk = thunk.format(os.path.dirname(python_path))

    def binfile(self, filename):
        dirname = os.path.dirname(filename)
        basename = os.path.basename(filename)
        return os.path.join(dirname, "libexec", basename)

    def fix_shebang(self, dest, original_stat, bytes_stream):
        pass

    def copy_as_is(self, orig, dest):
        pass

    def generate_thunk(self, out):
        pass

class TarFileFixup(BaseFixup):
    def __init__(self, python_path, archive):
        super().__init__(python_path)
        self.ar = archive

    def fix_shebang(self, dest, original_stat, bytes_stream):
        bytes_stream.seek(0, 2)
        ti = tarfile.TarInfo(name=self.binfile(dest))
        ti.size = bytes_stream.tell()
        ti.mode = original_stat.st_mode
        ti.mtime = original_stat.st_mtime
        bytes_stream.seek(0, 0)
        self.ar.addfile(ti, fileobj=bytes_stream)

    def copy_as_is(self, orig, dest):
        self.ar.add(orig, arcname=dest)

    def generate_thunk(self, original_stat, out):
        ti = tarfile.TarInfo(out)
        ti.size = len(self.thunk)
        ti.mode = 0o755
        ti.mtime = original_stat.st_mtime
        self.ar.addfile(ti, fileobj=io.BytesIO(self.thunk.encode()))

class FilesystemFixup(BaseFixup):
    def __init__(self, python_path, installroot):
        super().__init__(python_path)
        self.installroot = installroot
        pathlib.Path(self.installroot).mkdir(parents=False, exist_ok=True)

    def fix_shebang(self, dest, original_stat, bytes_stream):
        dest = os.path.join(self.installroot, self.binfile(dest))
        pathlib.Path(os.path.dirname(dest)).mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as out:
            bytes_stream.seek(0)
            os.chmod(dest, original_stat.st_mode)
            out.write(bytes_stream.read())

    def copy_as_is(self, orig, dest):
        dest = os.path.join(self.installroot, dest)
        pathlib.Path(os.path.dirname(dest)).mkdir(parents=True, exist_ok=True)
        shutil.copy2(orig, dest)

    def generate_thunk(self, original_stat, out):
        bash_thunk = os.path.join(self.installroot, out)
        with open(bash_thunk, "w") as f:
            os.chmod(bash_thunk, original_stat.st_mode)
            f.write(self.thunk)

def fixup_script(output, script_name):
    '''Given a script as a parameter, fixup the script so it can be transparently called with a non-standard python3 interpreter.
    This will generate a copy of the script in the libexec/ inside the script's directory location with the shebang pointing to env
    instead of a hardcoded python location, and will replace the main script with a thunk that calls into the right interpreter
    '''

    script = os.path.realpath(script_name)
    newpath = "libexec"
    newpy = script_name + ".bin"
    orig_stat = os.stat(script)

    if not os.access(script, os.X_OK):
        output.copy_as_is(script, script_name)
        return

    with open(script, "r") as f:
        firstline = f.readline()
        if not firstline.startswith("#!") or not "python" in firstline:
            output.copy_as_is(script, script_name)
            return

        obj = io.BytesIO()
        shebang = "#!/usr/bin/env python3\n"

        obj.write(shebang.encode())
        for l in f:
            obj.write(l.encode())

        output.fix_shebang(newpy, orig_stat, obj)

    output.generate_thunk(orig_stat, script_name)

def fixup_scripts(output, scripts):
    for script in scripts:
        fixup_script(output, script)

def fixup_scripts_in_directory(output, directory):
    fixup_scripts(output, [ os.path.join(directory, x) for x in os.listdir(directory) ])

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description='Modify a python3 script adding indirection to its execution so it can run with a non-standard interpreter.')
    ap.add_argument('--with-python3', required=True,
                    help='path of the python3 interepreter')
    ap.add_argument('--installroot', required=True,
                    help='directory where to copy the files')
    ap.add_argument('scripts', nargs='+', help='list of python modules scripts to modify')

    args = ap.parse_args()
    archive = FilesystemFixup(args.with_python3, args.installroot)
    fixup_scripts(archive, args.scripts)
