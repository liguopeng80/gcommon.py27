# -*- coding: utf-8 -*-
#
# author: Guopeng Li
# created: 27 Aug 2008

import glob

import os
import shutil
from zipfile import ZipFile, ZIP_DEFLATED, ZipInfo


def remove_trailing_slash(path):
    if path.endswith('/'):
        path = path[:-1]

    return path


def write_file(filename, content):
    with open(filename, 'w+') as f:
        f.write(content)


def read_file(filename):
    with open(filename) as f:
        content = f.read()
        return content


def join_path(*paths):
    path = os.path.join(*paths)
    return os.path.abspath(path)


def empty_folder(folder_path):
    """Delete all files and sub-folders in current folder."""
    abspath = os.path.abspath(folder_path)
    if not folder_path.endswith(os.path.sep):
        abspath = abspath + os.path.sep

    if os.path.isdir(abspath):
        shutil.rmtree(abspath)
    else:
        """wrong"""


def delete_file(filename):
    """delete_file(file) -> None

    Remove a file if it exists.

    If the file is readonly, the function will firstly remove the readonly
    property of the file, and then try to delete it.
    """
    if not os.path.exists(filename):
        return
    try:
        os.remove(filename)
    except:
        # os.chmod(file, stat.S_IWRITE)
        os.chmod(filename, 0x80)
        os.remove(filename)


def delete_files(pat):
    """delete_files(pat) -> None

    Delete the source files whose names match the patten. For example:
    delete_files("c:/backup/*.doc")
    """
    for filename in glob.glob(pat):
        delete_file(filename)


def copy_files(src_pat, dest):
    """copy_files(src_pat, dest) -> None

    Copy the source files into the dest folder. A file will only be copied
    when its path and name matches the patten defined in pat.

    for example:
    copy_files("c:/workspace/*.doc", "c:/backup")
    """
    for filename in glob.glob(src_pat):
        shutil.copy(filename, dest)


def copy_files_ext(src_pat, dest):
    """Copy files and keep their folder structure.

    copy_files_ext(r'c:/test/source/*', "c:/dest/win32")
    """
    for filename in glob.glob(src_pat):
        if filename == src_pat:
            continue

        if os.path.isdir(filename):
            # copy a folder
            dir_name = os.path.join(dest, filename[len(src_pat) - 1:])
            if not os.path.exists(dir_name):
                os.mkdir(dir_name)
                pass
            copy_files_ext(filename + '/*', dir_name)
        else:
            # copy a source file
            shutil.copy(filename, dest)


def delete_folder(folder_path):
    abspath = os.path.abspath(folder_path)
    if abspath.endswith(os.path.sep):
        abspath = abspath[:-1]

    if os.path.isdir(abspath):
        shutil.rmtree(abspath)
    else:
        """wrong"""


def make_directories(folders):
    """make_directories(folders)

    Create directories listed in the folders. If the parent of one new folder
    is not existed, a exception will be thrown out.
    """
    for folder in folders:
        make_dir(folder)


def make_dir(path):
    if not os.path.exists(path):
        os.mkdir(path)


def zip(archive_name, *targets):
    """Create a zip file and add all targets into it.

    zip('c:/temp/test.zip', 'c:/guli.py', 'c:/guli', 'd:/guli/test/')
    """
    z = ZipFile(archive_name, "w", ZIP_DEFLATED)

    for path in targets:
        if os.path.isdir(path):
            _zip_a_folder(z, path)
        else:
            _zip_a_file(z, path)

    z.close()


def _zip_a_folder(z, path):
    # put all contents in a folder into the root directory of the zip file
    assert(os.path.isdir(path))

    _, filename = os.path.split(path)
    if filename:
        # no os sep on the trail
        base_path = filename + os.sep
    else:
        base_path = ''

    if path.endswith(os.sep):
        path = path[:-len(os.sep)]

    for root, dirs, files in os.walk(path):
        # "root" is based on path, for example:
        # path = . -> root = ./xxx
        # path = /var/log -> root = /var/log/xxx

        # NOTE: ignore empty directories
        for fn in files:
            abs_fn = os.path.join(root, fn)
            zfn = base_path + abs_fn[len(path)+len(os.sep):]  # XXX: relative path
            z.write(abs_fn, zfn)

        for dn in dirs:
            abs_fn = os.path.join(root, dn)
            zfn = base_path + abs_fn[len(path)+len(os.sep):] + '\\'  # XXX: relative path

            zfi = ZipInfo(zfn)
            zfi.external_attr = 48
            z.writestr(zfi, '')


def _zip_a_file(z, path):
    # put the file into the root directory of the zip file
    assert(os.path.isfile(path))

    _, filename = os.path.split(path)
    z.write(path, filename)

