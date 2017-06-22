import argparse
import contextlib as cl
import functools as ft
import hashlib
import os
import os.path as op
import pathlib as pl
import sys

from tornado import ioloop as ti, locks as tl, gen as tg
import yaml
import wcpan.logger as wl

from .drive import Drive
from .util import stream_md5sum
from .network import NetworkError


async def verify_upload(drive, local_path, remote_node):
    if local_path.is_dir():
        await verify_upload_directory(drive, local_path, remote_node)
    else:
        await verify_upload_file(drive, local_path, remote_node)


async def verify_upload_directory(drive, local_path, remote_node):
    dir_name = local_path.name

    child_node = drive.get_child_by_id(remote_node.id_, dir_name)
    if not child_node:
        wl.ERROR('wcpan.gd') << 'not found : {0}'.format(local_path)
        return
    if not child_node.is_folder:
        wl.ERROR('wcpan.gd') << 'should be folder : {0}'.format(local_path)
        return

    wl.INFO('wcpan.gd') << 'ok : {0}'.format(local_path)

    for child_path in local_path.iterdir():
        await verify_upload(drive, child_path, child_node)


async def verify_upload_file(drive, local_path, remote_node):
    file_name = local_path.name
    remote_path = drive.get_path_by_id(remote_node.id_)
    remote_path = pl.Path(remote_path, file_name)

    child_node = drive.get_child_by_id(remote_node.id_, file_name)

    if not child_node:
        wl.ERROR('wcpan.gd') << 'not found : {0}'.format(local_path)
        return
    if child_node.is_folder:
        wl.ERROR('wcpan.gd') << 'should be file : {0}'.format(local_path)
        return
    if not child_node.available:
        wl.ERROR('wcpan.gd') << 'trashed : {0}'.format(local_path)
        return

    with open(local_path, 'rb') as fin:
        local_md5 = stream_md5sum(fin)
    if local_md5 != child_node.md5:
        wl.ERROR('wcpan.gd') << 'md5 mismatch : {0}'.format(local_path)
        return

    wl.INFO('wcpan.gd') << 'ok : {0}'.format(local_path)


class UploadQueue(object):

    def __init__(self):
        self._lock = tl.Semaphore(value=8)
        self._final = tl.Condition()
        self._counter = 0
        self._failed = []

    def push(self, runnable):
        loop = ti.IOLoop.current()
        fn = ft.partial(self._do_push, runnable)
        loop.add_callback(fn)
        self._counter = self._counter + 1

    def add_failed(self, local_path):
        self._failed.append(local_path)

    async def wait_for_complete(self):
        await self._final.wait()

    @property
    def failed(self):
        return self._failed

    async def _do_push(self, runnable):
        async with self._lock:
            with self._upload_counter():
                wl.DEBUG('wcpan.gd') << 'tasks' << self._counter
                await runnable()

    @cl.contextmanager
    def _upload_counter(self):
        try:
            yield
        finally:
            self._counter = self._counter - 1
            if self._counter <= 0:
                self._final.notify()


async def upload(queue_, drive, local_path, parent_node):
    if op.isdir(local_path):
        rv = await retry_create_folder(queue_, drive, local_path, parent_node)
    else:
        rv = await retry_upload_file(queue_, drive, local_path, parent_node)
    return rv


async def retry_upload_file(queue_, drive, local_path, parent_node):
    while True:
        try:
            rv = await drive.upload_file(local_path, parent_node)
            break
        except NetworkError as e:
            wl.EXCEPTION('wcpan.gd', e) << e.error
            if e.status not in ('599',) and e.fatal:
                queue_.add_failed(local_path)
                raise
    return rv


async def retry_create_folder(queue_, drive, local_path, parent_node):
    while True:
        try:
            rv = await drive.create_folder(local_path, parent_node)
            break
        except NetworkError as e:
            wl.EXCEPTION('wcpan.gd', e) << e.error
            if e.status not in ('599',) and e.fatal:
                queue_.add_failed(local_path)
                raise

    for child_path in os.listdir(local_path):
        child_path = op.join(local_path, child_path)
        fn = ft.partial(upload, queue_, drive, child_path, rv)
        queue_.push(fn)

    return rv


async def upload_local_to_remote(drive, local_path, remote_path):
    queue_ = UploadQueue()

    node = drive.get_node_by_path(remote_path)
    fn = ft.partial(upload, queue_, drive, local_path, node)
    queue_.push(fn)
    await tg.sleep(1)
    await queue_.wait_for_complete()
    print(queue_.failed)


async def main(args=None):
    if args is None:
        args = sys.argv

    wl.setup((
        'tornado.access',
        'tornado.application',
        'tornado.general',
        'wcpan.drive.google',
    ))

    args = parse_args(args[1:])

    path = op.expanduser('~/.cache/wcpan/drive/google')
    drive = Drive(path)
    drive.initialize()

    return await args.action(drive, args)

    local_path = args[1]
    remote_path = args[2]

    await upload_local_to_remote(drive, local_path, remote_path)

    return 0

    local_path = pl.Path(local_path)
    remote_path = drive.get_node_by_path(remote_path)
    await verify_upload(drive, local_path, remote_path)

    return 0


def parse_args(args):
    parser = argparse.ArgumentParser('wdg')
    commands = parser.add_subparsers()

    sync_parser = commands.add_parser('sync', aliases=['s'])
    sync_parser.set_defaults(action=action_sync)

    dl_parser = commands.add_parser('find', aliases=['f'])
    dl_parser.set_defaults(action=action_find)
    dl_parser.add_argument('pattern', type=str)

    dl_parser = commands.add_parser('download', aliases=['dl'])
    dl_parser.set_defaults(action=action_download)
    dl_parser.add_argument('id_or_path', type=str)
    dl_parser.add_argument('destination', type=str)

    tree_parser = commands.add_parser('tree')
    tree_parser.set_defaults(action=action_tree)
    tree_parser.add_argument('id_or_path', type=str)

    args = parser.parse_args(args)

    return args


async def action_sync(drive, args):
    await drive.sync()
    return 0


async def action_find(drive, args):
    nodes = await drive.find_nodes_by_regex(args.pattern)
    nodes = {_.id_: drive.get_path(_) for _ in nodes}
    nodes = await tg.multi(nodes)
    yaml.dump(nodes, stream=sys.stdout, default_flow_style=False)
    return 0


async def action_download(drive, args):
    node = await get_node_by_id_or_path(drive, args.id_or_path)
    await drive.download_file(node, args.destination)
    return 0


async def action_tree(drive, args):
    node = await get_node_by_id_or_path(drive, args.id_or_path)
    await traverse_node(drive, node, 0)
    return 0


async def get_node_by_id_or_path(drive, id_or_path):
    if id_or_path[0] == '/':
        node = await drive.get_node_by_path(id_or_path)
    else:
        node = await drive.get_node_by_id(id_or_path)
    return node


async def traverse_node(drive, node, level):
    if node.is_root:
        print_node('/', level)
    elif level == 0:
        top_path = await drive.get_path(node)
        print_node(top_path, level)
    else:
        print_node(node.name, level)

    if node.is_folder:
        children = await drive.get_children_by_id(node.id_)
        for child in children:
            await traverse_node(drive, child, level + 1)


def print_node(name, level):
    level = ' ' * level
    print(level + name)


main_loop = ti.IOLoop.instance()
exit_code = main_loop.run_sync(main)
main_loop.close()
sys.exit(exit_code)
