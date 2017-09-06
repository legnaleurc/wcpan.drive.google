import argparse
import contextlib as cl
import functools as ft
import hashlib
import io
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
        wl.ERROR('wcpan.drive.google') << 'not found : {0}'.format(local_path)
        return
    if not child_node.is_folder:
        wl.ERROR('wcpan.drive.google') << 'should be folder : {0}'.format(local_path)
        return

    wl.INFO('wcpan.drive.google') << 'ok : {0}'.format(local_path)

    for child_path in local_path.iterdir():
        await verify_upload(drive, child_path, child_node)


async def verify_upload_file(drive, local_path, remote_node):
    file_name = local_path.name
    remote_path = drive.get_path_by_id(remote_node.id_)
    remote_path = pl.Path(remote_path, file_name)

    child_node = drive.get_child_by_id(remote_node.id_, file_name)

    if not child_node:
        wl.ERROR('wcpan.drive.google') << 'not found : {0}'.format(local_path)
        return
    if child_node.is_folder:
        wl.ERROR('wcpan.drive.google') << 'should be file : {0}'.format(local_path)
        return
    if not child_node.available:
        wl.ERROR('wcpan.drive.google') << 'trashed : {0}'.format(local_path)
        return

    with open(local_path, 'rb') as fin:
        local_md5 = stream_md5sum(fin)
    if local_md5 != child_node.md5:
        wl.ERROR('wcpan.drive.google') << 'md5 mismatch : {0}'.format(local_path)
        return

    wl.INFO('wcpan.drive.google') << 'ok : {0}'.format(local_path)


class UploadQueue(object):

    def __init__(self, drive):
        self._drive = drive
        self._lock = tl.Semaphore(value=8)
        self._final = tl.Condition()
        self._counter = 0
        self._total = 0
        self._failed = []

    async def upload(self, local_path_list, parent_node):
        self._counter = 0
        self._total = sum(self._count_tasks(_) for _ in local_path_list)
        for local_path in local_path_list:
            fn = ft.partial(self._internal_upload, local_path, parent_node)
            self._push(fn)
        await self._wait_for_complete()

    @property
    def failed(self):
        return self._failed

    def _count_tasks(self, local_path):
        total = 1
        for root, folders, files in os.walk(local_path):
            total = total + len(folders) + len(files)
        return total

    async def _wait_for_complete(self):
        await self._final.wait()

    async def _internal_upload(self, local_path, parent_node):
        if op.isdir(local_path):
            rv = await self._retry_create_folder(local_path, parent_node)
        else:
            rv = await self._retry_upload_file(local_path, parent_node)
        return rv

    async def _retry_create_folder(self, local_path, parent_node):
        await self._log_begin(local_path)

        folder_name = op.basename(local_path)
        while True:
            try:
                node = await self._drive.create_folder(parent_node, folder_name)
                break
            except NetworkError as e:
                wl.EXCEPTION('wcpan.drive.google', e) << e.error
                if e.status not in ('599',) and e.fatal:
                    self._add_failed(local_path)
                    raise

        await self._log_end(node)

        for child_path in os.listdir(local_path):
            child_path = op.join(local_path, child_path)
            fn = ft.partial(self._internal_upload, child_path, node)
            self._push(fn)

        return node

    async def _retry_upload_file(self, local_path, parent_node):
        await self._log_begin(local_path)

        while True:
            try:
                node = await self._drive.upload_file(local_path, parent_node)
                break
            except NetworkError as e:
                wl.EXCEPTION('wcpan.drive.google', e) << e.error
                if e.status not in ('599',) and e.fatal:
                    self._add_failed(local_path)
                    raise

        await self._log_end(node)

        return node

    def _push(self, runnable):
        loop = ti.IOLoop.current()
        fn = ft.partial(self._do_upload_task, runnable)
        loop.add_callback(fn)

    async def _do_upload_task(self, runnable):
        async with self._lock:
            await runnable()
            self._counter = self._counter + 1
            if self._counter == self._total:
                self._final.notify()

    def _add_failed(self, local_path):
        self._failed.append(local_path)

    async def _log_begin(self, local_path):
        progress = self._get_progress()
        wl.INFO('wcpan.drive.google') << '{0} begin {1}'.format(progress, local_path)

    async def _log_end(self, remote_node):
        progress = self._get_progress()
        remote_path = await self._drive.get_path(remote_node)
        wl.INFO('wcpan.drive.google') << '{0} end {1}'.format(progress, remote_path)

    def _get_progress(self):
        return '[{0}/{1}]'.format(self._counter, self._total)


class DownloadQueue(object):

    def __init__(self, drive):
        self._drive = drive
        self._lock = tl.Semaphore(value=8)
        self._final = tl.Condition()
        self._counter = 0
        self._total = 0
        self._failed = []

    async def download(self, node_list, local_path):
        self._counter = 0
        total = (self._count_tasks(_) for _ in node_list)
        total = await tg.multi(total)
        self._total = sum(total)
        for node in node_list:
            fn = ft.partial(self._internal_download, node, local_path)
            self._push(fn)
        await self._wait_for_complete()

    @property
    def failed(self):
        return self._failed

    async def _count_tasks(self, node):
        total = 1
        children = await self._drive.get_children(node)
        count = (self._count_tasks(_) for _ in children)
        count = await tg.multi(count)
        return total + sum(count)

    async def _wait_for_complete(self):
        await self._final.wait()

    async def _internal_download(self, node, local_path):
        if node.is_folder:
            rv = await self._create_folder(node, local_path)
        else:
            rv = await self._retry_download_file(node, local_path)
        return rv

    async def _create_folder(self, node, local_path):
        await self._log_begin(node)

        full_path = op.join(local_path, node.name)
        try:
            os.makedirs(full_path, exist_ok=True)
        except Exception as e:
            wl.EXCEPTION('wcpan.drive.google', e) << e.error
            self._add_failed(node)

        await self._log_end(full_path)

        children = await self._drive.get_children(node)
        for child in children:
            fn = ft.partial(self._internal_download, child, full_path)
            self._push(fn)

        return full_path

    async def _retry_download_file(self, node, local_path):
        await self._log_begin(node)

        while True:
            try:
                rv = await self._drive.download_file(node, local_path)
                break
            except NetworkError as e:
                wl.EXCEPTION('wcpan.drive.google', e) << e.error
                if e.status not in ('599',) and e.fatal:
                    self._add_failed(node)
                    raise

        full_path = op.join(local_path, node.name)
        await self._log_end(full_path)

        return rv

    def _push(self, runnable):
        loop = ti.IOLoop.current()
        fn = ft.partial(self._do_download_task, runnable)
        loop.add_callback(fn)

    async def _do_download_task(self, runnable):
        async with self._lock:
            await runnable()
            self._counter = self._counter + 1
            if self._counter == self._total:
                self._final.notify()

    def _add_failed(self, local_path):
        self._failed.append(local_path)

    async def _log_begin(self, remote_node):
        progress = self._get_progress()
        remote_path = await self._drive.get_path(remote_node)
        wl.INFO('wcpan.drive.google') << '{0} begin {1}'.format(progress, remote_path)

    async def _log_end(self, local_path):
        progress = self._get_progress()
        wl.INFO('wcpan.drive.google') << '{0} end {1}'.format(progress, local_path)

    def _get_progress(self):
        return '[{0}/{1}]'.format(self._counter, self._total)


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
    if not args.action:
        await args.fallback_action()
        return 0

    path = op.expanduser('~/.cache/wcpan/drive/google')
    drive = Drive(path)
    await drive.initialize()

    return await args.action(drive, args)


def parse_args(args):
    parser = argparse.ArgumentParser('wdg')

    commands = parser.add_subparsers()

    sync_parser = commands.add_parser('sync', aliases=['s'])
    sync_parser.set_defaults(action=action_sync)

    dl_parser = commands.add_parser('find', aliases=['f'])
    add_bool_argument(dl_parser, 'id_only')
    add_bool_argument(dl_parser, 'include_trash')
    dl_parser.add_argument('pattern', type=str)
    dl_parser.set_defaults(action=action_find, id_only=False,
                           include_trash=False)

    list_parser = commands.add_parser('list', aliases=['ls'])
    list_parser.set_defaults(action=action_list)
    list_parser.add_argument('id_or_path', type=str)

    tree_parser = commands.add_parser('tree')
    tree_parser.set_defaults(action=action_tree)
    tree_parser.add_argument('id_or_path', type=str)

    dl_parser = commands.add_parser('download', aliases=['dl'])
    dl_parser.set_defaults(action=action_download)
    dl_parser.add_argument('id_or_path', type=str, nargs='+')
    dl_parser.add_argument('destination', type=str)

    ul_parser = commands.add_parser('upload', aliases=['ul'])
    ul_parser.set_defaults(action=action_upload)
    ul_parser.add_argument('source', type=str, nargs='+')
    ul_parser.add_argument('id_or_path', type=str)

    rm_parser = commands.add_parser('remove', aliases=['rm'])
    rm_parser.set_defaults(action=action_remove)
    rm_parser.add_argument('id_or_path', type=str, nargs='+')

    sout = io.StringIO()
    parser.print_help(sout)
    fallback = ft.partial(action_help, sout.getvalue())
    parser.set_defaults(action=None, fallback_action=fallback)

    args = parser.parse_args(args)

    return args


def add_bool_argument(parser, name):
    flag = name.replace('_', '-')
    pos_flag = '--' + flag
    neg_flag = '--no-' + flag
    parser.add_argument(pos_flag, dest=name, action='store_true')
    parser.add_argument(neg_flag, dest=name, action='store_false')


async def action_help(message):
    print(message)


async def action_sync(drive, args):
    await drive.sync()
    return 0


async def action_find(drive, args):
    nodes = await drive.find_nodes_by_regex(args.pattern)
    if not args.include_trash:
        nodes = (_ for _ in nodes if _.is_available)
    nodes = {_.id_: drive.get_path(_) for _ in nodes}
    nodes = await tg.multi(nodes)

    if args.id_only:
        for id_ in nodes:
            print(id_)
    else:
        print_as_yaml(nodes)

    return 0


async def action_list(drive, args):
    node = await get_node_by_id_or_path(drive, args.id_or_path)
    nodes = await drive.get_children(node)
    nodes = {_.id_: _.name for _ in nodes}
    print_as_yaml(nodes)
    return 0


async def action_tree(drive, args):
    node = await get_node_by_id_or_path(drive, args.id_or_path)
    await traverse_node(drive, node, 0)
    return 0


async def action_download(drive, args):
    node_list = (get_node_by_id_or_path(drive, _) for _ in args.id_or_path)
    node_list = await tg.multi(node_list)
    queue_ = DownloadQueue(drive)
    await queue_.download(node_list, args.destination)
    return 0


async def action_upload(drive, args):
    node = await get_node_by_id_or_path(drive, args.id_or_path)
    queue_ = UploadQueue(drive)
    await queue_.upload(args.source, node)
    return 0


async def action_remove(drive, args):
    rv = (trash_node(drive, _) for _ in args.id_or_path)
    rv = await tg.multi(rv)
    rv = filter(None, rv)
    rv = list(rv)
    if rv:
        print_as_yaml(rv)
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


async def trash_node(drive, id_or_path):
    '''
    :returns: None if succeed, id_or_path if failed
    '''
    node = await get_node_by_id_or_path(drive, id_or_path)
    if not node:
        return id_or_path
    try:
        rv = await drive.trash_node(node)
    except Exception as e:
        return id_or_path
    if not rv:
        return id_or_path
    return None


def print_node(name, level):
    level = ' ' * level
    print(level + name)


def print_as_yaml(data):
    yaml.safe_dump(data, stream=sys.stdout, allow_unicode=True,
                   encoding=sys.stdout.encoding, default_flow_style=False)


main_loop = ti.IOLoop.instance()
exit_code = main_loop.run_sync(main)
main_loop.close()
sys.exit(exit_code)
