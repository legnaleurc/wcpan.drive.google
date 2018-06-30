import argparse
import asyncio
import contextlib as cl
import functools as ft
import hashlib
import io
import os
import os.path as op
import pathlib as pl
import sys

import yaml
import wcpan.logger as wl

from .drive import Drive, DownloadError
from .util import stream_md5sum
from .network import NetworkError


async def verify_upload(drive, local_path, remote_node):
    if local_path.is_dir():
        await verify_upload_directory(drive, local_path, remote_node)
    else:
        await verify_upload_file(drive, local_path, remote_node)


async def verify_upload_directory(drive, local_path, remote_node):
    dir_name = local_path.name

    child_node = drive.get_child_by_name_from_parent(dir_name, remote_node)
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

    child_node = drive.get_child_by_name_from_parent(file_name, remote_node)

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


class AbstractQueue(object):

    def __init__(self, drive):
        self._drive = drive
        self._lock = asyncio.Semaphore(value=8)
        self._final = asyncio.Condition()
        self._counter = 0
        self._total = 0
        self._failed = []

    @property
    def drive(self):
        return self._drive

    @property
    def failed(self):
        return self._failed

    async def run(self, src_list, dst):
        if not src_list:
            return
        self._counter = 0
        total = (self.count_tasks(_) for _ in src_list)
        total = await asyncio.gather(*total)
        self._total = sum(total)
        for src in src_list:
            self._push_task(self._run_one_task(src, dst))
        await self._wait_for_complete()

    async def count_tasks(self, src):
        raise NotImplementedError()

    def source_is_folder(self, src):
        raise NotImplementedError()

    async def do_folder(self, src, dst):
        raise NotImplementedError()

    async def get_children(self, src):
        raise NotImplementedError()

    async def do_file(self, src, dst):
        raise NotImplementedError()

    async def _run_one_task(self, src, dst):
        if self.source_is_folder(src):
            rv = await self._run_for_folder(src, dst)
        else:
            rv = await self._run_for_file(src, dst)
        return rv

    async def _run_for_folder(self, src, dst):
        await self._log_begin(src)
        try:
            rv = await self.do_folder(src, dst)
        except Exception as e:
            self._add_failed(src)
            rv = None
        await self._log_end(rv)

        if not rv:
            return None

        children = await self.get_children(src)
        for child in children:
            self._push_task(self._run_one_task(child, rv))

        return rv

    async def _run_for_file(self, src, dst):
        await self._log_begin(src)
        try:
            rv = await self.do_file(src, dst)
        except Exception as e:
            self._add_failed(src)
            rv = None
        await self._log_end(rv)
        return rv

    async def _wait_for_complete(self):
        async with self._final:
            await self._final.wait()

    def _push_task(self, future):
        future = self._task_runner(future)
        loop = asyncio.get_event_loop()
        loop.create_task(future)

    async def _task_runner(self, future):
        async with self._lock:
            await future
            self._counter += 1
            if self._counter == self._total:
                async with self._final:
                    self._final.notify()

    def _add_failed(self, src):
        self._failed.append(src)

    async def _log_begin(self, node_or_path):
        await self._log('begin', node_or_path)

    async def _log_end(self, node_or_path):
        await self._log('end', node_or_path)

    async def _log(self, begin_or_end, node_or_path):
        progress = self._get_progress()
        if node_or_path is None:
            node_or_path = '__FATAL_ERROR__'
        elif not isinstance(node_or_path, str):
            node_or_path = await self._drive.get_path(node_or_path)
        wl.INFO('wcpan.drive.google') << '{0} {1} {2}'.format(progress, begin_or_end, node_or_path)

    def _get_progress(self):
        return '[{0}/{1}]'.format(self._counter, self._total)


class UploadQueue(AbstractQueue):

    def __init__(self, drive):
        super(UploadQueue, self).__init__(drive)

    async def count_tasks(self, local_path):
        total = 1
        for root, folders, files in os.walk(local_path):
            total = total + len(folders) + len(files)
        return total

    def source_is_folder(self, local_path):
        return op.isdir(local_path)

    async def do_folder(self, local_path, parent_node):
        folder_name = op.basename(local_path)
        while True:
            try:
                node = await self.drive.create_folder(parent_node, folder_name,
                                                      exist_ok=True)
                break
            except NetworkError as e:
                wl.EXCEPTION('wcpan.drive.google', e)
                if e.status not in ('599',) and e.fatal:
                    raise
        return node

    async def get_children(self, local_path):
        rv = os.listdir(local_path)
        rv = (op.join(local_path, _) for _ in rv)
        return rv

    async def do_file(self, local_path, parent_node):
        while True:
            try:
                node = await self.drive.upload_file(local_path, parent_node,
                                                    exist_ok=True)
                break
            except NetworkError as e:
                wl.EXCEPTION('wcpan.drive.google', e)
                if e.status not in ('599',) and e.fatal:
                    raise
        return node


class DownloadQueue(AbstractQueue):

    def __init__(self, drive):
        super(DownloadQueue, self).__init__(drive)

    async def count_tasks(self, node):
        total = 1
        children = await self.drive.get_children(node)
        count = (self.count_tasks(_) for _ in children)
        count = await asyncio.gather(*count)
        return total + sum(count)

    def source_is_folder(self, node):
        return node.is_folder

    async def do_folder(self, node, local_path):
        full_path = op.join(local_path, node.name)
        try:
            os.makedirs(full_path, exist_ok=True)
        except Exception as e:
            wl.EXCEPTION('wcpan.drive.google', e)
            raise
        return full_path

    async def get_children(self, node):
        return await self.drive.get_children(node)

    async def do_file(self, node, local_path):
        while True:
            try:
                rv = await self.drive.download_file(node, local_path)
                break
            except DownloadError as e:
                wl.EXCEPTION('wcpan.drive.google', e)
                raise
            except NetworkError as e:
                wl.EXCEPTION('wcpan.drive.google', e)
                if e.status not in ('599',) and e.fatal:
                    raise
        return rv


async def main(args=None):
    if args is None:
        args = sys.argv

    wl.setup((
        'wcpan.drive.google',
    ))

    args = parse_args(args[1:])
    if not args.action:
        await args.fallback_action()
        return 0

    path = op.expanduser('~/.cache/wcpan/drive/google')

    async with Drive(path) as drive:
        return await args.action(drive, args)


def parse_args(args):
    parser = argparse.ArgumentParser('wdg')

    commands = parser.add_subparsers()

    sync_parser = commands.add_parser('sync', aliases=['s'])
    sync_parser.set_defaults(action=action_sync)

    find_parser = commands.add_parser('find', aliases=['f'])
    add_bool_argument(find_parser, 'id_only')
    add_bool_argument(find_parser, 'include_trash')
    find_parser.add_argument('pattern', type=str)
    find_parser.set_defaults(action=action_find, id_only=False,
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

    mv_parser = commands.add_parser('rename', aliases=['mv'])
    mv_parser.set_defaults(action=action_rename)
    mv_parser.add_argument('source_id_or_path', type=str)
    mv_parser.add_argument('destination_path', type=str)

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
    nodes = (wait_for_value(_.id_, drive.get_path(_)) for _ in nodes)
    nodes = await asyncio.gather(*nodes)
    nodes = dict(nodes)

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
    node_list = await asyncio.gather(*node_list)
    node_list = [_ for _ in node_list if not _.is_trashed]
    queue_ = DownloadQueue(drive)
    await queue_.run(node_list, args.destination)
    return 0


async def action_upload(drive, args):
    node = await get_node_by_id_or_path(drive, args.id_or_path)
    queue_ = UploadQueue(drive)
    await queue_.run(args.source, node)
    return 0


async def action_remove(drive, args):
    rv = (trash_node(drive, _) for _ in args.id_or_path)
    rv = await asyncio.gather(*rv)
    rv = filter(None, rv)
    rv = list(rv)
    if rv:
        print_as_yaml(rv)
    return 0


async def action_rename(drive, args):
    node = await get_node_by_id_or_path(drive, args.source_id_or_path)
    node = await drive.rename_node(node, args.destination_path)
    path = await drive.get_path(node)
    return path


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
        wl.EXCEPTION('wcpan.drive.google', e) << 'trash failed'
        return id_or_path
    if not rv:
        return id_or_path
    return None


async def wait_for_value(k, v):
    return k, await v


def print_node(name, level):
    level = ' ' * level
    print(level + name)


def print_as_yaml(data):
    yaml.safe_dump(data, stream=sys.stdout, allow_unicode=True,
                   encoding=sys.stdout.encoding, default_flow_style=False)


main_loop = asyncio.get_event_loop()
exit_code = main_loop.run_until_complete(main())
main_loop.close()
sys.exit(exit_code)
