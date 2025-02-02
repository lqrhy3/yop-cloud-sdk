import os.path
import subprocess
from typing import Iterable
from urllib.parse import urljoin

import requests
from requests import Response
from tqdm import tqdm
from tabulate import tabulate

DOWNLOAD_CHUNK_SIZE = 64 * 1024  # kb


def generate_chunks_from_file(file_descriptor, pbar):
    while chunk := file_descriptor.read(DOWNLOAD_CHUNK_SIZE):
        pbar.update(len(chunk))
        yield chunk


def print_ls(list_files):
    table = []
    for file in list_files:
        table.append([file.get('name'), file.get('type'), file.get('size_human')])
    print(tabulate(table, headers=['Name', 'Type', 'Size'], tablefmt='pretty'))


class YOPStorage:
    def __init__(self, host_url: str, token: str):
        self._host_url = host_url
        self._token = token

        self._headers = {"Authorization": f"Bearer {self._token}"}

    def upload(self, src_file_path: str, dst_file_path: str, force: bool = False):
        """
        Uploads a file to the specified destination directory on the server.

        :param src_file_path: The path to the source file to be uploaded.
        :param dst_file_path: The path to the destination file on the server.
        """

        if not os.path.exists(src_file_path):
            raise RuntimeError(f'File {src_file_path} not found')

        isdir = os.path.isdir(src_file_path)
        if isdir:
            src_dir_path = src_file_path
            src_parent_dir_path, src_dir_basename = os.path.split(src_dir_path)
            src_file_path = os.path.join(src_parent_dir_path,  f'.{src_dir_basename}.tar.gz')
            try:
                subprocess.run(
                    ['tar', '--disable-copyfile', '-cz', '--no-xattrs', '-f', src_file_path, '-C', src_dir_path, '.'],
                    check=True
                )
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f'Failed to archive folder: {e}')

        file_size = os.path.getsize(src_file_path)

        try:
            with open(src_file_path, 'rb') as src_file:
                with tqdm(total=file_size, unit='B', unit_scale=True, desc=src_file_path) as pbar:
                    file_chunks_generator = generate_chunks_from_file(src_file, pbar)
                    self._do_upload(
                        file_chunks_generator=file_chunks_generator,
                        dst_file_path=dst_file_path,
                        isdir=isdir,
                        file_size=file_size,
                        force=force
                    )
        finally:
            # remove temp folder archive
            if isdir:
                os.remove(src_file_path)

    def download(self, src_file_path: str, dst_file_path: str):
        """
        Downloads a file from the server to the specified destination directory.

        :param src_file_path: The path to the source file (or folder) on the server.
        :param dst_file_path: The path to the destination file (or folder) on the local machine.
        """
        dst_dir_path, dst_file_name = os.path.split(dst_file_path)

        isdir = self._is_file_on_server_dir(src_file_path)
        if isdir:
            dst_dir_path = dst_file_path
            dst_parent_dir_path, dst_dir_basename = os.path.split(dst_dir_path)
            dst_file_path = os.path.join(dst_parent_dir_path, f'.{dst_dir_basename}.tar.gz')

        if dst_dir_path and not os.path.exists(dst_dir_path):
            os.makedirs(dst_dir_path)

        self._do_download(src_file_path, dst_file_path, isdir)
        if isdir:
            try:
                subprocess.run(['tar', '-xf', dst_file_path, '-C', dst_dir_path], check=True)
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f'Failed to unzip folder: {e}')
            finally:
                os.remove(dst_file_path)

    def delete(self, file_path: str):
        """
        Deletes a file from the server.

        :param file_path: The path to the file on the server.
        """
        self._do_delete(file_path)

    def list_files(self, list_path: str, print_result: bool = False, verbose: bool = False) -> list[dict]:
        """
        Lists files on the server by the specified path.
        :param print_result: If True, prints the list of files.
        :param list_path: The path to be listed on the server.
        :param verbose: If True, return with list of files their sizes.
        """
        response = self._do_list_files(list_path=list_path, verbose=verbose)
        list_files = response.json()
        if print_result:
            print_ls(list_files)
        return list_files

    def _do_delete(self, file_path: str) -> Response:
        """
        Handles the actual delete process from the server.

        :param file_path: The file path for deleting on the server.
        :return: The response from the server.
        """

        url = urljoin(self._host_url, f'delete/{file_path}')

        headers = self._headers
        response = requests.delete(url, headers=headers)
        if response.status_code == 404:
            raise FileNotFoundError(f'File "{file_path}" not found on server')
        elif response.status_code != 204:
            raise Exception(f'Failed to delete file: {response.status_code} {response.text}')
        return response

    def _do_upload(
            self,
            file_chunks_generator: Iterable,
            dst_file_path: str,
            isdir: bool,
            file_size: int,
            force: bool,
    ) -> Response:
        """
        Handles the actual upload process to the server.

        :param file_chunks_generator: An iterable that yields file chunks to be uploaded.
        :param dst_file_path: The destination file path on the server.
        :param isdir: Whether uploaded file is archive of a directory
        :return: The response from the server.
        """
        url = urljoin(self._host_url, f'upload/?force={str(force).lower()}')

        headers = {
            **self._headers,
            'Content-Disposition': f'attachment; filename="{dst_file_path}"',
            'X-File-Size': str(file_size),
        }
        if isdir:
            headers['X-Is-Archive'] = 'true'

        with requests.Session() as session:
            # Check headers only aka "expect: 100-continue"
            pre_request = session.post(
                url,
                headers={**headers, 'X-Expect': '100-continue'},
                stream=True  # TODO: why stream
            )

            if pre_request.status_code >= 400:
                raise RuntimeError(
                    f'Upload failed: {pre_request.status_code} {pre_request.text}'
                )

            response = session.post(
                url, headers=headers, data=file_chunks_generator, stream=True
            )

            if response.status_code != 200:
                raise RuntimeError(f'Upload failed: {response.status_code} {response.text}')

            return response

    def _do_download(self, src_file_path: str, dst_file_path: str, isdir: bool) -> None:
        """
        Handles the actual download process from the server.

        :param src_file_path: The path to the source file on the server.
        :param dst_file_path: The destination file path on the local machine.
        :param isdir: Whether downloaded file is archive of a directory
        """
        url = urljoin(self._host_url, f'download/{src_file_path}')

        headers = self._headers
        if isdir:
            headers['X-Is-Archive'] = 'true'
        response = requests.get(url, headers=self._headers, stream=True)

        if response.status_code == 404:
            raise FileNotFoundError(f'File "{src_file_path}" not found on server')
        elif response.status_code != 200:
            raise Exception(f'Failed to download file: {response.text}')

        file_size = int(response.headers.get('Content-Length', 0))

        with open(dst_file_path, 'wb') as f:
            with tqdm(total=file_size, unit='B', unit_scale=True, desc=dst_file_path) as pbar:
                for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    f.write(chunk)
                    pbar.update(len(chunk))

    def _is_file_on_server_dir(self, src_file_path: str) -> bool:
        response = self._do_list_files(src_file_path, verbose=False)
        base_name = os.path.basename(src_file_path)
        if response.status_code == 404:
            raise FileNotFoundError(f'File "{src_file_path}" not found on server')
        elif response.status_code != 200:
            raise Exception(f'Failed to browse file on server: {response.text}')

        return not (len(response.json()) == 1 and base_name == response.json()[0]['name'])

    def _do_list_files(self, list_path: str, verbose: bool) -> Response:
        """
        Handles the actual ls (list_files) process on the server.
        :param list_path: The path to the directory for listing files.
        :param verbose: If True, return with list of files their sizes.
        :return:
        """
        url = urljoin(self._host_url, f'ls/{list_path}/?verbose={str(verbose).lower()}')
        response = requests.get(url, headers=self._headers)
        if response.status_code == 404:
            raise FileNotFoundError(f'File "{list_path}" not found on server')
        elif response.status_code != 200:
            raise Exception(f'Failed to browse files on server: {response.text}')

        return response

