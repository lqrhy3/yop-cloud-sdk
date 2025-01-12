import os.path
import subprocess
from typing import Iterable
from urllib.parse import urljoin

import requests
from requests import Response
from tqdm import tqdm

DOWNLOAD_CHUNK_SIZE = 64 * 1024  # kb


def generate_chunks_from_file(file_descriptor, pbar):
    while chunk := file_descriptor.read(DOWNLOAD_CHUNK_SIZE):
        pbar.update(len(chunk))
        yield chunk


class YOPStorage:
    def __init__(self, host_url: str, token: str):
        self._host_url = host_url
        self._token = token

        self._headers = {"Authorization": f"Bearer {self._token}"}

    def upload(self, src_file_path: str, dst_file_path: str):
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
                    self._do_upload(file_chunks_generator, dst_file_path, isdir=isdir)
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

    def _do_upload(self, file_chunks_generator: Iterable, dst_file_path: str, isdir: bool) -> Response:
        """
        Handles the actual upload process to the server.

        :param file_chunks_generator: An iterable that yields file chunks to be uploaded.
        :param dst_file_path: The destination file path on the server.
        :param isdir: Whether uploaded file is archive of a directory
        :return: The response from the server.
        """
        url = urljoin(self._host_url, 'upload/')

        headers = {**self._headers, 'Content-Disposition': f'attachment; filename="{dst_file_path}"'}
        if isdir:
            headers['X-Is-Folder'] = 'true'
        response = requests.post(url, headers=headers, data=file_chunks_generator, stream=True)

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
            headers['X-Is-Folder'] = 'true'
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
        response = self._do_ls(src_file_path)
        base_name = os.path.basename(src_file_path)
        if response.status_code == 404:
            raise FileNotFoundError(f'File "{src_file_path}" not found on server')
        elif response.status_code != 200:
            raise Exception(f'Failed to browse file on server: {response.text}')

        return not (len(response.json()) == 1 and base_name == response.json()[0]['file_name'])

    def _do_ls(self, file_path: str) -> Response:
        url = urljoin(self._host_url, file_path)
        response = requests.get(url, headers=self._headers)
        return response
