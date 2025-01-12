import os.path
import subprocess
from typing import Iterable, Optional
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

        self._headers = {}

    def upload(self, src_file_path: str, dst_dir_path: Optional[str] = None, dst_file_name: Optional[str] = None):
        """
        Uploads a file to the specified destination directory on the server.

        :param src_file_path: The path to the source file to be uploaded.
        :param dst_dir_path: The destination directory path on the server. If None, the file will be uploaded to the root directory.
        :param dst_file_name: The name of the file on the server. If None, the source file name will be used.
        :raises RuntimeError: If the source file does not exist or is a directory.
        """

        if not os.path.exists(src_file_path):
            raise RuntimeError(f'File {src_file_path} not found')

        isdir = os.path.isdir(src_file_path)
        if not isdir:
            src_dir_path, src_file_name = os.path.split(src_file_path)
            dst_file_path = os.path.join(dst_dir_path or '', dst_file_name or src_file_name)
        else:
            if dst_file_name is not None:
                raise RuntimeError('If folder is uploaded file name must not be specified')
            src_dir_path = src_file_path
            src_file_path = '.' + src_dir_path + '.tar.gz'
            dst_file_path = dst_dir_path or src_dir_path

            try:
                subprocess.run(['tar', '-czf', src_dir_path, '-C', src_file_path, '.'], check=True)
            except subprocess.CalledProcessError as e:
                raise RuntimeError(f'Failed to archive folder: {e}')

        file_size = os.path.getsize(src_file_path)

        with open(src_file_path, 'rb') as src_file:
            with tqdm(total=file_size, unit='B', unit_scale=True, desc=src_file_path) as pbar:
                file_chunks_generator = generate_chunks_from_file(src_file, pbar)
                self._do_upload(file_chunks_generator, dst_file_path, isdir=isdir)

    def download(self, src_file_path: str, dst_dir_path: Optional[str] = None, dst_file_name: Optional[str] = None):
        """
        Downloads a file from the server to the specified destination directory.

        :param src_file_path: The path to the source file on the server.
        :param dst_dir_path: The destination directory path on the local machine. If None, the file will be downloaded to the current directory.
        :param dst_file_name: The name of the file on the local machine. If None, the source file name will be used.
        :raises FileNotFoundError: If the source file does not exist on the server.
        :raises Exception: If the download fails for any other reason.
        """
        if dst_dir_path and not os.path.exists(dst_dir_path):
            os.makedirs(dst_dir_path)

        # TODO: folder download
        src_file_name = os.path.basename(src_file_path)
        dst_file_path = os.path.join(dst_dir_path or '', dst_file_name or src_file_name)
        self._do_download(src_file_path, dst_file_path)

    def _do_upload(self, file_chunks_generator: Iterable, dst_file_path: str, isdir: bool) -> Response:
        """
        Handles the actual upload process to the server.

        :param file_chunks_generator: An iterable that yields file chunks to be uploaded.
        :param dst_file_path: The destination file path on the server.
        :raises RuntimeError: If the upload fails.
        :return: The response from the server.
        """
        url = urljoin(self._host_url, 'upload/' if not isdir else 'upload-folder')

        headers = {**self._headers, 'Content-Disposition': f'attachment; filename="{dst_file_path}"'}
        response = requests.post(url, headers=headers, data=file_chunks_generator, stream=True)

        if response.status_code != 200:
            raise RuntimeError(f'Upload failed: {response.status_code} {response.text}')

        return response

    def _do_download(self, src_file_path: str, dst_file_path: str) -> None:
        """
        Handles the actual download process from the server.

        :param src_file_path: The path to the source file on the server.
        :param dst_file_path: The destination file path on the local machine.
        :raises FileNotFoundError: If the source file does not exist on the server.
        :raises Exception: If the download fails for any other reason.
        :return: None
        """
        url = urljoin(self._host_url, f'download/{src_file_path}')
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
