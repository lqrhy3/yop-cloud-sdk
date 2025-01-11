import os.path
from typing import Iterable, Optional
from urllib.parse import urljoin

import requests
from tqdm import tqdm

DOWNLOAD_CHUNK_SIZE = 8192  # kb


def generate_chunks_from_file(file_descriptor, pbar):
    while chunk := file_descriptor.read(8192):
        pbar.update(len(chunk))
        yield chunk


class YOPStorage:
    def __init__(self, host_url: str, token: str):
        self._host_url = host_url
        self._token = token

        self._headers = {}

    def upload(self, src_file_path: str, dst_dir_path: str, dst_file_name: Optional[str] = None):
        if not os.path.exists(src_file_path):
            raise RuntimeError(f'File {src_file_path} not found')
        if os.path.isdir(src_file_path):
            raise RuntimeError('Cannot upload directory')

        src_dir_path, src_file_name = os.path.split(src_file_path)
        dst_file_path = os.path.join(dst_dir_path, dst_file_name or src_file_name)
        file_size = os.path.getsize(src_file_path)

        with open(src_file_path, 'rb') as src_file:
            with tqdm(total=file_size, unit='B', unit_scale=True, desc=src_file_path) as pbar:
                file_chunks_generator = generate_chunks_from_file(src_file, pbar)
                self._do_upload(file_chunks_generator, dst_file_path)

    def download(self, src_file_path: str, dst_dir_path: str, dst_file_name: Optional[str] = None):
        if not os.path.exists(dst_dir_path):
            os.makedirs(dst_dir_path)

        dst_file_path = os.path.join(dst_dir_path, dst_file_name or src_file_path)
        self._do_download(src_file_path, dst_file_path)

    def _do_upload(self, file_chunks_generator: Iterable, dst_file_path: str):
        url = urljoin(self._host_url, 'upload')

        headers = {**self._headers, 'Content-Disposition': f'attachment; filename="{dst_file_path}"'}
        response = requests.post(url, headers=headers, data=file_chunks_generator, stream=True)

        if response.status_code != 200:
            raise RuntimeError(f'Upload failed: {response.status_code} {response.text}')

        return response

    def _do_download(self, src_file_path: str, dst_file_path: str):
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
