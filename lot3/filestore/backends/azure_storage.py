import logging
import os
import shutil
import tempfile
from urllib import parse

import fsspec
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient

from .storage_manager import BaseStorageConnector
from ..log import set_azure_log_level


class AzureObjectStore(BaseStorageConnector):
    # https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python
    fsspec_filesystem_class = fsspec.get_filesystem_class('abfs')

    def __init__(
        self,
        account_name,
        account_key,
        azure_container,
        *args,
        location='',
        connection_string: str = None,
        shared_container=True,
        azure_ssl=True,
        upload_max_conn=2,
        timeout=20,
        max_memory_size=2 * 1024 * 1024,
        expiration_secs: int = None,
        overwrite_files=True,
        default_content_type='application/octet-stream',
        cache_control: str = None,
        sas_token: str = None,
        custom_domain: str = None,
        token_credential=None,
        azure_log_level=logging.ERROR,
        **kwargs,
    ):
        self._service_client = None
        self._client = None

        # Required
        self.account_name = account_name
        self.account_key = account_key
        self.azure_container = azure_container

        # Optional
        self.location = location
        self.connection_string = connection_string
        self.shared_container = shared_container
        self.azure_ssl = azure_ssl
        self.upload_max_conn = upload_max_conn
        self.timeout = timeout
        self.max_memory_size = max_memory_size
        self.expiration_secs = expiration_secs
        self.overwrite_files = overwrite_files
        self.default_content_type = default_content_type
        self.cache_control = cache_control
        self.sas_token = sas_token
        self.custom_domain = custom_domain
        self.token_credential = token_credential
        self.azure_log_level = azure_log_level
        self.azure_protocol = 'https' if self.azure_ssl else 'http'
        set_azure_log_level(self.azure_log_level)
        super(AzureObjectStore, self).__init__(*args, **kwargs)

    def _get_service_client(self):
        if self.connection_string is not None:
            return BlobServiceClient.from_connection_string(self.connection_string)

        account_domain = self.custom_domain or "{}.blob.core.windows.net".format(
            self.account_name)
        account_url = "{}://{}".format(self.azure_protocol, account_domain)

        credential = None
        if self.account_key:
            credential = {
                "account_name": self.account_name,
                "account_key": self.account_key,
            }
        elif self.sas_token:
            credential = self.sas_token
        elif self.token_credential:
            credential = self.token_credential
        return BlobServiceClient(account_url, credential=credential)

    def get_fsspec_storage_options(self):
        return {
            "connection_string": self.connection_string,
            "account_name": self.account_name,
            "account_key": self.account_key,
            "sas_token": self.sas_token,
            "anon": not (self.sas_token or self.account_key or self.connection_string),
        }

    @property
    def service_client(self):
        if self._service_client is None:
            self._service_client = self._get_service_client()
        return self._service_client

    @property
    def client(self):
        if self._client is None:
            self._client = self.service_client.get_container_client(
                self.azure_container
            )
        return self._client

    # def _is_stored(self, object_key):
    #     if not isinstance(object_key, str):
    #         return False
    #     try:
    #         blob_client = self.client.get_blob_client(object_key)
    #         blob_client.get_blob_properties()
    #         return True
    #     except ResourceNotFoundError:
    #         return False
    #
    # def _fetch_file(self, reference, output_path=""):
    #     blob_client = self.client.get_blob_client(reference)
    #
    #     if output_path:
    #         os.makedirs(os.path.dirname(output_path), exist_ok=True)
    #         fpath = output_path
    #     else:
    #         fpath = os.path.basename(reference)
    #
    #     logging.info('Get Azure Blob: {}'.format(reference))
    #     with open(fpath, "wb") as download_file:
    #         download_file.write(blob_client.download_blob().readall())
    #     return os.path.abspath(fpath)
    #
    # def _store_file(self, file_path, storage_fname=None, storage_subdir='', suffix=None, **kwargs):
    #     ext = file_path.split('.')[-1] if not suffix else suffix
    #     filename = storage_fname if storage_fname else self._get_unique_filename(ext)
    #     object_name = os.path.join(storage_subdir, filename)
    #
    #     if self.cache_root:
    #         os.makedirs(self.cache_root, exist_ok=True)
    #         cached_fp = os.path.join(self.cache_root, filename)
    #         shutil.copy(file_path, cached_fp)
    #
    #     self.upload(object_name, file_path)
    #     logging.info('Stored Azure Blob: {} -> {}'.format(file_path, object_name))
    #
    #     if self.shared_container:
    #         return os.path.join(self.location, object_name)
    #     else:
    #         return self.url(object_name)
    #
    # def _store_dir(self, directory_path, storage_fname=None, storage_subdir='', suffix=None, arcname=None):
    #     ext = 'tar.gz' if not suffix else suffix
    #     filename = storage_fname if storage_fname else self._get_unique_filename(ext)
    #     object_name = os.path.join(storage_subdir, filename)
    #
    #     with tempfile.TemporaryDirectory() as tmpdir:
    #         archive_path = os.path.join(tmpdir, filename)
    #         self.compress(archive_path, directory_path, arcname)
    #         self.upload(object_name, archive_path)
    #
    #         if self.cache_root:
    #             os.makedirs(self.cache_root, exist_ok=True)
    #             cached_fp = os.path.join(self.cache_root, filename)
    #             shutil.copy(archive_path, cached_fp)
    #
    #     logging.info('Stored Azure Blob: {} -> {}'.format(directory_path, object_name))
    #
    #     if self.shared_container:
    #         return os.path.join(self.location, object_name)
    #     else:
    #         return self.url(object_name)

    # def upload(self, object_name, filepath):
    #     blob_key = os.path.join(self.location, object_name)
    #     blob_client = self.client.get_blob_client(blob_key)
    #     with open(filepath, "rb") as data:
    #         blob_client.upload_blob(data)
    #
    # def url(self, object_name, parameters=None, expire=None):
    #     blob_key = os.path.join(self.location, object_name)
    #     blob_client = self.client.get_blob_client(blob_key)
    #     return blob_client.url
    #
    # def delete_file(self, reference):
    #     """ Marks blob for deletion, will also remove snapshots
    #     """
    #     blob_client = self.client.get_blob_client(reference)
    #     blob_client.delete_blob()
    #     logging.info(f'Deleted Azure Blob: {reference}')
    #
    # def delete_dir(self, reference):
    #     """ Delete multiple Objects
    #     """
    #     if not (reference and reference.strip()):
    #         raise ValueError('reference must be a non-emtpry string holding the dir name')
    #
    #     key_prefix = os.path.join(self.location, reference)
    #     matching_objs = self.client.list_blobs(name_starts_with=key_prefix)
    #     for blob in matching_objs:
    #         self.delete_file(blob.name)

    def get_storage_url(self, filename=None, suffix="tar.gz"):
        filename = filename or self._get_unique_filename(suffix)

        params = {}
        if self.connection_string:
            params["connection_string"] = self.connection_string
        else:
            if self.account_name:
                params["account"] = self.account_name

            if self.account_key:
                params["key"] = self.account_key

            if self.location:
                params["endpoint"] = self.location

        return (
            filename,
            f"abfs://{self.azure_container}/{filename}?{parse.urlencode(params)}",
        )