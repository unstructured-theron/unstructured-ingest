from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from datetime import datetime
from time import time
from typing import TYPE_CHECKING, Any, Generator, List, Optional
from urllib.parse import urlparse

from pydantic import Field, root_validator

from unstructured_ingest.error import SourceConnectionError
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    DownloadResponse,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    download_responses
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    SourceRegistryEntry
)

CONNECTOR_TYPE = 'github'
if TYPE_CHECKING:
    from github.Repository import Repository


class GitHubAccessConfig(AccessConfig):
    access_token: Optional[str] = Field(
        default=None,
        sensitive=False,
        overload_name="git_access_token"
    )


class GitHubConnectionConfig(ConnectionConfig):
    url: str
    access_config: GitHubAccessConfig
    branch: Optional[str] = Field(
        default=None,
        overload_name="git_branch"
    )

    git_file_glob: Optional[List[str]] = Field(
        default=None,
        overload_name="git_file_glob"
    )
    repo_path: str = field(init=False, repr=False, default=None)

    @root_validator(pre=True)
    def set_repo_path(cls, values):
        # Parse the URL
        url = values.get('url')
        if url:
            parsed_gh_url = urlparse(url)
            path_fragments = [fragment for fragment in parsed_gh_url.path.split("/") if fragment]

            # Validate the URL and construct the repo_path
            if (
                (parsed_gh_url.scheme and parsed_gh_url.scheme != "https")
                or (parsed_gh_url.netloc and parsed_gh_url.netloc != "github.com")
                or len(path_fragments) != 2
            ):
                raise ValueError(
                    'Please provide a valid URL, e.g. "https://github.com/owner/repo" or "owner/repo".'
                )

            # Set the repo_path based on URL fragments
            values['repo_path'] = "/".join(path_fragments)
        return values

    @SourceConnectionError.wrap
    @requires_dependencies(["github"], extras="github")
    def get_repo(self) -> "Repository":
        from github import Github

        github = Github(self.access_config.access_token)
        return github.get_repo(self.repo_path)


class GitHubIndexerConfig(IndexerConfig):
    pass


@dataclass
class GitHubIndexer(Indexer):
    connection_config: GitHubConnectionConfig
    index_config: GitHubIndexerConfig

    @staticmethod
    def is_file_type_supported(path: str) -> bool:
        # Workaround to ensure that auto.partition isn't fed with .yaml, .py, etc. files
        # TODO: What to do with no filenames? e.g. LICENSE, Makefile, etc.
        supported = path.endswith(
            (
                ".md",
                ".txt",
                ".pdf",
                ".doc",
                ".docx",
                ".eml",
                ".heic",
                ".html",
                ".png",
                ".jpg",
                ".ppt",
                ".pptx",
                ".xml",
            ),
        )
        if not supported:
            logger.debug(
                f"The file {path!r} is discarded as it does not contain a supported filetype.",
            )
        return supported

    def precheck(self) -> None:
        from github import Consts
        from github.GithubRetry import GithubRetry
        from github.Requester import Requester

        try:
            requester = Requester(
                auth=self.connection_config.access_config.access_token,
                base_url=Consts.DEFAULT_BASE_URL,
                timeout=Consts.DEFAULT_TIMEOUT,
                user_agent=Consts.DEFAULT_USER_AGENT,
                per_page=Consts.DEFAULT_PER_PAGE,
                verify=True,
                retry=GithubRetry(),
                pool_size=None,
            )
            url_base = (
                "/repositories/" if isinstance(self.connection_config.repo_path, int) else "/repos/"
            )
            url = f"{url_base}{self.connection_config.repo_path}"
            headers, _ = requester.requestJsonAndCheck("HEAD", url)
            logger.debug(f"headers from HEAD request: {headers}")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"failed to validate connection: {e}")

    def does_path_match_glob(self, path: str) -> bool:
        if not self.connection_config.git_file_glob:
            return True

        patterns = self.connection_config.git_file_glob
        for pattern in patterns:
            if fnmatch.filter([path], pattern):
                return True

        logger.debug(f"the file {path!r} is discarded as it does not match any given glob.")
        return False

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        repo = self.connection_config.get_repo()
        # Load the Git tree with all files, and then create Ingest docs
        # for all blobs, i.e. all files, ignoring directories
        sha = self.connection_config.branch or repo.default_branch
        git_tree = repo.get_git_tree(sha, recursive=True)

        for element in git_tree.tree:
            rel_path = element.path.replace(self.connection_config.repo_path, "").lstrip("/")
            if (
                element.type == "blob"
                and self.is_file_type_supported(element.path)
                and (not self.connection_config.git_file_glob or self.does_path_match_glob(element.path))
            ):
                record_locator = {
                    "repo_path": self.connection_config.repo_path,
                    "file_path": element.path,
                }
                if self.connection_config.branch is not None:
                    record_locator["branch"] = self.connection_config.branch

                yield FileData(
                    identifier=element.sha,
                    connector_type=CONNECTOR_TYPE,
                    source_identifiers=SourceIdentifiers(
                        fullpath=element.path,
                        filename=element.path.split("/")[-1],
                        rel_path=rel_path,
                    ),
                    metadata=FileDataSourceMetadata(
                        url=element.url,
                        version=element.etag,
                        record_locator=record_locator
                    ),
                    additional_metadata={
                        "content-type": element._headers["content-type"],
                        "content-length": element._headers["content-length"],
                        "mode": element._rawData["mode"],
                        "type": element._rawData["type"],
                        "size": element._rawData["size"], 
                    },
                )


class GitHubDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class GitHubDownloader(Downloader):
    connection_config: GitHubConnectionConfig
    download_config: GitHubDownloaderConfig

    @SourceConnectionError.wrap
    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        return DownloadResponse(file_data=file_data, path=file_data.local_download_path)


github_source_entry = SourceRegistryEntry(
    connection_config=GitHubConnectionConfig,
    indexer_config=GitHubIndexerConfig,
    indexer=GitHubIndexer,
    downloader_config=GitHubDownloaderConfig,
    downloader=GitHubDownloader,
)
