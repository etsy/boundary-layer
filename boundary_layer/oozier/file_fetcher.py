# -*- coding: utf-8 -*-
# Copyright 2018 Etsy Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import abc
import base64
import os


class BaseFileFetcher(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def fetch_file_content(self, rel_path):
        pass


class LocalFileFetcher(BaseFileFetcher):
    def __init__(self, base_path):
        self.base_path = base_path

    def fetch_file_content(self, rel_path):
        full_path = os.path.join(self.base_path, rel_path.lstrip('/'))

        content = None
        with open(full_path) as _in:
            content = _in.read()

        return content


class GithubFileFetcher(BaseFileFetcher):
    """ A fetcher for files stored in Github.
        :param base_path: base path of files in the repository
        :type base_path: str
        :param repository: an already-initialized github3 Repository object
        :type repository: github3.repos.repo.Repository
        :param ref: branch from which to fetch files
            Default: master
        :type ref: str
    """

    def __init__(
            self,
            base_path,
            repo_owner,
            repo_name,
            repo_ref=None,
            github_url=None,
            github_token=None,
            github_username=None,
            github_password=None):

        self.base_path = base_path
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.repo_ref = repo_ref or 'master'
        self.github_url = github_url
        self.github_token = github_token
        self.github_username = github_username
        self.github_password = github_password

        self.github = self._get_github()
        self.repository = self._get_repository()

    def _get_github(self):
        try:
            import github3
        except ImportError:
            raise Exception("""
            ERROR: github3.py not installed!  Please install via
              pip install boundary-layer[github]
            and try again.""")

        if self.github_url:
            return github3.GitHubEnterprise(
                url=self.github_url,
                username=self.github_username,
                password=self.github_password,
                token=self.github_token)

        return github3.GitHub(
            username=self.github_username,
            password=self.github_password,
            token=self.github_token)

    def _get_repository(self):
        return self.github.repository(
            self.repo_owner,
            self.repo_name)

    def fetch_file_content(self, rel_path):
        full_path = os.path.join(self.base_path, rel_path.lstrip('/'))
        content_b64 = self.repository.file_contents(
            path=full_path,
            ref=self.repo_ref).content

        return base64.b64decode(content_b64)
