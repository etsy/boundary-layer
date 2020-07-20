#!/usr/bin/env python3
"""

This script is used for releasing boundary-layer.  It can only be run by
administrators of the repository at github.com/etsy/boundary-layer.

To create a release, run:
    ./release.py

This will:
    1. Fetch the remote state of the repository, to make sure that you have an up-to-date copy
    2. Run some checks on your local state, comparing it to the remote state, to help prevent you from executing a release that fails to capture your desired changes
    3. Check out the local copy of the remote state
    4. Create a tag
    5. Push the tag
    6. Return you to whatever branch you were on previously

Once the tag is created remotely, a github actions workflow should run the publish build, and release the new version of boundary-layer to pypi.
"""
import argparse
from collections import namedtuple
import re
import shlex
import subprocess
import semver
import versioneer

def build_parser():
    parser = argparse.ArgumentParser()

    bump_type = parser.add_mutually_exclusive_group()
    bump_type.add_argument(
        '--bump',
        choices=['major', 'minor', 'patch'],
        default='patch',
        help='Select the portion of the version string to bump. default: `patch`'
    )
    bump_type.add_argument(
        '--force-version',
        type=semver.VersionInfo.parse,
        help='Force the new version to this value.  Must be a valid semver.'
    )

    parser.add_argument(
        '--git-remote-name',
        default='origin',
        help='Name of the git remote from which to release. default: `origin`'
    )

    parser.add_argument(
        '--remote-branch-name',
        default='master',
        help='Name of the remote branch to use as the basis for the release. default: `master`'
    )

    return parser


def bump_version(version, bump_type):
    bumpers = {
        'major': semver.bump_major,
        'minor': semver.bump_minor,
        'patch': semver.bump_patch,
    }

    return bumpers[bump_type](str(version))


def check_remote(remote_name):
    output = subprocess.check_output(
        shlex.split(f'git remote get-url {remote_name}'),
        encoding='utf-8'
    ).strip()

    if output not in [
        'git@github.com:etsy/boundary-layer',
        'https://github.com/etsy/boundary-layer',
        'https://www.github.com/etsy/boundary-layer',
    ]:
        print(f'''
    WARNING: Remote `{remote_name}` corresponding to `{output}` will not trigger the release build!
    We recommend releasing to `git@github.com:etsy/boundary-layer`''')


def fetch_latest(remote_name, branch_name):
    print(f'Fetching latest commits from {remote_name}/{branch_name}')
    subprocess.check_call(
        shlex.split(
            f'git fetch {remote_name} {branch_name}'
        )
    )


def git_checkout(remote_name, branch_name):
    ref = f'{remote_name}/{branch_name}' if remote_name else branch_name
    print(f'Checking out {ref}')

    subprocess.check_call(shlex.split(f'git checkout {ref}'))


def get_git_state():
    output = subprocess.check_output(
        shlex.split('git status -b --porcelain'),
        encoding='utf-8'
    ).strip().split('\n')

    return namedtuple('GitState', ['branch', 'files'])(
        branch=output_lines[0],
        files=output_lines[1:],
    )


def check_git_state(state):
    if state.files:
        raise Exception(
            f'Cannot release: unclean git state:\n{output}'
        )


def create_and_push_tag(remote_name, tag_name):
    print(f'Creating tag {tag_name}')
    subprocess.check_call(
        shlex.split(
            f'''
                git tag -a -m "Bumping to version {tag_name}" {tag_name}
            '''
        )
    )

    print(f'Pushing tag {tag_name}')
    #subprocess.check_call(
    #    shlex.split(
    #        f'''
    #            git push --tags {remote_name}
    #        '''
    #    )
    #)
    print('Tag pushed successfully.')


def parse_current_branch(state):
    # state.branch has the form
    # `## <branch-name>...<upstream> [<ahead|behind> <number-of-commits>]`

    pattern = r'## (?P<branch_name>[a-zA-Z0-9_/-]+)(\.\.\.(?P<remote>[a-z0-9A-Z_/-]+)( \[(?P<ahead_behind>(ahead|behind) \d+)\])?)?'
    m = re.match(pattern, state.branch).groupdict()

    return namedtuple('GitBranch', ['name', 'remote', 'ahead_behind'])(
        m.get('branch_name'),
        m.get('remote'),
        m.get('ahead_behind')
    )


def get_version(bump_type, force_version):
    current_version = semver.VersionInfo.parse(versioneer.get_version())

    print(f'Current version is: {current_version}')

    new_version = str(force_version) if force_version else bump_version(current_version, bump_type)
    print('New version: {}'.format(new_version))

    okay = input('Continue? [y/N] ')
    if not okay.lower().startswith('y'):
        raise Exception('Aborted by user')

    return new_version


def do_release(*, remote_name, branch_name, bump_type, force_version):
    check_remote(remote_name)
    fetch_latest(remote_name, branch_name)

    current_branch = get_current_branch(get_git_state())
    if current_branch.remote == f'{remote_name}/{branch_name}' and current_branch.ahead_behind:
        raise Exception(
            f'Local changes found on branch `{current_branch.name}` which tracks `{remote_name}/{branch_name}`!  This is probably unintended.  Please reconcile your local state before proceeding.'
        )

    try:
        git_checkout(remote_name, branch_name)
        check_git_state(get_git_state())
        create_and_push_tag(
            remote_name,
            tag_name=get_version(bump_type, force_version),
        )
    finally:
        if current_branch.name:
            git_checkout(None, current_branch.name)


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()

    do_release(
        remote_name=args.git_remote_name,
        branch_name=args.remote_branch_name,
        bump_type=args.bump,
        force_version=args.force_version,
    )
