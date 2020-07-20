import argparse
import re
import shlex
import subprocess
import semver
from versioneer import get_version

def build_parser():
    parser = argparse.ArgumentParser()

    bump_type = parser.add_mutually_exclusive_group()
    bump_type.add_argument(
        '--bump',
        choices=['major', 'minor', 'patch'],
        default='patch',

    )
    bump_type.add_argument(
        '--force-version',
        type=semver.VersionInfo.parse,
        help='Force the new version to this value.  Must be a valid semver.'
    )

    parser.add_argument(
        '--remote-branch-name',
        default='master',
        help='Name of the remote branch to use as the basis for the release'
    )

    parser.add_argument(
        '--git-remote-name',
        default='origin',
        help='Name of the git remote from which to release'
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


def check_git_state():
    output = subprocess.check_output(
        shlex.split('git status --porcelain'),
        encoding='utf-8'
    ).strip()
    if output:
        raise Exception(
            f'Cannot release: unclean git state:\n{output}'
        )


def push_tag(tag_name):
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
    #            git push --tags
    #        '''
    #    )
    #)
    print('Tag pushed successfully.')


def get_current_branch():
    output = subprocess.check_output(
        shlex.split('git status -b --porcelain'), encoding='utf-8'
    ).strip().split('\n')[0]

    # at this point, output has the form
    # `## <branch-name>...<upstream> [<ahead|behind> <number-of-commits>]`

    m = re.match(
        r'## (?P<branch_name>[a-zA-Z0-9_/-]+)(\.\.\.(?P<remote>[a-z0-9A-Z_/-]+) (\[(?P<ahead_behind>(ahead|behind) \d+)\])?)?',
        output
    )

    return m.groupdict()


def verify_and_push_tag(remote_name, branch_name, tag_version):
    check_remote(remote_name)
    fetch_latest(remote_name, branch_name)

    current_branch = get_current_branch()
    if current_branch.get('remote') == f'{remote_name}/{branch_name}' and current_branch.get('ahead_behind'):
        raise Exception(
            'Local changes found on branch that tracks the remote that we are publishing!  This is probably an error'
        )

    try:
        git_checkout(remote_name, branch_name)
        check_git_state()
        push_tag(tag_version)
    finally:
        original_branch = current_branch.get('branch_name')
        if original_branch:
            git_checkout(None, original_branch)


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()
    current_version = semver.VersionInfo.parse(get_version())

    print(f'Current version is: {current_version}')

    new_version = str(args.force_version) if args.force_version else bump_version(current_version, args.bump)
    print('New version: {}'.format(new_version))
    verify_and_push_tag(args.git_remote_name, args.remote_branch_name, new_version)
