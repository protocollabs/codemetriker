#!/usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
import json
import os
import sys
import types
import pandas
import subprocess
import lizard
import shutil
import tempfile
import pprint
import pandas
import collections
import concurrent.futures
import urllib.parse
import enum
import re
import numpy
import pathlib
import argparse
import mimetypes

import pandas.plotting

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
#from pandas.tools.plotting import parallel_coordinates




DEFAULT_STEPS=20

class Db:

    def __init__(self):
        self._timeline = Db.Timeline()

    @property
    def timeline(self):
        ''' timeline is only a getter '''
        return self._timeline

    class Timeline():

        def __init__(self):
            self._entries = []

        def append(self, timelineentry):
            self._entries.append(timelineentry)

        def __iter__(self):
            for entry in self._entries:
                yield entry

        def __getitem__(self, index):
            return self._entries[index]

        def __len__(self):
            return len(self._entries)

        class TimelineEntry():

            def __init__(self, id, date):
                self.id = id
                self.date = date
                # harvester.loc.df, ...
                self.harvester = types.SimpleNamespace()


# NOTE: harvester data is NEVER modified by other
# harvester or analyzers. All harvester collect data, they never
# correlate or analyze the data itself.

# Harvester with no dependencies
class HarvesterStageOne: pass
# Harvester with dependencies to results from Stage One Harvester
class HarvesterStageTwo: pass

class HarvesterFileAuthors(HarvesterStageOne):

    def __init__(self, path_root, config):
        self._path_root = path_root
        self._authors = None
        self._config = config
        self._thread_executors = 16

    def run(self):
        self._calc_authors()

    def _process_file(self, filename):
        """
        the workaround to cwd etc is done because there
        can be submodules. git -C <root> blame  will not work
        for submodules
        """
        filepath = os.path.dirname(os.path.realpath(filename))
        filename_only = os.path.basename(filename)
        if not os.path.isfile(filename):
            return None
        authors = git_blame_by_line(filepath, filename_only, aliases=config.aliases, cwd=filepath)
        return pandas.DataFrame(authors, columns=['author', 'commit_id', 'codeline'])

    def _calc_authors_parallel(self, filenamelist):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._thread_executors) as executor:
            future_to_filename = {executor.submit(self._process_file, filename): filename for filename in filenamelist}
            for future in concurrent.futures.as_completed(future_to_filename):
                filename = future_to_filename[future]
                data = future.result()
                if data is None:
                    continue
                sanitized_filename = filename[len(self._path_root):] # we remove the tmp dir
                self._authors[sanitized_filename] = data

    def _calc_authors_single_threaded(self, filenamelist):
        for filename in filenamelist:
            data = self._process_file(filename)
            if data is None:
                continue
            sanitized_filename = filename[len(self._path_root):] # we remove the tmp dir
            self._authors[sanitized_filename] = data

    def _calc_authors(self):
        self._authors = dict()
        for dirpath, dirnames, filenames in os.walk(self._path_root):
            filenamelist = list()
            if '.git' in dirpath:
                continue
            for f in filenames:
                if f in ('.git'):
                    continue
                filenamelist.append(os.path.join(self._path_root, dirpath, f))
            self._calc_authors_parallel(filenamelist)

    @property
    def authors(self):
        """
           author
        0  hagen@jauu.net
        1  hagen@jauu.net
        2  hagen@jauu.net
        3  hagen@jauu.net
        4  hagen@jauu.net
        5  hagen@jauu.net
        6  hagen@jauu.net
        """
        assert(self._authors is not None)
        return self._authors

class HarvesterFunction(HarvesterStageOne):

    def __init__(self, path_root, config):
        self._path_root = path_root
        self._config = config
        self._thread_executors = 16
        self._init_df_functions()

    def run(self):
        self._calc_lizard()

    def _init_df_functions(self):
        columns=['filename', 'function', 'cc', 'nloc', 'token',
                 'line_start', 'line_end']
        self._df_functions = pandas.DataFrame(columns=columns)

    def _lizard_file(self, filename):
        columns=['filename', 'function', 'cc', 'nloc', 'token',
                 'line_start', 'line_end']
        result = lizard.analyze_file(filename)
        if result is None:
            return None
        df = pandas.DataFrame(columns=columns)
        for function in result.function_list:
            line_start = int(function.start_line)
            line_end = int(function.end_line)
            sanitized_filename = filename[len(self._path_root):] # we remove the tmp dir
            df.loc[len(df)] = [sanitized_filename, function.name, function.cyclomatic_complexity,
                               function.nloc, function.token_count,
                               function.start_line, function.end_line]
        return df

    def _lizard_parallel(self, filenamelist):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self._thread_executors) as executor:
            future_to_filename = {executor.submit(self._lizard_file, filename): filename for filename in filenamelist}
            for future in concurrent.futures.as_completed(future_to_filename):
                filename = future_to_filename[future]
                df = future.result()
                if df is None:
                    continue
                self._df_functions = self._df_functions.append(df)

    def _lizard_single_threaded(self, filenamelist):
        for filename in filenamelist:
            df = self._lizard_file(filename)
            if df is None:
                continue
            self._df_functions = self._df_functions.append(df)

    def _calc_lizard(self):
        """
           filename      function cc nloc token line_start line_end
        0   /main.c       read_cb  1    7    36         11       18
        1   /main.c          main  3   20    97         20       45
        2     /ev.c        ev_new  1    4    11          5        8
        3     /ev.c       ev_free  1    4    15         10       13
        4     /ev.c  ev_entry_new  1    4    40         15       18
        5     /ev.c  ev_timer_new  1    4    33         20       23
        6     /ev.c ev_entry_free  1    4    14         25       28
        7     /ev.c        ev_add  1    3    22         30       32
        """
        for dirpath, dirnames, filenames in os.walk(self._path_root):
            filenamelist = list()
            if '.git' in dirpath:
                continue
            for f in filenames:
                fullpath = os.path.join(self._path_root, dirpath, f)
                if f in ('.git'):
                    continue
                if f.endswith('.template'):
                    continue
                if f.endswith('.json'):
                    continue
                mimetype, _ = mimetypes.guess_type(fullpath)
                if mimetype is None:
                    # ignore any files not guessable, this boils
                    # down to binary files, gitignore, et cetera
                    #print('ignoreing: ' + filename)
                    continue
                filenamelist.append(fullpath)
            self._lizard_parallel(filenamelist)
            #self._lizard_single_threaded(filenamelist)

    @property
    def df_functions(self):
        return self._df_functions

class HarvesterLoc(HarvesterStageOne):

    def __init__(self, path_root, config):
        self._path_root = path_root
        self._config = config
        self._df_lang_loc = None
        self._df_files_loc = None

    def run(self):
        self._calc_file()
        self._calc_lang()

    def _calc_lang(self):
        """
                      language    blank     code  comment  files
        0                    c  1239882  6330958  1308187  13134
        1         c/c++ header   263591  1347971   429617  11365
        2             assembly    36952   191853    84899   1081
        3                  xml     2644    32966      867    105
        4                 make     5529    19576     6173   1207
        """
        data = self._data_by_lang()
        flatten = []
        for k, v in data.items():
            language = k.lower()
            flatten.append([language, v['blank'], v['code'], v['comment'],v['nFiles']])
        df = pandas.DataFrame(flatten, columns=['language', 'blank', 'code', 'comment', 'files'])
        self._df_lang_loc = df

    def _calc_file(self):
        '''
                                    filename  blank    code  comment      language
        0      /drivers/asic_reg/nbio/nbi...    253  111547    22084  c/c++ header
        1      /drivers/asic_reg/nbio/nbi...    423  104083    14439  c/c++ header
        2      /drivers/asic_reg/dce/dce_...    478   54923     9395  c/c++ header
        3      /drivers/asic_reg/dcn/dcn_...    631   46355     7357  c/c++ header
        4      /drivers/asic_reg/nbio/nbi...    280   41941     6215  c/c++ header
        5                  /crypto/testmgr.h    228   34915      342  c/c++ header
        6      /drivers/asic_reg/bif/bif_...      3   33055       22  c/c++ header
        '''
        data = self._data_by_file()
        flatten = []
        for k, v in data.items():
            filename = k[len(self._path_root):] # we remove the tmp dir
            language = v['language'].lower()
            flatten.append([filename, v['blank'], v['code'], v['comment'], language])
        df = pandas.DataFrame(flatten, columns=['filename', 'blank', 'code', 'comment', 'language'])
        self._df_files_loc = df

    def _data_by_file(self):
        devnull = open(os.devnull, 'w')
        cmd = 'cloc --by-file --json {}'.format(self._path_root)
        result = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=devnull)
        loc = result.stdout.decode('utf-8')
        try:
            cloc = json.loads(loc)
        except json.decoder.JSONDecodeError:
            # may happend that the first commit includes no
            # files at all
            return {}
        del cloc['SUM']
        del cloc['header']
        return cloc

    def _data_by_lang(self):
        devnull = open(os.devnull, 'w')
        cmd = 'cloc --json {}'.format(self._path_root)
        result = subprocess.run(cmd.split(), stdout=subprocess.PIPE, stderr=devnull)
        loc = result.stdout.decode('utf-8')
        try:
            cloc = json.loads(loc)
        except json.decoder.JSONDecodeError:
            # may happend that the first commit includes no
            # files at all
            return {}
        del cloc['header']
        return cloc

    @property
    def df_lang_loc(self):
        assert(self._df_lang_loc is not None)
        return self._df_lang_loc

    @property
    def df_files_loc(self):
        assert(self._df_files_loc is not None)
        return self._df_files_loc


class HarvesterFunctionAuthors(HarvesterStageTwo):

    def __init__(self, path_root, config, harvester):
        self._path_root = path_root
        self._config = config
        self._harvester = harvester
        self._function_authors = dict()

    def run(self):
        for index, row in self._harvester.functions_df.iterrows():
            function = row['function']
            cc = row['cc']
            nloc = row['nloc']
            token = row['token']
            filename = row['filename']
            # self._harvester.authors count line numbers starting
            # with 0, so synchronize lizard and HarvesterFileAuthors
            # here and subtract one
            line_start = row['line_start'] - 1
            line_end = row['line_end'] - 1
            authors_df = self._harvester.authors[filename]
            author = authors_df.iloc[line_start:line_end + 1]['author'].value_counts().idxmax()

            obj = types.SimpleNamespace()
            obj.author = author
            obj.cc = cc
            obj.nloc = nloc
            obj.token = token
            obj.filename = filename
            self._function_authors[function] = obj

    @property
    def function_authors(self):
        return self._function_authors


class CodeMetric:

    def __init__(self, project_root, config, components=(),
                 worktreepath=None, harvester=None):
        self._root = project_root
        self._detect_schema_project_root()
        self._config = config
        self._components = components
        self._db = None
        self._init_path_worktree(worktreepath)
        self._init_repo()
        self._clean_output_directory()

    def _init_repo(self):
        # clone the repo first, if argument is an URL
        git_worktree_clone(self._root_scheme, self._root, self._path_worktree)
        # after cloning an URL, we cloned everything into
        # self._path_worktree which is now also the new self.root
        if self._root_scheme == 'URL':
            self._root = self._path_worktree

    def _clean_output_directory(self):
        path = self._config.output_directory
        if not path.exists():
            return
        shutil.rmtree(str(path))

    def _detect_schema_project_root(self):
        """
        urllib.parse.urlparse(self.root) will
        not work for ssh git@code.rsint.net:...
        based URLs, they will not detect the right
        scheme. Therefore we simple parse it by ourself
        """
        if self._root.startswith('.') or self._root.startswith('/'):
            self._root_scheme = 'FILE'
        else:
            self._root_scheme = 'URL'

    def _init_path_worktree(self, worktreepath):
        if worktreepath:
            if os.path.exists(worktreepath):
                shutil.rmtree(worktreepath)
            os.makedirs(worktreepath, exist_ok=True)
            self._path_worktree = worktreepath
            return
        self._path_worktree = tempfile.TemporaryDirectory().name

    @property
    def db(self):
        if self._db is None:
            msg  = 'CodeMetric.db was not initialized, please call '
            msg += 'calculate_by_time() or calculate_by_tags() before'
            raise Exception(msg)
        return self._db

    def _process_commits(self, commits):

        for commit in commits:
            print('harvest commit {}'.format(commit[0]))
            git_worktree_checkout(self._root_scheme,  self._root, self._path_worktree, commit[0])
            tle = Db.Timeline.TimelineEntry(commit[0], commit[1])

            # #
            # # STAGE ONE Harvester
            # #

            # # Harvester: Lines of Code
            # print('  harvest loc')
            h = HarvesterLoc(self._path_worktree, self._config)
            h.run()
            tle.harvester.loc_lang_df = h.df_lang_loc
            tle.harvester.loc_files_df = h.df_files_loc

            # # Harvester: Functions Info
            # print('  harvest function')
            h = HarvesterFunction(self._path_worktree, self._config)
            h.run()
            tle.harvester.functions_df = h.df_functions

            # print('  harvest author')
            h = HarvesterFileAuthors(self._path_worktree, self._config)
            h.run()
            tle.harvester.authors = h.authors

            # #
            # # STAGE TWO Harvester Starts here
            # #

            # print('  harvest function author')
            h = HarvesterFunctionAuthors(self._path_worktree, self._config, tle.harvester)
            h.run()
            tle.harvester.function_authors = h.function_authors

            self._db.timeline.append(tle)


    def calculate_by_time(self, commitish_range=None, steps=DEFAULT_STEPS):
        if steps < 3:
            raise ValueError('steps must be larger 3')
        self._db = Db() # this reset the DB
        if commitish_range:
            commitish_start = commitish_range[0]
            commitish_end = commitish_range[1]
        else:
            commitish_start = git_first_commit(self._root)
            commitish_end = 'HEAD'
        commits, step_distance = git_time_equidistant_commits(self._root,
                                               commitish_start,
                                               commitish_end, steps)
        self._process_commits(commits)

    def calculate_by_tags(self):
        self._db = Db() # this reset the DB
        commits = git_tags_sorted(self._root)
        self._process_commits(commits)


def git_first_commit(gitdir):
    """
    return the first commit of a repository
    """
    cmd = 'git -C {} rev-list HEAD'.format(gitdir)
    output = subprocess.check_output(cmd.split(), shell=False).decode("utf-8")
    lines = output.rstrip().split('\n')
    return lines[-1]


def git_commits(gitdir, filter_merges=False):
    """
    return all commits, starting with the oldest
    """
    if filter_merges:
        filter_ = '--no-merges'
    else:
        filter_ = ''
    cmd = 'git -C {} log {} --pretty=format:"%H,%ae"'.format(gitdir, filter_)
    output = subprocess.check_output(cmd.split(), shell=False).decode("utf-8")
    lines = output.split('\n')
    ret = []
    for line in lines:
        id_, email = line.split(',')
        e = GitCommits(id_, email.lower())
        ret.append(e)
    return reversed(ret)

def git_id_by_name(gitdir, name):
    cmd = 'git -C {} rev-list -1 {}'.format(gitdir, name)
    return subprocess.check_output(cmd.split(), shell=False).decode("utf-8").strip()

def git_tag_to_ids(gitdir, tag):
    cmd  = 'git -C {} show-ref --dereference {}'.format(gitdir, tag)
    # 2ef4d46d02937a82e6a2446d41f209a998f3b7fd refs/tags/v4.13-rc5
    # ef954844c7ace62f773f4f23e28d2d915adc419f refs/tags/v4.13-rc5^{}
    # we search for ef.. - the real commit object, not the tag object itself
    lines = subprocess.check_output(cmd.split(), shell=False).decode("utf-8").rstrip()
    line = lines.split('\n')
    return line[1].split()[0]


def git_tags_sorted(gitdir):
    """
    oldest first ordering
    """
    cmd  = 'git -C {} for-each-ref --sort=taggerdate '
    cmd += '--format %(refname:short),%(taggerdate:short) refs/tags'
    cmd = cmd.format(gitdir)
    taglines = subprocess.check_output(cmd.split(), shell=False).decode("utf-8").strip()
    tags = taglines.split('\n')
    ret = []
    for tag in tags:
        if len(tag) < 2:
            # not valid tag
            continue
        tag, datestr = tag.split(',')
        if len(datestr) < 6:
            # seems invalid
            continue
        date = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        id_ = git_tag_to_ids(gitdir, tag)
        ret.append([id_, date, tag])
    return ret

def git_date_by_commitishes(gitdir, commitishes):
    dates = []
    for commitish in commitishes:
        date = git_date_by_commitish(gitdir, commitish)
        dates.append(date)
    return dates

def git_date_by_commitish(gitdir, commitish):
    cmd = 'TZ=UTC git -C {} show -s --format="%at" {}'.format(gitdir, commitish)
    output = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
    date = datetime.datetime.fromtimestamp(int(output))
    return date

def git_date_by_tag(gitdir, tag):
    cmd = 'TZ=UTC git -C {} log -1 --format="%at" {}'.format(gitdir, tag)
    output = subprocess.check_output(cmd, shell=True).decode("utf-8").strip()
    date = datetime.datetime.fromtimestamp(int(output))
    return date

def git_date_by_tags(gitdir, tags):
    dates = []
    for tag in tags:
        date = git_date_by_tag(gitdir, tag)
        dates.append(date)
    return dates

def git_id_by_date(gitdir, date):
    formated_date = date.strftime('%Y-%m-%d')
    cmd = 'TZ=UTC git -C {} rev-list -1 --before="{}" HEAD'.format(gitdir, formated_date)
    return subprocess.check_output(cmd, shell=True).decode("utf-8").strip()

RE_BLAME = re.compile(r'^\^?(\S+)\W+<(\S+)>.*?\)(.*)')

def git_blame_by_line(gitdir, filepath, aliases=None, cwd=os.getcwd()):
    cmd = 'git -C {} blame -l -b -c --root --encoding=utf-8 --date=unix -e -- {}'.format(gitdir, filepath)
    output = subprocess.check_output(cmd.split(), shell=False, cwd=cwd)
    try:
        decoded = output.decode("utf-8").rstrip()
    except UnicodeDecodeError:
        decoded = output.decode("ISO-8859-1").rstrip()
    res = []
    for line in decoded.split('\n'):
        m = RE_BLAME.match(line)
        if m is None:
            continue
        commit_id, email, codeline = m.group(1, 2, 3)
        if aliases and email in aliases:
            email = aliases[email]
        res.append((email, commit_id, codeline))
    return res

def git_worktree_remove(gitdir, path_worktree):
    '''
    Remove an previous worktree, ignore errors

    This call can fail if the worktree was not previously
    added. But we ignore such kinds of erros.
    '''
    if not os.path.isdir(path_worktree):
        return
    cmd = 'git -C {} worktree remove --force {}'.format(gitdir, path_worktree)
    process = subprocess.Popen(cmd.split(), shell=False)
    stdout, stderr = process.communicate()
    process.wait()
    if os.path.exists(path_worktree):
        shutil.rmtree(path_worktree)

def git_worktree_checkout_file(gitdir, path_worktree, id_):
    devnull = open(os.devnull, 'w')
    git_worktree_remove(gitdir, path_worktree)
    cmd = 'git -C {} worktree add -f {} {}'.format(gitdir, path_worktree, id_)
    process = subprocess.Popen(cmd.split(), stderr=devnull, stdout=devnull, shell=False)
    process.wait()

def git_worktree_checkout_url(gitdir, path_worktree, id_):
    devnull = open(os.devnull, 'w')
    cmd = 'git -C {} submodule deinit --all'.format(gitdir, id_)
    process = subprocess.Popen(cmd.split(), shell=False)
    process.wait()
    cmd = 'git -C {} checkout -b {}-rev --force {}'.format(gitdir, id_, id_)
    process = subprocess.Popen(cmd.split(), shell=False)
    process.wait()
    cmd = 'git -C {} submodule sync --recursive'.format(gitdir)
    process = subprocess.Popen(cmd.split(), shell=False)
    process.wait()
    cmd = 'git -C {} submodule update --force --init --recursive'.format(gitdir)
    process = subprocess.Popen(cmd.split(), shell=False)
    process.wait()
    cmd = 'git -C {} clean -fdx'.format(gitdir)
    process = subprocess.Popen(cmd.split(), shell=False)
    process.wait()

def git_worktree_checkout(scheme, gitdir, path_worktree, id_):
    if scheme == 'FILE':
        git_worktree_checkout_file(gitdir, path_worktree, id_)
    elif scheme == 'URL':
        git_worktree_checkout_url(gitdir, path_worktree, id_)
    else:
        raise Exception('scheme not supported')

def git_worktree_clone_file(gitdir, path_worktree):
    '''
    nothing to do, just to keep the interface
    identical to the *_url one
    '''
    pass

def git_worktree_clone_url(url, path_worktree):
    devnull = open(os.devnull, 'w')
    shutil.rmtree(path_worktree, ignore_errors=True)
    os.makedirs(path_worktree)
    cmd = 'git clone --recurse-submodules {} {}'.format(url, path_worktree)
    process = subprocess.Popen(cmd.split(), shell=False)
    process.wait()
    cmd = 'git -C {} submodule sync'.format(path_worktree)
    process = subprocess.Popen(cmd.split(), stderr=devnull, stdout=devnull, shell=False)
    process.wait()
    cmd = 'git -C {} submodule update --init --recursive'.format(path_worktree)
    process = subprocess.Popen(cmd.split(), stderr=devnull, stdout=devnull, shell=False)
    process.wait()

def git_worktree_clone(scheme, url_path, path_worktree):
    if scheme == 'FILE':
        git_worktree_clone_file(url_path, path_worktree)
    elif scheme == 'URL':
        git_worktree_clone_url(url_path, path_worktree)
    else:
        raise Exception('scheme not supported')

def git_time_equidistant_commits(gitdir, commitish_start, commitish_end, steps):
    """ ok, equidistant cannot be done guaranteed. If there is no commit
    at all in a certain period no algorithm can calculate one. The
    algorithm try to search via git rev-list --before the best matching
    one. Another problem is the author date. It looks commitor date is
    more natural, but there is no change to get this information somehow.

    The return list look like:
        [['f720db4a68fbdd72ed670382eafc71cba89b80c9', datetime.datetime(2012, 6, 4, 17, 54, 59)],
         ['ac0b4cdb47f275c76fa592acaeead9f22bff2e93', datetime.datetime(2012, 6, 22, 16, 51, 35)],
         ['0d4ab07b04704a8cf6db64c0c09d8a7a6b5682fc', datetime.datetime(2012, 7, 15, 22, 15, 30)],
         ['35021d7b42fd0b3d25adfa0b937760707db8ff48', datetime.datetime(2012, 8, 10, 12, 17, 49)],
         ['35021d7b42fd0b3d25adfa0b937760707db8ff48', datetime.datetime(2012, 8, 10, 12, 17, 49)],
         [...]
         ['fda0fe47de00bb7dde35d9fbb35d2f623cbf6001', datetime.datetime(2012, 9, 21, 21, 56, 55)]]

    NOTE: it is possible that two identical ids are returned in a row!
    Imagine a stepwith of 30 days. And within two months nobody has commited
    anything. The same id is used (because it is still the nearest). Upper
    level users should take care of identical id. But it is fine for this
    function.
    """
    ret = [[commitish_start, git_date_by_commitish(gitdir, commitish_start)]]
    date_start = git_date_by_commitish(gitdir, commitish_start)
    date_end = git_date_by_commitish(gitdir, commitish_end)
    step_distance = (date_end - date_start) / (steps - 1)
    seconds_delta = seconds_between(date_start, date_end)
    stepwide = seconds_delta // (steps - 1)
    offset = stepwide
    while True:
        date = date_start + datetime.timedelta(seconds=offset)
        if date > date_end:
            break
        entry_id = git_id_by_date(gitdir, date)
        if len(entry_id) < 6:
            # handle garbage, may happens
            offset += stepwide
            continue
        entry_date = git_date_by_commitish(gitdir, entry_id)
        ret.append([entry_id, entry_date])
        offset += stepwide
    # smaller cleanup, due to to rounding issues the last
    # entry may not the specified one, just replace it now here
    # to make it sure
    ret[-1] = ([git_id_by_name(gitdir, commitish_end), date_end])
    return ret, step_distance

def seconds_between(date_start, date_end):
    return (date_end - date_start).total_seconds()

class AnalyzerType(enum.Enum):
    Authors = 1
    Full = 1023
    Minimal = 1024

class AnalyzerAuthors:

    def __init__(self, config, db, limits=None):
        self._config = config
        self._db = db

    def _check_required_harvester(self):
        if not self._db.timeline[-1].harvester.function_authors:
            return False
        return True

    def _save_author_best_worse(self, authors):
        # create parent dir
        for author, data in authors.items():
            directory = self._config.output_directory / 'author' / author
            directory.mkdir(parents=True, exist_ok=True)
            filepath = directory / 'best-worst-cc.md'
            with filepath.open(mode='w') as fd:
                fd.write('# Best and Worst Function\n')
                fd.write('\n')
                fd.write('## Best Function (lowest cc)\n')
                fd.write('\n')
                fd.write('CC: {}\n'.format(data['best-cc-val']))
                fd.write('{}:{}\n'.format(data['best-cc-filename'], data['best-cc-function']))
                fd.write('\n')
                fd.write('## Worst Function (highest cc)\n')
                fd.write('\n')
                fd.write('CC: {}\n'.format(data['worst-cc-val']))
                fd.write('{}:{}\n'.format(data['worst-cc-filename'], data['worst-cc-function']))


    def _calc_author_best_worse(self, function_authors):
        authors = dict()
        for function, info in function_authors.items():
            if info.author not in authors:
                authors[info.author] = dict()
                authors[info.author]['best-cc-val'] = info.cc
                authors[info.author]['best-cc-filename'] = info.filename
                authors[info.author]['best-cc-function'] = function
                authors[info.author]['worst-cc-val'] = info.cc
                authors[info.author]['worst-cc-filename'] = info.filename
                authors[info.author]['worst-cc-function'] = function
            if info.cc < authors[info.author]['best-cc-val']:
                authors[info.author]['best-cc-val'] = info.cc
                authors[info.author]['best-cc-filename'] = info.filename
                authors[info.author]['best-cc-function'] = function
            elif info.cc > authors[info.author]['worst-cc-val']:
                authors[info.author]['worst-cc-val'] = info.cc
                authors[info.author]['worst-cc-filename'] = info.filename
                authors[info.author]['worst-cc-function'] = function
        self._save_author_best_worse(authors)

    def _draw_authors_average_cc(self):
        authors = dict()
        for i, entry in enumerate(self._db.timeline):
            function_authors = entry.harvester.function_authors
            tmp = dict()
            for function, info in function_authors.items():
                cc = info.cc
                author = info.author
                if author not in tmp:
                    tmp[author] = list()
                tmp[author].append(cc)
            for author, data in tmp.items():
                avg = sum(data) / float(len(data))
                if author not in authors:
                    authors[author] = types.SimpleNamespace()
                    authors[author].x = list()
                    authors[author].y = list()
                authors[author].x.append(i)
                authors[author].y.append(avg)
        # and draw the dataset now
        fig = plt.figure()
        ax = fig.add_subplot(1,1,1)
        for author, data in authors.items():
            if len(data.x) < 2:
                continue
            ax.plot(data.x, data.y, label=author)
        plt.legend(loc='upper left')
        outpath = self._config.output_directory / 'cc-all-authors.png'
        fig.savefig(str(outpath), dpi=300, bbox_inches="tight")


    def _draw_function_cc_with_authors(self):
        # nice example:
        # https://stackoverflow.com/questions/29803480/plotting-parallel-coordinates-in-pandas-with-different-colours
        data = dict()
        columns = []
        for entry in self._db.timeline:
            columns.append(entry.date)
            function_authors = entry.harvester.function_authors
            for function, info in function_authors.items():
                if function not in data:
                    data[function] = dict()
                    data[function]['data'] = list()
                cc = int(info.cc)
                if cc > 100:
                    cc = numpy.nan
                data[function]['data'].append(cc)
                data[function]['author'] = info.author
        columns.append('Author')
        # normalize (e.g. some functions may appear or disappear
        # over time, make NaNs of them
        df = pandas.DataFrame(columns=columns)
        for function, data_data in data.items():
            number_entries = len(data_data['data'])
            adjustment_no = len(self._db.timeline) - number_entries
            for _ in range(adjustment_no):
                data_data['data'].insert(0, numpy.nan)
            data_data['data'].append(data_data['author'])
            df.loc[len(df)] = data_data['data']
        fig = plt.figure()
        ax1 = fig.add_subplot(1,1,1)
        ax = pandas.plotting.parallel_coordinates(df, "Author", ax=ax1, colormap=plt.cm.tab10,
                                                  alpha=0.9, linewidth=.7)
        ax.grid(False)
        outpath = self._config.output_directory / 'cc-all-functions-author.png'
        fig.savefig(str(outpath), dpi=300, bbox_inches="tight")

    def _draw_function_cc_without_authors(self):
        data = dict()
        columns = []
        for entry in self._db.timeline:
            columns.append(entry.date)
            function_authors = entry.harvester.function_authors
            for function, info in function_authors.items():
                if function not in data:
                    data[function] = dict()
                    data[function]['data'] = list()
                cc = int(info.cc)
                if cc > 100:
                    cc = numpy.nan
                data[function]['data'].append(cc)
                data[function]['function'] = function
        columns.append('Function')
        # normalize (e.g. some functions may appear or disappear
        # over time, make NaNs of them
        df = pandas.DataFrame(columns=columns)
        for function, data_data in data.items():
            number_entries = len(data_data['data'])
            adjustment_no = len(self._db.timeline) - number_entries
            for _ in range(adjustment_no):
                data_data['data'].insert(0, numpy.nan)
            data_data['data'].append(data_data['function'])
            df.loc[len(df)] = data_data['data']
        fig = plt.figure()
        ax1 = fig.add_subplot(1,1,1)
        ax = pandas.plotting.parallel_coordinates(df, "Function", ax=ax1, colormap=plt.cm.tab10,
                                                  alpha=0.8, linewidth=.5)
        ax.grid(False)
        ax.get_legend().remove()
        outpath = self._config.output_directory / 'cc-all-functions-without-author.png'
        fig.savefig(str(outpath), dpi=300, bbox_inches="tight")



    def _calc_data(self):
        # just for the last (actual) commit, this reflects
        # the current state, not some outdated, year old commits
        function_authors = self._db.timeline[-1].harvester.function_authors
        self._calc_author_best_worse(function_authors)
        self._draw_function_cc_with_authors()
        self._draw_authors_average_cc()
        self._draw_function_cc_without_authors()


    def run(self):
        ok = self._check_required_harvester()
        if not ok:
            # return false is not an error (-> exception
            # is an error), but signals that run was not
            # possible
            return False
        self._calc_data()
        return True

class Config:

    def __init__(self, aliases, output_directory, components=None):
        self.aliases = aliases
        self.components = components or dict()
        self.output_directory = pathlib.Path(output_directory)


if __name__ == "__main__":
    # --minmal-analysis

    #path = '/home/pfeifer/src/code/linux'
    #path = '/home/pfeifer/src/code/misc/libeve'
    path = '/home/pfeifer/src/code/foreign/ngtcp2'

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--commit-range", help="commit-range", type=str, default=None)
    parser.add_argument('location')
    args = parser.parse_args()

    commitish_range=None
    if args.commit_range:
        commitish_range = args.commit_range.split(':')
        #commitish_range=['449005104fb312c0e52cd41f86194fa8bc12b631', 'HEAD']
    if args.location:
        path = args.location

    aliases = {
        'hagen@jauu.net' : 'Hagen'
    }

    components = {
        '/lib/foo'     : 'foo',
        '/package/foo' : 'foo',
    }

    output_directory = '/tmp/codemetric-analyzer'

    config = Config(aliases, output_directory, components=components)
    #harvester = None
    harvester = [ AnalyzerType.Full ]
    #harvester = CodeMetric.minimal_... # preset
    cm = CodeMetric(path, config, worktreepath='/tmp/codemetric',  harvester=harvester)
    #cm.calculate_by_tags()
    #for entry in cm.db.timeline:
    #    print(entry.__dict__)
    cm.calculate_by_time(commitish_range=commitish_range)

    # #for entry in cm.db.timeline:

    # print(entry.__dict__)
    config.output_directory /= 'time-equidistant'
    config.output_directory.mkdir(parents=True, exist_ok=True)
    a = AnalyzerAuthors(config, cm.db, limits=None)
    a.run()

    # a = AnalyzerBar(config, cm.db)
    # r.run()

