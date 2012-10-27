'''Simple content-addressed store'''

__copyright__ = '''\
Copyright (C) 2012  Volker Grabsch <vog@notjusthosting.com>

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
'''

import argparse
import hashlib
import io
import logging
import os
import os.path
import re
import struct
import subprocess
import sys
import uuid

# TODO: Add SftpStorage using paramiko or sftp command line tool ("sftp -q ...")
#       - trigger this via "scs -h HOST" argument
#       - rename "-s" (--storage) to "-f" (--folder)
#       - allow for combining "-h ..." with "-t" to run the test suite via SFTP
#       - allow for "-h ''" to use FileStorage, i.e. use default='' instead of default=None
#       - Remove leading '~/' of folder in SftpStorage
# TODO: Add "description" field (saved in .scs/HEXDIGEST.txt) to store() method
# TODO: Add method to show all descriptions and timestamps, ordered by timestamp (using file timestamp)

class FileStorage:

    '''File storage for Scs'''

    def __init__(self, folder):
        self.expanded_folder = os.path.expanduser(folder)
        if os.path.exists(self.expanded_folder):
            logging.debug('Using existing folder %r', self.expanded_folder)
        else:
            logging.info('Creating folder %r', self.expanded_folder)
            os.mkdir(self.expanded_folder, 0700)

    def _path(self, filename):
        assert re.match('^[0-9a-z.-]+$', filename)
        return os.path.join(self.expanded_folder, filename)

    def filenames(self):
        '''Retrieve set of all filenames'''
        return set(os.listdir(self.expanded_folder))

    def exists(self, filename):
        '''Check if file exists'''
        return os.path.exists(self._path(filename))

    def read(self, filename):
        '''Read all data from file'''
        with open(self._path(filename), 'rb') as f:
            return f.read()

    def create(self, filename, data):
        '''Create file and write data into it, overwrite possibly existing file'''
        with open(self._path(filename), 'wb') as f:
            f.write(data)

    def rename(self, old_filename, new_filename):
        '''Rename existing file in an atomic way, overwrite possibly existing file'''
        os.rename(self._path(old_filename), self._path(new_filename))

    def remove(self, filename):
        '''Remove file in an atomic way'''
        os.remove(self._path(filename))

    def rmdir(self):
        '''Remove storage folder, assuming the folder to be empty'''
        logging.info('Removing folder %r', self.expanded_folder)
        os.rmdir(self.expanded_folder)

class SftpStorage:

    '''SFTP storage for Scs'''

    def __init__(self, host, folder):
        # TODO
        self.host = host
        self.folder = folder

    def _path(self, filename):
        # TODO
        assert re.match('^[0-9a-z.-]+$', filename)
        return os.path.join(self.folder, filename)

    def filenames(self):
        '''Retrieve set of all filenames'''
        # TODO
        pass

    def exists(self, filename):
        '''Check if file exists'''
        # TODO
        pass

    def read(self, filename):
        '''Read all data from file'''
        # TODO
        pass

    def create(self, filename, data):
        '''Create file and write data into it, overwrite possibly existing file'''
        # TODO
        pass

    def rename(self, old_filename, new_filename):
        '''Rename existing file in an atomic way, overwrite possibly existing file'''
        # TODO
        pass

    def remove(self, filename):
        '''Remove file in an atomic way'''
        # TODO
        pass

    def rmdir(self):
        '''Remove storage folder, assuming the folder to be empty'''
        # TODO
        pass

class Scs:

    '''Simple content-addressed store'''

    def __init__(self, storage, blocksize, algorithm):
        self.storage = storage
        self.blocksize = blocksize
        self.algorithm = algorithm
        self.hexlen = 2 * hashlib.new(self.algorithm).digestsize

    def gc(self, hexdigests):
        '''Run garbage collector

        Remove all temporary files and unneeded bins.
        '''
        # TODO: Implement support for removing everything that is not
        #       reachable from a specific set of hexdigests,
        #       None means: remove nothing except temp files
        #       [] would mean "remove everything" but we should handle this like None to avoid bad accidents
        for filename in self.storage.filenames():
            if len(filename) == self.hexlen + len('.???') and re.match('^[0-9a-f]+\.(bin|cat)$', filename):
                logging.debug('Skipping data file %r', filename)
            elif re.match('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.tmp$', filename):
                logging.info('Removing temporary file %r', filename)
                self.storage.remove(filename)
            else:
                logging.warn('Ignoring unknown file %r', filename)

    def check(self):
        '''Check all bins'''
        filenames = self.storage.filenames()
        for filename in filenames:
            if len(filename) == self.hexlen + len('.???') and re.match('^[0-9a-f]+\.(bin|cat)$', filename):
                hexdigest_total = filename[:-len('.???')]
                if filename[len(hexdigest_total):] == '.cat' and (hexdigest_total + '.bin') in filenames:
                    raise RuntimeError('Redundant concatenation file %r (there is a data file for the same digest)' % (filename,))
                for block in self.load(hexdigest_total):
                    pass
            elif re.match('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.tmp$', filename):
                logging.warn('Ignoring temporary file %r', filename)
            else:
                logging.warn('Ignoring unknown file %r', filename)

    def load(self, hexdigest):
        '''Load data of bin'''
        if len(hexdigest) == self.hexlen and re.match('^[0-9a-f]+$', hexdigest):
            hash_total = hashlib.new(self.algorithm)
            if self.storage.exists(hexdigest + '.bin'):
                block = self.storage.read(hexdigest + '.bin')
                hash_total.update(block)
                yield block
            elif self.storage.exists(hexdigest + '.cat'):
                hexdigest_list = self.storage.read(hexdigest + '.cat')
                for i in xrange(0, len(hexdigest_list), self.hexlen + 1):
                    block = self.storage.read(hexdigest_list[i:i+self.hexlen] + '.bin')
                    hash_total.update(block)
                    yield block
            else:
                raise RuntimeError('Unknown digest %r' % (hexdigest,))
            if hash_total.hexdigest() != hexdigest:
                raise RuntimeError('Wrong checksum %r, expected %r' % (hash_total.hexdigest(), hexdigest))
        else:
            raise RuntimeError('Invalid digest %r' % (hexdigest,))

    def store(self, input_io):
        '''Store new bin'''
        hash_total = hashlib.new(self.algorithm)
        cat = ''
        while True:
            block = input_io.read(self.blocksize)
            if len(block) == 0:
                break
            hexdigest = hashlib.new(self.algorithm, block).hexdigest()
            cat += hexdigest + '\n'
            hash_total.update(block)
            filename = hexdigest + '.bin'
            if self.storage.exists(filename):
                logging.debug('Skipping existing block file %r', filename)
            else:
                logging.debug('Creating block file %r', filename)
                filename_tmp = str(uuid.uuid4()) + '.tmp'
                self.storage.create(filename_tmp, block)
                self.storage.rename(filename_tmp, filename)
        hexdigest_total = hash_total.hexdigest()
        filename_total = hexdigest_total + '.cat'
        if len(cat) == self.hexlen + 1:
            logging.debug('Skipping concatenation file because there is exactly one block')
        elif self.storage.exists(filename_total):
            logging.debug('Skipping existing concatenation file %r', filename_total)
        else:
            logging.debug('Creating concatenation file %r', filename_total)
            filename_tmp = str(uuid.uuid4()) + '.tmp'
            self.storage.create(filename_tmp, cat)
            self.storage.rename(filename_tmp, filename_total)
        return hexdigest_total

class ScsCommandline:

    '''Command line wrapper for Scs, used for testing'''

    def __init__(self, storage, blocksize, algorithm):
        self.blocksize = blocksize
        self.args = ['scs', '-s', storage.expanded_folder, '-b', str(blocksize), '-a', algorithm]

    def gc(self, hexdigests):
        assert hexdigests is None
        process = subprocess.Popen(self.args + ['-g'])
        process.wait()
        if process.returncode != 0:
            raise RuntimeError('Subprocess failed with returncode %r' % (process.returncode,))

    def check(self):
        process = subprocess.Popen(self.args + ['-c'])
        process.wait()
        if process.returncode != 0:
            raise RuntimeError('Subprocess failed with returncode %r' % (process.returncode,))

    def load(self, hexdigest):
        process = subprocess.Popen(self.args + ['-l', hexdigest], stdout=subprocess.PIPE)
        while True:
            block = process.stdout.read(self.blocksize)
            if len(block) == 0:
                break
            yield block
        process.wait()
        if process.returncode != 0:
            raise RuntimeError('Subprocess failed with returncode %r' % (process.returncode,))

    def store(self, input_io):
        process = subprocess.Popen(self.args, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        while True:
            block = input_io.read(self.blocksize)
            if len(block) == 0:
                break
            process.stdin.write(block)
        process.stdin.close()
        hexdigest_line = process.stdout.read()
        process.wait()
        if process.returncode != 0:
            raise RuntimeError('Subprocess failed with returncode %r' % (process.returncode,))
        assert hexdigest_line[-1] == '\n'
        return hexdigest_line[:-1]

def test_scs(scs_class, storage):
    '''Run all tests for one Scs instance'''
    scs = scs_class(storage, 10, 'sha1')
    content = {
        'da39a3ee5e6b4b0d3255bfef95601890afd80709': '',
        '356a192b7913b04c54574d18c28d46e6395428ab': '1',
        'f7c3bc1d808e04732adf679965ccc34ca7ae3441': '123456789',
        '01b307acba4f54f55aafc33bb06bbbf6ca803e9a': '1234567890',
        '266dc053a8163e676e83243070241c8917f8a8a3': '12345678901',
        '691bef900d9d408fb4c74f9f503ccd79ab440c4b': 'abcdefghij123456789',
        '787d559439cfd927780996d2c78f635acca40c37': 'abcdefghij1234567890',
        '7ff1b2bc3f8b9f0f40260f91714bc4d2250aab84': 'abcdefghij12345678901',
    }
    expected_chunks = {
        '356a192b7913b04c54574d18c28d46e6395428ab.bin':
            '1',
        'f7c3bc1d808e04732adf679965ccc34ca7ae3441.bin':
            '123456789',
        '01b307acba4f54f55aafc33bb06bbbf6ca803e9a.bin':
            '1234567890',
        'd68c19a0a345b7eab78d5e11e991c026ec60db63.bin':
            'abcdefghij',
        'da39a3ee5e6b4b0d3255bfef95601890afd80709.cat': # ''
            '',
        '266dc053a8163e676e83243070241c8917f8a8a3.cat': # '12345678901'
            '01b307acba4f54f55aafc33bb06bbbf6ca803e9a\n'
            '356a192b7913b04c54574d18c28d46e6395428ab\n',
        '691bef900d9d408fb4c74f9f503ccd79ab440c4b.cat': # 'abcdefghij123456789'
            'd68c19a0a345b7eab78d5e11e991c026ec60db63\n'
            'f7c3bc1d808e04732adf679965ccc34ca7ae3441\n',
        '787d559439cfd927780996d2c78f635acca40c37.cat': # 'abcdefghij1234567890'
            'd68c19a0a345b7eab78d5e11e991c026ec60db63\n'
            '01b307acba4f54f55aafc33bb06bbbf6ca803e9a\n',
        '7ff1b2bc3f8b9f0f40260f91714bc4d2250aab84.cat': # 'abcdefghij12345678901'
            'd68c19a0a345b7eab78d5e11e991c026ec60db63\n'
            '01b307acba4f54f55aafc33bb06bbbf6ca803e9a\n'
            '356a192b7913b04c54574d18c28d46e6395428ab\n',
    }
    for hexdigest, c in content.iteritems():
        assert scs.store(io.BytesIO(c)) == hexdigest
    chunks = {}
    for filename in storage.filenames():
        chunks[filename] = storage.read(filename)
    assert chunks == expected_chunks
    for hexdigest, expected_content in content.iteritems():
        retrieved_content = ''.join(scs.load(hexdigest))
        assert retrieved_content == expected_content
    scs.check()
    storage.create('073c-425f-b797-d2d2be42d263.tmp', '')
    scs.check()
    scs.gc(None)
    assert not storage.exists('f83f48d0-073c-425f-b797-d2d2be42d263.tmp')
    for block in scs.load('266dc053a8163e676e83243070241c8917f8a8a3'):
        pass
    storage.create('01b307acba4f54f55aafc33bb06bbbf6ca803e9a.bin', '0987654321')
    try:
        for block in scs.load('266dc053a8163e676e83243070241c8917f8a8a3'):
            pass
        assert False
    except RuntimeError, e:
        logging.debug('Caught expected %r', e)
    try:
        scs.check()
        assert False
    except RuntimeError, e:
        logging.debug('Caught expected %r', e)

def test():
    '''Run all tests'''
    for scs_class in [Scs, ScsCommandline]:
        logging.info('Testing class %r with file storage', scs_class.__name__)
        storage = FileStorage('~/.scs_test_' + str(uuid.uuid4()))
        try:
            test_scs(scs_class, storage)
        finally:
            for filename in storage.filenames():
                storage.remove(filename)
            storage.rmdir()
    logging.info('____________')
    logging.info('All tests OK')

def main():
    '''Run Scs as command line tool'''
    def blocksize(s):
        i = int(s)
        if not (i > 0):
            raise TypeError('Block size is not positive')
        return i
    def digest(s):
        if re.match('^[0-9a-f]+$', s) is None:
            raise TypeError('Invalid digest')
        return s
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose output')
    parser.add_argument('-t', '--test', action='store_true', help='run test suite and exit')
    parser.add_argument('-b', '--blocksize', default=64*1024, type=blocksize, help='set block size')
    parser.add_argument('-a', '--algorithm', default='sha1', choices=hashlib.algorithms, help='set hash algorithm')
    parser.add_argument('-s', '--storage', default='~/.scs', help='set storage folder')
    parser.add_argument('-c', '--check', action='store_true', help='check whole storage and exit')
    parser.add_argument('-g', '--gc', action='store_true', help='run garbage collector')
    parser.add_argument('-l', '--load', type=digest, metavar='DIGEST', help='load data instead of storing data')
    args = parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s:%(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(message)s')
    logging.debug('Received arguments %r', args)
    if args.test:
        test()
        sys.exit(0)
    scs = Scs(FileStorage(args.storage), args.blocksize, args.algorithm)
    if args.check:
        scs.check()
    elif args.gc:
        scs.gc(None)
    elif args.load is not None:
        hexdigest = args.load
        logging.info('Loading data of digest %r into stdout', hexdigest)
        for block in scs.load(hexdigest):
            sys.stdout.write(block)
    else:
        logging.info('Reading data for storage from stdin')
        sys.stdout.write(scs.store(sys.stdin) + '\n')
    sys.exit(0)

if __name__ == '__main__':
    main()
