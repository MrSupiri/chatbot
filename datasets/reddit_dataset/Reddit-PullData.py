import bs4 as bs
from urllib.request import Request, urlopen
import sqlite3
import json
import os
import sys
from termcolor import colored
import logging
import time
import pickle
import _thread

time.sleep(10)

LOG_DIR = '/root/SupiriChat/log/'
DOWNLOAD_DIR = '/mnt/volume-sgp1-01/SupriChat'
DOWNLOADED_LOGFILE = LOG_DIR + 'downloaded.log'
OUTPUT_LOGFILE = LOG_DIR + 'output.log'
DATABASE_LOCATION = '/root/SupiriChat/database/'
DATA_FILE = '/root/SupiriChat/data.dat'
HM_THREADS = 2
# Don't Modify This
DOWNLOADING = False


class Storage(object):
    def __init__(self, Database):
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        self.conn = sqlite3.connect(Database)
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS parent_reply(comment_id TEXT PRIMARY KEY,
                            parent_id TEXT,
                            parent TEXT,
                            comment TEXT,
                            subreddit TEXT,
                            score INT)''')
        self.sqlBatch = []

    def get(self, query, *pars, readOne=False):
        self.c.execute(query, pars)
        if readOne:
            return self.c.fetchone()
        else:
            return self.c.fetchall()

    def put(self, query, *pars):
        self.c.execute(query, pars)
        self.conn.commit()

    def batchPut(self, query, *pars):
        self.sqlBatch.append([query, pars])
        if len(self.sqlBatch) > 2000:
            self.pushBatch()

    def pushBatch(self):
        self.c.execute('BEGIN TRANSACTION')
        for sql in self.sqlBatch:
            try:
                self.c.execute(sql[0], sql[1])
            except Exception as e:
                log.log.critical('Error while Inserting : ' + str(type(e).__name__) + " : " + str(e))
                log.log.critical(log.getError())
        self.conn.commit()
        self.c.execute('VACUUM')
        self.conn.commit()
        self.sqlBatch = []

    def ReplaceComment(self, commentid, parentid, comment, subreddit, score):
        try:
            self.put('''UPDATE parent_reply SET parent_id = (?),
                          comment_id = (?),
                          comment = (?),
                          subreddit = (?),
                          score = (?)
                        WHERE
                          parent_id =(?)''', parentid, commentid, comment, subreddit, score, parentid)
        except Exception as e:
            log.log.critical('ReplaceComment' + str(type(e).__name__) + " : " + str(e))
            log.log.critical(log.getError())

    def NewComment(self, commentid, parentid, parent, comment, subreddit, score):
        try:
            self.put('''INSERT INTO parent_reply ( parent_id,
                          comment_id,
                          parent,
                          comment,
                          subreddit,
                          score) 
                        VALUES (?, ?, ?, ?, ?, ?)''', parentid, commentid, parent, comment, subreddit, score)
        except Exception as e:
            log.log.critical('NewComment' + str(type(e).__name__) + " : " + str(e))
            log.log.critical(log.getError())

    def NewComment_NoParent(self, commentid, parentid, comment, subreddit, score):
        try:
            self.put('''INSERT INTO parent_reply (parent_id,
                          comment_id,
                          comment,
                          subreddit,
                          score) 
                        VALUES (?, ?, ?, ?, ?)''', parentid, commentid, comment, subreddit, score)
        except Exception as e:
            log.log.critical('NewComment_NoParent' + str(type(e).__name__) + " : " + str(e))
            log.log.critical(log.getError())

    def CurrentBestScore(self, pid):
        try:
            result = self.get('''SELECT score FROM parent_reply WHERE parent_id = (?) LIMIT 1''', pid, readOne=True)
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print(str(type(e).__name__) + " : " + str(e))
            return False

    def findParent(self, ParentID):
        try:
            result = self.get('''SELECT comment FROM parent_reply WHERE comment_id = (?) LIMIT 1''', ParentID,
                              readOne=True)
            if result is not None:
                return result[0]
            else:
                return False
        except Exception as e:
            print(str(type(e).__name__) + " : " + str(e))
            return False

    @staticmethod
    def acceptable(string):
        if len(string.split(' ')) > 100 or len(string) < 3:
            return False
        elif len(string) > 250:
            return False
        elif string == '[deleted]':
            return False
        elif string == '[removed]':
            return False
        else:
            return True

    @staticmethod
    def formatData(string):
        string = string.replace('\n', ' ')
        string = string.replace('\r', ' ')
        string = string.replace('"', "'")
        string = string.replace('&gt;', ' ')
        string = string.replace('&le;', ' ')
        string = string.replace('&lt;', ' ')
        string = string.replace('&ge;', ' ')
        string = string.replace('i&gt;', ' ')
        string = string.replace('  ', ' ')
        string = string.replace('   ', ' ')
        string = string.replace('    ', ' ')
        string = string.replace('     ', ' ')
        string = string.replace('      ', ' ')
        string = string.replace('       ', ' ')
        string = string.replace('**', '')
        return string

    def cleanUP(self):
        print("😈😈😈 Vacuuming out Kids without Parents from the Database 😈😈😈")
        self.c.execute('''DELETE FROM parent_reply WHERE parent IS NULL OR length(parent) < 3''')
        self.conn.commit()
        self.c.execute("VACUUM")
        self.conn.commit()


class Logger(object):
    def __init__(self):
        try:
            logFormatter = logging.Formatter(
                fmt='%(asctime)-10s %(levelname)-10s: %(module)s:%(lineno)-d -  %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S')

            self.log = logging.getLogger()
            self.log.setLevel(20)
            fileHandler = logging.FileHandler(OUTPUT_LOGFILE, 'a')
            fileHandler.setFormatter(logFormatter)
            self.log.addHandler(fileHandler)
            consoleHandler = logging.StreamHandler()
            consoleHandler.setFormatter(logFormatter)
            self.log.addHandler(consoleHandler)
        except Exception as e:
            self.log.critical(str(type(e).__name__) + " : " + str(e))
            self.log.critical(self.getError())

    @staticmethod
    def getError():
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        e = "{} {} {}".format(exc_type, fname, exc_tb.tb_lineno)
        return e


def parseFile(ThreadNumber, file_name, dataDict):
    row_counter = 0
    last_time = int(time.time())

    DB = Storage(DATABASE_LOCATION + "RedditDumpTemp-{}.db".format(ThreadNumber))
    with open(file_name, buffering=2000) as k:
        for row in k:
            row_counter += 1
            if row_counter > dataDict['TRPT'] - dataDict['LTFPR']:
                dataDict['TRPT'] += 1
                try:
                    row = json.loads(row)
                    parent_id = row['parent_id'].split('_')[1]
                    body = DB.formatData(row['body'])
                    score = row['score']
                    comment_id = row['id']
                    subreddit = row['subreddit']
                    if score >= 5 and DB.acceptable(body):
                        DB.batchPut(
                            '''INSERT INTO parent_reply(parent_id, comment, score, comment_id, subreddit) VALUES (?, ?, ?, ?, ?)''',
                            parent_id, body, score, comment_id, subreddit)
                except Exception as e:
                    log.log.critical(str(type(e).__name__) + " : " + str(e))
                    log.log.critical(log.getError())

                if row_counter % 1000000 == 0:
                    DB.pushBatch()
                    DB = dbSeparation(ThreadNumber)

            if len(DB.sqlBatch) == 0:
                pickle.dump(dataDict, open(DATA_FILE, 'wb'))

            if row_counter % 100000 == 0:
                last_time = int(int(time.time()) - last_time)
                o = '{} - Total Rows Read: {}, Row Count {}, Database RedditComment-{}.db, LoopTime {} sec'.format(
                    file_name.split('/')[-1], dataDict['TRPT'], row_counter, ThreadNumber, int(last_time))
                log.log.info(colored(o, 'green'))
                last_time = int(time.time())

    return dataDict, DB


def PreProcess(ThreadNumber, path, dataDict):
    filesize = os.path.getsize(path)
    dataDict['TotalSize'] += filesize
    log.log.info('Started Processing {}'.format(path.split('/')[-1]))
    dataDict_, DB = parseFile(ThreadNumber, path, dataDict)
    dataDict['NewDBSize'] = os.path.getsize(DATABASE_LOCATION) - dataDict['DatabaseSize']
    dataDict['DatabaseSize'] = os.path.getsize(DATABASE_LOCATION)
    os.remove(path)
    DB.pushBatch()


def freeSpace():
    s = os.statvfs('/')
    return toGB(s.f_bsize * s.f_bavail)


def toGB(_bytes):
    return round(_bytes / 1048576, 3)


def Download(path, url):
    os.system("wget -O {0} {1} -c >/dev/null 2>&1".format(path, url))


# noinspection SqlResolve
def dbSeparation(ThreadNumber):
    MasterDB.c.execute("ATTACH 'RedditDumpTemp-{}.db' as toMerge".format(ThreadNumber))
    MasterDB.c.execute("INSERT INTO parent_reply SELECT * FROM toMerge.parent_reply")
    MasterDB.c.execute("DETACH database toMerge")
    os.remove(DATABASE_LOCATION + "RedditDumpTemp-{}.db".format(ThreadNumber))
    DB = Storage(DATABASE_LOCATION + "RedditDumpTemp-{}.db".format(ThreadNumber))
    LOG.info('Database Separation - Push Data Dump to Master DB')

    return DB


def PullData(ThreadNumber, sites, steps, dataDict):
    for i in range(0, sites, steps):
        link = sites[i]
        try:
            lastTime = int(time.time())
            filename = link[43:]
            size = int(urlopen(Request(link, headers={'User-Agent': 'Mozilla/5.0'})).headers["Content-Length"])
            path = os.path.join(DOWNLOAD_DIR, filename)
            LOG.info('Downloading {} ({} MB) to {}'.format(filename, toGB(size), path))
            Download(path, filename)

            if filename.endswith('.bz2'):
                os.system('bzip2 -d ' + path)
                path = path[:-4]
            elif filename.endswith('.xz'):
                os.system('xz -d ' + path)
                path = path[:-3]
            else:
                LOG.critical("I Don't know what to do with {}".format(filename))
                os.remove(path)
                continue

            dataDict['DBS'] = os.path.getsize(DATABASE_LOCATION + 'RedditDump.db')

            PreProcess(ThreadNumber, path, dataDict)

            lastTime = int((int(time.time()) - lastTime) / 60)

            outputs = ['Finished Processing {} ({} MB)'.format(filename, size),
                       'Time Taken - {}'.format(lastTime),
                       'Sum of the File Sizes Processed - {}'.format(dataDict['TFSP']),
                       'Sum of all rows Parsed Through - {}'.format(dataDict['TRPT']),
                       'Rows added to the DataBase - {}'.format(dataDict['TRPT'] - dataDict['LTFPR']),
                       'Size added to DataBase - {}'.format(dataDict['DBS'] - os.path.getsize(DATABASE_LOCATION + 'RedditDump.db')),
                       'DataBase Size - {}'.format(os.path.getsize(DATABASE_LOCATION + 'RedditDump.db'))
                       ]

            for output in outputs:
                LOG.info(colored(output, 'red'))

            dataDict['LTFPR'] = dataDict['TRPT']

            with open(DOWNLOADED_LOGFILE, 'a') as f:
                f.write(link + '\n')
            pickle.dump(dataDict, open(DATA_FILE, 'wb'))

        except Exception as error:
            LOG.critical(str(type(error).__name__) + " : " + str(error))
            LOG.critical(log.getError())
            break


def setup():
    if not os.path.isdir(DOWNLOAD_DIR):
        os.mkdir(DOWNLOAD_DIR)

    if not os.path.exists(DATABASE_LOCATION):
        os.mkdir(DATABASE_LOCATION)
    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    if not os.path.exists(DATA_FILE):
        if os.path.exists(DOWNLOADED_LOGFILE):
            os.remove(DOWNLOADED_LOGFILE)
        if os.path.exists(OUTPUT_LOGFILE):
            os.remove(OUTPUT_LOGFILE)
        for file in os.listdir(DATABASE_LOCATION):
            if os.path.exists(DATABASE_LOCATION + file):
                os.remove(DATABASE_LOCATION + file)

        # Total File Size Processed - TFSP      || Total Rows Parsed Through - TRPT
        # Last Total For Parsed Rows - LTFPR    || Database Size - DBS
        dataDict = {'TFSP': 0, 'TRPT': 0, 'LTFPR': 0, 'DBS': 0}
        LOG.info('Starting Fresh !!')
    else:
        dataDict = pickle.load(open(DATA_FILE, 'rb'))

    sou = urlopen(Request('https://files.pushshift.io/reddit/comments/', headers={'User-Agent': 'Mozilla/5.0'})).read()
    soup = bs.BeautifulSoup(sou, 'lxml')
    links = ['Remove This']

    for url in soup.find_all('a'):
        _url = url.get('href')
        if _url.startswith('./RC_'):
            new_url = 'https://files.pushshift.io/reddit/comments' + _url[1:]
            if not links[-1] == new_url:
                if new_url[:53] == links[-1][:53]:
                    links.remove(links[-1])
                links.append(new_url)

    links.remove('Remove This')

    for link in links[1:]:
        if os.path.exists(DOWNLOADED_LOGFILE):
            with open(DOWNLOADED_LOGFILE, 'r') as f:
                if link + '\n' in f.readlines():
                    links.remove(link)

    return links, dataDict


log = Logger()
LOG = log.log

MasterDB = Storage(DATABASE_LOCATION + 'RedditDump.db')

links_, dataList_ = setup()

for i_ in range(HM_THREADS):
    while DOWNLOADING:
        continue
    _thread.start_new_thread(PullData, (i_, links_, HM_THREADS, dataList_))
