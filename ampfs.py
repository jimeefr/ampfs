#!/usr/bin/env python3

from __future__ import with_statement

import requests
import lxml.html
import re
import sqlite3 as sql
import os
import gzip

import sys
import errno
import argparse

from fuse import FUSE, FuseOSError, Operations

debugmask = 0

DEBUG_HTTP=1
DEBUG_FUSE=2

def debug_print(level,msg):
    global debug
    if level & debugmask: print(msg)

class AMPCache():
    def http_retrieve(self,url,retry=3):
        debug_print(DEBUG_HTTP,url)
        while True:
            try:
                resp = self.sess.get(url)
                if resp.status_code != 200:
                    debug_print(DEBUG_HTTP,"HTTP response code {}".format(resp.status_code))
                    raise
                return resp.content
            except e:
                debug_print(DEBUG_HTTP, e)
            debug_print(DEBUG_HTTP, "Retry #{}".format(retry))
            retry += 1
            if retry >= 3: raise

    def getModuleFilesize(self,moduleid):
        filename = os.path.join(self.moddir,str(moduleid))
        if os.path.exists(filename):
            st = os.lstat(filename)
            return getattr(st,'st_size')
        url = "https://amp.dascene.net/analyzer2.php?idx={}".format(moduleid)
        html = self.http_retrieve(url)
        root = lxml.html.fromstring(html)
        e=root.xpath("//table/tr")
        if not e: return 0
        e = e[-1]
        td = e.xpath("td")
        if td[0].text != 'file size : ': return 0
        size = int(td[1].text)
        return size

    def getAuthorDir(self,authorid):
        url = "http://amp.dascene.net/detail.php?detail=modules&view={}".format(authorid)
        html = self.http_retrieve(url)
        root = lxml.html.fromstring(html)
        for element in root.xpath("//table/tr[td/a[starts-with(@href,\"downmod.php\")]]"):
            href = element.xpath("td/a[@href]")[0].attrib["href"]
            moduleid = re.match(r".*index=([0-9]*)",href).group(1)
            td = element.xpath("td")
            ta = element.xpath("td/a")
            name = td[2].text + "." + ta[0].text
            name = name.replace('\xa0',' ')
            m = re.match(r'([0-9]*)[KkBb]*',td[3].text)
            if m: size = int(m.group(1)) * 1024
            else: size = 0
            yield (moduleid, name, size)

    def getAuthorList(self,letter):
        url = "https://amp.dascene.net/newresult.php?request=list&search={}".format(letter)
        fin = False
        while not fin:
            html = self.http_retrieve(url)
            root = lxml.html.fromstring(html)
            for element in root.xpath("//table/tr/td/table"):
                td = element.xpath("tr/td")
                if td and td[0].text == "Handle: ":
                   a = td[1].xpath("a")[0]
                   handle = a.text
                   link = a.attrib["href"]
                   authorid = re.match(r".*view=([0-9]*)",link).group(1)
                   realname = td[3].text
                   if realname and realname != 'n/a': handle += '('+realname+')'
                   yield (handle, authorid)
            fin = True
            a = root.xpath("//table/caption/a[img[starts-with(@src,\"images/right\")]]")
            if a:
                m = re.match(r".*position=([0-9]*)",a[0].attrib["href"])
                if not m: break
                cursor = m.group(1)
                fin = False
                url = "https://amp.dascene.net/newresult.php?request=list&search={}&position={}".format(letter,cursor)

    def __init__(self,path,reversemode=False):
        self.REVERSE = reversemode
        if not os.path.exists(path): os.mkdir(path)
        self.dbfile = os.path.join(path,"cache.db")
        self.moddir = os.path.join(path,"mods")
        if not os.path.exists(self.moddir): os.mkdir(self.moddir)
        self.sess = requests.Session()
        db = sql.connect(self.dbfile)
        c = db.cursor()
        c.execute("create table if not exists authors (letter varchar(3), authorid int, handle varchar(256))")
        c.execute("create table if not exists modules (authorid int, moduleid int, name varchar(256), filesize int)")
        db.commit()
        db.close()

    def cacheFile(self,moduleid):
        filename = os.path.join(self.moddir,str(moduleid))
        if not os.path.exists(filename):
            url = "https://amp.dascene.net/downmod.php?index={}".format(moduleid)
            content = gzip.decompress(self.http_retrieve(url))
            f = open(filename,"wb")
            f.write(content)
            f.close()

    def getModuleRealPath(self,moduleid):
        filename = os.path.join(self.moddir,str(moduleid))
        if not os.path.exists(filename): self.cacheFile(moduleid)
        return filename

    def cacheAuthorDir(self,authorid):
        db = sql.connect(self.dbfile)
        c = db.cursor()
        for moduleid,name,size in self.getAuthorDir(authorid):
            c.execute("insert into modules values (?,?,?,?)",(authorid,moduleid,name,size))
            yield moduleid,name
        db.commit()
        db.close()

    def listAuthorDir(self,authorid):
        db = sql.connect(self.dbfile)
        c = db.cursor()
        c.execute("select count(*) from modules where authorid=?",(authorid,))
        cached = c.fetchone()[0]
        if not cached: 
            for m,n in self.cacheAuthorDir(authorid):
                if self.REVERSE:
                    mm = re.match(r'([^.]*)\.(.*)',n)
                    if mm: n = mm.group(2) + '.' + mm.group(1)
                yield (m,n)
        else:
            c.execute("select * from modules where authorid=?",(authorid,))
            for a,m,n,s in c.fetchall():
                if self.REVERSE:
                    mm = re.match(r'([^.]*)\.(.*)',n)
                    if mm: n = mm.group(2) + '.' + mm.group(1)
                yield (m,n)
        db.close()

    def cacheLetterDir(self,letter):
        db = sql.connect(self.dbfile)
        c = db.cursor()
        for handle,authorid in self.getAuthorList(letter):
            c.execute("insert into authors values (?,?,?)",(letter,authorid,handle))
            yield authorid,handle
        db.commit()
        db.close()

    def listLetterDir(self,letter):
        db = sql.connect(self.dbfile)
        c = db.cursor()
        c.execute("select count(*) from authors where letter=?",(letter,))
        cached = c.fetchone()[0]
        if not cached: 
            for i,h in self.cacheLetterDir(letter): yield (i,h)
        else:
            c.execute("select * from authors where letter=?",(letter,))
            for l,i,h in c.fetchall():
                yield (i,h)
        db.close()

    def getFileInfo(self,path):
        elems = path.split('/')
        if elems[0] == '': elems = elems[1:]
        m = re.match(r'.*-([0-9]*)',elems[1])
        if not m: return 0,"",0
        authorid,modulename = m.group(1),elems[2]
        db = sql.connect(self.dbfile)
        c = db.cursor()
        c.execute("select * from modules where authorid=? and name=?",(authorid,modulename))
        m = c.fetchone()
        if not m:
            mm = re.match(r'(.*)\.([^.]*)',elems[2])
            if mm:
                revertedname = mm.group(2) + '.' + mm.group(1)
                c.execute("select * from modules where authorid=? and name=?",(authorid,revertedname))
                m = c.fetchone()
            if not m:
                return 0,"",0
        a,m,n,s = m
        filename = os.path.join(self.moddir,str(m))
        if os.path.exists(filename):
            st = os.lstat(filename)
            s = getattr(st,'st_size')
        else:
            if s==0:
                s = self.getModuleFilesize(m)
                if s==0:
                    self.getModuleRealPath(m)
                    st = os.lstat(filename)
                    s = getattr(st,'st_size')
            c.execute("update modules set filesize=? where moduleid=?",(s,m))
            db.commit()
        db.close()
        return m,filename,s

    def resolveFile(self,path):
        m,f,s = self.getFileInfo(path)
        if not m: return "/nonexistingfile"
        return self.getModuleRealPath(m)

class AMPFuseOperations(Operations):
    def __init__(self, cache, safenames=False):
        self.cache = cache
        self.safenames = safenames
        self.firstdir = [ ".", "..", "0-9" ] + [ str(c) for c in "abcdefghijklmnopqrstuvwxyz" ]

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        debug_print(DEBUG_FUSE,"access({},{})".format(path,mode))
        if (mode & 2) > 0:
            raise FuseOSError(errno.EACCES)

    def getattr(self, path, fh=None):
        debug_print(DEBUG_FUSE,"getattr({},{})".format(path,fh))
        if path and path[0]=='/': path=path[1:]
        if path and path[-1]=='/': path=path[:-1]
        pe = path.split('/')
        if len(pe)<3:
            st = os.lstat("/")
            return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                         'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        else:
            try:
                moduleid,real_path,size = self.cache.getFileInfo(path)
            except:
                raise FuseOSError(errno.EACCES)
            if os.path.exists(real_path):
                st = os.lstat(real_path)
                return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                             'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
            return { 'st_atime':1612298166, 'st_ctime':1612298166,
                     'st_gid':0, 'st_mode':33188, 
                     'st_mtime':1612298166, 'st_nlink':1,
                     'st_size':size, 'st_uid':0}

    def readdir(self, path, fh):
        debug_print(DEBUG_FUSE,"readdir({},{})".format(path,fh))

        if path == '/':
            for n in self.firstdir: yield n
        else:
            elem = path.split('/')
            if len(elem) == 2:
                try:
                    gen = self.cache.listLetterDir(elem[1])
                except:
                    raise FuseOSError(errno.EACCES)
                yield '.'
                yield '..'
                for id,author in gen:
                    name = "{}-{}".format(author,id)
                    if self.safenames: name = re.sub(r'[^A-Za-z0-9()\[\] #.-]','_',name)
                    else: name = name.replace('/','_')
                    yield name
            elif len(elem) == 3:
                m = re.match(r'.*-([0-9]*)$',elem[2])
                if not m: return
                try:
                    gen = self.cache.listAuthorDir(m.group(1))
                except:
                    raise FuseOSError(errno.EACCES)
                yield '.'
                yield '..'
                for id,name in gen:
                    yield name

    def readlink(self, path):
        debug_print(DEBUG_FUSE,"readlink({})".format(path))
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def statfs(self, path):
        debug_print(DEBUG_FUSE,"statfs({})".format(path))
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def utimens(self, path, times=None):
        debug_print(DEBUG_FUSE,"utimens({},{})".format(path,times))
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        debug_print(DEBUG_FUSE,"open({},{})".format(path,flags))
        try:
            full_path = self.cache.resolveFile(path)
        except:
            raise FuseOSError(errno.EACCES)
        debug_print(DEBUG_FUSE,full_path)
        return os.open(full_path, flags)

    def read(self, path, length, offset, fh):
        debug_print(DEBUG_FUSE,"read({},{},{},{})".format(path,length,offset,fh))
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def flush(self, path, fh):
        debug_print(DEBUG_FUSE,"flush({},{})".format(path,fh))
        return os.fsync(fh)

    def release(self, path, fh):
        debug_print(DEBUG_FUSE,"release({},{})".format(path,fh))
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        debug_print(DEBUG_FUSE,"fsync({},{},{})".format(path,fdatasync,fh))
        return self.flush(path, fh)


def main(mountpoint,cachedir,reverse=False,safenames=False):
    cache = AMPCache(cachedir,reverse)
    FUSE(AMPFuseOperations(cache,safenames), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    default_cachedir = os.path.join(os.path.dirname(sys.argv[0]),"cache")
    parser = argparse.ArgumentParser(description='Userland filesystem for browsing Amiga Music Preservation (https://amp.dascene.net)')
    parser.add_argument('-c','--cachedir',  default=default_cachedir, help='Where to store the cache files')
    parser.add_argument('-r','--reverse',   action='store_true', help='Reversed mode: mod.foobar -> foobar.mod')
    parser.add_argument('-s','--safenames', action='store_true', help='Safe authornames mode')
    parser.add_argument('-d','--debug',     type=int, default=0, help='Debug level')
    parser.add_argument('mountpoint')
    args = parser.parse_args()
    debugmask = args.debug
    main(args.mountpoint, args.cachedir, args.reverse, args.safenames)
