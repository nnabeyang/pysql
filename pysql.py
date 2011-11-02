#!/usr/bin/env python2.6
# -*- coding: utf-8 -*-
import bitstring
import struct
import random
import os
HEADER_OFFSET_PAGE1 = 100
DEFAULT_PAGESIZE = 1024
MAGIC_HEADER = 'PySQLite'
O_RDONLY = 0x01
O_WRITE = 0x02
#page type
INTKEY = 0x01
ZERO_DATA = 0x02
LEAF_DATA = 0x04
LEAF = 0x08
SIZE = [0,1,2,3,4,6,8,8,0,0,0,0]
def get2byte(fp):
    return fp.read('uint:8') << 8 | fp.read('uint:8')
def put2byte(v, fp):
    putNbyte(v, fp, 2)
def get4byte(fp):
    return fp.read('uint:8') << 24 | fp.read('uint:8') << 16 |\
    fp.read('uint:8') << 8 | fp.read('uint:8')
def put4byte(v, fp):
    putNbyte(v, fp, 4)
def putNbyte(v, fp, n):
    r = [None]*n
    for i in range(n):
        r[i] = v & 0xff
        v >>= 8
    assert(v == 0)
    n = len(r)
    fp += bitstring.BitString(bytes=struct.pack('B'*n, *reversed(r)), length=n*8)
def overwrite2byte(v, fp):
    overwriteNbyte(v, fp, 2)
def overwrite4byte(v, fp):
    overwriteNbyte(v, fp, 4)
def overwriteNbyte(v, fp, n):
    r = [None]*n
    for i in range(n):
        r[i] = v & 0xff
        v >>= 8
    assert(v == 0)
    n = len(r)
    fp.overwrite(bitstring.BitString(bytes=struct.pack('B'*n, *reversed(r)), length=n*8))
def putVarint(v, fp):
    r = []
    if v & 0xff000000 << 32:        
        r.append(v & 0xff)
        v >>= 8
        while v != 0:
            r.append((v & 0x7f) | 0x80)
            v >>= 7
    else:
        r.append(v & 0x7f)
        v >>= 7
        while v != 0:
            r.append((v & 0x7f) | 0x80)
            v >>= 7
    n = len(r)
    s = bitstring.BitString(bytes=struct.pack('B'*n, *reversed(r)), length=n*8)
    fp += s
    return n
def overwriteVarint(v, fp):
    r = []
    if v & 0xff000000 << 32:        
        r.append(v & 0xff)
        v >>= 8
        while v != 0:
            r.append((v & 0x7f) | 0x80)
            v >>= 7
    else:
        r.append(v & 0x7f)
        v >>= 7
        while v != 0:
            r.append((v & 0x7f) | 0x80)
            v >>= 7
    n = len(r)
    s = bitstring.BitString(bytes=struct.pack('B'*n, *reversed(r)), length=n*8)
    fp.overwrite(s)
def getVarint(fp):
    v = 0
    p = [fp.read('uint:8')]
    if not (p[0] & 0x80):
        return p[0] & 0x7f, 1
    depth = 0
    while (p[depth] & 0x80) and depth < 8:
        p.append(fp.read('uint:8'))
        depth += 1
    for i in range(depth):
        v |= p[i] & 0x7f
        v <<= 7
    if depth == 8:
        v <<= 1
        v |= p[i+1]
    else:
        v |= p[i+1] & 0x7f
    return v, depth+1
def getVarintLen(v):
    i = 0
    while v != 0 and i < 9:
        i += 1
        v >>= 7
    return i
def get_fieldsize(serial_type):
    if serial_type >= 12:
        return (serial_type-12)/2
    else:
        return SIZE[serial_type];
def get_serialtype(v):
    type_ = type(v)
    if int == type_ or long == type_ :
        if v < 0:
            v = -v
        if v <= 0x7f:
            return 1
        elif v <= 0x7fff:
            return 2
        elif v <= 0x7fffff:
            return 3
        elif v <= 0x7fffffff:
            return 4
        elif v <= 0x7fffffffff:
            return 5
        return 6
    elif str == type_:
        return 2*len(v)+12 + 1
def put_serial(v, mem):
    serial_type = get_serialtype(v)
    n = get_fieldsize(serial_type)    
    if 0 < serial_type and serial_type <= 7:
        p = [0x00] * n
        for i in reversed(range(n)):
            p[i] = v&0xff
            v >>= 8
            i += 1
        s = bitstring.BitString(bytes=struct.pack('B'*n, *p), length=n*8)
        mem += s
        return n
    else:
        s = bitstring.BitString(bytes="\x00"*n, length=n*8)
        s.overwrite(bitstring.pack('bytes:%d' % len(v), v))
        mem += s
        return n 
def get_pagesize(fp):
    fp.pos = 16*8
    return fp.read('uint:8') << 8 | fp.read('uint:8') << 16  
def put_pagesize(size, fp):
    size >>= 8
    assert((size >> 16) == 0)
    v = bitstring.BitString(bytes=struct.pack('BB', size & 0xff, size>>8), length=16)
    fp.pos = 16*8
    fp.overwrite(v)
def set_fileformat(fp):
    fp.pos = 18*8
    fp.overwrite(bitstring.pack('uint:8',0x01))
    fp.overwrite(bitstring.pack('uint:8',0x01))
def set_nReverse(fp, nrev):
    fp.pos = 20*8
    fp.overwrite(bitstring.pack('uint:8', nrev))
class Pager(object):
    def __init__(self, fpath, rwflag=None):
        if os.path.exists(fpath):            
            if rwflag is None or rwflag & O_RDONLY:
                self.rwflag = O_RDONLY
                mode = 'rb'
            elif O_WRITE & rwflag:
                self.rwflag = rwflag
                mode = 'r+b'
            self.fd = open(fpath, mode)
            self.fp = bitstring.BitStream(bytes=self.fd.read(HEADER_OFFSET_PAGE1), length=800)
            self.pagesize = self.get_pagesize()
            self.fp.pos = 20*8
            nReserve = self.fp.read('uint:8')
            self.usableSize = self.pagesize - nReserve
            size = os.stat(fpath).st_size
            self.nPage = size/self.pagesize
        else:
            self.rwflag = O_WRITE
            self.fd = open(fpath, 'wba')
            self.pagesize = DEFAULT_PAGESIZE            
            self.usableSize = DEFAULT_PAGESIZE
            fp = bitstring.BitStream(bytes="\x00"*DEFAULT_PAGESIZE, length=DEFAULT_PAGESIZE*8)
            s = bitstring.BitString(bytes='PySQLite', length=64)
            fp.overwrite(s)
            put_pagesize(self.pagesize, fp)
            set_fileformat(fp)
            set_nReverse(fp, 0)
            self.fp = fp
            self.nPage = 1
        self.maxLeaf = self.usableSize - 35
        self.minLeaf = (self.usableSize - 12) * 32/255 - 23
        self.maxLocal = (self.usableSize - 12) * 64/255 - 23
        self.minLocal = self.minLeaf 
        self.pages = {}
    def __del__(self):
        if self.rwflag == O_WRITE and self.pages:
                self.write()
        self.fd.close()
    def write(self):
            for page in self.pages.values():
                self.fd.seek((page.pageno - 1)* self.pagesize)
                self.fd.write(page.fp.bytes) 
    def init_freelist(self, fp, offset, pgno=0x00):
        fp.pos = (offset + 1)*8
        overwrite2byte(pgno, fp)
        fp.pos = (offset + 5)*8
        overwrite2byte(self.usableSize, fp)
    def read(self, type_fmt, pos):
        self.fp.pos = pos
        return self.fp.read(type_fmt)
    def getPage(self, iTab):
        page = self.pages.get(iTab)
        if page is None:
            self.fd.seek(self.pagesize*(iTab-1))
            mem = self.fd.read(self.pagesize)
            page = Page(mem, self.pagesize, iTab, self)
        return page
    def createPage(self, iTab, flag):        
        if iTab > self.nPage:
            self.nPage += 1
        if iTab == 1:
            offset = HEADER_OFFSET_PAGE1
            fp = self.fp
        else:
            offset = 0
            fp = bitstring.BitString(bytes="\x00"*DEFAULT_PAGESIZE, length=DEFAULT_PAGESIZE*8)
        self.init_freelist(fp, offset, iTab)
        fp.pos = offset*8
        fp.overwrite(bitstring.pack('uint:8', flag))
        page = Page(fp.bytes, self.pagesize, iTab, self)
        return page
    def clonePage(self, pgno):
        origine = self.getPage(pgno)
        fp = bitstring.BitString(bytes="\x00"*DEFAULT_PAGESIZE, length=DEFAULT_PAGESIZE*8)
        celloffset = origine.hdroffset + 8 + origine.childPtrSize
        end = celloffset + origine.nCell*2
        data = origine.fp.bytes
        s = bitstring.pack('bytes:%d' % (end-origine.hdroffset), data[origine.hdroffset:end])
        fp.pos = 0 
        fp.overwrite(s)
        origine.fp.pos = (origine.hdroffset + 5)*8
        top = get2byte(origine.fp)
        size = self.usableSize - top
        self.nPage += 1
        
        s = bitstring.pack('bytes:%d' % size, data[top:self.usableSize])
        fp.pos = top*8
        fp.overwrite(s)
               
        page = Page(fp.bytes, self.pagesize, self.nPage, self)
        page.overflow = origine.overflow        
        fp = page.fp
        self.init_freelist(fp, page.hdroffset, page.pageno)
        fp.pos = page.hdroffset*8
        fp.overwrite(bitstring.pack('uint:8', origine.flag))
        return page
    def get_pagesize(self):
        return get_pagesize(self.fp)
    def get_pagetype(self, page):
        self.fp.pos = (page.hdroffset + self.pagesize * (page.pageno-1)) * 8
        return self.fp.read('uint:8')
    def get_fileformat(self, mode):
        fp = self.fp
        if mode == 'r':
            fp.pos = 18*8
        elif mode == 'w':
            fp.pos = 19*8
        return self.fp.read('uint:8')
MAX_DEPTH = 20
CURSOR_INVALID = 0x00
CURSOR_VALID = 0x01
class Cursor(object):
    def __init__(self, pager, pgno):
        self.pager = pager
        self.rootno = pgno
        self.pgno = pgno
        self.cell = None
        self.pages = [None]*MAX_DEPTH
        self.iCells = [None]*MAX_DEPTH
        self.depth = -1
        self.state = CURSOR_INVALID
    def getrowid(self):
        return self.cell.rowid
    def insert_index(self, keys, values):
        keyvalues = Cursor._make_keyvalue(keys, values)
        idx = self.MoveTo(tuple(keyvalues[:-1]))
        pager = self.pager
        page = self.pages[self.depth]
        intKey = 1
        if self.cell:
            intKey = self.cell.rowid + 1
        page.insertCell(pager, keyvalues, intKey, idx)
        if page.overflow and len(page.overflow) > 0:
            self.balance(pager)    
    @staticmethod
    def _make_keyvalue(keys, values):
        keyvalue = []
        rec = Record()
        for v in values:
            rec.add(v)
        rec.make_record(1)
        for key in keys:
            keyvalue.append(values[key])
        keyvalue.append(rec.mem.bytes)
        return keyvalue
    def insert(self, pager, value):
        self.pgno = self.rootno
        page = pager.getPage(self.pgno)
        if page.leaf and page.nCell == 0:
            intKey = 1
        else:
            self.depth = -1
            self.moveToRightMost()
            intKey = self.cell.rowid + 1
            page = self.pages[self.depth]
        assert(page.leaf)
        page.insertCell(pager, value, intKey)
        if page.overflow and len(page.overflow) == 1:
            self.balance(pager)
    def balance(self, pager):
        while 1:
            depth = self.depth
            page = self.pages[depth]
            if depth == 0:
                if page.overflow:
                    self.balance_deeper(pager, page)
                else:
                    break
            else:                
                parent = self.pages[depth-1]
                pidx = self.iCells[depth-1]
                if 0 < len(page.overflow):
                    self.balance_nonroot(pager, parent, pidx)
                else:
                    break
                self.depth -= 1
                self.pgno = self.pages[self.depth].pageno
    def balance_deeper(self, pager, page):
        leaf = page.leaf
        clone = pager.clonePage(self.pgno)
        assert(clone.overflow)
        assert(clone.leaf == leaf) 
        page.clear(pager, page.flag & ~LEAF)             
        assert(not page.leaf)
        page.add_right(pager, clone.pageno)
        self.depth = 1
        self.pages[1] = clone
        self.iCells[0] = 0
        self.iCells[1] = 0    
    def balance_nonroot(self, pager, parent, pIdx):
        assert(0 == len(parent.overflow))
        pages = []
        cells = []
        cell = parent.find_cell(self.pager, pIdx)
        pgno = cell.get_pgno()
        assert(pgno <= pager.nPage)
        page = pager.getPage(pgno)
        page.extract_cells(pager, cells, parent, pIdx)
        pages.append(page)
        parent.dropcell(pager, pIdx)
        parent.redistribute(pager, cells, pages, pIdx)
    def balance_quick(self, pager, parent, page, intKey):
        assert(page.leaf)
        cell = page.find_cell(pager, page.nCell-1)
        parent.insertCell(pager, cell.getmem(), intKey, iChild = page.pageno)        
        newpage = pager.createPage(pager.nPage+1, LEAF)
        assert(LEAF == newpage.flag)
        cell = page.find_cell_overflow(pager, page.nCell)
        page.overflow = []
        newpage.assemble([cell])
        parent.add_right(pager, newpage)
    @staticmethod
    def cell_compare(key1, page, cell):
        key2 = []
        for iField in range(len(key1)):
            key2.append(cell.getvalue(iField, page.flag))        
        if None in key2:
            return -1
        key2 = tuple(key2)
        if key1 > key2:
            return 1
        elif key1 == key2:
            return 0
        else:
            return -1
    def getvalue(self, iField):
        return self._getvalue(self.cell, iField)
    def _getvalue(self, cell, iField):
        v = cell.getvalue(iField)
        if isinstance(v, tuple):
            pager = self.pager
            v, payload_size, npgno = v
            buf = [v]
            ovflSize = pager.usableSize - 4
            i = 0
            while payload_size > 0 and npgno != 0:
                page = pager.getPage(npgno)
                fp = page.fp
                if payload_size > ovflSize:
                    nbytes = ovflSize
                    payload_size -= ovflSize
                else:
                    nbytes = payload_size
                    payload_size = 0                    
                fp.pos = 4*8
                buf.append(page.read('bytes:%d' % nbytes, 4*8))
                i+=1
                fp.pos = 0
                npgno = get4byte(fp) 
            if payload_size != 0:
                raise Exception("database file is broken")
            return ''.join(buf) 
        else:
            return v
    def getvalue_index(self, iField):
        pager = self.pager
        cell = self.cell
        page = self.pages[self.depth]
        mem = self.getvalue(len(cell.stypes)-1)
        size = len(mem)
        fp = bitstring.BitStream(bytes=mem, length=size*8)
        nLocal = page.getnLocal(pager, size)
        stypes, offsets = page.read_cellheader(fp, 0)
        cell = Cell(fp, 0, size, 0, 0, stypes, offsets, nLocal)
        return self._getvalue(cell, iField)
    def moveToRoot(self):
        self.pgno = self.rootno
        self.depth = -1
        self.iCells[0] = 0
        self.state = CURSOR_INVALID
    #linear-search
    def search(self, pager, page, keys):
        mx = page.nCell
        if not page.leaf:
            mx += 1
        for i in range(mx):
            cell = page.find_cell(pager, i)
            if 0 > Cursor.cell_compare(keys, page, cell):
                self.iCells[self.depth] = i
                return cell
        self.iCells[self.depth] = mx
        return page.find_cell(pager, mx-1)
    def MoveTo(self, keys):
        self.moveToRoot()        
        page = self.pager.getPage(self.pgno)
        pager = self.pager
        self.depth += 1
        self.pages[self.depth] = page
        self.iCells[self.depth] = 0
        while 1:
            cell = self.search(pager, page, keys)
            if not page.leaf:
                self.depth += 1             
                pgno = cell.get_pgno()
                page = pager.getPage(pgno)
                self.pages[self.depth] = page
            else:
                break
        self.cell = cell
        assert(page.leaf)
        return self.iCells[self.depth]
    def moveToLeftMost(self):
        page = self.pager.getPage(self.pgno)
        self.depth += 1
        self.pages[self.depth] = page
        self.iCells[self.depth] = 0
        while not page.leaf:
            page = page.find_entry(self.pager, 0)
            self.depth += 1             
            self.iCells[self.depth] = 0
            self.pages[self.depth] = page
        assert(page.leaf)
        if page.nCell == 0:# for empty table
            raise StopIteration
        self.state = CURSOR_VALID
        self.cell = page.find_entry(self.pager, self.iCells[self.depth])
    def moveToRightMost(self):
        page = self.pager.getPage(self.rootno)
        self.depth += 1
        self.pages[self.depth] = page
        nCell = page.nCell - 1
        if not page.leaf:
            nCell += 1
        self.iCells[self.depth] = nCell
        while not page.leaf:
            page = page.find_entry(self.pager, nCell)
            self.depth += 1
            nCell = page.nCell - 1
            if not page.leaf:
                nCell += 1
            assert(self.depth < MAX_DEPTH)
            self.iCells[self.depth] = nCell
            self.pages[self.depth] = page        
        self.pgno = page.pageno
        if page.nCell == 0:# for empty table
            raise StopIteration
        assert(page.leaf)
        self.state = CURSOR_VALID
        self.cell = page.find_entry(self.pager, self.iCells[self.depth]) 
    def moveNextLeaf(self):
        page = self.pages[self.depth]
        self.iCells[self.depth] += 1
        iCell = self.iCells[self.depth]
        nCell = page.nCell
        if not page.leaf:
            nCell += 1
        if iCell == nCell:
            if self.depth == 0:
                raise StopIteration 
            self.depth -= 1
            self.pgno = page.pageno
            return self.moveNextLeaf()
        else:
            entry = page.find_entry(self.pager, iCell)
            entry.setcell(self)
            return self.cell
    def next(self):
        if self.state == CURSOR_INVALID:
            self.moveToLeftMost()
            return self
        else:
            self.moveNextLeaf() 
            return self
    def __iter__(self):
        return self
    def moveTo(self, iCell, pgno=None):
        if pgno is None:
            page = self.pages[self.depth]
        else:
            page = self.pager.getPage(pgno)
            self.pages[self.depth] = page
        assert(page.leaf)
        self.iCells[self.depth] = iCell
        self.cell = page.find_entry(self.pager, iCell)
class Page(object):
    def __init__(self, mem, pagesize, pageno, pager=None):
        self.overflow = []
        self.fp = bitstring.BitStream(bytes=mem, length=pagesize*8)
        if pageno == 1:
            self.hdroffset = HEADER_OFFSET_PAGE1
        else:
            self.hdroffset = 0
        self.pageno = pageno
        leaf = False
        childPtrSize = 4
        self.fp.pos = self.hdroffset*8
        self.flag = self.fp.read('uint:8')
        if LEAF & self.flag:
            leaf = True
            childPtrSize = 0
        self.has_data = False
        if LEAF_DATA & self.flag:
            self.has_data = True
        self.leaf = leaf
        self.childPtrSize = childPtrSize
        self.nCell = None
        self.nCell = self.get_cellsize()
        self.nField = None
        if pager is not None: 
            self.maxLocal = pager.maxLeaf
            self.minLocal = pager.minLeaf
            pager.pages[pageno] = self 
    def clear(self, pager, flag):
        n = pager.usableSize - self.hdroffset
        s = bitstring.BitString(bytes="\x00"*n, length=n*8)
        self.fp.pos = self.hdroffset * 8
        self.fp.overwrite(s)
        pager.init_freelist(self.fp, self.hdroffset, self.pageno)
        self.fp.pos = self.hdroffset*8
        self.fp.overwrite(bitstring.pack('uint:8', flag))
        self.flag = flag
        self.overflow = []
        self.leaf = False
        self.childPtrSize = 4
        if flag & LEAF:
            self.leaf = True
            self.childPtrSize = 0
        self.nCell = 0
    def add_right(self, pager, pgno):
        self.fp.pos = self.find_cell_offset(self.nCell, pager)
        overwrite4byte(pgno, self.fp)    
    def assemble(self, cells):
        fp = self.fp
        fp.pos = (self.hdroffset + 5) * 8
        top = get2byte(fp)               
        celloffset = self.hdroffset + 8 + self.childPtrSize
        for idx, cell in enumerate(cells):
            mbytes = cell.getmem()
            mem = bitstring.pack('bytes:%d' % cell.size, mbytes)            
            top -= cell.size
            fp.pos = top*8
            fp.overwrite(mem)
            cellarray_pos = celloffset + idx*2
            fp.pos = cellarray_pos*8
            overwrite2byte(top, fp)        
        self.nCell = len(cells)
        fp.pos = (self.hdroffset + 5) * 8
        overwrite2byte(top, fp)        
        self.put_cellsize()
    def dropcell(self, pager, idx):
        fp = self.fp
        if not self.leaf and self.nCell == idx:
            fp.pos = (self.hdroffset + 8)*8
            fp.overwrite(bitstring.pack('bytes:4', '\x00\x00\x00\x00'))
        else:
            cell = self.find_cell(pager, idx)            
            fp.pos = cell.pos
            fp.overwrite(bitstring.pack("bytes:%d" % cell.size, '\x00'*cell.size))
            
            celloffset = self.hdroffset + 8 + self.childPtrSize
            cellarray_end = celloffset + self.nCell*2
            cellarray_begin = celloffset + (idx+1)*2

            size = cellarray_end - cellarray_begin
            assert(size >= 0)
            if size >= 0:
                fp.pos = cellarray_begin*8
                cellarray = fp.read('bytes:%d' % size)
                assert((cellarray_end)*8 == fp.pos)        
                fp.pos = (cellarray_begin - 2)*8
                fp.overwrite(bitstring.pack('bytes:%d' % (size+2), cellarray + '\x00\x00'))
            self.nCell -= 1
            self.put_cellsize()
    def get_cellsize(self):
        self.fp.pos = (self.hdroffset + 3)*8
        return get2byte(self.fp) 
    def put_cellsize(self):
        self.fp.pos = (self.hdroffset + 3)*8
        overwrite2byte(self.nCell, self.fp)
    def find_entry(self, pager, iCell):
        fp = self.fp
        pos = self.find_cell_offset(iCell, pager)
        fp.pos = pos        
        if not self.leaf:
            pgno = get4byte(fp) 
            return pager.getPage(pgno) 
        n = 0
        nPayload, tn =  getVarint(fp)
        n += tn
        intKey, tn = getVarint(fp)
        n += tn
        stypes, offsets = self.read_cellheader(fp, n)
        nLocal = self.getnLocal(pager, nPayload)
        return Cell(fp, pos, nPayload, intKey, n, stypes, offsets, nLocal)    
    def find_cell(self, pager, iCell):
        fp = self.fp
        pos = self.find_cell_offset(iCell, pager)
        return self.find_cell_(fp, pos, pager, iCell)
    def find_cell_(self, fp, pos, pager, iCell):
        res = 0
        n = 0
        if self.leaf:
            fp.pos = pos
        elif not self.has_data and (iCell < self.nCell):
            res = 4
            fp.pos = pos + res*8
        else:
            fp.pos = pos + 4*8            
            intKey, n = getVarint(fp)
            return Cell(fp, pos, 0, intKey, 0, [], [], 77)
        nPayload, tn =  getVarint(fp)
        n += tn
        intKey, tn = getVarint(fp)
        n += tn
        stypes, offsets = self.read_cellheader(fp, n)
        nLocal = self.getnLocal(pager, nPayload)
        return Cell(fp, pos, nPayload, intKey, n, stypes, offsets, nLocal, res)        
    def find_cell_overflow(self, pager, iCell):
        for idx, cellmem in reversed(self.overflow):
            if idx <= iCell:
                if idx == iCell:
                    assert(not isinstance(cellmem, list))
                    fp = bitstring.BitString(bytes=cellmem, length=len(cellmem)*8)
                    return self.find_cell_(fp, 0, pager, iCell)
                iCell -= 1
        cell = self.find_cell(pager, iCell)
        cellmem = cell.getmem()
        fp = bitstring.BitString(bytes=cellmem, length=cell.size*8)
        return self.find_cell_(fp, 0, pager, iCell)
    def extract_cells(self, pager, cells, parent=None, pidx=None):
        limit = self.nCell + len(self.overflow)
        for i in range(limit):
            cell = self.find_cell_overflow(pager, i)
            cells.append(cell)
        if not self.leaf:
            if self.has_data or pidx == parent.nCell:
                cell = parent.find_cell(pager, pidx)
                cell.fp.pos = cell.pos + 4*8
                rowid, n = getVarint(cell.fp)
                cell = self.find_cell(pager, self.nCell)
                cellsize, fp = Page.make_nonleaf_cell(rowid, cell.get_pgno())
                cells.append(Cell(fp, 0, 0, rowid, 0, [], [], 77))
            elif not self.has_data:
                cell = self.find_cell(pager, self.nCell)
                pgno = cell.get_pgno()
                pcell = parent.find_cell(pager, pidx)
                mem = pcell.getmem()
                size = len(mem)
                fp = bitstring.pack('bytes:%d' % size, mem)
                fp.pos = 0
                overwrite4byte(pgno, fp)
                cells.append(Cell(fp, 0, pcell.nPayload, 0, pcell.hdr/8,\
                  pcell.stypes, pcell.offsets, pcell.nLocal, 4))
            else:
                raise Exception(self)
        self.clear(pager, self.flag) 
        return cells
    def redistribute(self, pager, cells, pages, pIdx=None):
        rcell = self.find_cell(pager, self.nCell)
        rpgno = rcell.get_pgno()
        offsets = []
        newpages = []
        usableSpace = pager.usableSize - 8 - self.childPtrSize
        size = 0
        page_flag = pages[0].flag
        i = 0
        limit = len(cells)
        rowids = []
        idoff = cells[0].rowid
        npage = len(pages)
        ipage = 0
        while i < limit:
            cell = cells[i]
            size += cell.size + 2
            if usableSpace < size:
                if page_flag & LEAF:
                    size = cell.size + 2
                else:
                    rowids.append(i+idoff)
                    size = cells[i].size + 2
                    i += 1
                if i != limit:
                    offsets.append(i)
                    ipage += 1
                    if ipage >= npage:
                        newpages.append(pager.createPage(pager.nPage+1, page_flag))
            i += 1
        offsets.append(limit)
        start = 0
        pages.extend(newpages)
        for i, offset in enumerate(offsets): 
            last = offset
            page = pages[i]
            if page.leaf:
                page.assemble(cells[start: last])
                cell = cells[last-1]
            else:
                page.assemble(cells[start: last-1])            
                cell = cells[last-1]
                pgno = cell.get_pgno()
                page.add_right(pager, pgno)
            start = last        
            if i < len(offsets)-1:
                if page.leaf:
                    rowid = cell.rowid
                else:
                    rowid = rowids[i]
                keys = None
                if not page.has_data:
                    keys = cell.getmem()
                    if not page_flag & LEAF:
                        keys = keys[4:]
                self.insertCell(pager, keys, rowid, pIdx, page.pageno) 
                if pIdx is not None:
                    pIdx += 1
            else:
                if 0 < rpgno and rpgno <= pager.nPage:
                    if page.leaf:
                        rowid = cell.rowid
                    else:
                        rowid = -1
                        #rowids[i]
                    keys = None
                    if not page.has_data:
                        keys = cell.getmem()
                        if not page.leaf:
                            keys = keys[4:]
                    self.insertCell(pager, keys, rowid, pIdx, page.pageno) 
                    if pIdx is not None:
                        pIdx += 1
                else:
                    self.add_right(pager, page.pageno)
    def read_cellheader(self, fp, hdr_off):
        stypes = []
        keyoff, tn = getVarint(fp)
        n = tn
        offset = (hdr_off + keyoff)*8
        offsets = [offset]
        while n < keyoff:
            serial_type, tn = getVarint(fp)
            n += tn
            stypes.append(serial_type)
            offset += get_fieldsize(serial_type)*8
            offsets.append(offset)
        assert(n == keyoff)
        return stypes, offsets
    def getnLocal(self, pager, nPayload):
        if nPayload <= self.maxLocal:
            nLocal = nPayload
        else:
            minLocal = self.minLocal
            maxLocal = self.maxLocal
            surplus = minLocal + (nPayload - minLocal) % (pager.usableSize - 4)
            if surplus <= maxLocal:
                nLocal = surplus
            else:
                nLocal = minLocal
        return nLocal
    def find_cell_offset(self, iCell, pager):
        mask = pager.pagesize - 0x01
        celloffset = self.hdroffset + 8 + self.childPtrSize
        if iCell == self.nCell:
            self.fp.pos = (self.hdroffset + 8)*8
            return self.fp.pos
        self.fp.pos = (celloffset + iCell*2)*8
        return (mask & get2byte(self.fp))*8
    def fillInCell(self, pager, rec, intKey):
        rec.make_record(intKey)
        tmem = rec.hdrmem + rec.mem
        nPayload = rec.nPayload
        nLocal = self.getnLocal(pager, nPayload)
        nPayload -= nLocal
        cellsize = rec.hdr + nLocal
        if nPayload:
            pgno = self.pageno
            mem = tmem[:cellsize*8]
            rmem = tmem[cellsize*8:]
            page = pager.createPage(pager.nPage+1, 0x00)
            pgno = page.pageno
            put4byte(pgno, mem)
            cellsize += 4
            space = pager.usableSize - 4
        else:
            mem = tmem
        while nPayload :
            if nPayload > space:
                cmem = rmem[:space*8]
                rmem = rmem[space*8:]
                nPayload -= space
                n = space
            else:
                cmem = rmem
                n = nPayload
                nPayload = 0
                assert(len(cmem) == n*8)            
            fp = page.fp
            fp.pos = 0
            overwrite4byte(pgno, fp) 
            
            fp = page.fp
            fp.pos = 4*8
            fp.overwrite(cmem)

            page = pager.createPage(pager.nPage+1, 0x00)
            pgno = page.pageno        
        return mem, cellsize
    def read(self, type_fmt, pos):
        self.fp.pos = pos
        return self.fp.read(type_fmt) 
    def setcell(self, cursor):
        cursor.pgno = self.pageno
        cursor.moveToLeftMost()
    def insertCell(self, pager, values, intKey=0, idx=None, iChild=None):
        fp = self.fp
        fp.pos = (self.hdroffset + 5) * 8
        top = get2byte(fp)
        if self.leaf:
            rec = Record()
            for v in values:
                rec.add(v)
            mem, cellsize = self.fillInCell(pager, rec, intKey)
        elif self.has_data and iChild and intKey:
            cellsize, mem = Page.make_nonleaf_cell(intKey, iChild)
        elif not self.has_data and iChild and intKey:
            v = '\x00' * 4
            v += values
            cellsize = len(v)
            mem = bitstring.BitStream(bytes=v, length=cellsize*8)
            mem.pos = 0
            overwrite4byte(iChild, mem) 
        else:
            msg = "has_data=%s, ichild=%s, intkey=%s" % (self.has_data, iChild, intKey)
            raise Exception(msg)
        celloffset = self.hdroffset + 8 + self.childPtrSize
        cellarray_end = celloffset + self.nCell*2
        if idx is not None:
            cellarray_pos = celloffset + idx*2
        else:
            idx = self.nCell
            cellarray_pos = cellarray_end
        if cellarray_end + cellsize > top:
            self.overflow.append((idx, mem.bytes))
            return
        top -= cellsize
        fp.pos = top*8
        fp.overwrite(mem)

        fp.pos = (self.hdroffset + 5) * 8
        overwrite2byte(top, fp)

        size = cellarray_end - cellarray_pos
        if idx is not None and size:
            fp.pos = cellarray_pos*8
            cellarray = fp.read('bytes:%d' % size)
            assert((cellarray_end)*8 == fp.pos)        
            fp.pos = (cellarray_pos + 2)*8
            fp.overwrite(bitstring.pack('bytes:%d' % size, cellarray))
        fp.pos = cellarray_pos*8
        overwrite2byte(top, fp)        
        self.nCell += 1
        self.put_cellsize()
    @staticmethod
    def make_nonleaf_cell(intKey, iChild):
        cellsize = 77
        v = '\x00' * cellsize
        mem = bitstring.BitStream(bytes=v, length=cellsize*8)
        mem.pos = 0
        overwrite4byte(iChild, mem)        
        overwriteVarint(intKey, mem)
        return  cellsize, mem
class Cell(object):
    def __init__(self, fp, pos, nPayload, rowid, hdr_size, stypes, offsets, nLocal, res=0):
        self.offsets = offsets
        self.fp = fp
        self.pos = pos
        self.hdr = hdr_size*8
        self.nPayload = nPayload
        self.rowid = rowid
        self.stypes = stypes
        self.nLocal = nLocal
        self.nField = len(stypes)
        self.size = hdr_size + nLocal + res
    def getvalue(self, iField, flag=LEAF):
        if not self.stypes and not self.offsets:
            return None
        fp = self.fp        
        serial_type = self.stypes[iField]
        offset = self.pos + self.offsets[iField]       
        payload_size = get_fieldsize(serial_type)
        if not flag & LEAF:
            offset += 4*8
        fp.pos = offset
        if serial_type == 0 or serial_type == 10 or serial_type == 11:
            return None
        elif 1 <= serial_type and serial_type <= 6:
            return fp.read('int:%d' % (payload_size*8))
        else:
            if payload_size > self.nLocal:
                keyoffset = (offset - self.pos - self.hdr)/8
                size = self.nLocal - keyoffset
                value = fp.read('bytes:%d' % size)
                fp.pos = self.pos + self.hdr + self.nLocal * 8
                npgno = get4byte(fp)
                payload_size -= size
                return value, payload_size, npgno
            else:
                return fp.read('bytes:%d' % payload_size) 
    def setcell(self, cursor):
        cursor.cell = self
    def getmem(self):
        self.fp.pos = self.pos
        return self.fp.read('bytes:%d' % (self.size))
    def get_pgno(self):
        self.fp.pos = self.pos
        return get4byte(self.fp)
class Record(object):
    def __init__(self):
        self.nPayload = 0
        self.keyoff = 0
        self.hdr = 0
        self.values = []
        self.mem = bitstring.BitString()
        self.hdrmem = bitstring.BitString()        
    def add(self, v):
        serial_type = get_serialtype(v)
        self.nPayload += get_fieldsize(serial_type)
        self.values.append(v)
        self.keyoff += putVarint(serial_type, self.mem)
    def make_record(self, intKey):       
        keyofflen = getVarintLen(self.keyoff)
        self.keyoff += keyofflen
        if keyofflen < getVarintLen(self.keyoff):
            self.keyoff += 1
        n = 0
        for v in self.values:
            n += put_serial(v, self.mem)
        assert(self.nPayload == n) 
        self.nPayload += self.keyoff
        
        self.hdr += putVarint(self.nPayload, self.hdrmem)
        self.hdr += putVarint(intKey, self.hdrmem)
        data = bitstring.BitString()
        putVarint(self.keyoff, data)
        self.mem = data + self.mem
TABLES = {'pysql_master': (1, 3, {'tab_name':0, 'rootpage':1, 'fields':2})}
ALPNUM = "abcdefghijklmnopqrstuvwxyz1234567890"
def make_tmpfilename():
    s = list(ALPNUM)
    random.shuffle(s)
    return ''.join(s)[:10] 

class DB(object):
    def __init__(self, path, flag= LEAF_DATA | LEAF):
        pager = Pager(path, O_WRITE)
        self.pager = pager
        self.cursors = {1:Cursor(pager, 1)}
        if pager.nPage == 1:
            pager.createPage(1, flag)
        else:
            for itab in range(2, pager.nPage+1):
                self.cursors[itab] = Cursor(pager, itab)
            rows = self.find({'from': 'pysql_master'})
            for row in rows:
                fields = {}
                for i, field in enumerate(row[2].split(',')):
                    fields[field] = i 
                TABLES[row[0]] = (row[1], i+1, fields)
    def create(self, tab_name, fields):
        pager = self.pager
        pgno = pager.nPage + 1
        self.insert('pysql_master', (tab_name, pgno, ','.join(fields)))
        pager.createPage(pgno, LEAF_DATA | LEAF)
        self.cursors[pgno] = Cursor(pager, pgno)
        field_dic = {}
        for i, field in enumerate(fields):
            field_dic[field] = i
        TABLES[tab_name] = (pgno, len(fields), field_dic)
    
    def find(self, dic):
        tab_name = dic.get('from')
        if tab_name is None:
            tab_name = 'pysql_master'
        pgno, nfield, fields = TABLES[dic.get('from')]
        col_names = dic.get('cols')
        cols = []
        if col_names:
            for col_name in col_names:
                cols.append(fields[col_name])
        else:
            cols = range(nfield)
        keys = dic.get('orderby')
        kindices = []
        cursor = self.cursors[pgno]
        if keys:
            for col_name in keys:
                kindices.append(fields[col_name])            
            values = self.find({'from':tab_name, })
            tmpname = make_tmpfilename()
            pager = Pager(tmpname)
            pager.createPage(1, ZERO_DATA | LEAF)
            cursor = Cursor(pager, 1)
            for v in values:
                cursor.insert_index(kindices, v)        
            cursor.moveToRoot()
            pager = self.pager
            for nxt in cursor:
                v = []
                for i in cols:
                    v.append(nxt.getvalue_index(i))
                yield tuple(v)
            del pager
            assert(os.path.exists(tmpname))
            os.remove(tmpname)
        else:
            cursor.moveToRoot()
            pager = self.pager
            for nxt in cursor:
                v = []
                for i in cols:
                    v.append(nxt.getvalue(i))
                yield tuple(v)
    def insert(self, tab_name, values):
        pgno, nfield, fields = TABLES[tab_name]
        cursor = self.cursors[pgno]
        cursor.insert(self.pager, values)
    def commit(self):        
        self.pager.write()
        self.pager.fd.close()
import sys
if __name__ == '__main__':
    argc = len(sys.argv)
    if argc < 3:
        print "usage:%s <subcmd> <dabasefile> <options>" % sys.argv[0]
        sys.exit(1)        
    db = DB(sys.argv[2])
    subcmd = sys.argv[1]
    if 'create' == subcmd:
        if argc < 5:
            print "usage:%s <subcmd> <dabasefile> <table> <cols>" % sys.argv[0]
            sys.exit(1)            
        db.create(sys.argv[3], sys.argv[4:])
        db.commit()
    elif 'insert' == subcmd:
        if argc < 5:
            print "usage:%s <subcmd> <dabasefile> <table> <cols>" % sys.argv[0]
            sys.exit(1) 
        db.insert(sys.argv[3], sys.argv[4:])
        db.commit() 
    elif 'find' == subcmd:
        if argc < 4:
            print "usage:%s <subcmd> <dabasefile> <table> <cols>?" % sys.argv[0]
            sys.exit(1)
        rows = db.find({'from':sys.argv[3], 'cols': sys.argv[4:]})
        for row in rows:
            print row
    else:
        print "no such subcommand"
        sys.exit(1)

