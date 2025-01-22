package buffer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"
	"unsafe"
)

type file struct {
	file FS
	lock *sync.RWMutex
}

type Buffer struct {
	writeDirtyCycle int32 // How often to write dirty pages
	maxExpireTime   int32
	fileMode        int32
	size            int64
	mu              *sync.RWMutex // OpenFiles map protection
	openFiles       map[uint32]file
	fsMgr           FS
	baseDir         string
	pages           []*Page // Fixed. Should not grow
}

func NewBuffer(writeDirtyCycle int32, bufSize int64, baseDir string, fileMode, maxExpireTime int32, fs FS) *Buffer {
	if bufSize < PAGESIZE {
		log.Fatalf("bufSize=%d cannot be less than pagesize=%d", bufSize, PAGESIZE)
	}

	buf := &Buffer{
		writeDirtyCycle: writeDirtyCycle,
		maxExpireTime:   maxExpireTime,
		fileMode:        fileMode,
		size:            bufSize,
		mu:              &sync.RWMutex{},
		openFiles:       make(map[uint32]file),
		fsMgr:           fs,
		baseDir:         baseDir,
	}

	sz := unsafe.Sizeof(Page{}) + PAGESIZE
	numPages := bufSize / int64(sz)

	buf.pages = make([]*Page, numPages)

	// Allocate pages at startup
	// Test: Async alloc? Separate Goroutines?
	for i := 0; i < int(numPages); i++ {
		buf.pages[i] = NewPage(nil, &sync.RWMutex{})
	}

	return buf
}

func (bp *Buffer) slot(hash uint32) int {
	return int(hash) & (cap(bp.pages) - 1)
}

func (bp *Buffer) nextSlot(hash uint32) int {
	next := int(hash+1) & (cap(bp.pages) - 1)
	if next >= len(bp.pages) {
		next = 0
	}
	return next
}

func (bp *Buffer) evictPages(numPages int, doSync bool) {
	evictfunc := func(page *Page) bool {
		ret := false
		if !page.TryLock(EXCLUSIVE) {
			return ret
		}
		defer page.Unlock()

		if page.isFree {
			return ret
		}
		currentTime := time.Now()

		if (time.Duration(bp.maxExpireTime) * time.Second) < currentTime.Sub(page.accessed) {
			if page.dirty {
				bp.writePage(page)
			} else {
				page.Reset()
			}
			ret = true
		} else if page.dirty {
			bp.writePage(page)
			ret = true
		}

		return ret
	}

	numEvicted := 0
	for _, page := range bp.pages {
		if evictfunc(page) {
			numEvicted++
		}

		if numEvicted >= numPages {
			break
		}
	}

	if doSync {
		for _, _file := range bp.openFiles {
			func() {
				_file.lock.Lock()
				defer _file.lock.Unlock()

				_file.file.Sync()
			}()
		}
	}
}

func (bp *Buffer) openFile(key *Key) (FS, error) {
	f, err := bp.fsMgr.Open(
		fmt.Sprintf("%s/%d/%d/%d", bp.baseDir, key.dbId, key.nsId, key.tblId),
		BP_RDWR, int(bp.fileMode))

	if err != nil {
		return nil, fmt.Errorf("openFile: %v", err)
	}

	bp.mu.Lock()
	var _file file
	_file.file = f
	_file.lock = &sync.RWMutex{}
	bp.openFiles[key.tblId] = _file
	bp.mu.Unlock()
	return _file.file, nil
}

// Page should be locked before `writePage` method is called
func (bp *Buffer) writePage(page *Page) error {
	bp.mu.RLock()
	_file, ok := bp.openFiles[page.key.tblId]
	bp.mu.RUnlock()

	if !ok {
		_, err := bp.openFile(page.key)

		if err != nil {
			return err
		}
		_file = bp.openFiles[page.key.tblId]
	}

	_, err := _file.file.Write(page.data, int64(page.key.pageId))
	if err != nil {
		return fmt.Errorf("writePage: %v", err)
	}

	page.Reset()

	return nil
}

// Page should be locked before `readPage` method is called
func (bp *Buffer) readPage(page *Page) error {
	bp.mu.RLock()
	_file, ok := bp.openFiles[page.key.tblId]
	bp.mu.RUnlock()

	if !ok {
		f, err := bp.openFile(page.key)

		if err != nil {
			return err
		}
		_file.file = f
	}

	_, err := _file.file.Read(page.data, int64(page.key.pageId))
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return fmt.Errorf("readPage: %v", err)
		}
	}
	hdr := (*PageHDR)(unsafe.Pointer(&page.data[0]))
	if hdr.offset > 0 {
		page.hdr = hdr
	} else if page.hdr.offset <= 0  {
		page.hdr.size = int32(unsafe.Sizeof(PageHDR{}))
		page.hdr.offset = page.hdr.size
	}
	return nil
}

func (bp *Buffer) Close() {
	for _, page := range bp.pages {
		func() {
			page.Lock(EXCLUSIVE)
			defer page.Unlock()
			if page.dirty && page.hdr != nil && page.hdr.offset > 0 {
				bp.writePage(page)
			}
		}()

	}

	for _, handle := range bp.openFiles {
		func() {
			handle.lock.Lock()
			defer handle.lock.Unlock()

			handle.file.Close()
		}()
	}
}

// Convinient method that can be used as background writer to write pages
// in an async manner. Best to be ran as a separate goroutine
func (bp *Buffer) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(bp.writeDirtyCycle) * time.Microsecond)
	defer ticker.Stop() // Not required in Go v1.23 and above

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bp.evictPages(len(bp.pages), true)
		}
	}
}

func (bp *Buffer) emptyPage(ctx context.Context, key *Key, lockType lock_t) *Page {
	h := key.Hash()
	slot := bp.slot(h)
	var page *Page

	for {
		select {
		case <-ctx.Done():
			log.Println("Timed out while searching for an empty page")
			return nil
		default:
			if bp.pages[slot].TryLock(lockType) {
				page = bp.pages[slot]
				if page.isFree {
					return page
				}
				page.Unlock()

			}

			slot = bp.nextSlot(uint32(slot))
		}
	}
}

// Check requested page in the buffer pool
func (bp *Buffer) getPage(ctx context.Context, key *Key, lockType lock_t) *Page {
	h := key.Hash()
	slot := bp.slot(h)
	startPos := slot
	var page *Page

	for {
		select {
		case <-ctx.Done():
			log.Println("Timed out while retrieving page")
			return nil
		default:
			if bp.pages[slot].TryLock(lockType) {
				page = bp.pages[slot]
				if page.key != nil && page.key.dbId == key.dbId && page.key.nsId == key.nsId && page.key.tblId == key.tblId && page.key.pageId == key.pageId {
					return page
				}
				page.Unlock()
			}

			slot = bp.nextSlot(uint32(slot))

			if slot == startPos {
				return nil
			}
		}
	}
}

// Caller should call unlock on page when done
func (bp *Buffer) GetPage(ctx context.Context, key *Key, timeOut int32, lockType lock_t) (*Page, error) {
	if timeOut < 1 {
		timeOut = 1000
	}

	ctx2, cancel := context.WithTimeout(ctx, time.Millisecond*time.Duration(timeOut))
	defer cancel()

	page := bp.getPage(ctx2, key, lockType)
	if page == nil {
		/* Block is not in the buffer pool. Get a free page from buffer pool
		 * and read in the block from disk
		 */
		page = bp.emptyPage(ctx2, key, lockType)
		if page == nil {
			return nil, fmt.Errorf("GetPage: Unable to find an unused page")
		}

		page.key = key
		err := bp.readPage(page)
		if err != nil {
			return nil, fmt.Errorf("GetPage: %v", err)
		}
	}
	return page, nil
}

func (bp *Buffer) pageWithSpace(ctx context.Context, k *Key, lockType lock_t, size int32) *Page {
	// TODO: Need a way keep track of pages with space, instead of just looping through pages in the buffer[Something like FSM in PG but simpler]
	key := *k

restart:
	h := key.Hash()
	slot := bp.slot(h)
	startPos := slot
	var page *Page

	for {
		select {
		case <-ctx.Done():
			log.Println("Timed out while searching for page with sufficient space")
			return nil
		default:
			if bp.pages[slot].TryLock(lockType) {
				page = bp.pages[slot]
				/* Lest we pick a page that don't belong to us */
				if !page.isFree && page.key.dbId == key.dbId && page.key.nsId == key.nsId && page.key.tblId == key.tblId {
					if (PAGESIZE - page.hdr.offset) >= size {
						return page
					}
				}

				if page.isFree {
					/* Use cached page contents with added benefit of not needing to read disk */
					if page.key != nil && page.key.dbId == key.dbId && page.key.nsId == key.nsId && page.key.tblId == key.tblId {
						if (PAGESIZE-page.hdr.offset) >= size {
							page.isFree = false
							return page
						}
					}
					page.key = &key
					page.hdr.offset = 0
					err := bp.readPage(page)
					if err == nil && (PAGESIZE-page.hdr.offset) >= size {
						page.isFree = false
						return page
					}
				}
				page.Unlock()
			}

			slot = bp.nextSlot(uint32(slot))

			if slot == startPos {

				/* Try next page and rehash */
				key.pageId += PAGESIZE

				goto restart
			}
		}
	}
}

// Convienient method to write data to page. Searches for empty page to
// write to and optionaly syncs page to disk.
func (bp *Buffer) PutRecord(ctx context.Context, key *Key, data *[]byte, timeOut int32, doSync bool) bool {
	if timeOut < 1 {
		timeOut = 1000
	}

	ctx2, cancel := context.WithTimeout(ctx, time.Millisecond*time.Duration(timeOut))
	defer cancel()

	recHdrSize := unsafe.Sizeof(Record{})
	size := len(*data) + int(recHdrSize)
	page := bp.pageWithSpace(ctx2, key, EXCLUSIVE, int32(size))
	if page == nil {
		return false
	}
	defer page.Unlock()

	ret := page.PutRecord(data)
	if ret && doSync {
		ret = bp.writePage(page) == nil
	}
	return ret
}

// Caller still needs unlock page when done
func (bp *Buffer) PutPage(page *Page, doSync bool) error {
	err := bp.writePage(page)
	return err
}
