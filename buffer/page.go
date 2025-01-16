package buffer

import (
	"hash/fnv"
	"log"
	"strconv"
	"time"
	"unsafe"
)

const PAGESIZE = 4096

var ZEROBUF = [PAGESIZE]byte{0}

type Key struct {
	dbId   uint32
	nsId   uint32
	tblId  uint32
	pageId uint64
}

func NewKey(dbId, nsId, tblId uint32) *Key {
	return &Key{dbId: dbId, nsId: nsId, tblId: tblId}
}

func (k *Key) SetPage(pageId uint64) {
	k.pageId = pageId
}

func (k Key) Hash() uint32 {
	key := uint64(k.dbId+k.nsId+k.tblId) + k.pageId
	str := strconv.Itoa(int(key))
	h := fnv.New32a()
	h.Write([]byte(str))
	return h.Sum32()
}

type PageHDR struct {
	size   int32 // Size of header
	offset int32 // next write position within page
}

type Record struct {
	size    int32 // Every record begins with record size
	nextRec int32
}

type Page struct {
	lockType lock_t
	dirty    bool
	hdr      *PageHDR
	key      *Key
	mu       *Mutex
	accessed time.Time
	data     []byte
}

func NewPage(key *Key, mu MutexType) *Page {
	page := &Page{
		data:     make([]byte, PAGESIZE),
		mu:       &Mutex{lock: mu},
		dirty:    false,
		key:      key,
		lockType: NOLOCK,
	}
	page.hdr = (*PageHDR)(unsafe.Pointer(&page.data[0]))
	page.hdr.size = int32(unsafe.Sizeof(PageHDR{}))
	page.hdr.offset = page.hdr.size
	return page
}

func (p *Page) GetRecord(searchKey *[]byte, searchFunc func(key *[]byte, data *[]byte) bool) *[]byte {
	recHdrSize := unsafe.Sizeof(Record{})

	for i := p.hdr.size; i <= int32(cap(p.data)); {
		rec := *(*Record)(unsafe.Pointer(&p.data[i]))
		if rec.size <= 0 {
			break
		}
		recData := p.data[recHdrSize+uintptr(i) : recHdrSize+uintptr(i+rec.size)]

		if searchFunc(searchKey, &recData) {
			return &recData
		}

		i += rec.size + int32(recHdrSize)
	}
	return nil
}

func (p *Page) PutRecord(data *[]byte) bool {
	recHdrSize := unsafe.Sizeof(Record{})

	if (p.hdr.offset + int32(recHdrSize) + int32(len(*data))) > int32(cap(p.data)) {
		log.Println("Page full")
		return false
	}

	v := Record{size: int32(len(*data))}
	copy(p.data[p.hdr.offset:p.hdr.offset+int32(recHdrSize)], unsafe.Slice((*byte)(unsafe.Pointer(&v)), recHdrSize))
	p.hdr.offset += int32(recHdrSize)
	copy(p.data[p.hdr.offset:p.hdr.offset+int32(len(*data))], *data)
	p.hdr.offset += int32(len(*data))

	copy(p.data[:p.hdr.size], unsafe.Slice((*byte)(unsafe.Pointer(p.hdr)), p.hdr.size))
	p.dirty = true
	return true
}

func (p *Page) Copy() *Page {
	page := NewPage(
		NewKey(p.key.dbId, p.key.nsId, p.key.tblId), nil)

	page.key.pageId = p.key.pageId

	copy(page.data, p.data)
	page.dirty = p.dirty
	page.accessed = p.accessed
	page.lockType = p.lockType
	return page
}

func (p *Page) SetKey(key *Key) {
	p.key = key
}

func (p *Page) Lock(lockType lock_t) {
	p.mu.Lock(lockType)
	p.accessed = time.Now()
	p.lockType = lockType
}

func (p *Page) Unlock() {
	lockType := p.lockType // save old
	p.lockType = NOLOCK
	if p.lockType == SHARED && p.mu.ReaderCount() >= 1 {
		p.lockType = SHARED
	}

	p.mu.Unlock(lockType)
}

func (p *Page) TryLock(lockType lock_t) bool {
	locked := p.mu.TryLock(lockType)
	if locked {
		p.lockType = lockType
		p.accessed = time.Now()
	}
	return locked
}

func (p *Page) Reset() {
	/* We have to clear the buffer contents to be reused later
	 * A poor attempt at `memset` behaviour for Go. Slightly better than looping 
	 * for small slices(assuming pages are never that big, 4kb, 8kb, 16kb etc)
	 */
	copy(p.data, ZEROBUF[:])

	p.accessed = time.Time{} // Reset time
	p.dirty = false
	p.key = nil
	p.hdr = nil
}
