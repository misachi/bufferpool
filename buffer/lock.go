package buffer

type lock_t byte

const (
	SHARED    lock_t = 'S'
	EXCLUSIVE lock_t = 'E'
	NOLOCK    lock_t = 'N'
)

type MutexType interface {
	Lock()
	RLock()
	TryLock() bool
	TryRLock() bool
	Unlock()
	RUnlock()
}

type Mutex struct {
	sharedLocksNum int32
	lock           MutexType
}

func (lo *Mutex) Lock(lockType lock_t) {
	if lockType == EXCLUSIVE {
		lo.lock.Lock()
	} else if lockType == SHARED {
		lo.lock.RLock()
		lo.sharedLocksNum++
	}
}

func (lo *Mutex) Unlock(lockType lock_t) {
	if lockType == EXCLUSIVE {
		lo.lock.Unlock()
	} else if lockType == SHARED {
		lo.sharedLocksNum--
		if lo.sharedLocksNum < 0 {
			lo.sharedLocksNum = 0
		}
		lo.lock.RUnlock()
	}
}

func (lo *Mutex) TryLock(lockType lock_t) bool {
	if lockType == EXCLUSIVE {
		return lo.lock.TryLock()
	} else if lockType == SHARED {
		locked := lo.lock.TryRLock()
		if locked {
			lo.sharedLocksNum++
		}
		return locked
	}
	return false
}

func (lo Mutex) ReaderCount() int32 {
	return lo.sharedLocksNum
}
