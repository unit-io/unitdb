package wal

import "sync"

type (
	WAL struct {
		upperSeq uint64
		nBlocks  uint32

		// wg is a WaitGroup that allows us to wait for the syncThread to finish to
		// ensure a clean shutdown
		wg sync.WaitGroup

		startSeq uint64

		opts  Options
		index file
		data  file
	}

	Options struct {
		Dirname    string
		TargetSize int64
	}
)

func newWal(opts Options) (wal *WAL, err error) {
	// Create a new WAL.
	newWal := &WAL{
		opts: opts,
	}

	return newWal, nil
}

// Close closes the wal, frees used resources and checks for active
// transactions.
func (wal *WAL) Close() error {
	// Make sure sync thread isn't running
	wal.wg.Wait()

	// Close the logFile
	if err := wal.index.Sync(); err != nil {
		return err
	}
	return wal.data.Sync()
}

// New will open a WAL. If the previous run did not shut down cleanly, a set of
// upper seq will be returned which got committed successfully to the WAL, but
// were never signaled as fully completed.
//
// If no WAL exists, a new one will be created.
//
func New(opts Options) (*WAL, error) {
	// Create a wal
	return newWal(opts)
}
