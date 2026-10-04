//go:build !lockcheck

package lockcheck

// Enabled reports whether lock order is checked.
const Enabled = false

// Acquire does nothing without the lockcheck tag.
func Acquire(rank int, name string) {}

// Release does nothing without the lockcheck tag.
func Release(rank int) {}
