//go:build !unix

package teststate

import "io/fs"

// ownedByCurrentUser reports whether info describes an entry this user owns.
// Without POSIX ownership every entry in the temp dir belongs to this user.
func ownedByCurrentUser(fs.FileInfo) bool { return true }
