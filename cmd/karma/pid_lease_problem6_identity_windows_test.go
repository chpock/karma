//go:build windows

package main

func problem6NonZeroIdentity() pidFileIdentity {
	return pidFileIdentity{volumeSerial: 1, fileIndexHigh: 2, fileIndexLow: 3}
}
