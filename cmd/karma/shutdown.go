package main

import (
	"context"
	"errors"
)

type startupCleanup struct {
	stopHistory      func()
	releasePID       func() error
	stopHousekeeping func()
	stopRegroup      func()
	closeStore       func() error
}

func (s startupCleanup) run() error {
	var returnErrors []error
	if s.stopHistory != nil {
		s.stopHistory()
	}
	if s.releasePID != nil {
		returnErrors = append(returnErrors, s.releasePID())
	}
	if s.stopHousekeeping != nil {
		s.stopHousekeeping()
	}
	if s.stopRegroup != nil {
		s.stopRegroup()
	}
	if s.closeStore != nil {
		returnErrors = append(returnErrors, s.closeStore())
	}
	return errors.Join(returnErrors...)
}

type applicationShutdown struct {
	markPollStopping func()
	stopTicker       func()
	closePollStop    func()
	waitPoll         func()
	newHTTPContext   func() (context.Context, context.CancelFunc)
	shutdownHTTP     func(context.Context) error
	closeHTTP        func() error
	stopHistory      func()
	stopRegroup      func()
	stopHousekeeping func()
	closeStore       func() error
	releasePID       func() error
}

func (s applicationShutdown) run() error {
	s.markPollStopping()
	s.stopTicker()
	s.closePollStop()
	s.waitPoll()

	ctx, cancel := s.newHTTPContext()
	defer cancel()
	if err := s.shutdownHTTP(ctx); err != nil {
		closeErr := s.closeHTTP()
		return errors.Join(err, closeErr, releasePID(s.releasePID))
	}

	s.stopHistory()
	s.stopHousekeeping()
	s.stopRegroup()
	return errors.Join(s.closeStore(), releasePID(s.releasePID))
}

func releasePID(release func() error) error {
	if release == nil {
		return nil
	}
	return release()
}
