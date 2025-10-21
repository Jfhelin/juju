// Copyright 2025 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package service

import (
	"context"
	"testing"

	"github.com/juju/tc"
	"go.uber.org/mock/gomock"

	"github.com/juju/juju/core/changestream"
	changestreammock "github.com/juju/juju/core/changestream/mocks"
	"github.com/juju/juju/core/watcher"
	"github.com/juju/juju/core/watcher/eventsource"
	applicationerrors "github.com/juju/juju/domain/application/errors"
	"github.com/juju/juju/domain/life"
	machineerrors "github.com/juju/juju/domain/machine/errors"
	modelerrors "github.com/juju/juju/domain/model/errors"
	"github.com/juju/juju/domain/removal"
	removalerrors "github.com/juju/juju/domain/removal/errors"
	relationerrors "github.com/juju/juju/domain/relation/errors"
	"github.com/juju/juju/internal/errors"
	loggertesting "github.com/juju/juju/internal/logger/testing"
)

type watchSuite struct {
	baseSuite

	watcherFactory *MockWatcherFactory
	stringsWatcher *MockStringsWatcher
}

func TestWatchSuite(t *testing.T) {
	tc.Run(t, &watchSuite{})
}

func (s *watchSuite) setupWatchMocks(c *tc.C) *gomock.Controller {
	ctrl := s.setupMocks(c)
	s.watcherFactory = NewMockWatcherFactory(ctrl)
	s.stringsWatcher = NewMockStringsWatcher(ctrl)
	return ctrl
}

func (s *watchSuite) newWatchableService(c *tc.C) *WatchableService {
	return &WatchableService{
		Service: Service{
			controllerState:   s.controllerState,
			modelState:        s.modelState,
			leadershipRevoker: s.revoker,
			modelUUID:         s.modelUUID,
			provider: func(ctx context.Context) (Provider, error) {
				return s.provider, nil
			},
			clock:  s.clock,
			logger: loggertesting.WrapCheckLog(c),
		},
		watcherFactory: s.watcherFactory,
	}
}

// TestWatchRemovals tests that WatchRemovals correctly constructs a watcher
// with the expected namespace and change mask.
func (s *watchSuite) TestWatchRemovals(c *tc.C) {
	defer s.setupWatchMocks(c).Finish()

	namespace := "removal_jobs"
	s.modelState.EXPECT().NamespaceForWatchRemovals().Return(namespace)

	s.watcherFactory.EXPECT().NewUUIDsWatcher(
		gomock.Any(),
		namespace,
		"removals watcher",
		changestream.Changed,
	).Return(s.stringsWatcher, nil)

	watcher, err := s.newWatchableService(c).WatchRemovals(c.Context())
	c.Assert(err, tc.ErrorIsNil)
	c.Assert(watcher, tc.Equals, s.stringsWatcher)
}

// TestWatchRemovalsError tests that WatchRemovals returns an error when
// the watcher factory fails.
func (s *watchSuite) TestWatchRemovalsError(c *tc.C) {
	defer s.setupWatchMocks(c).Finish()

	namespace := "removal_jobs"
	s.modelState.EXPECT().NamespaceForWatchRemovals().Return(namespace)

	expectedErr := errors.New("watcher creation failed")
	s.watcherFactory.EXPECT().NewUUIDsWatcher(
		gomock.Any(),
		namespace,
		"removals watcher",
		changestream.Changed,
	).Return(nil, expectedErr)

	watcher, err := s.newWatchableService(c).WatchRemovals(c.Context())
	c.Assert(err, tc.ErrorMatches, "creating watcher for removals: watcher creation failed")
	c.Assert(watcher, tc.IsNil)
}

// TestWatchEntityRemovals tests that WatchEntityRemovals correctly constructs
// a namespace mapper watcher with the expected filters.
func (s *watchSuite) TestWatchEntityRemovals(c *tc.C) {
	defer s.setupWatchMocks(c).Finish()

	initialQuery := eventsource.NamespaceQuery(nil)
	filterNames := map[string]string{
		"relation_table": "relation",
		"unit_table":     "unit",
		"machine_table":  "machine",
	}

	s.modelState.EXPECT().NamespaceForWatchEntityRemovals().Return(initialQuery, filterNames)

	// Capture the mapper function so we can test it later
	var capturedMapper eventsource.Mapper
	var capturedFilters []eventsource.FilterOption

	s.watcherFactory.EXPECT().NewNamespaceMapperWatcher(
		gomock.Any(),
		initialQuery,
		"entity removals watcher",
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(
		ctx context.Context,
		query eventsource.NamespaceQuery,
		summary string,
		mapper eventsource.Mapper,
		filter eventsource.FilterOption,
		filters ...eventsource.FilterOption,
	) (watcher.StringsWatcher, error) {
		capturedMapper = mapper
		capturedFilters = append([]eventsource.FilterOption{filter}, filters...)
		return s.stringsWatcher, nil
	})

	watcher, err := s.newWatchableService(c).WatchEntityRemovals(c.Context())
	c.Assert(err, tc.ErrorIsNil)
	c.Assert(watcher, tc.Equals, s.stringsWatcher)
	c.Assert(capturedMapper, tc.NotNil)
	c.Assert(len(capturedFilters), tc.Equals, 3)
}

// TestWatchEntityRemovalsNoFilterNames tests that WatchEntityRemovals returns
// an error when no filter names are provided.
func (s *watchSuite) TestWatchEntityRemovalsNoFilterNames(c *tc.C) {
	defer s.setupWatchMocks(c).Finish()

	initialQuery := eventsource.NamespaceQuery(nil)
	filterNames := map[string]string{}

	s.modelState.EXPECT().NamespaceForWatchEntityRemovals().Return(initialQuery, filterNames)

	watcher, err := s.newWatchableService(c).WatchEntityRemovals(c.Context())
	c.Assert(err, tc.ErrorMatches, "no filter names provided for entity removals watcher")
	c.Assert(watcher, tc.IsNil)
}

// TestWatchEntityRemovalsError tests that WatchEntityRemovals returns an error
// when the watcher factory fails.
func (s *watchSuite) TestWatchEntityRemovalsError(c *tc.C) {
	defer s.setupWatchMocks(c).Finish()

	initialQuery := eventsource.NamespaceQuery(nil)
	filterNames := map[string]string{
		"relation_table": "relation",
	}

	s.modelState.EXPECT().NamespaceForWatchEntityRemovals().Return(initialQuery, filterNames)

	expectedErr := errors.New("watcher creation failed")
	s.watcherFactory.EXPECT().NewNamespaceMapperWatcher(
		gomock.Any(),
		initialQuery,
		"entity removals watcher",
		gomock.Any(),
		gomock.Any(),
	).Return(nil, expectedErr)

	watcher, err := s.newWatchableService(c).WatchEntityRemovals(c.Context())
	c.Assert(err, tc.ErrorMatches, "creating watcher for entity removals: watcher creation failed")
	c.Assert(watcher, tc.IsNil)
}

// TestGetEntityLifeFiltering tests the mapper callback behavior for filtering
// entities by their life state.
func (s *watchSuite) TestGetEntityLifeFiltering(c *tc.C) {
	tests := []struct {
		name          string
		namespace     string
		entityType    string
		entityUUID    string
		life          life.Life
		err           error
		expectEmit    bool
		expectSkip    bool
		expectError   bool
		errorContains string
	}{
		{
			name:       "relation alive - skip",
			namespace:  "relation_table",
			entityType: "relation",
			entityUUID: "rel-1",
			life:       life.Alive,
			expectSkip: true,
		},
		{
			name:       "relation dying - emit",
			namespace:  "relation_table",
			entityType: "relation",
			entityUUID: "rel-2",
			life:       life.Dying,
			expectEmit: true,
		},
		{
			name:       "relation dead - emit",
			namespace:  "relation_table",
			entityType: "relation",
			entityUUID: "rel-3",
			life:       life.Dead,
			expectEmit: true,
		},
		{
			name:       "unit alive - skip",
			namespace:  "unit_table",
			entityType: "unit",
			entityUUID: "unit-1",
			life:       life.Alive,
			expectSkip: true,
		},
		{
			name:       "unit dying - emit",
			namespace:  "unit_table",
			entityType: "unit",
			entityUUID: "unit-2",
			life:       life.Dying,
			expectEmit: true,
		},
		{
			name:       "machine alive - skip",
			namespace:  "machine_table",
			entityType: "machine",
			entityUUID: "machine-1",
			life:       life.Alive,
			expectSkip: true,
		},
		{
			name:       "machine dying - emit",
			namespace:  "machine_table",
			entityType: "machine",
			entityUUID: "machine-2",
			life:       life.Dying,
			expectEmit: true,
		},
		{
			name:       "application alive - skip",
			namespace:  "application_table",
			entityType: "application",
			entityUUID: "app-1",
			life:       life.Alive,
			expectSkip: true,
		},
		{
			name:       "application dying - emit",
			namespace:  "application_table",
			entityType: "application",
			entityUUID: "app-2",
			life:       life.Dying,
			expectEmit: true,
		},
		{
			name:       "model alive - skip",
			namespace:  "model_table",
			entityType: "model",
			entityUUID: "model-1",
			life:       life.Alive,
			expectSkip: true,
		},
		{
			name:       "model dying - emit",
			namespace:  "model_table",
			entityType: "model",
			entityUUID: "model-2",
			life:       life.Dying,
			expectEmit: true,
		},
		{
			name:       "relation not found - skip",
			namespace:  "relation_table",
			entityType: "relation",
			entityUUID: "rel-missing",
			err:        relationerrors.RelationNotFound,
			expectSkip: true,
		},
		{
			name:       "unit not found - skip",
			namespace:  "unit_table",
			entityType: "unit",
			entityUUID: "unit-missing",
			err:        applicationerrors.UnitNotFound,
			expectSkip: true,
		},
		{
			name:       "application not found - skip",
			namespace:  "application_table",
			entityType: "application",
			entityUUID: "app-missing",
			err:        applicationerrors.ApplicationNotFound,
			expectSkip: true,
		},
		{
			name:       "machine not found - skip",
			namespace:  "machine_table",
			entityType: "machine",
			entityUUID: "machine-missing",
			err:        machineerrors.MachineNotFound,
			expectSkip: true,
		},
		{
			name:       "model not found - skip",
			namespace:  "model_table",
			entityType: "model",
			entityUUID: "model-missing",
			err:        modelerrors.NotFound,
			expectSkip: true,
		},
		{
			name:          "unknown namespace - error",
			namespace:     "unknown_table",
			entityType:    "unknown",
			entityUUID:    "unknown-1",
			expectError:   true,
			errorContains: "unknown entity type",
		},
		{
			name:          "relation error - error",
			namespace:     "relation_table",
			entityType:    "relation",
			entityUUID:    "rel-error",
			err:           errors.New("database error"),
			expectError:   true,
			errorContains: "getting life for relation",
		},
	}

	for _, tt := range tests {
		c.Logf("running test: %s", tt.name)

		ctrl := s.setupWatchMocks(c)

		initialQuery := eventsource.NamespaceQuery(nil)
		filterNames := map[string]string{
			tt.namespace: tt.entityType,
		}

		s.modelState.EXPECT().NamespaceForWatchEntityRemovals().Return(initialQuery, filterNames)

		var capturedMapper eventsource.Mapper

		s.watcherFactory.EXPECT().NewNamespaceMapperWatcher(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(
			ctx context.Context,
			query eventsource.NamespaceQuery,
			summary string,
			mapper eventsource.Mapper,
			filter eventsource.FilterOption,
			filters ...eventsource.FilterOption,
		) (watcher.StringsWatcher, error) {
			capturedMapper = mapper
			return s.stringsWatcher, nil
		})

		svc := s.newWatchableService(c)
		_, err := svc.WatchEntityRemovals(c.Context())
		c.Assert(err, tc.ErrorIsNil)

		// Now test the mapper callback
		event := changestreammock.NewMockChangeEvent(ctrl)
		event.EXPECT().Namespace().AnyTimes().Return(tt.namespace)
		event.EXPECT().Changed().AnyTimes().Return(tt.entityUUID)

		// Set up the life getter expectation
		switch tt.entityType {
		case "relation":
			if tt.err != nil {
				s.modelState.EXPECT().GetRelationLife(gomock.Any(), tt.entityUUID).Return(life.Life(0), tt.err)
			} else if !tt.expectError {
				s.modelState.EXPECT().GetRelationLife(gomock.Any(), tt.entityUUID).Return(tt.life, nil)
			}
		case "unit":
			if tt.err != nil {
				s.modelState.EXPECT().GetUnitLife(gomock.Any(), tt.entityUUID).Return(life.Life(0), tt.err)
			} else if !tt.expectError {
				s.modelState.EXPECT().GetUnitLife(gomock.Any(), tt.entityUUID).Return(tt.life, nil)
			}
		case "machine":
			if tt.err != nil {
				s.modelState.EXPECT().GetMachineLife(gomock.Any(), tt.entityUUID).Return(life.Life(0), tt.err)
			} else if !tt.expectError {
				s.modelState.EXPECT().GetMachineLife(gomock.Any(), tt.entityUUID).Return(tt.life, nil)
			}
		case "application":
			if tt.err != nil {
				s.modelState.EXPECT().GetApplicationLife(gomock.Any(), tt.entityUUID).Return(life.Life(0), tt.err)
			} else if !tt.expectError {
				s.modelState.EXPECT().GetApplicationLife(gomock.Any(), tt.entityUUID).Return(tt.life, nil)
			}
		case "model":
			if tt.err != nil {
				s.modelState.EXPECT().GetModelLife(gomock.Any(), tt.entityUUID).Return(life.Life(0), tt.err)
			} else if !tt.expectError {
				s.modelState.EXPECT().GetModelLife(gomock.Any(), tt.entityUUID).Return(tt.life, nil)
			}
		}

		results, mapperErr := capturedMapper(c.Context(), []changestream.ChangeEvent{event})

		if tt.expectError {
			c.Assert(mapperErr, tc.ErrorMatches, ".*"+tt.errorContains+".*")
		} else if tt.expectEmit {
			c.Assert(mapperErr, tc.ErrorIsNil)
			c.Assert(len(results), tc.Equals, 1)
			c.Assert(results[0], tc.Equals, tt.entityType+":"+tt.entityUUID)
		} else if tt.expectSkip {
			c.Assert(mapperErr, tc.ErrorIsNil)
			c.Assert(len(results), tc.Equals, 0)
		}

		ctrl.Finish()
	}
}

// TestExecuteJobIncomplete tests that ExecuteJob does not delete the job
// when RemovalJobIncomplete is returned.
func (s *watchSuite) TestExecuteJobIncomplete(c *tc.C) {
	defer s.setupMocks(c).Finish()

	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.RelationJob,
		EntityUUID:  "rel-1",
	}

	// Mock the relation removal job to return RemovalJobIncomplete
	// because there are units still in scope
	s.modelState.EXPECT().GetRelationLife(gomock.Any(), job.EntityUUID).Return(
		life.Dying, nil,
	)
	s.modelState.EXPECT().UnitNamesInScope(gomock.Any(), job.EntityUUID).Return(
		[]string{"unit-1", "unit-2"}, nil,
	)

	// DeleteJob should NOT be called
	s.modelState.EXPECT().DeleteJob(gomock.Any(), gomock.Any()).Times(0)

	err := s.newService(c).ExecuteJob(c.Context(), job)
	c.Assert(err, tc.ErrorIsNil)
}

// TestExecuteJobModelRemoved tests that ExecuteJob deletes the job and
// propagates the RemovalModelRemoved sentinel error.
func (s *watchSuite) TestExecuteJobModelRemoved(c *tc.C) {
	defer s.setupMocks(c).Finish()

	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.ModelJob,
		EntityUUID:  "model-1",
	}

	// Mock the model removal job to return RemovalModelRemoved
	// when the model doesn't exist in either controller or model state
	s.controllerState.EXPECT().ModelExists(gomock.Any(), job.EntityUUID).Return(
		false, nil,
	)
	s.modelState.EXPECT().GetModelLife(gomock.Any(), job.EntityUUID).Return(
		life.Life(0), modelerrors.NotFound,
	)

	// DeleteJob should be called
	s.modelState.EXPECT().DeleteJob(gomock.Any(), job.UUID.String()).Return(nil)

	err := s.newService(c).ExecuteJob(c.Context(), job)
	c.Assert(err, tc.ErrorIs, removalerrors.RemovalModelRemoved)
}

// TestExecuteJobSuccess tests that ExecuteJob deletes the job when
// the job completes successfully.
func (s *watchSuite) TestExecuteJobSuccess(c *tc.C) {
	defer s.setupMocks(c).Finish()

	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.RelationJob,
		EntityUUID:  "rel-1",
	}

	// Mock the relation removal job to succeed
	s.modelState.EXPECT().GetRelationLife(gomock.Any(), job.EntityUUID).Return(
		life.Dying, nil,
	)
	s.modelState.EXPECT().UnitNamesInScope(gomock.Any(), job.EntityUUID).Return(
		[]string{}, nil,
	)
	s.modelState.EXPECT().DeleteRelation(gomock.Any(), job.EntityUUID).Return(nil)

	// DeleteJob should be called
	s.modelState.EXPECT().DeleteJob(gomock.Any(), job.UUID.String()).Return(nil)

	err := s.newService(c).ExecuteJob(c.Context(), job)
	c.Assert(err, tc.ErrorIsNil)
}

// TestExecuteJobDeleteJobError tests that ExecuteJob returns an error
// when DeleteJob fails.
func (s *watchSuite) TestExecuteJobDeleteJobError(c *tc.C) {
	defer s.setupMocks(c).Finish()

	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.RelationJob,
		EntityUUID:  "rel-1",
	}

	// Mock the relation removal job to succeed
	s.modelState.EXPECT().GetRelationLife(gomock.Any(), job.EntityUUID).Return(
		life.Dying, nil,
	)
	s.modelState.EXPECT().UnitNamesInScope(gomock.Any(), job.EntityUUID).Return(
		[]string{}, nil,
	)
	s.modelState.EXPECT().DeleteRelation(gomock.Any(), job.EntityUUID).Return(nil)

	// DeleteJob fails
	expectedErr := errors.New("database error")
	s.modelState.EXPECT().DeleteJob(gomock.Any(), job.UUID.String()).Return(expectedErr)

	err := s.newService(c).ExecuteJob(c.Context(), job)
	c.Assert(err, tc.ErrorMatches, `completing removal "job-1": database error`)
}

// TestExecuteJobOtherError tests that ExecuteJob returns an error (other than
// RemovalJobIncomplete or RemovalModelRemoved) without deleting the job.
func (s *watchSuite) TestExecuteJobOtherError(c *tc.C) {
	defer s.setupMocks(c).Finish()

	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.RelationJob,
		EntityUUID:  "rel-1",
	}

	// Mock the relation removal job to fail with a non-sentinel error
	expectedErr := errors.New("database error")
	s.modelState.EXPECT().GetRelationLife(gomock.Any(), job.EntityUUID).Return(
		life.Life(0), expectedErr,
	)

	// DeleteJob should NOT be called
	s.modelState.EXPECT().DeleteJob(gomock.Any(), gomock.Any()).Times(0)

	err := s.newService(c).ExecuteJob(c.Context(), job)
	c.Assert(err, tc.ErrorMatches, ".*database error.*")
}
