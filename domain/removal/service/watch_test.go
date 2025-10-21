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
	relationerrors "github.com/juju/juju/domain/relation/errors"
	"github.com/juju/juju/domain/removal"
	removalerrors "github.com/juju/juju/domain/removal/errors"
	"github.com/juju/juju/internal/errors"
	loggertesting "github.com/juju/juju/internal/logger/testing"
)

// watchSuite tests the watcher construction and job execution control flow
// in the removal service. It extends baseSuite to inherit common mock setup.
type watchSuite struct {
	baseSuite

	// watcherFactory is a mock that simulates creating watchers for removal events.
	watcherFactory *MockWatcherFactory
	// stringsWatcher is a mock watcher that would normally emit UUIDs or entity identifiers.
	stringsWatcher *MockStringsWatcher
}

func TestWatchSuite(t *testing.T) {
	tc.Run(t, &watchSuite{})
}

// setupWatchMocks initializes the base mocks (from baseSuite) and adds
// watcher-specific mocks needed for testing watcher construction.
func (s *watchSuite) setupWatchMocks(c *tc.C) *gomock.Controller {
	ctrl := s.setupMocks(c)
	s.watcherFactory = NewMockWatcherFactory(ctrl)
	s.stringsWatcher = NewMockStringsWatcher(ctrl)
	return ctrl
}

// newWatchableService creates a WatchableService instance for testing,
// using all the mocked dependencies. This allows us to test watcher
// construction without needing real database connections or event streams.
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

	// The namespace identifies which database table we're watching for changes.
	namespace := "removal_jobs"
	// Mock the call to get the namespace - this would normally query the state layer.
	s.modelState.EXPECT().NamespaceForWatchRemovals().Return(namespace)

	// Expect the service to create a UUIDs watcher with:
	// - The correct namespace (removal_jobs table)
	// - A descriptive summary for debugging
	// - changestream.Changed to watch for insert/update events
	s.watcherFactory.EXPECT().NewUUIDsWatcher(
		gomock.Any(),
		namespace,
		"removals watcher",
		changestream.Changed,
	).Return(s.stringsWatcher, nil)

	// Call the method under test
	watcher, err := s.newWatchableService(c).WatchRemovals(c.Context())
	// Verify no error occurred
	c.Assert(err, tc.ErrorIsNil)
	// Verify we got back our mock watcher
	c.Assert(watcher, tc.Equals, s.stringsWatcher)
}

// TestWatchRemovalsError tests that WatchRemovals returns an error when
// the watcher factory fails.
func (s *watchSuite) TestWatchRemovalsError(c *tc.C) {
	defer s.setupWatchMocks(c).Finish()

	namespace := "removal_jobs"
	s.modelState.EXPECT().NamespaceForWatchRemovals().Return(namespace)

	// Simulate a failure when creating the watcher (e.g., database unavailable)
	expectedErr := errors.New("watcher creation failed")
	s.watcherFactory.EXPECT().NewUUIDsWatcher(
		gomock.Any(),
		namespace,
		"removals watcher",
		changestream.Changed,
	).Return(nil, expectedErr)

	// Call the method under test
	watcher, err := s.newWatchableService(c).WatchRemovals(c.Context())
	// Verify we got the expected wrapped error message
	c.Assert(err, tc.ErrorMatches, "creating watcher for removals: watcher creation failed")
	// Verify no watcher was returned
	c.Assert(watcher, tc.IsNil)
}

// TestWatchEntityRemovals tests that WatchEntityRemovals correctly constructs
// a namespace mapper watcher with the expected filters.
func (s *watchSuite) TestWatchEntityRemovals(c *tc.C) {
	defer s.setupWatchMocks(c).Finish()

	// NamespaceQuery is a function that queries initial state from the database.
	// For this test, we use nil since we're only testing watcher construction.
	initialQuery := eventsource.NamespaceQuery(nil)
	// filterNames maps database table names to entity type names.
	// This allows the watcher to monitor multiple entity types simultaneously.
	filterNames := map[string]string{
		"relation_table": "relation",
		"unit_table":     "unit",
		"machine_table":  "machine",
	}

	// Mock the call to get namespace configuration
	s.modelState.EXPECT().NamespaceForWatchEntityRemovals().Return(initialQuery, filterNames)

	// Capture the mapper function so we can test it later.
	// The mapper transforms change events into entity identifiers.
	var capturedMapper eventsource.Mapper
	// Capture filters to verify one was created for each entity type.
	var capturedFilters []eventsource.FilterOption

	// Expect the service to create a namespace mapper watcher.
	// We use DoAndReturn to capture the mapper function for testing.
	s.watcherFactory.EXPECT().NewNamespaceMapperWatcher(
		gomock.Any(),
		initialQuery,
		"entity removals watcher",
		gomock.Any(), // mapper function
		gomock.Any(), // first filter
		gomock.Any(), // remaining filters (variadic)
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

	// Call the method under test
	watcher, err := s.newWatchableService(c).WatchEntityRemovals(c.Context())
	c.Assert(err, tc.ErrorIsNil)
	c.Assert(watcher, tc.Equals, s.stringsWatcher)
	// Verify the mapper function was provided
	c.Assert(capturedMapper, tc.NotNil)
	// Verify we got 3 filters (one for each entity type)
	c.Assert(len(capturedFilters), tc.Equals, 3)
}

// TestWatchEntityRemovalsNoFilterNames tests that WatchEntityRemovals returns
// an error when no filter names are provided.
func (s *watchSuite) TestWatchEntityRemovalsNoFilterNames(c *tc.C) {
	defer s.setupWatchMocks(c).Finish()

	initialQuery := eventsource.NamespaceQuery(nil)
	// Empty map means no entity types to watch - this is a configuration error
	filterNames := map[string]string{}

	s.modelState.EXPECT().NamespaceForWatchEntityRemovals().Return(initialQuery, filterNames)

	// Call the method under test
	watcher, err := s.newWatchableService(c).WatchEntityRemovals(c.Context())
	// Verify we got the expected error
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

	// Simulate a failure when creating the watcher
	expectedErr := errors.New("watcher creation failed")
	s.watcherFactory.EXPECT().NewNamespaceMapperWatcher(
		gomock.Any(),
		initialQuery,
		"entity removals watcher",
		gomock.Any(),
		gomock.Any(),
	).Return(nil, expectedErr)

	// Call the method under test
	watcher, err := s.newWatchableService(c).WatchEntityRemovals(c.Context())
	// Verify we got the expected wrapped error
	c.Assert(err, tc.ErrorMatches, "creating watcher for entity removals: watcher creation failed")
	c.Assert(watcher, tc.IsNil)
}

// TestGetEntityLifeFiltering tests the mapper callback behavior for filtering
// entities by their life state. The mapper is invoked by the watcher for each
// change event and decides whether to emit the entity UUID to watchers.
//
// This is a table-driven test that covers all entity types (relation, unit,
// machine, application, model) and various scenarios:
// - Alive entities should be skipped (not emitted)
// - Dying/Dead entities should be emitted with format "{type}:{uuid}"
// - NotFound errors should be skipped (entity already removed)
// - Unknown entity types should return an error
// - Other errors should be propagated
func (s *watchSuite) TestGetEntityLifeFiltering(c *tc.C) {
	tests := []struct {
		name          string    // Test case description
		namespace     string    // Database table name
		entityType    string    // Entity type (relation, unit, etc.)
		entityUUID    string    // Entity UUID being tested
		life          life.Life // Life state of the entity
		err           error     // Error to simulate (if any)
		expectEmit    bool      // Should the entity be emitted?
		expectSkip    bool      // Should the entity be skipped?
		expectError   bool      // Should an error be returned?
		errorContains string    // Expected error message substring
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

		// Set up fresh mocks for each test case
		ctrl := s.setupWatchMocks(c)

		// Configure the namespace query for this entity type
		initialQuery := eventsource.NamespaceQuery(nil)
		filterNames := map[string]string{
			tt.namespace: tt.entityType,
		}

		s.modelState.EXPECT().NamespaceForWatchEntityRemovals().Return(initialQuery, filterNames)

		// Capture the mapper function that will be tested
		var capturedMapper eventsource.Mapper

		// Set up the watcher factory to capture the mapper
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

		// Create the watcher (which captures the mapper function)
		svc := s.newWatchableService(c)
		_, err := svc.WatchEntityRemovals(c.Context())
		c.Assert(err, tc.ErrorIsNil)

		// Now test the mapper callback with a mock change event
		event := changestreammock.NewMockChangeEvent(ctrl)
		event.EXPECT().Namespace().AnyTimes().Return(tt.namespace)
		event.EXPECT().Changed().AnyTimes().Return(tt.entityUUID)

		// Set up the life getter expectation based on entity type.
		// The mapper will call one of these GetXXXLife methods to determine
		// if the entity should be emitted or skipped.
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

		// Invoke the captured mapper with the mock event
		results, mapperErr := capturedMapper(c.Context(), []changestream.ChangeEvent{event})

		// Verify the mapper's behavior based on test expectations
		if tt.expectError {
			// Error cases: verify the error message matches
			c.Assert(mapperErr, tc.ErrorMatches, ".*"+tt.errorContains+".*")
		} else if tt.expectEmit {
			// Emit cases: verify entity was emitted with correct format
			c.Assert(mapperErr, tc.ErrorIsNil)
			c.Assert(len(results), tc.Equals, 1)
			c.Assert(results[0], tc.Equals, tt.entityType+":"+tt.entityUUID)
		} else if tt.expectSkip {
			// Skip cases: verify no entities were emitted
			c.Assert(mapperErr, tc.ErrorIsNil)
			c.Assert(len(results), tc.Equals, 0)
		}

		ctrl.Finish()
	}
}

// TestExecuteJobIncomplete tests that ExecuteJob does not delete the job
// when RemovalJobIncomplete is returned. This sentinel error indicates that
// the job attempted to execute but couldn't complete (e.g., units still in
// scope of a relation). The job should remain in the database to be retried.
func (s *watchSuite) TestExecuteJobIncomplete(c *tc.C) {
	defer s.setupMocks(c).Finish()

	// Create a relation removal job for testing
	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.RelationJob,
		EntityUUID:  "rel-1",
	}

	// Mock the relation removal job to return RemovalJobIncomplete
	// because there are units still in scope.
	// First, the job checks if the relation is dying (it is).
	s.modelState.EXPECT().GetRelationLife(gomock.Any(), job.EntityUUID).Return(
		life.Dying, nil,
	)
	// Then it checks if units are still in scope (they are).
	// This triggers RemovalJobIncomplete to be returned.
	s.modelState.EXPECT().UnitNamesInScope(gomock.Any(), job.EntityUUID).Return(
		[]string{"unit-1", "unit-2"}, nil,
	)

	// DeleteJob should NOT be called because the job is incomplete
	// and needs to be retried later.
	s.modelState.EXPECT().DeleteJob(gomock.Any(), gomock.Any()).Times(0)

	// Execute the job
	err := s.newService(c).ExecuteJob(c.Context(), job)
	// ExecuteJob returns nil when RemovalJobIncomplete is encountered
	// (the error is logged but not propagated)
	c.Assert(err, tc.ErrorIsNil)
}

// TestExecuteJobModelRemoved tests that ExecuteJob deletes the job and
// propagates the RemovalModelRemoved sentinel error. This special case
// occurs when a model removal job is executed but the model has already
// been removed. The job should be deleted (it succeeded) but the sentinel
// error should still be returned to notify listeners.
func (s *watchSuite) TestExecuteJobModelRemoved(c *tc.C) {
	defer s.setupMocks(c).Finish()

	// Create a model removal job for testing
	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.ModelJob,
		EntityUUID:  "model-1",
	}

	// Mock the model removal job to return RemovalModelRemoved.
	// This happens when the model doesn't exist in either controller
	// or model state, indicating it was already removed.
	// First, check if model exists in controller state (it doesn't).
	s.controllerState.EXPECT().ModelExists(gomock.Any(), job.EntityUUID).Return(
		false, nil,
	)
	// Then check model life in model state (not found).
	s.modelState.EXPECT().GetModelLife(gomock.Any(), job.EntityUUID).Return(
		life.Life(0), modelerrors.NotFound,
	)

	// DeleteJob SHOULD be called because the job technically succeeded
	// (the model is removed, which was the goal).
	s.modelState.EXPECT().DeleteJob(gomock.Any(), job.UUID.String()).Return(nil)

	// Execute the job
	err := s.newService(c).ExecuteJob(c.Context(), job)
	// The RemovalModelRemoved sentinel error should be propagated
	// to notify listeners that the model is gone.
	c.Assert(err, tc.ErrorIs, removalerrors.RemovalModelRemoved)
}

// TestExecuteJobSuccess tests that ExecuteJob deletes the job when
// the job completes successfully. This is the normal happy path where
// a removal job executes without errors or sentinel conditions.
func (s *watchSuite) TestExecuteJobSuccess(c *tc.C) {
	defer s.setupMocks(c).Finish()

	// Create a relation removal job for testing
	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.RelationJob,
		EntityUUID:  "rel-1",
	}

	// Mock the relation removal job to succeed.
	// First, verify the relation is dying (ready for removal).
	s.modelState.EXPECT().GetRelationLife(gomock.Any(), job.EntityUUID).Return(
		life.Dying, nil,
	)
	// Then verify no units are in scope (safe to delete).
	s.modelState.EXPECT().UnitNamesInScope(gomock.Any(), job.EntityUUID).Return(
		[]string{}, nil,
	)
	// Finally, delete the relation from the database.
	s.modelState.EXPECT().DeleteRelation(gomock.Any(), job.EntityUUID).Return(nil)

	// DeleteJob SHOULD be called because the job completed successfully
	// and should be removed from the removal jobs table.
	s.modelState.EXPECT().DeleteJob(gomock.Any(), job.UUID.String()).Return(nil)

	// Execute the job
	err := s.newService(c).ExecuteJob(c.Context(), job)
	// No error should be returned on successful completion
	c.Assert(err, tc.ErrorIsNil)
}

// TestExecuteJobDeleteJobError tests that ExecuteJob returns an error
// when DeleteJob fails. Even though the removal job itself succeeded,
// if we can't clean up the job record from the database, that's a
// serious error that should be propagated.
func (s *watchSuite) TestExecuteJobDeleteJobError(c *tc.C) {
	defer s.setupMocks(c).Finish()

	// Create a relation removal job for testing
	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.RelationJob,
		EntityUUID:  "rel-1",
	}

	// Mock the relation removal job to succeed (same as TestExecuteJobSuccess)
	s.modelState.EXPECT().GetRelationLife(gomock.Any(), job.EntityUUID).Return(
		life.Dying, nil,
	)
	s.modelState.EXPECT().UnitNamesInScope(gomock.Any(), job.EntityUUID).Return(
		[]string{}, nil,
	)
	s.modelState.EXPECT().DeleteRelation(gomock.Any(), job.EntityUUID).Return(nil)

	// DeleteJob fails with a database error
	expectedErr := errors.New("database error")
	s.modelState.EXPECT().DeleteJob(gomock.Any(), job.UUID.String()).Return(expectedErr)

	// Execute the job
	err := s.newService(c).ExecuteJob(c.Context(), job)
	// The error should be wrapped with context about which job failed to delete
	c.Assert(err, tc.ErrorMatches, `completing removal "job-1": database error`)
}

// TestExecuteJobOtherError tests that ExecuteJob returns an error (other than
// RemovalJobIncomplete or RemovalModelRemoved) without deleting the job.
// When a job encounters an unexpected error (e.g., database failure), it
// should propagate that error and leave the job in place for manual
// intervention or retry.
func (s *watchSuite) TestExecuteJobOtherError(c *tc.C) {
	defer s.setupMocks(c).Finish()

	// Create a relation removal job for testing
	job := removal.Job{
		UUID:        "job-1",
		RemovalType: removal.RelationJob,
		EntityUUID:  "rel-1",
	}

	// Mock the relation removal job to fail with a non-sentinel error.
	// For example, GetRelationLife might fail due to database issues.
	expectedErr := errors.New("database error")
	s.modelState.EXPECT().GetRelationLife(gomock.Any(), job.EntityUUID).Return(
		life.Life(0), expectedErr,
	)

	// DeleteJob should NOT be called because the job didn't complete
	// (it failed with an unexpected error).
	s.modelState.EXPECT().DeleteJob(gomock.Any(), gomock.Any()).Times(0)

	// Execute the job
	err := s.newService(c).ExecuteJob(c.Context(), job)
	// The database error should be propagated to the caller
	c.Assert(err, tc.ErrorMatches, ".*database error.*")
}
