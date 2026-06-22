package context

import (
	"testing"

	"github.com/free5gc/nwdaf/pkg/factory"
)

// =============================================================================
// GroupResolver Tests
// =============================================================================

func TestNewGroupResolver_NilConfig(t *testing.T) {
	resolver := NewGroupResolver(nil)

	if resolver == nil {
		t.Fatal("NewGroupResolver should not return nil")
	} else if len(resolver.groups) != 0 {
		t.Errorf("Expected empty groups map, got %d entries", len(resolver.groups))
	}
}

func TestNewGroupResolver_EmptyConfig(t *testing.T) {
	cfg := &factory.GroupMembershipConfig{
		Groups: []factory.GroupDefinition{},
	}

	resolver := NewGroupResolver(cfg)

	if len(resolver.groups) != 0 {
		t.Errorf("Expected empty groups map, got %d entries", len(resolver.groups))
	}
}

func TestNewGroupResolver_ValidConfig(t *testing.T) {
	cfg := &factory.GroupMembershipConfig{
		Groups: []factory.GroupDefinition{
			{
				GroupId: "group-test-001",
				Supis:   []string{"imsi-001", "imsi-002", "imsi-003"},
			},
			{
				GroupId: "group-test-002",
				Supis:   []string{"imsi-004", "imsi-005"},
			},
		},
	}

	resolver := NewGroupResolver(cfg)

	if len(resolver.groups) != 2 {
		t.Fatalf("Expected 2 groups, got %d", len(resolver.groups))
	}

	if len(resolver.groups["group-test-001"]) != 3 {
		t.Errorf("Expected 3 SUPIs in group-test-001, got %d", len(resolver.groups["group-test-001"]))
	}

	if len(resolver.groups["group-test-002"]) != 2 {
		t.Errorf("Expected 2 SUPIs in group-test-002, got %d", len(resolver.groups["group-test-002"]))
	}
}

func TestNewGroupResolver_SkipsEmptyGroupId(t *testing.T) {
	cfg := &factory.GroupMembershipConfig{
		Groups: []factory.GroupDefinition{
			{
				GroupId: "", // Empty group ID should be skipped
				Supis:   []string{"imsi-001"},
			},
			{
				GroupId: "valid-group",
				Supis:   []string{"imsi-002"},
			},
		},
	}

	resolver := NewGroupResolver(cfg)

	if len(resolver.groups) != 1 {
		t.Fatalf("Expected 1 group (skipping empty), got %d", len(resolver.groups))
	}

	if !resolver.HasGroup("valid-group") {
		t.Error("Expected valid-group to be registered")
	}
}

func TestGroupResolver_ResolveGroupId_Success(t *testing.T) {
	cfg := &factory.GroupMembershipConfig{
		Groups: []factory.GroupDefinition{
			{
				GroupId: "group-test-001",
				Supis:   []string{"imsi-001", "imsi-002", "imsi-003"},
			},
		},
	}

	resolver := NewGroupResolver(cfg)

	supis, err := resolver.ResolveGroupId("group-test-001")
	if err != nil {
		t.Fatalf("ResolveGroupId failed: %v", err)
	}

	if len(supis) != 3 {
		t.Fatalf("Expected 3 SUPIs, got %d", len(supis))
	}

	// Verify all SUPIs are present
	supiSet := make(map[string]bool)
	for _, supi := range supis {
		supiSet[supi] = true
	}

	expectedSupis := []string{"imsi-001", "imsi-002", "imsi-003"}
	for _, expected := range expectedSupis {
		if !supiSet[expected] {
			t.Errorf("Expected SUPI %s not found in result", expected)
		}
	}
}

func TestGroupResolver_ResolveGroupId_NotFound(t *testing.T) {
	cfg := &factory.GroupMembershipConfig{
		Groups: []factory.GroupDefinition{
			{
				GroupId: "group-test-001",
				Supis:   []string{"imsi-001"},
			},
		},
	}

	resolver := NewGroupResolver(cfg)

	_, err := resolver.ResolveGroupId("non-existent-group")

	if err == nil {
		t.Error("Expected error for non-existent group")
	}
}

func TestGroupResolver_ResolveGroupId_EmptyMembers(t *testing.T) {
	cfg := &factory.GroupMembershipConfig{
		Groups: []factory.GroupDefinition{
			{
				GroupId: "empty-group",
				Supis:   []string{}, // Empty member list
			},
		},
	}

	resolver := NewGroupResolver(cfg)

	_, err := resolver.ResolveGroupId("empty-group")

	if err == nil {
		t.Error("Expected error for group with no members")
	}
}

func TestGroupResolver_HasGroup(t *testing.T) {
	cfg := &factory.GroupMembershipConfig{
		Groups: []factory.GroupDefinition{
			{
				GroupId: "group-test-001",
				Supis:   []string{"imsi-001"},
			},
		},
	}

	resolver := NewGroupResolver(cfg)

	if !resolver.HasGroup("group-test-001") {
		t.Error("HasGroup should return true for existing group")
	}

	if resolver.HasGroup("non-existent") {
		t.Error("HasGroup should return false for non-existent group")
	}
}

func TestGroupResolver_GetAllGroups(t *testing.T) {
	cfg := &factory.GroupMembershipConfig{
		Groups: []factory.GroupDefinition{
			{GroupId: "group-a", Supis: []string{"imsi-001"}},
			{GroupId: "group-b", Supis: []string{"imsi-002"}},
			{GroupId: "group-c", Supis: []string{"imsi-003"}},
		},
	}

	resolver := NewGroupResolver(cfg)

	groups := resolver.GetAllGroups()

	if len(groups) != 3 {
		t.Fatalf("Expected 3 groups, got %d", len(groups))
	}

	groupSet := make(map[string]bool)
	for _, g := range groups {
		groupSet[g] = true
	}

	for _, expected := range []string{"group-a", "group-b", "group-c"} {
		if !groupSet[expected] {
			t.Errorf("Expected group %s not found", expected)
		}
	}
}

// =============================================================================
// NWDAFContext GroupResolver Integration Tests
// =============================================================================

func TestNWDAFContext_GroupResolver(t *testing.T) {
	Init()
	ctx := GetSelf()

	// Initially nil
	if ctx.GetGroupResolver() != nil {
		t.Error("GroupResolver should be nil initially")
	}

	// Set resolver
	cfg := &factory.GroupMembershipConfig{
		Groups: []factory.GroupDefinition{
			{GroupId: "test-group", Supis: []string{"imsi-001", "imsi-002"}},
		},
	}
	resolver := NewGroupResolver(cfg)
	ctx.SetGroupResolver(resolver)

	// Retrieve
	retrieved := ctx.GetGroupResolver()
	if retrieved == nil {
		t.Fatal("GetGroupResolver should return set resolver")
	}

	// Use it
	supis, err := retrieved.ResolveGroupId("test-group")
	if err != nil {
		t.Fatalf("ResolveGroupId failed: %v", err)
	}

	if len(supis) != 2 {
		t.Errorf("Expected 2 SUPIs, got %d", len(supis))
	}
}
