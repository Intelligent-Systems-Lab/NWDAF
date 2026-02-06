package context

import (
	"fmt"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// GroupResolver resolves Group ID to SUPI list
// Per TS 23.502 §4.15.4.5.2: In production, NWDAF should query UDM via Nudm_SDM_Get
// This implementation uses config-based lookup as a substitute when UDM is not available
type GroupResolver struct {
	groups map[string][]string // groupId -> []supi
}

// NewGroupResolver creates a GroupResolver from config
func NewGroupResolver(cfg *factory.GroupMembershipConfig) *GroupResolver {
	resolver := &GroupResolver{
		groups: make(map[string][]string),
	}

	if cfg == nil || len(cfg.Groups) == 0 {
		logger.CtxLog.Warnf("GroupResolver: no group membership configured")
		return resolver
	}

	for _, group := range cfg.Groups {
		if group.GroupId == "" {
			continue
		}
		resolver.groups[group.GroupId] = group.Supis
		logger.CtxLog.Infof("GroupResolver: registered group %s with %d SUPIs",
			group.GroupId, len(group.Supis))
	}

	return resolver
}

// ResolveGroupId returns the list of SUPIs belonging to the given Group ID
// Returns error if the group is not found
func (r *GroupResolver) ResolveGroupId(groupId string) ([]string, error) {
	supis, found := r.groups[groupId]
	if !found {
		return nil, fmt.Errorf("group ID %s not found in configuration", groupId)
	}
	if len(supis) == 0 {
		return nil, fmt.Errorf("group ID %s has no members", groupId)
	}
	return supis, nil
}

// HasGroup checks if a group ID is configured
func (r *GroupResolver) HasGroup(groupId string) bool {
	_, found := r.groups[groupId]
	return found
}

// GetAllGroups returns all configured group IDs
func (r *GroupResolver) GetAllGroups() []string {
	groups := make([]string, 0, len(r.groups))
	for groupId := range r.groups {
		groups = append(groups, groupId)
	}
	return groups
}
