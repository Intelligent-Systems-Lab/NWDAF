package accuracy

import (
	"fmt"
	"slices"
	"strings"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/openapi/models"
)

func resolveMonitoringScope(
	nwdafSubId string,
	ctx *nwdaf_context.NWDAFContext,
) (string, bool) {
	if ctx == nil {
		return "", false
	}

	if sub := ctx.GetSubscription(nwdafSubId); sub != nil {
		if scopeKey, ok := resolveMonitoringScopeFromSubscription(sub); ok {
			return scopeKey, true
		}
	}

	return resolveMonitoringScopeFromResources(ctx.GetNwdafSubResources(nwdafSubId))
}

func resolveMonitoringScopeFromSubscription(sub *nwdaf_context.Subscription) (string, bool) {
	if sub == nil {
		return "", false
	}

	for i := range sub.EventSubs {
		eventSub := &sub.EventSubs[i]
		if eventSub.Event == models.NwdafEvent_UE_COMMUNICATION && eventSub.TgtUe != nil {
			return canonicalizeTargetUe(eventSub.TgtUe)
		}
	}

	for i := range sub.EventSubs {
		if sub.EventSubs[i].TgtUe != nil {
			return canonicalizeTargetUe(sub.EventSubs[i].TgtUe)
		}
	}

	return "", false
}

func resolveMonitoringScopeFromResources(resources []nwdaf_context.NwdafSubResource) (string, bool) {
	var groupIDs []string
	var directSupis []string

	for _, resource := range resources {
		if resource.OriginalGroupId != "" {
			groupIDs = append(groupIDs, resource.OriginalGroupId)
			continue
		}
		if resource.Supi != "" {
			directSupis = append(directSupis, resource.Supi)
		}
	}

	return formatMonitoringScope(groupIDs, directSupis)
}

func canonicalizeTargetUe(tgtUe *models.TargetUeInformation) (string, bool) {
	if tgtUe == nil {
		return "", false
	}

	return formatMonitoringScope(tgtUe.IntGroupIds, tgtUe.Supis)
}

func formatMonitoringScope(groupIDs, supis []string) (string, bool) {
	groupIDs = normalizeScopeValues(groupIDs)
	supis = normalizeScopeValues(supis)

	switch {
	case len(groupIDs) == 1 && len(supis) == 0:
		return "group:" + groupIDs[0], true
	case len(groupIDs) > 1 && len(supis) == 0:
		return "groups:" + strings.Join(groupIDs, ","), true
	case len(supis) == 1 && len(groupIDs) == 0:
		return "supi:" + supis[0], true
	case len(supis) > 1 && len(groupIDs) == 0:
		return "supis:" + strings.Join(supis, ","), true
	case len(groupIDs) > 0 && len(supis) > 0:
		return fmt.Sprintf("target:groups=%s;supis=%s",
			strings.Join(groupIDs, ","), strings.Join(supis, ",")), true
	default:
		return "", false
	}
}

func normalizeScopeValues(values []string) []string {
	cleaned := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		cleaned = append(cleaned, value)
	}

	if len(cleaned) == 0 {
		return nil
	}

	slices.Sort(cleaned)
	return slices.Compact(cleaned)
}
