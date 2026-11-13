// 职责: 定义插件自动化的项目及 API Key 授权范围。
// 关联文件:
//
//	internal/ent/schema/plugin.go
//	internal/server/plugin/automation.go
//	internal/server/biz/plugin_automation_access.go
//
// 注意事项: 空范围拒绝所有访问；项目全选动态包含新 Key，排除项始终优先。
package objects

import (
	"fmt"
	"slices"
)

const (
	PluginPermissionUsageMetrics   = "read_usage_metrics"
	PluginPermissionKeySuspensions = "manage_key_suspensions"
	MaxPluginAutomationProjects    = 100
	MaxPluginAutomationKeys        = 1000
)

// PluginAutomationScope is an owner-approved ceiling, independent of plugin config.
type PluginAutomationScope struct {
	Projects []PluginProjectAccess `json:"projects"`
}

type PluginProjectAccess struct {
	ProjectID         int   `json:"projectId"`
	AllKeys           bool  `json:"allKeys"`
	APIKeyIDs         []int `json:"apiKeyIds,omitempty"`
	ExcludedAPIKeyIDs []int `json:"excludedApiKeyIds,omitempty"`
}

func (s PluginAutomationScope) Validate() error {
	if len(s.Projects) > MaxPluginAutomationProjects {
		return fmt.Errorf("automation scope allows at most %d projects", MaxPluginAutomationProjects)
	}
	seen := make(map[int]bool, len(s.Projects))
	keys := 0
	for _, p := range s.Projects {
		if p.ProjectID <= 0 || seen[p.ProjectID] {
			return fmt.Errorf("automation scope project ids must be positive and unique")
		}
		seen[p.ProjectID] = true
		if p.AllKeys && len(p.APIKeyIDs) != 0 {
			return fmt.Errorf("allKeys cannot be combined with apiKeyIds")
		}
		if !p.AllKeys && len(p.ExcludedAPIKeyIDs) != 0 {
			return fmt.Errorf("excludedApiKeyIds requires allKeys")
		}
		for _, ids := range [][]int{p.APIKeyIDs, p.ExcludedAPIKeyIDs} {
			seenKeys := make(map[int]bool, len(ids))
			for _, id := range ids {
				if id <= 0 || seenKeys[id] {
					return fmt.Errorf("automation scope key ids must be positive and unique")
				}
				seenKeys[id] = true
				keys++
			}
		}
	}
	if keys > MaxPluginAutomationKeys {
		return fmt.Errorf("automation scope allows at most %d key ids", MaxPluginAutomationKeys)
	}
	return nil
}

func (s PluginAutomationScope) Allows(projectID, apiKeyID int) bool {
	for _, p := range s.Projects {
		if p.ProjectID == projectID {
			return apiKeyID > 0 && !slices.Contains(p.ExcludedAPIKeyIDs, apiKeyID) &&
				(p.AllKeys || slices.Contains(p.APIKeyIDs, apiKeyID))
		}
	}
	return false
}
