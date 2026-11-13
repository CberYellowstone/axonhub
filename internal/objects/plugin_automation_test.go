// 职责: 验证插件自动化范围的拒绝默认、项目隔离及动态 Key 授权。
// 关联文件:
//
//	internal/objects/plugin_automation.go
package objects

import "testing"

func TestPluginAutomationScope(t *testing.T) {
	scope := PluginAutomationScope{Projects: []PluginProjectAccess{
		{ProjectID: 1, AllKeys: true, ExcludedAPIKeyIDs: []int{9}},
		{ProjectID: 4, APIKeyIDs: []int{20}},
	}}
	if err := scope.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		project, key int
		allowed      bool
	}{{1, 1, true}, {1, 999, true}, {1, 9, false}, {4, 20, true}, {4, 21, false}, {5, 20, false}, {1, 0, false}} {
		if got := scope.Allows(tc.project, tc.key); got != tc.allowed {
			t.Errorf("Allows(%d, %d)=%v; want %v", tc.project, tc.key, got, tc.allowed)
		}
	}
	if (PluginAutomationScope{}).Allows(1, 1) {
		t.Fatal("empty scope must deny all keys")
	}
}

func TestPluginAutomationScopeRejectsAmbiguousGrants(t *testing.T) {
	for _, projects := range [][]PluginProjectAccess{
		{{ProjectID: 0, AllKeys: true}},
		{{ProjectID: 1, AllKeys: true}, {ProjectID: 1}},
		{{ProjectID: 1, AllKeys: true, APIKeyIDs: []int{2}}},
		{{ProjectID: 1, ExcludedAPIKeyIDs: []int{2}}},
		{{ProjectID: 1, APIKeyIDs: []int{-1}}},
		{{ProjectID: 1, APIKeyIDs: []int{2, 2}}},
	} {
		if err := (PluginAutomationScope{Projects: projects}).Validate(); err == nil {
			t.Errorf("accepted invalid scope: %+v", projects)
		}
	}
}
