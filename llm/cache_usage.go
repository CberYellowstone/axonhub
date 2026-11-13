// 职责: 表达缓存读取用量是否由上游显式上报，并保留跨转换的可信度。
// 关联文件:
//
//	llm/model.go
//	llm/transformer/openai/usage.go
//	internal/server/biz/usage_log.go
//
// 注意事项:
//
//	零值和旧序列化数据均为 unknown；details 对象存在不能证明 cached_tokens 被上报。
package llm

import "encoding/json"

type CacheReadStatus string

const (
	CacheReadUnknown  CacheReadStatus = "unknown"
	CacheReadReported CacheReadStatus = "reported"
)

// MarshalJSON preserves unknown through provider-shaped JSON containing
// synthetic cached_tokens:0 fields, including when status is the old zero value.
func (status CacheReadStatus) MarshalJSON() ([]byte, error) {
	if status == CacheReadReported {
		return json.Marshal(string(CacheReadReported))
	}
	return json.Marshal(string(CacheReadUnknown))
}

// CacheReadReporting normalizes missing/unsupported metadata conservatively.
func (u *Usage) CacheReadReporting() CacheReadStatus {
	if u != nil && u.CacheReadStatus == CacheReadReported {
		return CacheReadReported
	}
	return CacheReadUnknown
}

// ParseCacheReadStatus detects a non-null numeric field, including explicit zero.
// An explicit status always wins, preserving unknown through JSON round trips
// whose provider-shaped numeric fields contain synthetic zero placeholders.
func ParseCacheReadStatus(data []byte, paths ...[]string) CacheReadStatus {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return CacheReadUnknown
	}
	if raw, ok := fields["cache_read_status"]; ok {
		var status CacheReadStatus
		if json.Unmarshal(raw, &status) == nil && status == CacheReadReported {
			return CacheReadReported
		}
		return CacheReadUnknown
	}
	for _, path := range paths {
		current := fields
		for i, key := range path {
			raw, ok := current[key]
			if !ok || string(raw) == "null" {
				break
			}
			if i == len(path)-1 {
				var value int64
				if json.Unmarshal(raw, &value) == nil {
					return CacheReadReported
				}
			} else {
				var nested map[string]json.RawMessage
				if json.Unmarshal(raw, &nested) != nil {
					break
				}
				current = nested
			}
		}
	}
	return CacheReadUnknown
}
