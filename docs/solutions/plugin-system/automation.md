# 插件自动化接口

## 职责与授权

插件通过持久调度查询缓存用量，对已批准的网关 API Key 创建独立暂停，并将通知写入宿主 outbox。宿主负责授权、统计可信度、幂等、租约和鉴权门禁；阈值、冷却期以及时间窗口与最近请求数条件的 AND/OR 组合由插件实现。[中文开发指南](../../zh/development/plugins.md)和[英文开发指南](../../en/development/plugins.md)提供入口说明。

权限与自动化范围分别授权。`read_usage_metrics` 允许读取范围内聚合指标和 Key ID、名称等非敏感元数据；`manage_key_suspensions` 允许临时或永久阻断范围内 Key，以及解除本安装创建的暂停；`webhook` 允许向宿主已配置目标入队通知。返回指标不包含 Key 明文、提示词或请求正文。个人 Key 和 `noauth` Key 禁止作为暂停目标。

自动化范围绑定数据库安装 ID，由直接登录的系统 Owner 单独设置。空 `projects` 拒绝全部项目与 Key 访问。`allKeys: true` 动态包含该项目以后新增的 Key，排除项始终优先；`apiKeyIds` 指定固定集合。插件配置、页面参数和 KV 只能收窄此上限。Key 移动项目后按当前归属重新授权，历史统计按请求发生时的项目归属查询。

具有已授予 `manage_key_suspensions` 权限的插件，其配置及页面 KV 写入受直接 Owner 约束。普通 `write_plugins` 用户可查看只读配置。插件页面查询指标时还需声明并获授 `read_dashboard`，同时满足当前用户的 dashboard 读取权限及安装项目/Key 上限；当前用户的 `read_dashboard` 检查为系统级，仅有同名项目成员权限不足以满足此检查。页面暂停操作及特权证据查询由 Owner 执行。每次受控宿主操作都会重新检查安装启停、权限和范围。

## Owner 设置范围

使用直接 Owner 身份访问 `/admin/graphql`。先通过 `plugins { id pluginID automationScope { projects { projectId allKeys apiKeyIds excludedApiKeyIds } } }` 取得目标安装的 `id`，将原值传给下列 mutation。Plugin 的 GraphQL `ID` 是 `gid://axonhub/Plugin/<安装数字ID>` 形式的 GUID；`pluginID` 是清单字符串，两者有独立用途。范围内的 `projectId` 和 Key ID 均为正整数。

```graphql
mutation SetAutomationScope($id: ID!, $scope: PluginAutomationScopeInput!) {
  setPluginAutomationScope(id: $id, scope: $scope) {
    id
    pluginID
    automationScope {
      projects { projectId allKeys apiKeyIds excludedApiKeyIds }
    }
  }
}
```

变量示例中的安装、项目和 Key ID 应替换为实际值：

```json
{
  "id": "gid://axonhub/Plugin/12",
  "scope": {
    "projects": [
      { "projectId": 7, "allKeys": false, "apiKeyIds": [101, 102], "excludedApiKeyIds": [] },
      { "projectId": 8, "allKeys": true, "apiKeyIds": [], "excludedApiKeyIds": [203] }
    ]
  }
}
```

此 mutation 整体替换范围；撤销全部范围时提交 `{"projects": []}`。最多 100 个唯一项目，所有项目显式列出的 Key ID 合计最多 1000 个；每个列表中的 ID 必须为正且唯一。`allKeys` 与非空 `apiKeyIds` 互斥，`excludedApiKeyIds` 要求 `allKeys: true`。宿主校验项目、Key 的实际归属。插件管理详情可检查范围；范围配置通过上述 GraphQL 接口完成。

## 清单与调度入口

以下是可安装清单的最小结构；配套 `plugin.js` 必须导出 `onSchedule`。实际插件按所用 API 声明权限和能力要求。

```json
{
  "schemaVersion": 1,
  "id": "example.cache-monitor",
  "name": "Cache monitor",
  "version": "1.0.0",
  "axonhubVersion": "^1.0.0",
  "pluginApiVersion": 1,
  "permissions": ["read_usage_metrics", "manage_key_suspensions", "webhook"],
  "schedules": [{ "key": "check-cache", "intervalSeconds": 300 }],
  "requiredCapabilities": [
    "scheduler.durable.v1", "usage.metrics.v1", "key.suspensions.v1",
    "kv.cas.v1", "notifications.webhook.v1"
  ]
}
```

`schedules` 最多 8 项，`key` 在插件内唯一并匹配 `^[a-z0-9][a-z0-9-]{0,31}$`，`intervalSeconds` 为 60–86400 的整数。`requiredCapabilities` 接受上例列出的宿主能力名，条目非空、无首尾空白、最多 128 字节且不可重复；宿主拒绝不支持的要求。管理查询 `pluginHostCapabilities` 返回本进程支持的能力。升级预览列出新增任务、加快频率的任务 key，以及新增能力要求。

```js
export function onSchedule(ctx, job) {
  if (job.key !== "check-cache") return;
  ctx.log.info("cache check", {
    runId: job.runId,
    scheduledAt: job.scheduledAt,
    attempt: job.attempt,
  });
  // 在这里执行有界查询；授权范围由 Owner 另行批准。
}
```

运行时以 `onSchedule(ctx, job)` 调用；`job` 为 `{ key, runId, scheduledAt, attempt }`，时间为 UTC RFC 3339，`attempt` 从 1 开始。调度持久化于数据库，至少一次执行；失败或租约接管复用 `runId`。停机遗漏的周期合并为一次当前状态检查，成功后从当前数据库时间计算下一次执行。单次执行总预算为 10 秒，包含等待 VM；分页和 CAS 重试需要自行限界，剩余工作可保存游标后交给下次执行。

租约、配置、包、授权或启停状态变化会使旧调度执行者失效。短数据库事务为受控写入提供围栏；JavaScript 和 HTTP 执行位于这些事务之外。`runId` 可用于调度级去重，业务处罚还须绑定观测证据，避免下一个周期重复处罚同一批请求。

## JavaScript 宿主 API

下列方法同步返回 JSON 值，失败时抛出 `PluginHostError`。自动化 options/input/filter 必须为一个最多 64 KiB 的 JSON 对象，未知字段和类型不符会被拒绝。单次 JSON 返回值最多 2 MiB，超限时应减少页大小或样本数量。`ctx.keySuspensions` 与 `ctx.notifications` 仅在 `onEvent`、`onSchedule` 中提供；`ctx.usage` 可用于其他宿主入口，仍需通过实时授权。

| 调用 | 权限与返回值 |
| --- | --- |
| `ctx.usage.cacheStats(options)` | `read_usage_metrics`；返回 `{ end, items, nextAfterKeyId? }` |
| `ctx.keySuspensions.apply(input)` | `manage_key_suspensions`；创建或重放本安装暂停，返回暂停记录 |
| `ctx.keySuspensions.release(id)` | `manage_key_suspensions`；解除本安装且仍在当前范围内的记录，返回暂停记录 |
| `ctx.keySuspensions.list(filter)` | `manage_key_suspensions`；返回本安装在当前范围内的暂停数组 |
| `ctx.notifications.enqueue(input)` | `webhook`；返回 `{ id, eventId, status }` |
| `ctx.kv.getEntry(key, { namespace }?)` | 返回带 `revision` 的条目，缺失或过期为 `null` |
| `ctx.kv.compareAndSet(key, expectedRevision, value, { namespace, ttlSeconds }?)` | 返回 `{ swapped, entry }`；冲突时 `swapped: false, entry: null` |

### 缓存查询选项

宿主只查询已完成的 API 来源请求，默认排除 Playground 与渠道测试。JavaScript 和 GraphQL 选项均未开放 `sources` 字段。已删除 Key 不参与查询。首次完成请求时，宿主在同一状态更新语句中使用数据库时钟写入固定的 `Request.completed_at`；后续元数据或附件下载更新保持此时间。每个 Key 按该完成时间、`Request.id` 倒序取样。

旧请求的 `completed_at` 为 `null` 时，使用固定 `Request.created_at` 作为窗口及排序位置，并强制计为 unknown，占据请求数量与最近 N 次的样本位置，排除可信 Token 汇总；历史数据保留该状态。公开样本字段仍叫 `createdAt`，含义为固定完成观察时间，旧请求则返回其起始时间并标记 unknown。

| 字段 | 类型与限制 |
| --- | --- |
| `projectId` | 必填正整数，一次查询一个项目 |
| `keyIds` / `allKeys` | 非空正整数数组或 `true`，二选一；`allKeys` 自动与 Owner 范围求交 |
| `excludeKeyIds` | 正整数数组，要求 `allKeys: true`；宿主同时应用 Owner 排除项 |
| `afterKeyId` / `limit` | 非负整数游标；每页 1–100 个 Key，省略或 `0` 默认 100 |
| `end` | RFC 3339 时间字符串，包含结束边界；省略时宿主捕获数据库 UTC 时间，显式时区统一归一为 UTC |
| `windowSeconds` | 0–7776000 整数，正数限制最大样本年龄，窗口两端包含边界 |
| `lastRequests` | 0–1000 整数，正数时每个 Key 各取最近 N 次；与正数 `windowSeconds` 至少提供一项 |
| `minSamples` | 1–1000 整数；省略或 `0` 默认 `lastRequests`，纯时间窗口默认 1；不得超过正数 `lastRequests` |
| `lowThreshold` | 有限数值 0–1；提供时统计最新连续低命中，判定为严格小于此值 |
| `includeSamples` | boolean，默认 `false`；明细最多每个 Key 1000 条 |
| `modelIds` / `channelIds` | 模型字符串数组最多 100 项，每项 1–256 字节；渠道正整数数组 |

`keyIds`、`excludeKeyIds`、`channelIds` 每个列表最多 1000 个 ID。时间窗口和最近 N 次同时设置时，宿主先限制最大年龄，再在该范围内为每个 Key 取最近 N 次，形成交集；两个独立条件的 AND/OR 规则应分别查询，再由插件组合。

模型和渠道筛选优先使用最终结算日志中的元数据；缺失结算日志时使用请求记录中的模型和渠道，同时保留 unknown 状态。

```js
const options = {
  projectId: 7,
  keyIds: [101, 102],
  windowSeconds: 3600,
  lastRequests: 50,
  minSamples: 50,
  lowThreshold: 0.2,
  includeSamples: true,
  limit: 100,
};
const page = ctx.usage.cacheStats(options);
// 下一页保持其他筛选不变，并复用 page.end。
if (page.nextAfterKeyId) {
  const next = ctx.usage.cacheStats({
    ...options, end: page.end, afterKeyId: page.nextAfterKeyId,
  });
  // 有界处理 next.items；末页 nextAfterKeyId 为缺省或 0。
}
```

每次调用在一个数据库快照内计算授权和统计。分页复用首个响应的 `end`，固定查询时间上界；多次调用各自建立快照，范围撤销、Key 变动及并发用量写入仍可能影响后续页面。固定完成时间使后续元数据与附件处理保持原有窗口归属。

### 统计结果与动作门槛

每个 `items[]` 返回 `keyId`、`keyName`、`projectId`、`latestRequestId`、`latestUsageId`、`count`、`validCount`、`unknownCount`、`invalidCount`、`promptTokens`、`cachedTokens`、`cacheRate`、`enoughSamples`、`sampleComplete`、`trailingLowCount`、`trailingTruncated`，以及按需返回的 `samples`、`samplesTruncated`。无用量记录的最新请求仍有 `latestRequestId`，其 `latestUsageId` 为 0；无样本时两个标识均为 0。

有效样本要求缓存读取已明确报告、输入 Token 大于 0，且缓存 Token 在 0 与输入 Token 之间。`promptTokens` 包含缓存读取 Token；汇总只累计有效样本，`cacheRate = sum(cachedTokens) / sum(promptTokens)`，输入总量为 0 时返回 `null`。多个请求之间按 Token 加权，插件应直接使用此汇总，避免对单请求比例取算术平均。

`count` 包含选定范围内的未知与无效请求。上游明确报告 0 命中是有效样本；缺失 usage、未明确报告缓存读取、历史未知状态均计入 `unknownCount`。已报告但输入为 0、缓存 Token 为负或超过输入的记录计入 `invalidCount`。最近 N 次先选请求再判断有效性，未知和无效记录都保留在这 N 次中，并打断连续低命中。

`enoughSamples` 表示 `validCount >= minSamples`。`sampleComplete` 还要求 unknown 与 invalid 均为 0，最近 N 模式必须取满 N 条。有效覆盖率由插件计算 `validCount / count`，空集合按不可执行处理；采用保守规则时同时要求 `sampleComplete`、有效样本数、输入 Token 下限和覆盖率门槛。时间窗口汇总覆盖整个窗口，明细与连续低命中扫描最多 1000 条；`samplesTruncated`、`trailingTruncated` 标明截断。`trailingLowCount` 从最新请求向前计数，遇到比例达标、unknown 或 invalid 即停止。

样本为 `{ requestId, createdAt, status, promptTokens, cachedTokens, cacheRate }`，`status` 为 `reported` 或 `unknown`；`reported` 的样本仍须通过数值有效性检查，`cacheRate: null` 表示无法计算。宿主在返回统计前拒绝绝对值超过 `9007199254740991` 的整型计数及 Token 总量，插件应缩短时间窗口、减少最近请求数或收窄筛选后重试。异常与数据库失败均应停止自动动作，避免把失败当成零命中。GraphQL 的统计计数类型为 `MetricCount`，客户端同样需要保留整数精度。

### 暂停与通知

`apply` 输入含 `apiKeyId`、`projectId`、`idempotencyKey`、`reason`、`evidence`，并在 `durationSeconds` 与 `permanent` 中选择暂停方式。临时暂停要求 `durationSeconds` 为 1–31536000 秒且 `permanent: false`；永久暂停显式设置 `permanent: true` 并省略时长或设为 0。`reason` 为非空 UTF-8，最多 512 字节；幂等键为 1–256 字节 UTF-8；`evidence` 为 JSON 对象，最多 16 KiB，省略时为 `{}`。项目、Key 和暂停 ID 均使用正整数。

```js
// ruleVersion、item 来自已校验的插件规则与可信统计。
const idempotencyKey = `${ruleVersion}:key:${item.keyId}:request:${item.latestRequestId}`;
const hold = ctx.keySuspensions.apply({
  apiKeyId: item.keyId,
  projectId: item.projectId,
  idempotencyKey,
  reason: "cache rule matched after complete sample validation",
  durationSeconds: 900,
  permanent: false,
  evidence: {
    ruleVersion,
    latestRequestId: item.latestRequestId,
    end: page.end,
    validCount: item.validCount,
    promptTokens: item.promptTokens,
    cacheRate: item.cacheRate,
  },
  notification: {
    targetName: "ops-alerts",
    payload: { message: "API key temporarily suspended", apiKeyId: item.keyId },
  },
});
```

此片段应放在已通过门槛和新样本检查的动作分支。`ops-alerts` 必须是宿主已配置且启用的 Webhook 目标，插件还需 `webhook` 授权。`notification` 可省略；随 `apply` 提交时，宿主绑定通知的 `suspensionId`、`evidence` 和 `idempotencyKey`，忽略调用方对这三个绑定字段的选择。暂停创建与可选通知入队同事务提交，入队失败会回滚此次新建暂停。

JavaScript 与 GraphQL 的暂停附带通知均只需 `targetName` 和 `payload`。GraphQL 使用 `PluginKeySuspensionNotificationInput`，关联和幂等字段由宿主生成。

独立通知调用形如 `ctx.notifications.enqueue({ targetName, payload, idempotencyKey, suspensionId?, evidence? })`。目标名为非空 UTF-8，最多 128 字节；幂等键为非空白 UTF-8，最多 256 字节；payload 必须为 JSON 对象。payload、合并宿主字段后的数据和渲染后的正文各受 64 KiB 限制，关联暂停必须属于当前安装且仍在范围内。相同安装和幂等键返回首次入队记录。宿主保留 `eventId`、`idempotencyKey`、`installationId`、`suspensionId`、`evidence` 字段，并用宿主值覆盖 payload 中的同名项。

队列状态为 `pending`、`delivered`、`failed` 或 `cancelled`，提供持久重试与租约；HTTP 至少一次投递，接收端按稳定 `eventId` 去重，Webhook 模板应将该字段传递给接收端。HTTP 在数据库事务之外执行，后续投递失败保留失败状态，已生效暂停继续有效。宿主读取当前目标配置，插件通过目标名使用既有 URL、凭证和请求头。

`list` 的 filter 字段为 `projectId`、`apiKeyId`、`activeOnly`、`limit`、`beforeId`；JavaScript 中省略或 0 的项目/Key 表示在当前授权内跨该维度查询，GraphQL 要求提供 `projectId`。结果按暂停 ID 降序，`limit` 默认 100，非正值或大于 100 时也按 100；下一页使用最后一条记录 ID 作为 `beforeId`。`apply`、`release` 和列表元素包含 `id`、`createdAt`、`updatedAt`、`installationId`、`pluginId`、`apiKeyId`、`projectId`、`reason`、`evidence`、`createdBy`、`expiresAt`、`releasedAt`、`releasedBy`、`releaseReason`。时间为 RFC 3339；永久暂停的 `expiresAt` 为 `null`，未解除时 `releasedAt` 为 `null`。

### CAS 与恢复后的新样本

持久状态使用 `ctx.kv.getEntry` 和 `ctx.kv.compareAndSet`。条目包含 `{ namespace, key, value, revision, updatedAt, expiresAt }`。`expectedRevision: null` 仅创建缺失或已过期键；更新时原样回传读取到的 revision，包括旧条目可能携带的空字符串。revision 是不透明令牌，每次写入及删除重建都会变化。CAS 冲突返回 `{ swapped: false, entry: null }`，调用方重新读取并在有限次数内重算。

```js
const key = `state.key.${item.keyId}`;
const current = ctx.kv.getEntry(key);
const state = current ? JSON.parse(current.value) : {};
// 按最新暂停记录与完成时间更新检查点；CAS 失败后重新读取并重算。
const result = ctx.kv.compareAndSet(
  key,
  current ? current.revision : null,
  JSON.stringify({ ...state, latestObservedRequestId: item.latestRequestId }),
);
if (!result.swapped) return; // 交由下一次检查重新读取。
```

KV value 为 UTF-8 字符串，最多 256 KiB；namespace 默认为 `default`，namespace/key 匹配 `^[A-Za-z0-9_.:-]{1,128}$`。`ttlSeconds` 为 0 或省略表示不过期，正整数上限 7776000 秒。一次宿主执行最多写入 10 次，同一插件每分钟最多 60 次；CAS 仍受写入预算约束。

业务状态应持久保存规则版本、已处理的最新请求、暂停 ID 和恢复观察边界。临时暂停到期或管理员解除后，先查询本安装历史暂停并与 KV 对账，再等待足够的恢复后新样本。按完成时间判断样本是否晚于到期或解除时间，结合 request ID 去重；请求 ID 单独承担样本身份，完成顺序仍由完成时间决定。旧窗口即使仍然低命中，也应等待新的完整证据后再处罚。

幂等键采用规则版本、Key ID 和 `latestRequestId`，保证同一观测在跨周期重试中稳定；单独使用 `runId` 会在下个周期生成新处罚。相同安装下重放既有幂等键返回原暂停，保留原到期、解除和证据状态；若目标项目或 Key 不同则报冲突。CAS 可协调状态竞争，`apply` 提供动作幂等；多个独立 host call 之间不承诺单一原子事务，崩溃后须按暂停历史和持久状态恢复，不能依赖“写 KV 后调用 apply”整体原子提交。

## 管理员解除与鉴权行为

暂停叠加于 Key 的人工状态。Key 自身启用且所有暂停均已解除或到期时才可使用；解除某一记录保留管理员禁用、归档与其他来源暂停。暂停约束后续新请求，已通过鉴权的在途请求继续完成。门禁每次检查权威数据库，数据库故障返回服务错误。临时暂停可提供 `Retry-After`；客户端的展示由调用方决定。

插件停用、撤权或卸载后无法继续调用自身管理接口，永久暂停仍然阻断 Key，临时暂停按原到期时间自然失效。直接管理员使用宿主 `apiKeySuspensions`（服务方法 `AdminList`）及 `releaseAPIKeySuspension`（`AdminRelease`）处理遗留记录：

```graphql
query InspectHolds($projectId: Int!) {
  apiKeySuspensions(filter: { projectId: $projectId, activeOnly: true, limit: 100 }) {
    id pluginId installationId apiKeyId reason expiresAt releasedAt
  }
}

mutation ReleaseHold($id: Int!, $reason: String!) {
  releaseAPIKeySuspension(id: $id, reason: $reason) {
    id apiKeyId releasedAt releasedBy releaseReason
  }
}
```

系统 Owner 可直接管理；其他直接登录管理员需要对应系统权限或匹配项目上下文内的 `read_api_keys` / `write_api_keys` 权限。管理员解除原因须非空、有效 UTF-8 且最多 512 字节。查询使用正整数项目 ID，解除使用列表返回的正整数暂停 ID。管理员通道独立于插件安装状态，适用于停用、卸载和授权撤销后的人工恢复。

上线前先使用只记录模式核对覆盖率和阈值，再启用动作。验证至少覆盖明确零命中、unknown/invalid、缺失 usage、每个 Key 独立最近 N 次、时间交集、整数溢出、CAS 冲突、重复调度、通知重投、恢复后新样本、授权撤销与卸载后的永久暂停解除。
