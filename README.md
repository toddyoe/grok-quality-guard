# Grok Quality Guard

> 面向 CLIProxyAPI v8 的 Grok / xAI 账号质量守卫插件。

Grok Quality Guard 观测 Grok / xAI auth 的真实响应质量，识别 HTTP 200 但实际降智的输出，并提供账号级状态、冷却、恢复探测和可选的透明换号保护。质量异常默认只读观测；上游 401 凭据失效和明确额度耗尽仍会直接禁用或冷却账号。

设计细节和完整状态机见 [插件设计文档](docs/grok-quality-guard-plugin-design.md)。

## 功能

- 被动观测 `passive_guard`：通过 response、stream chunk 和 usage 分析已有请求，不改变路由
- 可选透明保护 `transparent_guard`：在首次向客户端 emit 前完成质量门，异常时关闭上游流并换号
- 真实 thinking 证据判定，不使用空 reasoning stub 或单独的 `reasoning_tokens` 判断健康
- 检测 `missing_thinking`、`buffered_burst`、`cipher_drool`、`fake_encrypted_dump`、`fast_reasoning_ratio_dump`、`soft_tps`、`hard_tps`、`empty_stream`、`probe_error`、`proxy_error`
- 账号状态机：`unknown`、`healthy`、`suspect`、`degraded`、`cooling`、`quota_exhausted`、`credential_invalid`、`disabled_by_guard`、`externally_disabled`
- 首次确认异常可配置为只观察或临时冷却，冷却到期通过固定 prompt 恢复探测
- Resin 账号级租约绑定、显式轮换、批量绑定和代理清除；无 API 时可仅本地改写已有代理用户名
- 内嵌中文化管理页，按概览、账号质量、Resin 租约、事件日志和设置分 Tab，账号、租约与事件列表支持分页
- 账号详情展示最近尝试与脱敏质量判定依据，明确区分非质量错误和上一次真实质量结论
- auth、proxy URL、Admin Token、xAI token 全程脱敏

## 质量判定要点

真实思考证据只有两类：

1. 非空 reasoning / thinking delta
2. encrypted reasoning 内容达到下限

密文下限固定为：

```text
max(min_encrypted_bytes, reasoning_tokens * encrypted_bytes_per_reasoning_token)
默认 max(256 bytes, reasoning_tokens * 4 bytes)
```

以下内容不作为思考证据：

- 空 reasoning item
- `: grok2api-reasoning-start` timing stub
- 仅报告 `usage.reasoning_tokens`
- 未达到密文下限的 encrypted stub

TPS 使用总 output tokens，其中包含 reasoning tokens：

```text
generationWindow = duration - firstToken
TPS = outputTokens * 1000 / generationWindow
```

当 `reasoningTokens > 0`、`generationWindow < firstToken` 且 `generationWindow < 1000ms` 时，使用完整 `duration` 修正生成窗口。

### 与参考实现的对齐

流式判定按 `references/grok2api` 的 `ClassifyQualityHold` 与 `egress-quality-guard` 的审计逻辑实现：

- dump 检测优先于“已有 thinking 就放行”，顺序为 `buffered_burst`、`cipher_drool`、`fake_encrypted_dump`、`fast_reasoning_ratio_dump`
- `hold_expired` 后的短 visible 文本只有在 `reasoning_tokens >= 80` 时按 burst 处理
- `fake_encrypted_dump` 与 `fast_reasoning_ratio_dump` 只在首个可见块后 2 秒内触发
- 聚合 message 文本计入 visible token，但不伪造流式首个可见块的 flush 时间
- `: grok2api-reasoning-evidence <bytes>` 作为密文字节数证据，仍必须满足密文下限
- Chat、Responses 和 Anthropic Messages 的流式与非流式响应都使用同一套内容、thinking、密文下限、TPS 和空流规则

被动审计只做可确认的样本速度审计：非流式、缺少 TTFT、输出不足 32 token 或生成窗口无效的样本记录为 `ignored`，不推断 `missing_thinking`。主动探测使用固定 marker，并在输出达到 64 token 且要求思考时检查 thinking 账单。非流式响应不做流式 burst/dump 推断，但仍检查空响应、缺少真实思考和可用的 TPS 证据。

### 非质量错误与证据保留

`ignored` 表示本次尝试不是质量信号，例如上游失败、非 Grok 请求、非流式被动样本、缺少 TTFT、输出不足或传输错误。它不会增加质量 strike，也不会覆盖上一次真实质量判定。

`probe_error` / `proxy_error` 会记录为最近一次尝试并参与错误退避，但不会把上一次质量指标清空。管理页的“最近尝试”和“最近质量判定”分开显示，因此可以直接看出某次 `ignored` 或探测错误是否只是噪声。

空流表示上游已经终止，但没有可见内容、真实思考证据、usage 输出或语义输出。空流使用独立的 15 分钟冷却，不会自动升级成 missing-thinking 永久禁用。

## 环境要求

- CLIProxyAPI v8，支持 plugin ABI
- Go 1.26.10 或与 `go.mod` 匹配的更新版本
- CGO 可用的 C 编译器
  - Windows：MinGW GCC
  - Linux：GCC
  - macOS：Clang
- 可选：Resin Admin API 和代理环境；仅本地轮换不需要 Resin API 或 Key

项目通过 Go module 引入 CLIProxyAPI v8，仅使用其中的 `sdk/pluginabi` 和 `sdk/pluginapi` 作为协议类型。

## 安装

### 在线安装

CPA 插件商店添加本仓库作为第三方源，之后在管理面板的插件商店中直接安装：

```yaml
plugins:
  enabled: true
  store-sources:
    - "https://raw.githubusercontent.com/toddyoe/grok-quality-guard/main/registry.json"
```

支持 `windows/amd64`、`linux/amd64`、`linux/arm64`、`darwin/amd64`、`darwin/arm64` 五个平台。Release 安装包同时提供 `checksums.txt`，安装前可进行 SHA-256 校验。

### 从 GitHub Release 安装

Release 资产使用 CPA 插件商店要求的命名：

```text
grok-quality-guard_<version>_<goos>_<goarch>.zip
```

将压缩包中的动态库安装到 CPA 插件目录：

```text
plugins/<goos>/<goarch>/grok-quality-guard-v<version>.<ext>
```

也可以直接放在 `plugins/` 根目录；CPA 会优先查找当前平台的子目录。支持的后缀：

- Windows：`.dll`
- Linux：`.so`
- macOS：`.dylib`

然后在 CPA 配置中启用插件并填写配置。

### 从源码构建

Windows：

```powershell
$env:CGO_ENABLED = '1'
go build -buildmode=c-shared -trimpath -buildvcs=false `
  -ldflags "-X main.pluginVersion=0.1.0" `
  -o bin/grok-quality-guard.dll .
```

Linux：

```bash
CGO_ENABLED=1 go build -buildmode=c-shared -trimpath -buildvcs=false \
  -ldflags "-X main.pluginVersion=0.1.0" \
  -o bin/grok-quality-guard.so .
```

macOS：

```bash
CGO_ENABLED=1 go build -buildmode=c-shared -trimpath -buildvcs=false \
  -ldflags "-X main.pluginVersion=0.1.0" \
  -o bin/grok-quality-guard.dylib .
```

版本号不要带 `v` 前缀。GitHub Actions 发布时会把 tag 版本自动注入插件元数据。

## 配置

```yaml
plugins:
  enabled: true
  dir: "plugins"
  configs:
    grok-quality-guard:
      enabled: true
      priority: 10

      # 质量异常默认只观测；401/额度耗尽仍直接处置
      passive_guard: true
      transparent_guard: false
      auto_action: observe
      model_prefixes:
        - "grok-"

      quality:
        soft_tps: 500
        hard_tps: 1000
        consecutive_soft: 2
        consecutive_errors: 2
        consecutive_missing_thinking: 1
        thinking_guard: true
        soft_cross_verify: false
        thinking_cross_verify: false
        min_generation_ms: 1000
        fail_closed: false
        min_passive_output_tokens: 32
        min_hold_output_tokens: 8
        hold_timeout: "30s"
        hold_max_buffer_bytes: 4194304
        min_encrypted_bytes: 256
        encrypted_bytes_per_reasoning_token: 4
        missing_thinking_cooldown: "12h"
        empty_stream_cooldown: "15m"
        max_attempts: 6
        on_exhausted: "fail_closed"

      probe:
        recovery_only: true
        model: "grok-4.5"
        timeout: "90s"
        max_output_tokens: 768
        concurrency: 1
        queue_size: 10000
        unknown_delay: "30s"
        unknown_jitter: "30s"
        suspect_backoff: "15m"
        max_backoff: "1h"

      resin:
        enabled: false
        local_only: false
        platform: "Default"
        admin_url: ""
        admin_token_env: "RESIN_ADMIN_TOKEN"
        proxy_token_env: "RESIN_PROXY_TOKEN"
        proxy_template: "socks5h://{platform}.{account}:{proxy_token}@resin:2260"
        auto_rotate_after_quality_failure: true

      state_file: "/CLIProxyAPI/plugin-data/grok-quality-guard/state.json"
      events_limit: 1000
```

### 自动处置

- `auto_action: observe`：质量异常只记录事件和状态，不修改 auth，不自动轮换 Resin；上游 401 凭据拒绝和明确的额度耗尽仍会直接执行账号处置
- `auto_action: cooldown_only`：首次确认异常时写入临时 `disabled=true`；冷却到期后执行恢复探测，仍异常则重新进入冷却，永不自动永久禁用
- `auto_action: cooldown_then_disable`：首次确认异常时写入临时 `disabled=true`，冷却到期后执行恢复探测；再次确认 missing-thinking 时永久禁用

插件只操作自己写入的 guard-owned 状态，不自动恢复管理员在插件外禁用的 auth。

连续阈值与旧 grok2api/egress guard 策略对齐：

- `consecutive_soft`：连续软 TPS 样本达到次数后才进入处置；默认 `2`
- `consecutive_errors`：连续传输/代理错误达到次数后才进入处置；上游 5xx、普通限流和无法确认的 403/429 只记录为 `ignored`，不计入 strike；默认 `2`
- 上游 `401` 标记为 `credential_invalid`，写入 `disabled=true` 并等待管理员更新凭据后手动恢复
- 上游 `402`、包含 free usage/quota/credits 等明确额度耗尽文案的 `403/429` 标记为 `quota_exhausted`，写入 24 小时冷却；到期后自动恢复探测
- `consecutive_missing_thinking`：连续缺少真实思考达到次数后才进入处置；默认 `1`
- `thinking_guard=false`：跳过缺少思考判定，只保留 TPS、空流等其他质量检查
- `fail_closed=true`：生成窗口小于 `min_generation_ms` 且速度达到 `soft_tps` 时判定为 `buffered_burst`；窗口不足但速度不高时判定为 `insufficient_generation_window`
- `ignored`、`probe_error`、`proxy_error` 不覆盖上一次真实质量指标；详情页会分别展示最近尝试和最近质量证据
- `soft_cross_verify=true` 或 `thinking_cross_verify=true`：被动样本达到阈值后先启动后台主动探测，由探测结果确认后再处置；默认关闭
- 任一健康样本会清零 `soft_strikes`、`error_strikes`、`thinking_strikes`，与旧插件的连续计数语义一致

主动探测调度会先过滤出当前到期的非健康账号，再放入有界队列。`probe.queue_size` 默认 `10000`，最大 `1000000`；`probe.concurrency` 控制探测以及批量账号操作实际使用的 worker 数。`probe.unknown_jitter` 为 `unknown` 账号的首次探测增加稳定抖动，避免大量账号在同一时刻到期。

### 透明保护

`transparent_guard: true` 时，插件只保护可安全重放的普通文本 chat / responses 请求。以下请求不会被透明接管：

- 图片、音频、视频等媒体请求
- `previous_response_id` 或 stored response
- compaction 请求
- 可能产生副作用的 hosted tools
- 禁用推理的请求
- 无法安全重放到其他账号的请求

透明模式在首次向客户端 emit 前完成质量门。一旦开始 emit，插件不能回滚已经发送的内容；如果所有候选账号耗尽，默认返回 `503 quality_degraded`。可配置 `on_exhausted: fail_open` 在失败时返回最后一个已缓冲响应，但这会牺牲 fail-closed 语义。

## Resin 与本地代理轮换

代理轮换支持两种模式。

### API 模式

API 模式用于创建新的代理绑定，并管理 Resin 远端租约。Admin API URL、Platform、Admin Token、Proxy Token 和代理模板在管理页中配置。用户只填写平台名称，保存时会按顺序验证：

1. 使用 Admin Token 请求 Resin `GET /api/v1/system/info`
2. 使用同一 Admin Token 请求 `GET /api/v1/platforms?keyword={platform}`，按名称精确解析平台 UUID 并读取规范平台名称
3. 按代理模板渲染验证账号，并执行 SOCKS5 RFC1929 用户名/密码认证，确认 Proxy Token 有效

任一步失败都不会保存配置。已保存的 token 不会回填到页面，也不会通过管理页或 JSON API 返回；再次保存时留空 Secret 输入框会保留已有值。YAML 中的 `admin_token_env` 和 `proxy_token_env` 仅作为 state 尚未配置时的环境变量回退来源。

Resin 平台名称在 Resin 中唯一。管理页只要求填写名称，保存时通过平台列表接口解析出 `platform_id` 并写入状态文件；管理员不需要手工填写 UUID。绑定和轮换运行时仍会按内部 `platform_id` 重新查询规范名称，并让 DELETE 租约和代理模板使用同一次解析结果。

Resin 绑定必须由管理员显式触发，不会在首次启动时自动覆盖已有 `proxy_url`。API 模式轮换顺序固定为：

1. 删除旧 Resin 租约，`204` 和 `404` 都视为成功
2. 生成新的 `grok-<email-local>-<4位随机 base36>` account
3. 写回 auth 的 `proxy_url`
4. 保留 access/refresh token 等所有非代理字段

API 模式清除 `proxy_url` 时，插件管理的 Resin 租约会先调用 `DELETE /api/v1/platforms/{platform_id}/leases/{account}`，`204` 或 `404` 后才会清空 auth 中的 `proxy_url`。如果旧 state 缺少 Resin 租约字段，但 auth 代理端点与已保存的 Resin 代理模板一致，插件会从代理用户名反推租约 account，仍执行远端删除。

### 仅本地轮换

仅本地轮换用于只修改 auth 中已有代理、但不管理 Resin 远端租约的场景。它不需要 Admin API URL、Platform ID、Admin Token、Proxy Token 或代理模板：

```yaml
resin:
  enabled: true
  local_only: true
```

本地轮换要求 auth 中已有代理包含用户名和密码，且用户名最后一个 `.` 后是当前 account。例如：

```text
socks5h://Default.grok-alice-old1:proxy-secret@resin:2260
```

轮换时只替换用户名中的 account 后缀，生成新的 `grok-<email-local>-<4位随机 base36>`，同时保留 scheme、代理主机、端口、密码和路径等其余部分：

```text
socks5h://Default.grok-alice-new1:proxy-secret@resin:2260
```

本地模式不会调用任何 Resin API，也不会删除远端旧租约。本地清除只移除 auth 中的 `proxy_url`，同样不会处理远端租约。由于没有 API 可用，本地模式不能创建新的代理绑定；首次绑定仍必须使用 API 模式。

Resin 没有“立即创建指定出口 IP 租约”的 API，插件不查询新 egress IP，也不保证轮换后一定使用不同出口 IP。批量绑定、绑定全部未绑定和批量清除均作为异步任务执行，内部使用有界工作队列，队列容量与 worker 数沿用 `probe.queue_size` 和 `probe.concurrency`。

## 管理页与 API

管理页：

```text
/v0/resource/plugins/grok-quality-guard/dashboard
```

Action API：

```text
POST /v0/management/plugins/grok-quality-guard/api
Content-Type: application/json
```

API 只使用请求体中的 `action` 和 `params`，不依赖自定义 HTTP Header，管理认证由 CPA management 层完成。

请求示例：

```bash
curl -X POST \
  "http://127.0.0.1:8317/v0/management/plugins/grok-quality-guard/api" \
  -H "Authorization: Bearer ${CLIPROXY_MANAGEMENT_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"action":"accounts.list","params":{"page":1,"page_size":50}}'
```

可用 action：

| Action | 说明 |
| --- | --- |
| `overview` | 汇总账号、状态、Resin 和最近异常 |
| `accounts.list` | 分页账号列表 |
| `accounts.get` | 账号详情 |
| `accounts.probe` | 提交异步固定 prompt 探测任务 |
| `accounts.probe_selected` | 检测选中的账号；需要恢复时在同一任务内先恢复再检测 |
| `accounts.cooldown` | 手动冷却 |
| `accounts.disable` | 手动永久禁用 |
| `accounts.restore` | 恢复 guard-owned 账号并清除 strike |
| `accounts.restore_selected` | 恢复选中的账号 |
| `resin.bind` | 提交异步绑定任务；仅 API 模式可用 |
| `resin.bind_unbound` | 提交异步批量绑定任务 |
| `resin.rotate` | 提交异步轮换任务；本地模式只改写已有代理用户名 |
| `resin.clear` | 提交异步清除任务；API 模式先删除管理租约，本地模式不处理远端租约 |
| `resin.config.get` | 查看脱敏的 Resin 配置与 Secret 是否已设置 |
| `resin.config.validate` | 提交异步 Resin 配置验证任务 |
| `resin.config.save` | 提交异步验证并保存任务 |
| `policy.get` | 查看运行时策略 |
| `policy.update` | 校验并持久化策略 |
| `policy.reset` | 重置为 YAML 初始值 |
| `state_file.update` | 迁移并切换状态文件路径；目标文件必须不存在 |
| `events.list` | 查看质量事件日志 |
| `tasks.get` | 查询异步任务状态、结果和错误 |
| `tasks.list` | 列出最近异步任务 |

主动探测、Resin 配置验证/保存、Resin 绑定/清除/轮换和批量绑定都是长耗时操作，会立即返回 `202 Accepted` 与 `task_id`，不会占住 oh-my-cpa 的 15 秒管理请求超时。调用方应轮询：

```json
{"action":"tasks.get","params":{"task_id":"<task_id>"}}
```

任务状态为 `queued`、`running`、`succeeded` 或 `failed`。失败时 `error` 包含脱敏后的原因，`error_status` 保留可用的上游 HTTP 状态。内嵌管理页已经自动轮询并展示进度。

API 只接受 POST，并由 CPA management 层完成认证。插件不要求自定义 Header 或请求体 UI 标记，避免管理网关剥离 Header 时页面请求失败。响应中的 email、proxy URL、token 和密码均会脱敏；账号列表同时提供 `auth_id_display` 供展示，操作 API 使用稳定的 `auth_id` 作为标识。`proxy_url` 可能内嵌 Proxy Token，因此管理页只显示 `scheme://host`，不会展示完整凭据。

## 状态文件与 Docker 持久化

默认路径按顺序选择第一个可写位置：

```text
/CLIProxyAPI/plugin-data/grok-quality-guard/state.json
plugin-data/grok-quality-guard/state.json
<UserConfigDir>/cliproxyapi/grok-quality-guard/state.json
<temp>/grok-quality-guard/state.json
```

Docker 部署必须把第一项所在目录挂载为宿主持久卷，否则容器重建后策略、账号状态、事件、Resin 配置和凭据仍会丢失：

```yaml
services:
  cliproxyapi:
    volumes:
      - ./plugin-data/grok-quality-guard:/CLIProxyAPI/plugin-data/grok-quality-guard
```

如果已经挂载了 `/CLIProxyAPI/plugins`，也可以显式把 `state_file` 指向该持久目录下的文件。显式配置的 `state_file` 是启动引导路径；路径不可写时插件会报错，而不是静默改写到临时目录。

管理页“设置 / 守护策略”中的状态文件路径可以直接编辑。保存新路径时，插件会把当前完整 state 写入目标文件并立即切换；为避免覆盖已有数据，目标文件必须尚不存在。引导路径旁会生成 `<state_file>.location` 指针文件，使插件重启后继续使用新路径。修改 YAML 中的 `state_file` 会产生新的引导位置，因此可以借此重新指定路径；如要回退到旧引导文件，需要先删除对应指针文件。

状态文件使用版本化 schema、原子替换写入，Unix 权限为目录 `0700`、文件 `0600`。策略首次从 YAML 初始化，之后管理页修改会持久化到 state；`policy.reset` 会恢复为当前 YAML 的默认值。通过管理页保存的 Resin 配置和 API 模式 token 也存储在 state 中，API 与页面只返回是否已设置的布尔标记。管理页设置区会显示当前实际使用的状态文件路径。

插件运行时只会修改 auth 文件中的原生字段 `disabled` 和 `proxy_url`。冷却原因、strike、禁用所有权、Resin 租约及其他控制信息全部保存在 `state.json`，不再向 auth 写入 `grok_quality_guard` 或其他插件私有字段。

从旧版本升级时，应先停止插件，再使用迁移脚本把历史 auth 元数据合并进 state，并清理 auth 文件：

```powershell
python scripts/migrate_auth_metadata_to_state.py /path/to/auths /path/to/state.json --dry-run
python scripts/migrate_auth_metadata_to_state.py /path/to/auths /path/to/state.json
```

脚本会先原子写入 state，再清理 auth 中的 `grok_quality_guard`，可以重复执行。迁移完成后重新启动插件。

## 开发与测试

```bash
gofmt -l ./*.go
go test ./...
go test -race ./...
go vet ./...
go build -buildmode=c-shared -o bin/grok-quality-guard.dll .
git diff --check
```

## 发布

仓库提供 `.github/workflows/release.yaml`。推送 `v*` tag 会：

1. 校验 tag 和代码格式
2. 运行测试与 vet
3. 构建 Windows、Linux、macOS 的 amd64/arm64 动态库
4. 生成 CPA 插件商店兼容的 zip 和 `checksums.txt`
5. 创建 GitHub Release
6. 尝试更新默认分支的 `registry.json`

示例：

```powershell
git tag v0.1.0
git push origin v0.1.0
```

## 已知边界

- streaming 开始后无法回滚，也无法在已经 emit 后换号
- stored responses、compaction、媒体输入和有副作用的 hosted tools 不属于透明重放范围
- CPA 不能直接以固定 AuthID 探测 disabled auth，恢复探测存在短暂重新启用窗口
- Resin 删除租约后不保证换到不同出口 IP；本地轮换不会删除远端租约
- 不实现 OAuth 登录或账号导入导出
