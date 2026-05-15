# MiniMax Token Plan + Codex CLI 集成

## 任务目标

实现 Codex CLI 通过 cc-switch 代理使用 MiniMax Token Plan API。主要工作是实现 **Chat Completions SSE** 到 **Responses API SSE** 的流式格式转换。

## 修改的文件

### 核心新增/修改

| 文件                                                             | 修改类型 | 说明                                            |
| ---------------------------------------------------------------- | -------- | ----------------------------------------------- |
| `src-tauri/src/proxy/providers/streaming_chat_to_responses.rs` | 新增     | Chat Completions SSE → Responses API SSE 转换  |
| `src-tauri/src/proxy/providers/streaming_responses_to_chat.rs` | 修改     | Responses API → Chat Completions 转换          |
| `src-tauri/src/proxy/providers/transform_chat_to_responses.rs` | 新增     | Chat Completions 请求 → Responses API 请求转换 |
| `src-tauri/src/proxy/providers/mod.rs`                         | 修改     | 导出新模块                                      |
| `src-tauri/src/services/stream_check.rs`                       | 修改     | 添加 MiniMax 特定检查                           |
| `src-tauri/src/proxy/providers/adapter.rs`                     | 修改     | 适配器支持                                      |

### 前端配置

| 文件                                                | 修改类型 | 说明               |
| --------------------------------------------------- | -------- | ------------------ |
| `src/config/codexProviderPresets.ts`              | 新增     | MiniMax Codex 预设 |
| `src/types.ts`                                    | 修改     | 类型定义           |
| `src/components/providers/forms/ProviderForm.tsx` | 修改     | 表单验证逻辑       |

## 遇到的问题及解决方案

### 问题 1：Codex "OutputTextDelta without active item" 错误

**原因**：转换器直接发送 `response.output_text.delta`，但没有先发送 `response.output_item.added` 建立输出项上下文。

**解决**：在发送文本 delta 前，先发送 `response.output_item.added` 事件（type: message），建立输出项后再发送 delta。

```rust
// 先发送 output_item.added
yield Ok(Bytes::from(format!(
    "event: response.output_item.added\ndata: {{\\"type\":\"response.output_item.added\",\"output_index\":{},\"item\":{{\"type\":\"function_call\",\"name\":\"{}\",\"arguments\":\"\",\"call_id\":\"{}\"}}\n}}\n\n", idx, name, call_id)));


### 问题 2：思考标签出现在回复中

**原因**：MiniMax 在 Chat Completions 格式的 delta.content 中包含 <think> 标签，这些标签直接被当作文本发送。
**解决**：实现 ThinkTagParser 解析器，过滤掉思考标签，只发送纯文本。


// ThinkTagParser 会解析内容：
// "Hello<think>thinking content</think>!"
// 分离为：ThinkContentKind::Text ("Hello") + ThinkContentKind::Thinking ("thinking content")
// 只输出 ThinkContentKind::Text 部分
### 问题 3：流异常终止
**原因**：Chat Completions 流结束后缺少 [DONE] 或 finish_reason 标记。

**解决**：添加状态跟踪 saw_done_marker 和 saw_terminal_finish_reason，检测异常后发送 response.failed 事件。

MiniMax 特定 vs 通用代码
MiniMax 特定（不可复用）
文件	功能
streaming_chat_to_responses.rs	MiniMax 特定：处理思考标签过滤 + reasoning_summary_text 事件
transform_chat_to_responses.rs	MiniMax 特定：input/output 结构转换
stream_check.rs	MiniMax 特定：响应格式检查
通用（可复用）
文件	功能
streaming_responses_to_chat.rs	Responses API → Chat Completions 通用转换
streaming_responses.rs	Responses API → Anthropic 通用转换
adapter.rs	代理适配器框架
架构图

┌─────────────────────────────────────────────────────────────────┐
│                    cc-switch 代理                               │
│                                                                 │
│  Codex CLI (Responses API)                                      │
│         ↓                                                        │
│  ┌───────────────────┐                                           │
│  │  ProxyAdapter    │ ← 通用框架                                │
│  └───────────────────┘                                           │
│         ↓                                                        │
│  ┌─────────────────────────────────────────┐                     │
│  │         Provider 适配层                  │                     │
│  │  (Claude/MiniMax/Gemini/...)            │                     │
│  └─────────────────────────────────────────┘                     │
│         ↓                                                         │
│  ┌─────────────────────────────────────────┐                     │
│  │         协议转换                         │                     │
│  │  • Chat ↔ Responses (通用)               │                     │
│  │  • MiniMax 特定处理                      │                     │
│  │  • ThinkTag 过滤                        │                     │
│  └─────────────────────────────────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
配置说明
MiniMax Codex 配置

model_provider = "minimax"
model = "codex-MiniMax-M2.7"
wire_api = "responses"
base_url = "http://127.0.0.1:15721/v1"
前端端口配置
如需修改前端端口（默认 3000）：

文件	修改位置
vite.config.ts	server.port: 3010
src-tauri/tauri.conf.json	devUrl: "http://localhost:3010"
构建说明

# Debug 构建（开发用）
cd src-tauri && cargo build

# Release 构建（生成 exe）
cd src-tauri && cargo tauri build

# 清理所有构建产物
cargo clean

# 后续合并上游更新后

cd src-tauri
cargo clean              # 清理旧构建
cargo tauri build       # 重新构建
快速增量构建（不清理）
如果只是小改动，不需要 cargo clean：
cd src-tauri
cargo tauri build       # 直接增量构建
---

## 修复记录

### 修复 1: MiniMax 不支持 namespace 类型工具 (2026-05-05)

**问题描述**：
Codex 与 MiniMax 对话时报错：`invalid tool type: namespace (2013)`

**原因分析**：
Codex 发送的 Tools 中包含 `namespace` 类型的工具（如 MCP 工具），但 MiniMax 只支持 `function` 类型的工具。

在 `transform_chat_to_responses.rs` 的 `responses_to_chat_completions_request` 函数中，原代码对非 `function` 类型的工具直接原样返回，导致 `namespace` 类型的工具被传递给 MiniMax，引发 400 错误。

**修复方案**：
综合处理选项 A、B、C：

1. **选项 A（尝试转换）**：预留扩展点，未来如果 MiniMax 支持其他工具类型可在此扩展
2. **选项 B（过滤 + 警告）**：对不支持的工具类型记录警告并跳过
3. **选项 C（全部过滤则报错）**：如果所有工具都被过滤掉（请求本意是使用工具但无法使用），返回明确的错误信息而不是发送空工具列表

**修改位置**：`src-tauri/src/proxy/providers/transform_chat_to_responses.rs`

**修改内容**：
```rust
// 修复前
if tool_type != "function" {
    return Some(t.clone());  // 直接返回，导致错误
}

// 修复后
if tool_type != "function" {
    log::warn!(
        "[transform] Skipping unsupported tool type '{}' for MiniMax (only 'function' type is supported)",
        tool_type
    );
    skipped_count += 1;
    continue;  // 过滤掉
}

// ... 处理 function 类型 ...

// 选项 C: 如果所有工具都被过滤掉，返回错误
if !tools.is_empty() && chat_tools.is_empty() {
    return Err(ProxyError::TransformError(format!(
        "No supported tools: MiniMax only accepts 'function' type, but got {} tool(s) of other types",
        skipped_count
    )));
}
```

**验证方式**：

1. 重启 cc-switch
2. 在 Codex 中执行需要工具调用的复杂任务
3. 观察日志中是否还有 `invalid tool type` 错误
4. 如果工具被过滤，日志会显示：`Skipping unsupported tool type 'namespace' for MiniMax`

### 修复 2: 工具调用参数为空导致 Codex UI 停止 (2026-05-05)

**问题描述**：
Codex 在执行复杂任务时，UI 显示 "Let me explore the project structure first." 后任务停止。

**原因分析**：
`streaming_chat_to_responses.rs` 在处理 MiniMax 返回的 Chat Completions 格式工具调用时：

1. `output_item.added` 事件中发送 `"arguments":""`（空字符串）
2. `output_item.done` 事件中同样发送空字符串
3. Codex CLI 可能期望 `"arguments":"{}"`（空 JSON 对象）而非空字符串

**修复方案**：
将空字符串替换为有效的 JSON 空对象 `{}`

**修改位置**：`src-tauri/src/proxy/providers/streaming_chat_to_responses.rs`

**修改内容**：

1. 修复 `output_item.added` 事件（第 473 行）：

```rust
// 修复前
"arguments":""

// 修复后
"arguments":"{}"
```

2. 修复 `output_item.done` 事件（第 603 行）：

```rust
// 修复前
buf.index, buf.name, escaped_args, buf.id

// 修复后
buf.index, buf.name, if escaped_args.is_empty() { "{}".to_string() } else { escaped_args }, buf.id
```

**验证方式**：

1. 重启 cc-switch
2. 在 Codex 中发送 "请阅读 cc-switch 项目代码，告诉我它的功能"
3. 观察 UI 是否能正常显示完整的工具调用结果
4. 检查日志中 tool_call 的 arguments 是否为 `{}` 而非 `""`

### 修复 3: tool_call 增量 delta 因缺少 id 字段导致匹配错误 (2026-05-05)

**问题描述**：
MiniMax 在发送 tool_call 增量时，第一个 chunk 包含完整的 `id`, `name`, `arguments`，后续 chunk 只有 `index` 和 `arguments` 增量，没有 `id`。原代码只用 `id` 匹配，导致后续增量被当作新的 tool_call 处理，发送多个错误的 `output_item.added` 事件。

**原因分析**：
日志显示：

- chunk 7: `{"id":"call_function_codtlrg6w2mr_1", "index":0, ...}` → 正常创建
- chunk 8: `{"index":0, ...}` (无 id) → 原代码创建新的错误条目

Codex CLI 收到多个 `output_item.added` 事件，但 name 为空，无法正确处理。

**修复方案**：
用 `index` 字段辅助匹配：当 `id` 为空时，用 `index` 查找对应的 `call_id`。

**修改位置**：`src-tauri/src/proxy/providers/streaming_chat_to_responses.rs`

**修改内容**：

1. 添加 `index → call_id` 映射（第 231 行）：

```rust
let mut tool_index_by_index: HashMap<u32, String> = HashMap::new();
```

2. 修改 tool_call 处理逻辑（第 458-501 行）：

```rust
// 解析 index 字段
let index = tc.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as u32;

// 如果 call_id 为空，尝试用 index 查找
let resolved_call_id = if call_id.is_empty() {
    if let Some(existing_id) = tool_index_by_index.get(&index) {
        existing_id.clone()
    } else {
        // 没有 call_id 且没有 index 映射 - 跳过无效 delta
        continue;
    }
} else {
    call_id.to_string()
};

// 在创建新项时，同时建立 index → call_id 映射
tool_index_by_index.insert(index, resolved_call_id.clone());

// 使用 resolved_call_id 替代 call_id
```

**验证方式**：

1. 重启 cc-switch
2. 在 Codex 中发送 "请阅读 cc-switch 项目代码，告诉我它的功能"
3. 观察日志中 tool_call 的 `output_item.added` 事件数量是否正确（应该是 3 个，而不是更多）
4. 每个 tool_call 的 `function_call_arguments.delta` 是否正确追加到同一个 call_id

### 修复 4: output_item.added 事件缺少完整参数 (2026-05-06)

**问题描述**：
Codex UI 显示 "Let me explore the project structure first." 后任务停止。日志中看到工具调用事件正常发送，但 Codex 无法正确处理。

**原因分析**：
对比 codex-bridge-build 的实现，发现 `output_item.added` 事件中应包含完整的 function_call 信息（包括 arguments），而不是空 `{}`。原代码在 `output_item.added` 中只发送了空 `arguments:"{}"`。

**修复方案**：
参考 codex-bridge-build 的 `responses_tool_call_item` 函数（streaming.rs:857），`output_item.added` 和 `output_item.done` 应发送相同的 function_call 数据结构。

**修改位置**：`src-tauri/src/proxy/providers/streaming_chat_to_responses.rs`（第 589-605 行）

**修改内容**：

```rust
// 修复前：output_item.added 只发送空 arguments
yield Ok(Bytes::from(format!(
    "event: response.output_item.added\ndata: {{\"type\":\"response.output_item.added\",\"output_index\":{},\"item\":{{\"type\":\"function_call\",\"name\":\"{}\",\"arguments\":\"{{}}\",\"call_id\":\"{}\"}}\n}}\n\n",
    output_idx, buf.name, buf.id
)));

// 修复后：output_item.added 发送完整 arguments（与 output_item.done 相同）
let escaped_args_for_item = escaped_args.clone();
yield Ok(Bytes::from(format!(
    "event: response.output_item.added\ndata: {{\"type\":\"response.output_item.added\",\"output_index\":{},\"item\":{{\"type\":\"function_call\",\"name\":\"{}\",\"arguments\":\"{}\",\"call_id\":\"{}\"}}\n}}\n\n",
    output_idx, buf.name, if escaped_args_for_item.is_empty() { "{}".to_string() } else { escaped_args_for_item }, buf.id
)));
```

**验证方式**：

1. 重启 cc-switch
2. 在 Codex 中发送 "请阅读 cc-switch 项目代码，告诉我它的功能"
3. 观察 UI 是否能正常显示完整的工具调用结果
4. 检查日志中 `output_item.added` 的 arguments 是否包含完整参数

### 修复 5: SSE 事件 JSON 序列化使用手动 format! 导致格式错误 (2026-05-06)

**问题描述**：
Codex CLI 在执行复杂任务时，UI 显示第一句话后冻结。日志显示所有必要事件都已发送，但 Codex 仍然无法正确处理。

**原因分析**：
对比 codex-bridge-build (reference implementation) 和 cc-switch 的 SSE 事件格式化方式，发现关键差异：

- **Reference** 使用 `serde_json::to_string()` 正确序列化 JSON
- **cc-switch** 使用手动 `format!` + `replace()` 转义，无法正确处理所有 JSON 特殊字符

**修复方案**：
添加 `sse_event` 辅助函数，使用 `serde_json::to_string()` 正确序列化所有 SSE 事件。

**修改位置**：`src-tauri/src/proxy/providers/streaming_chat_to_responses.rs`

**修改内容**：

1. 添加 `sse_event` 辅助函数，使用 `serde_json::to_string()` 序列化
2. 替换所有手动 `format!` SSE 格式化调用，使用 `sse_event` + `json!` 宏

**验证方式**：

1. 重启 cc-switch
2. 在 Codex 中发送复杂任务
3. 观察 UI 是否能正常执行完整任务而不冻结

### 修复 6: tool arguments 双重序列化导致 MiniMax 收到错误格式 (2026-05-06)

**问题描述**：
Codex 发送的工具调用参数中，某些字段（如 `command`）的值被双重序列化。
例如：实际应该是 `["echo", "hello"]` 但收到了 `"[\"echo\", \"hello\"]"`。

**原因分析**：

1. Codex 发送的 arguments 中 `command` 字段是 JSON 数组
2. cc-switch 序列化时将数组再次 JSON 序列化
3. MiniMax 收到的是字符串而非数组，导致 `invalid type: string, expected an array` 错误

**修复方案**：
在 `transform_chat_to_responses.rs` 中添加 `normalize_tool_arguments` 函数，在发送前规范化 arguments。

**修改位置**：`src-tauri/src/proxy/providers/transform_chat_to_responses.rs`

**修改内容**：
添加 `normalize_tool_arguments` 和 `normalize_tool_arguments_inner` 函数，处理：

- `command` 字段：如果值是 JSON 字符串且解析后是数组，保持数组格式
- `message`/`items` 字段：如果值是 JSON 字符串，尝试解析

**验证方式**：

1. 重启 cc-switch
2. 在 Codex 中发送需要工具调用的任务
3. 检查 MiniMax 收到的请求中 `command` 字段是否为数组格式

### 修复 7: normalize_tool_arguments 未递归处理解析后的值 (2026-05-06)

**问题描述**：
`normalize_tool_arguments` 函数在处理 JSON 字符串时，如果字符串解析后不是对象（如解析成数组），则直接返回而不继续递归规范化。

**原因分析**：
当 arguments 是字符串如 `"{\"command\": \"[{\\\"echo\\\"]\"}"` 时：

- 解析后得到对象 `{"command": "[{\"echo\"]"}`
- 但 `command` 字段的值仍是字符串，未被递归处理

**修复方案**：
将 `Ok(v) => v` 改为 `Ok(v) => normalize_tool_arguments_inner(&v)`，确保解析后的值也被递归规范化。

**修改位置**：`src-tauri/src/proxy/providers/transform_chat_to_responses.rs`（第 838 行）

**修改内容**：

```rust
// 修复前：
Ok(v) => v,

// 修复后：
Ok(v) => normalize_tool_arguments_inner(&v), // 递归处理解析出的值
```

**验证方式**：

1. 重启 cc-switch
2. 在 Codex 中发送复杂任务
3. 观察工具调用是否正常执行，不再报 2013 错误
