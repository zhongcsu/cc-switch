//! OpenAI Chat Completions ↔ Responses API 格式转换模块
//!
//! 实现 OpenAI Chat Completions ↔ OpenAI Responses API 双向格式转换。
//!
//! ## 背景
//! MiniMax Token Plan API 使用 OpenAI Chat Completions 格式
//! Codex CLI 使用 OpenAI Responses API 格式
//! 本模块提供两者之间的格式转换能力
//!
//! ## 主要差异
//! | Chat Completions | Responses API |
//! |-----------------|--------------|
//! | `messages: [{role, content}]` | `input: string or item array` |
//! | `system` role in messages | `instructions` field |
//! | `function_call` in message | `function_call` as top-level item |
//! | `tool_call_id` | `function_call_output.call_id` |

use crate::proxy::error::ProxyError;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};

/// OpenAI Chat Completions 请求 → OpenAI Responses API 请求
///
/// 将 MiniMax 等使用 Chat Completions 格式的请求转换为 Responses API 格式，
/// 以便发送给 Codex CLI 等使用 Responses API 的后端。
pub fn chat_completions_to_responses(body: Value) -> Result<Value, ProxyError> {
    let mut result = json!({});

    // model
    if let Some(model) = body.get("model").and_then(|m| m.as_str()) {
        result["model"] = json!(model);
    }

    // system prompt → instructions (Responses API 使用独立的 instructions 字段)
    if let Some(messages) = body.get("messages").and_then(|m| m.as_array()) {
        let mut system_instructions = Vec::new();
        let mut non_system_messages = Vec::new();

        for msg in messages {
            let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");
            if role == "system" {
                if let Some(content) = msg.get("content") {
                    match content {
                        Value::String(text) => {
                            system_instructions.push(text.clone());
                        }
                        Value::Array(blocks) => {
                            for block in blocks {
                                if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                                    system_instructions.push(text.to_string());
                                }
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                non_system_messages.push(msg.clone());
            }
        }

        if !system_instructions.is_empty() {
            result["instructions"] = json!(system_instructions.join("\n\n"));
        }

        // messages → input
        if !non_system_messages.is_empty() {
            let input = convert_chat_messages_to_input(&non_system_messages)?;
            result["input"] = json!(input);
        }
    }

    // max_tokens → max_output_tokens
    if let Some(v) = body.get("max_tokens") {
        result["max_output_tokens"] = v.clone();
    }

    // temperature
    if let Some(v) = body.get("temperature") {
        result["temperature"] = v.clone();
    }

    // top_p
    if let Some(v) = body.get("top_p") {
        result["top_p"] = v.clone();
    }

    // stream
    if let Some(v) = body.get("stream") {
        result["stream"] = v.clone();
    }

    // reasoning_effort (if present in Chat Completions format)
    if let Some(reasoning) = body.get("reasoning") {
        match reasoning {
            Value::Object(obj) => {
                if let Some(effort) = obj.get("effort") {
                    result["reasoning"] = json!({ "effort": effort });
                }
            }
            Value::String(s) => {
                result["reasoning"] = json!({ "effort": s });
            }
            _ => {}
        }
    }

    // tools (Chat Completions format: functions array or tools array)
    let tools = body.get("functions").or(body.get("tools"));
    if let Some(tools_array) = tools.and_then(|t| t.as_array()) {
        let response_tools: Vec<Value> = tools_array
            .iter()
            .filter(|t| {
                // 过滤 BatchTool
                t.get("type").and_then(|v| v.as_str()) != Some("BatchTool")
            })
            .map(|t| {
                json!({
                    "type": "function",
                    "name": t.get("name").and_then(|n| n.as_str()).unwrap_or(""),
                    "description": t.get("description"),
                    "parameters": super::transform::clean_schema(
                        t.get("parameters")
                            .or(t.get("input_schema"))
                            .cloned()
                            .unwrap_or(json!({}))
                    )
                })
            })
            .collect();

        if !response_tools.is_empty() {
            result["tools"] = json!(response_tools);
        }
    }

    // tool_choice
    if let Some(v) = body.get("tool_choice") {
        result["tool_choice"] = map_tool_choice_to_responses(v);
    }

    // parallel_tool_calls (Responses API specific)
    if let Some(v) = body.get("parallel_tool_calls") {
        result["parallel_tool_calls"] = v.clone();
    }

    // seed (Chat Completions) → seed (Responses API - same field name)
    if let Some(v) = body.get("seed") {
        result["seed"] = v.clone();
    }

    // presence_penalty, frequency_penalty (Chat Completions) → not supported in Responses API
    // These are dropped as Responses API doesn't support them

    // response_format (Chat Completions) → text.format (Responses API)
    if let Some(format) = body.get("response_format") {
        if let Some(obj) = format.as_object() {
            let responses_format = if obj.get("type").and_then(|t| t.as_str()) == Some("json_schema") {
                let schema = obj.get("json_schema").or(obj.get("schema"));
                json!({
                    "type": "json_object",
                    "schema": schema.cloned().unwrap_or(json!({}))
                })
            } else {
                format.clone()
            };
            result["text"] = json!({ "format": responses_format });
        }
    }

    Ok(result)
}

/// 将 Chat Completions messages 数组转换为 Responses API input 数组
///
/// 核心转换逻辑：
/// - user/assistant 的 text 内容 → 对应 role 的 message item
/// - function_call 从 message 中"提升"为独立的 function_call item
/// - function_call_output (tool result) → function_call_output item
///
/// 注意：采用预处理+立即配对策略，确保 tool_call 和 tool_result 按正确顺序输出，
/// 避免多 Agent 并行场景下的顺序错乱问题（Error 2013）
fn convert_chat_messages_to_input(messages: &[Value]) -> Result<Vec<Value>, ProxyError> {
    let mut input = Vec::new();

    // 预处理：构建 call_id -> tool_result 的哈希表（用于立即配对）
    let mut output_by_call_id: HashMap<String, Value> = HashMap::new();
    for msg in messages {
        let content = msg.get("content");
        if let Some(Value::Array(blocks)) = content {
            for block in blocks {
                if block.get("type").and_then(|t| t.as_str()) == Some("tool_result") {
                    let call_id = block
                        .get("tool_use_id")
                        .or(block.get("tool_call_id"))
                        .and_then(|i| i.as_str())
                        .unwrap_or("");
                    if !call_id.is_empty() {
                        output_by_call_id.insert(call_id.to_string(), block.clone());
                    }
                }
            }
        }
    }

    for msg in messages {
        let role = msg.get("role").and_then(|r| r.as_str()).unwrap_or("user");
        let content = msg.get("content");

        match content {
            // 字符串内容
            Some(Value::String(text)) => {
                let content_type = if role == "assistant" {
                    "output_text"
                } else {
                    "input_text"
                };
                input.push(json!({
                    "role": role,
                    "content": [{ "type": content_type, "text": text }]
                }));
            }

            // 数组内容（多模态/工具调用）
            Some(Value::Array(blocks)) => {
                let mut message_content = Vec::new();

                for block in blocks {
                    let block_type = block.get("type").and_then(|t| t.as_str()).unwrap_or("");

                    match block_type {
                        "text" => {
                            if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                                let content_type = if role == "assistant" {
                                    "output_text"
                                } else {
                                    "input_text"
                                };
                                message_content.push(json!({ "type": content_type, "text": text }));
                            }
                        }

                        "image" => {
                            if let Some(source) = block.get("source") {
                                let media_type = source
                                    .get("media_type")
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("image/png");
                                let data =
                                    source.get("data").and_then(|d| d.as_str()).unwrap_or("");
                                message_content.push(json!({
                                    "type": "input_image",
                                    "image_url": format!("data:{media_type};base64,{data}")
                                }));
                            }
                        }

                        "tool_use" => {
                            // 先刷新已累积的消息内容
                            if !message_content.is_empty() {
                                input.push(json!({
                                    "role": role,
                                    "content": message_content.clone()
                                }));
                                message_content.clear();
                            }

                            // 提升为独立的 function_call item
                            let id = block.get("id").and_then(|i| i.as_str()).unwrap_or("");
                            let name = block.get("name").and_then(|n| n.as_str()).unwrap_or("");
                            let arguments = block.get("input").cloned().unwrap_or(json!({}));

                            input.push(json!({
                                "type": "function_call",
                                "call_id": id,
                                "name": name,
                                "arguments": arguments
                            }));

                            // 立即配对：检查是否有对应的 tool_result
                            if let Some(tool_result) = output_by_call_id.remove(id) {
                                let call_id = tool_result
                                    .get("tool_use_id")
                                    .or(tool_result.get("tool_call_id"))
                                    .and_then(|i| i.as_str())
                                    .unwrap_or(id);
                                let content = tool_result.get("content");
                                let result_content = match content {
                                    Some(Value::String(text)) => {
                                        json!([{ "type": "output_text", "text": text }])
                                    }
                                    Some(Value::Array(arr)) => Value::Array(arr.clone()),
                                    _ => json!([{ "type": "output_text", "text": "" }]),
                                };
                                input.push(json!({
                                    "type": "function_call_output",
                                    "call_id": call_id,
                                    "output": result_content
                                }));
                            }
                        }

                        "tool_result" => {
                            // 先刷新已累积的消息内容
                            if !message_content.is_empty() {
                                input.push(json!({
                                    "role": role,
                                    "content": message_content.clone()
                                }));
                                message_content.clear();
                            }

                            // 跳过已配对的（在 tool_use 处理时已配对）
                            let call_id = block
                                .get("tool_use_id")
                                .or(block.get("tool_call_id"))
                                .and_then(|i| i.as_str())
                                .unwrap_or("");
                            if output_by_call_id.contains_key(call_id) {
                                continue;
                            }

                            // 提升为独立的 function_call_output item
                            let result_content = match block.get("content") {
                                Some(Value::String(text)) => {
                                    json!([{ "type": "output_text", "text": text }])
                                }
                                Some(Value::Array(arr)) => Value::Array(arr.clone()),
                                _ => json!([{ "type": "output_text", "text": "" }]),
                            };

                            input.push(json!({
                                "type": "function_call_output",
                                "call_id": call_id,
                                "output": result_content
                            }));
                        }

                        "input_image" => {
                            // Handle input_image blocks
                            if let Some(image_url) = block.get("image_url").and_then(|u| u.as_str()) {
                                message_content.push(json!({
                                    "type": "input_image",
                                    "image_url": image_url
                                }));
                            }
                        }

                        _ => {
                            // 忽略未知类型的 block
                        }
                    }
                }

                // 刷新剩余的消息内容
                if !message_content.is_empty() {
                    input.push(json!({
                        "role": role,
                        "content": message_content
                    }));
                }
            }

            // null 或 missing content
            Some(Value::Null) | None => {
                // 空消息，跳过
            }

            // 其他类型（如纯 object）
            _ => {}
        }
    }

    Ok(input)
}

/// 将 Chat Completions tool_choice 转换为 Responses API 格式
fn map_tool_choice_to_responses(tool_choice: &Value) -> Value {
    match tool_choice {
        Value::String(s) => match s.as_str() {
            "auto" => json!("auto"),
            "none" => json!("none"),
            "required" => json!("required"),
            _ => tool_choice.clone(),
        },
        Value::Object(obj) => {
            match obj.get("function") {
                Some(Value::Object(func)) => {
                    let name = func.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    json!({
                        "type": "function",
                        "name": name
                    })
                }
                _ => tool_choice.clone(),
            }
        }
        _ => tool_choice.clone(),
    }
}

// ============================================================
// Responses API → Chat Completions 请求转换
// ============================================================

/// OpenAI Responses API 请求 → OpenAI Chat Completions 请求
///
/// 将 Codex CLI 发送的 Responses API 格式请求转换为 Chat Completions 格式，
/// 以便发送给 MiniMax 等使用 Chat Completions API 的后端。
///
/// Codex CLI 发送的请求格式：
/// ```json
/// {
///   "model": "codex-MiniMax-M2.7",
///   "input": [{"role": "user", "content": [{"type": "input_text", "text": "..."}]}],
///   "stream": true
/// }
/// ```
///
/// 转换为 Chat Completions 格式：
/// ```json
/// {
///   "model": "codex-MiniMax-M2.7",
///   "messages": [{"role": "user", "content": "..."}],
///   "stream": true
/// }
/// ```
pub fn responses_to_chat_completions_request(body: Value) -> Result<Value, ProxyError> {
    log::debug!("[transform] Codex -> MiniMax request: {}", body);

    // ============================================================
    // [DEBUG_LOG_1] Codex 发送的原始 tools 定义
    // 请提供此日志以便诊断 tool schema 问题
    // ============================================================
    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        log::info!("[DEBUG_LOG_1] Codex 原始 tools 定义 (共 {} 个工具):", tools.len());
        for (idx, tool) in tools.iter().enumerate() {
            log::info!("[DEBUG_LOG_1] --- TOOL[{}] ---", idx);
            log::info!("[DEBUG_LOG_1] tool raw: {}", tool);
            let tool_name = tool.get("name").and_then(|n| n.as_str()).unwrap_or("(unnamed)");
            let tool_type = tool.get("type").and_then(|t| t.as_str()).unwrap_or("(no type)");
            let description = tool.get("description").and_then(|d| d.as_str()).unwrap_or("");
            let parameters = tool.get("parameters").or(tool.get("input_schema"));
            log::info!("[DEBUG_LOG_1] tool_name={}, tool_type={}, description={}", tool_name, tool_type, description);
            log::info!("[DEBUG_LOG_1] tool parameters schema: {}", parameters.as_ref().map_or("null".to_string(), |p| p.to_string()));
        }
    } else {
        log::info!("[DEBUG_LOG_1] Codex 请求中没有 tools 字段");
    }

    let mut result = json!({});

    // model - 直接传递
    if let Some(model) = body.get("model").and_then(|m| m.as_str()) {
        result["model"] = json!(model);
    }

    // stream - 直接传递
    if let Some(stream) = body.get("stream") {
        result["stream"] = stream.clone();
    }

    // temperature
    if let Some(v) = body.get("temperature") {
        result["temperature"] = v.clone();
    }

    // top_p
    if let Some(v) = body.get("top_p") {
        result["top_p"] = v.clone();
    }

    // max_tokens / max_output_tokens
    if let Some(v) = body.get("max_tokens") {
        result["max_tokens"] = v.clone();
    } else if let Some(v) = body.get("max_output_tokens") {
        result["max_tokens"] = v.clone();
    }

    // reasoning_effort - Responses API 格式: {"effort": "high"}
    // Chat Completions 格式: {"reasoning": {"effort": "high"}} 或直接字符串
    if let Some(reasoning) = body.get("reasoning") {
        match reasoning {
            Value::Object(obj) => {
                if let Some(effort) = obj.get("effort") {
                    result["reasoning"] = json!({ "effort": effort });
                }
            }
            Value::String(s) => {
                result["reasoning"] = json!({ "effort": s });
            }
            _ => {}
        }
    }

    // input → messages 转换
    if let Some(input) = body.get("input").and_then(|i| i.as_array()) {
        let messages = convert_responses_input_to_chat_messages(input)?;
        if !messages.is_empty() {
            result["messages"] = json!(messages);
        }
    }

    // instructions → system message (Chat Completions 没有独立的 instructions 字段)
    if let Some(instructions) = body.get("instructions").and_then(|i| i.as_str()) {
        if !instructions.is_empty() {
            // 将 instructions 作为 system message 添加到 messages 开头
            if let Some(messages) = result.get("messages").and_then(|m| m.as_array()) {
                let mut new_messages = vec![json!({
                    "role": "system",
                    "content": instructions
                })];
                new_messages.extend(messages.iter().cloned());
                result["messages"] = json!(new_messages);
            } else {
                result["messages"] = json!([{
                    "role": "system",
                    "content": instructions
                }]);
            }
        }
    }

    // tools - Responses API 格式 → Chat Completions 格式转换
    // Codex 发送: {"type": "function", "name": "...", "arguments": {...}}
    // MiniMax 期望: {"type": "function", "function": {"name": "...", "parameters": {...}}}
    if let Some(tools) = body.get("tools").and_then(|t| t.as_array()) {
        let mut chat_tools: Vec<Value> = Vec::new();
        let mut skipped_count = 0;

        for t in tools {
            let tool_type = t.get("type").and_then(|v| v.as_str()).unwrap_or("function");

            // MiniMax 只支持 "function" 类型的工具
            if tool_type != "function" {
                // 选项 A: 尝试转换非 function 类型为 function 格式
                // 目前只支持 conversion，如果将来 MiniMax 支持其他类型，可以在这里扩展
                // 对于无法转换的类型，执行选项 B: 过滤 + 警告
                log::warn!(
                    "[transform] Skipping unsupported tool type '{}' for MiniMax (only 'function' type is supported)",
                    tool_type
                );
                skipped_count += 1;
                continue;
            }

            let name = t.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let description = t.get("description");
            let arguments = t.get("arguments");

            // Convert arguments object to parameters schema
            let parameters: Value = match arguments {
                Some(Value::Object(obj)) => Value::Object(obj.clone()),
                Some(Value::String(s)) => {
                    // If arguments is a JSON string, try to parse it
                    serde_json::from_str(s).unwrap_or_else(|_| json!({}))
                }
                _ => json!({}),
            };

            chat_tools.push(json!({
                "type": "function",
                "function": {
                    "name": name,
                    "description": description,
                    "parameters": parameters
                }
            }));
        }

        // 选项 C: 如果所有工具都被过滤掉，返回错误而不是发送空工具列表
        if !tools.is_empty() && chat_tools.is_empty() {
            log::error!(
                "[transform] All {} tool(s) skipped - MiniMax only supports 'function' type. Cannot proceed without tools.",
                skipped_count
            );
            return Err(ProxyError::TransformError(format!(
                "No supported tools: MiniMax only accepts 'function' type, but got {} tool(s) of other types",
                skipped_count
            )));
        }

        if !chat_tools.is_empty() {
            result["tools"] = json!(chat_tools);
            if skipped_count > 0 {
                log::warn!("[transform] {} tool(s) skipped due to unsupported type", skipped_count);
            }
            log::debug!("[transform] tools transformed: {} tools ({} skipped)", chat_tools.len(), skipped_count);
        } else {
            log::debug!("[transform] no tools found in request body");
        }
    } else {
        log::debug!("[transform] no tools found in request body");
    }

    log::debug!("[transform] Final request body: {}", result);
    Ok(result)
}

/// 将 Responses API 的 input 数组转换为 Chat Completions 的 messages 数组
///
/// 正确处理 Responses API 的各种 item 类型：
/// - message: 转换为带 role 和 content 的消息
/// - function_call: 转换为带 tool_calls 的 assistant 消息
/// - function_call_output: 转换为带 tool_call_id 的 tool 消息
/// - reasoning: 跳过（不转发给 MiniMax）
/// - tool_result / custom_tool_call: 转换为 tool 消息
fn convert_responses_input_to_chat_messages(input: &[Value]) -> Result<Vec<Value>, ProxyError> {
    use std::collections::VecDeque;

    // ============================================================
    // DEBUG: Log the entire input array from Codex
    // ============================================================
    log::info!("[transform] ============================================================");
    log::info!("[transform] convert_responses_input_to_chat_messages ENTRY");
    log::info!("[transform] Input array size: {}", input.len());
    for (idx, item) in input.iter().enumerate() {
        log::info!("[transform] --- INPUT[{}] ---", idx);
        log::info!("[transform] item (FULL): {}", item);
    }
    log::info!("[transform] ============================================================");

    let mut messages = Vec::new();
    let mut pending_tool_call_ids: VecDeque<String> = VecDeque::new();

    // ============================================================
    // Pre-processing: Build HashMap index of tool outputs by call_id
    // This enables O(1) lookup for immediate pairing with tool calls
    // ============================================================
    let mut output_by_call_id: HashMap<String, &Value> = HashMap::new();
    for item in input.iter() {
        let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or_default();
        if item_type == "function_call_output" {
            if let Some(call_id) = item.get("call_id").and_then(|c| c.as_str()).filter(|v| !v.is_empty()) {
                output_by_call_id.insert(call_id.to_string(), item);
                log::info!("[transform] Indexed function_call_output: call_id={}", call_id);
            }
        } else if item_type == "custom_tool_call_output" {
            if let Some(call_id) = item.get("call_id").and_then(|c| c.as_str()).filter(|v| !v.is_empty()) {
                output_by_call_id.insert(call_id.to_string(), item);
                log::info!("[transform] Indexed custom_tool_call_output: call_id={}", call_id);
            }
        }
    }
    log::info!("[transform] Pre-processed {} tool outputs for O(1) lookup", output_by_call_id.len());

    for (item_idx, item) in input.iter().enumerate() {
        let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or_default();

        log::info!("[transform] --- PROCESSING ITEM[{}] type={} ---", item_idx, item_type);

        match item_type {
            "message" => {
                // MiniMax and some other providers don't support "developer" role,
                // so remap it to "user"
                let role = item
                    .get("role")
                    .and_then(|r| r.as_str())
                    .unwrap_or("user");
                let chat_role = if role == "developer" { "user" } else { role };

                // Get content and flatten it
                let content = item
                    .get("content")
                    .and_then(|c| c.as_array())
                    .map(flatten_content_items)
                    .unwrap_or_default();

                if !content.trim().is_empty() {
                    messages.push(json!({
                        "role": chat_role,
                        "content": content,
                    }));
                }
            }
            "reasoning" => {
                // Skip reasoning items - don't forward to MiniMax
            }
            "function_call" => {
                let name = item.get("name").and_then(|n| n.as_str()).unwrap_or_default();
                if name.is_empty() {
                    log::warn!("[transform] ignoring function_call item with empty name");
                    continue;
                }

                let call_id = item
                    .get("call_id")
                    .and_then(|c| c.as_str())
                    .filter(|v| !v.trim().is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4()));

                // 使用规范化函数处理 arguments，处理双重序列化和类型错误
                let raw_arguments = item.get("arguments").unwrap_or(&Value::Null);
                let normalized_arguments = normalize_tool_arguments(&name, raw_arguments);
                let arguments = serde_json::to_string(&normalized_arguments)
                    .unwrap_or_else(|_| "{}".to_string());

                log::info!("[transform] === FUNCTION_CALL ===");
                log::info!("[transform] item_idx: {}, name: {}, call_id: {}", item_idx, name, call_id);
                log::info!("[transform] ORIGINAL arguments: {}", raw_arguments);
                log::info!("[transform] NORMALIZED arguments: {}", arguments);

                messages.push(json!({
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": arguments,
                        }
                    }]
                }));

                // ============================================================
                // Immediate pairing: Check if output is already available
                // ============================================================
                if let Some(output_item) = output_by_call_id.get(&call_id) {
                    log::info!("[transform] Immediate pairing found for call_id={}", call_id);
                    let resolved_call_id = resolve_tool_output_call_id(output_item, &mut pending_tool_call_ids)?;
                    let output_text = output_item.get("output").map(function_output_to_text).unwrap_or_default();
                    log::info!("[transform] Emitting paired tool result for call_id={}, text_len={}", resolved_call_id, output_text.len());
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": resolved_call_id,
                        "content": output_text,
                    }));
                } else {
                    pending_tool_call_ids.push_back(call_id.clone());
                    log::info!("[transform] No output yet, queued call_id={}", call_id);
                }
                log::info!("[transform] pending_tool_call_ids after processing: {:?}", pending_tool_call_ids);
                log::info!("[transform] === END FUNCTION_CALL ===");
            }
            "custom_tool_call" => {
                let name = item.get("name").and_then(|n| n.as_str()).unwrap_or_default();
                if name.is_empty() {
                    log::warn!("[transform] ignoring custom_tool_call item with empty name");
                    continue;
                }

                let call_id = item
                    .get("call_id")
                    .and_then(|c| c.as_str())
                    .filter(|v| !v.trim().is_empty())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("call_{}", uuid::Uuid::new_v4()));

                // 使用规范化函数处理 arguments，处理双重序列化和类型错误
                let raw_arguments = item
                    .get("input")
                    .or_else(|| item.get("arguments"))
                    .unwrap_or(&Value::Null);
                let normalized_arguments = normalize_tool_arguments(&name, raw_arguments);

                log::info!("[transform] === CUSTOM_TOOL_CALL ===");
                log::info!("[transform] item_idx: {}, name: {}, call_id: {}", item_idx, name, call_id);
                log::info!("[transform] arguments: {:?}", normalized_arguments);

                messages.push(json!({
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [{
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": normalized_arguments,
                        }
                    }]
                }));

                // ============================================================
                // Immediate pairing: Check if output is already available
                // ============================================================
                if let Some(output_item) = output_by_call_id.get(&call_id) {
                    log::info!("[transform] Immediate pairing found for custom_tool_call, call_id={}", call_id);
                    let resolved_call_id = resolve_tool_output_call_id(output_item, &mut pending_tool_call_ids)?;
                    let output_text = output_item.get("output").map(function_output_to_text).unwrap_or_default();
                    messages.push(json!({
                        "role": "tool",
                        "tool_call_id": resolved_call_id,
                        "content": output_text,
                    }));
                } else {
                    pending_tool_call_ids.push_back(call_id);
                    log::info!("[transform] No output yet for custom_tool_call, queued call_id");
                }
                log::info!("[transform] pending_tool_call_ids after processing: {:?}", pending_tool_call_ids);
                log::info!("[transform] === END CUSTOM_TOOL_CALL ===");
            }
            "function_call_output" => {
                // ============================================================
                // Skip if already paired via immediate pairing in function_call
                // ============================================================
                if item.get("call_id")
                    .and_then(|c| c.as_str())
                    .map(|v| output_by_call_id.contains_key(v))
                    .unwrap_or(false)
                {
                    log::info!("[transform] Skipping already-paired function_call_output");
                    continue;
                }

                log::info!("[transform] === FUNCTION_CALL_OUTPUT (Codex->MiniMax) ===");
                log::info!("[transform] item_idx: {}", item_idx);
                log::info!("[transform] input item (FULL): {}", item);
                log::info!("[transform] pending_tool_call_ids BEFORE resolve: {:?}", pending_tool_call_ids);
                let call_id = resolve_tool_output_call_id(item, &mut pending_tool_call_ids)?;
                log::info!("[transform] resolved call_id: {}", call_id);
                log::info!("[transform] pending_tool_call_ids AFTER resolve: {:?}", pending_tool_call_ids);
                let output_text = item
                    .get("output")
                    .map(function_output_to_text)
                    .unwrap_or_default();
                log::info!("[transform] output_text: {}", output_text);

                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": output_text,
                }));
                log::info!("[transform] === END FUNCTION_CALL_OUTPUT ===");
            }
            "tool_result" => {
                // ============================================================
                // Skip if already paired via immediate pairing in custom_tool_call
                // ============================================================
                if item.get("call_id")
                    .and_then(|c| c.as_str())
                    .map(|v| output_by_call_id.contains_key(v))
                    .unwrap_or(false)
                {
                    log::info!("[transform] Skipping already-paired tool_result");
                    continue;
                }
                log::info!("[transform] === TOOL_RESULT ===");
                log::info!("[transform] item_idx: {}", item_idx);
                log::info!("[transform] input item (FULL): {}", item);
                log::info!("[transform] pending_tool_call_ids BEFORE resolve: {:?}", pending_tool_call_ids);
                // tool_result is similar to function_call_output but from a different source
                let call_id = resolve_tool_output_call_id(item, &mut pending_tool_call_ids)?;
                log::info!("[transform] resolved call_id: {}", call_id);
                log::info!("[transform] pending_tool_call_ids AFTER resolve: {:?}", pending_tool_call_ids);
                let output_text = item
                    .get("output")
                    .or_else(|| item.get("result"))
                    .map(function_output_to_text)
                    .unwrap_or_default();

                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": output_text,
                }));
                log::info!("[transform] === END TOOL_RESULT ===");
            }
            "custom_tool_call_output" => {
                log::info!("[transform] === CUSTOM_TOOL_CALL_OUTPUT ===");
                log::info!("[transform] item_idx: {}", item_idx);
                log::info!("[transform] input item (FULL): {}", item);
                let call_id = resolve_tool_output_call_id(item, &mut pending_tool_call_ids)?;
                let output_text = item
                    .get("output")
                    .map(function_output_to_text)
                    .unwrap_or_default();

                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": call_id,
                    "content": output_text,
                }));
                log::info!("[transform] === END CUSTOM_TOOL_CALL_OUTPUT ===");
            }
            _ => {
                // Log unknown item types for debugging
                log::debug!("[transform] ignoring unknown input item type: {}", item_type);
            }
        }
    }

    // ============================================================
    // DEBUG: Log the final messages array
    // ============================================================
    log::info!("[transform] ============================================================");
    log::info!("[transform] convert_responses_input_to_chat_messages EXIT");
    log::info!("[transform] Output messages count: {}", messages.len());
    for (idx, msg) in messages.iter().enumerate() {
        log::info!("[transform] --- OUTPUT MSG[{}] ---", idx);
        log::info!("[transform] msg.role: {:?}", msg.get("role"));
        log::info!("[transform] msg.content: {:?}", msg.get("content"));
        if let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array()) {
            log::info!("[transform] msg.tool_calls count: {}", tool_calls.len());
            for (tc_idx, tc) in tool_calls.iter().enumerate() {
                log::info!("[transform]   tool_call[{}]: id={:?}, name={:?}",
                    tc_idx,
                    tc.get("id"),
                    tc.get("function").and_then(|f| f.get("name"))
                );
            }
        }
        if msg.get("tool_call_id").is_some() {
            log::info!("[transform] msg.tool_call_id: {:?}", msg.get("tool_call_id"));
        }
    }
    log::info!("[transform] pending_tool_call_ids at end: {:?}", pending_tool_call_ids);
    log::info!("[transform] ============================================================");

    Ok(messages)
}

/// 解析工具输出的 call_id
fn resolve_tool_output_call_id(
    item: &Value,
    pending_ids: &mut VecDeque<String>,
) -> Result<String, ProxyError> {
    log::info!("[transform] resolve_tool_output_call_id called");
    log::info!("[transform] item.call_id: {:?}", item.get("call_id"));

    // First try to get call_id directly from the item
    if let Some(call_id) = item
        .get("call_id")
        .and_then(|c| c.as_str())
        .filter(|v| !v.trim().is_empty())
    {
        log::info!("[transform] Found call_id directly in item: {}", call_id);
        return Ok(call_id.to_string());
    }

    // Otherwise, pop from pending_ids queue
    log::info!("[transform] No call_id in item, popping from pending_ids: {:?}", pending_ids);
    pending_ids
        .pop_front()
        .ok_or_else(|| ProxyError::TransformError("No pending tool call id to resolve".to_string()))
}

/// 将内容项数组扁平化为字符串
///
/// 处理各种类型的内容项：
/// - input_text/output_text/summary_text: 提取 text 字段
/// - input_image: 格式化为 [input_image] url
/// - input_file: 格式化为 [input_file] file_id=xxx
/// - 嵌套数组: 递归处理
fn flatten_content_items(items: &Vec<Value>) -> String {
    let mut parts = Vec::new();

    for item in items {
        let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or_default();

        if matches!(
            item_type,
            "input_text" | "output_text" | "summary_text"
        ) && item.get("text").and_then(|t| t.as_str()).is_some()
        {
            if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                if !text.is_empty() {
                    parts.push(text.to_string());
                }
            }
            continue;
        }

        if item_type == "input_image" {
            if let Some(url) = item.get("image_url").and_then(|u| u.as_str()) {
                parts.push(format!("[input_image] {url}"));
            } else {
                parts.push("[input_image]".to_string());
            }
            continue;
        }

        if item_type == "input_file" {
            if let Some(file_id) = item.get("file_id").and_then(|f| f.as_str()) {
                parts.push(format!("[input_file] file_id={file_id}"));
            } else if let Some(file_data) = item.get("file_data").and_then(|f| f.as_str()) {
                parts.push(format!("[input_file] file_data={file_data}"));
            } else {
                parts.push("[input_file]".to_string());
            }
        }
    }

    parts.join("\n")
}

/// 将函数参数转换为文本
fn function_arguments_to_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// 解析 JSON 数组字符串的元素（处理转义引号等）
fn parse_json_array_elements(content: &str) -> Vec<Value> {
    let mut elements = Vec::new();
    let mut depth = 0;
    let mut current_element = String::new();
    let mut in_string = false;
    let mut escaped = false;

    for ch in content.chars() {
        if escaped {
            current_element.push(ch);
            escaped = false;
            continue;
        }

        match ch {
            '\\' if in_string => {
                escaped = true;
                current_element.push(ch);
            }
            '"' => {
                in_string = !in_string;
                current_element.push(ch);
            }
            '[' | '{' if !in_string => {
                depth += 1;
                current_element.push(ch);
            }
            ']' | '}' if !in_string => {
                depth -= 1;
                current_element.push(ch);
            }
            ',' if !in_string && depth == 0 => {
                let elem = current_element.trim();
                if !elem.is_empty() {
                    if let Ok(v) = serde_json::from_str(elem) {
                        elements.push(v);
                    } else {
                        elements.push(Value::String(elem.to_string()));
                    }
                }
                current_element.clear();
            }
            _ => {
                current_element.push(ch);
            }
        }
    }

    // 处理最后一个元素
    let elem = current_element.trim();
    if !elem.is_empty() {
        if let Ok(v) = serde_json::from_str(elem) {
            elements.push(v);
        } else {
            elements.push(Value::String(elem.to_string()));
        }
    }

    elements
}

/// Agent 管理工具列表
const AGENT_TOOLS: &[&str] = &["spawn_agent", "wait_agent", "send_input", "resume_agent"];

/// 规范化工具调用的 arguments，处理双重序列化等问题
/// Codex 发送的 arguments 中某些字段可能是被错误序列化的字符串
/// 例如: {"command": "[\"echo\", \"hello\"]"} 而非 {"command": ["echo", "hello"]}
fn normalize_tool_arguments(name: &str, arguments: &Value) -> Value {
    // ============================================================
    // [DEBUG_LOG_2] normalize_tool_arguments 入口日志
    // 请提供此日志以便追踪 command 字段的转换过程
    // ============================================================
    log::info!("[DEBUG_LOG_2] normalize_tool_arguments 入口: tool_name={}", name);
    log::info!("[DEBUG_LOG_2] raw_arguments type={:?}, value={}", arguments, arguments);

    match arguments {
        // 如果 arguments 本身就是 JSON 对象，检查是否有需要修复的字段
        Value::Object(obj) => {
            let mut fixed = serde_json::Map::new();
            for (key, value) in obj {
                let fixed_value = match (key.as_str(), value) {
                    // shell 工具的 command 字段：MiniMax 期望 string 类型
                    // 如果收到数组格式的字符串（如 "[\"powershell.exe\", \"-Command\"]"），
                    // 将其转换为用空格连接的字符串（如 "powershell.exe -Command"）
                    ("command", Value::String(s)) => {
                        // ============================================================
                        // [DEBUG_LOG_3] command 字段为字符串时的解析日志
                        // ============================================================
                        log::info!("[DEBUG_LOG_3] 发现 command 字段为字符串: key={}, value={}", key, s);

                        // 解析 JSON 数组字符串并用空格连接成字符串
                        fn parse_command_array(s: &str) -> String {
                            let trimmed = s.trim();
                            log::info!("[DEBUG_LOG_3] parse_command_array input: {}", trimmed);

                            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                                let inner = &trimmed[1..trimmed.len()-1];
                                log::info!("[DEBUG_LOG_3] parse_command_array 检测到数组格式, inner={}", inner);

                                let elements = parse_json_array_elements(inner);

                                // ============================================================
                                // [DEBUG_LOG_4] parse_json_array_elements 结果日志
                                // ============================================================
                                log::info!("[DEBUG_LOG_4] parse_json_array_elements 返回 {} 个元素:", elements.len());
                                for (i, elem) in elements.iter().enumerate() {
                                    log::info!("[DEBUG_LOG_4]   element[{}]: type={:?}, value={}", i, elem, elem);
                                }

                                if !elements.is_empty() {
                                    // 将数组元素用空格连接成字符串
                                    let parts: Vec<String> = elements
                                        .iter()
                                        .filter_map(|e| e.as_str().map(|v| v.to_string()))
                                        .collect();
                                    let joined = parts.join(" ");
                                    log::info!("[DEBUG_LOG_3] parse_command_array 返回连接后的字符串: {}", joined);
                                    return joined;
                                }
                            }
                            // 如果解析失败或不是数组格式，保持原样
                            log::info!("[DEBUG_LOG_3] parse_command_array 返回原始字符串");
                            s.to_string()
                        }
                        let result = Value::String(parse_command_array(s));
                        log::info!("[DEBUG_LOG_3] command 字段处理后: type={:?}, value={}", result, result);
                        result
                    }
                    // ============================================================
                    // [DEBUG_LOG_5] command 字段为数组时的日志
                    // ============================================================
                    ("command", Value::Array(arr)) => {
                        log::info!("[DEBUG_LOG_5] 发现 command 字段为数组: key={}, arr_len={}", key, arr.len());
                        log::info!("[DEBUG_LOG_5] command 数组元素:");
                        for (i, elem) in arr.iter().enumerate() {
                            log::info!("[DEBUG_LOG_5]   element[{}]: type={:?}, value={}", i, elem, elem);
                        }
                        Value::Array(arr.clone())
                    }
                    // agent 工具的 message/items 字段可能需要特殊处理
                    ("message", Value::String(s)) | ("items", Value::String(s)) => {
                        // 尝试解析字符串
                        match serde_json::from_str::<Value>(s) {
                            Ok(v) => v,
                            Err(_) => Value::String(s.clone()),
                        }
                    }
                    // 其他字段：递归处理
                    _ => normalize_tool_arguments_inner(value),
                };
                fixed.insert(key.clone(), fixed_value);
            }

            // ============================================================
            // [DEBUG_LOG_6] 规范化后的 arguments 日志
            // ============================================================
            log::info!("[DEBUG_LOG_6] normalize_tool_arguments 结果: {}", Value::Object(fixed.clone()));

            // Agent 工具特殊处理：将 "agent_id" 字符串转换为 "agent_ids" 数组
            if AGENT_TOOLS.contains(&name) {
                if let Some(Value::String(agent_id)) = fixed.get("agent_id") {
                    // 将 "agent_id": "uuid" 转换为 "agent_ids": ["uuid"]
                    fixed.insert(
                        "agent_ids".to_string(),
                        Value::Array(vec![Value::String(agent_id.clone())]),
                    );
                    log::debug!("[transform] Converted agent_id to agent_ids for tool '{}'", name);
                }
            }

            Value::Object(fixed)
        }
        // 如果 arguments 是字符串，尝试解析为 JSON 对象后处理
        Value::String(s) => {
            // ============================================================
            // [DEBUG_LOG_7] arguments 为字符串时的日志
            // ============================================================
            log::info!("[DEBUG_LOG_7] arguments 为字符串: s={}", s);
            match serde_json::from_str::<Value>(s) {
                Ok(Value::Object(obj)) => {
                    // ============================================================
                    // [DEBUG_LOG_8] 字符串成功解析为 JSON 对象
                    // ============================================================
                    log::info!("[DEBUG_LOG_8] 字符串解析为 JSON Object, 包含 {} 个字段", obj.len());
                    // 递归规范化内部对象
                    let mut fixed = serde_json::Map::new();
                    for (key, value) in obj {
                        let fixed_value = normalize_tool_arguments_inner(&value);
                        fixed.insert(key.clone(), fixed_value);
                    }

                    // Agent 工具特殊处理：将 "agent_id" 字符串转换为 "agent_ids" 数组
                    if AGENT_TOOLS.contains(&name) {
                        if let Some(Value::String(agent_id)) = fixed.get("agent_id") {
                            fixed.insert(
                                "agent_ids".to_string(),
                                Value::Array(vec![Value::String(agent_id.clone())]),
                            );
                            log::debug!("[transform] Converted agent_id to agent_ids for tool '{}' (from string)", name);
                        }
                    }

                    // ============================================================
                    // [DEBUG_LOG_9] 字符串路径规范化后的结果
                    // ============================================================
                    log::info!("[DEBUG_LOG_9] Value::String 路径结果: {}", Value::Object(fixed.clone()));
                    Value::Object(fixed)
                }
                Ok(v) => {
                    // ============================================================
                    // [DEBUG_LOG_10] 字符串解析为非对象类型
                    // ============================================================
                    log::info!("[DEBUG_LOG_10] 字符串解析为非对象类型: type={:?}, value={}", v, v);
                    normalize_tool_arguments_inner(&v)
                }
                Err(_) => {
                    // ============================================================
                    // [DEBUG_LOG_11] 字符串无法解析为 JSON
                    // ============================================================
                    log::info!("[DEBUG_LOG_11] 字符串无法解析为 JSON, 返回原始字符串");
                    Value::String(s.clone())
                }
            }
        }
        // 其他类型直接返回
        other => other.clone(),
    }
}

/// 内部递归处理函数
fn normalize_tool_arguments_inner(value: &Value) -> Value {
    // ============================================================
    // [DEBUG_LOG_12] normalize_tool_arguments_inner 入口日志
    // ============================================================
    log::info!("[DEBUG_LOG_12] normalize_tool_arguments_inner: type={:?}, value={}", value, value);

    match value {
        Value::Object(obj) => {
            let mut fixed = serde_json::Map::new();
            for (key, val) in obj {
                let fixed_val = match (key.as_str(), val) {
                    ("command", Value::String(s)) => {
                        // ============================================================
                        // [DEBUG_LOG_13] inner 函数中 command 字段处理日志
                        // ============================================================
                        log::info!("[DEBUG_LOG_13] inner: key={}, val type=String, val={}", key, s);

                        // 解析 JSON 数组字符串并用空格连接成字符串
                        fn parse_command_array(s: &str) -> String {
                            let trimmed = s.trim();
                            log::info!("[DEBUG_LOG_13] parse_command_array input: {}", trimmed);

                            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                                let inner = &trimmed[1..trimmed.len()-1];
                                log::info!("[DEBUG_LOG_13] parse_command_array inner (before parse): {}", inner);

                                let elements = parse_json_array_elements(inner);

                                // ============================================================
                                // [DEBUG_LOG_14] inner parse_json_array_elements 结果
                                // ============================================================
                                log::info!("[DEBUG_LOG_14] inner parse_json_array_elements 返回 {} 个元素:", elements.len());
                                for (i, elem) in elements.iter().enumerate() {
                                    log::info!("[DEBUG_LOG_14]   element[{}]: type={:?}, value={}", i, elem, elem);
                                }

                                if !elements.is_empty() {
                                    // 将数组元素用空格连接成字符串
                                    let parts: Vec<String> = elements
                                        .iter()
                                        .filter_map(|e| e.as_str().map(|v| v.to_string()))
                                        .collect();
                                    let joined = parts.join(" ");
                                    log::info!("[DEBUG_LOG_13] parse_command_array 返回连接后的字符串: {}", joined);
                                    return joined;
                                }
                            }
                            // 如果解析失败或不是数组格式，保持原样
                            log::info!("[DEBUG_LOG_13] parse_command_array 返回原始字符串");
                            s.to_string()
                        }
                        let result = Value::String(parse_command_array(s));
                        log::info!("[DEBUG_LOG_13] inner command field result: type={:?}, value={}", result, result);
                        result
                    }
                    ("command", Value::Array(arr)) => {
                        // ============================================================
                        // [DEBUG_LOG_15] inner 中 command 字段已为数组
                        // ============================================================
                        log::info!("[DEBUG_LOG_15] inner: key={}, val type=Array, arr_len={}", key, arr.len());
                        Value::Array(arr.clone())
                    }
                    ("message", Value::String(s)) | ("items", Value::String(s)) => {
                        match serde_json::from_str::<Value>(s) {
                            Ok(v) => v,
                            Err(_) => Value::String(s.clone()),
                        }
                    }
                    _ => normalize_tool_arguments_inner(val),
                };
                fixed.insert(key.clone(), fixed_val);
            }
            // ============================================================
            // [DEBUG_LOG_16] inner Object 结果
            // ============================================================
            log::info!("[DEBUG_LOG_16] inner Object 结果: {}", Value::Object(fixed.clone()));
            Value::Object(fixed)
        }
        Value::Array(arr) => {
            // ============================================================
            // [DEBUG_LOG_17] inner Array 处理
            // ============================================================
            log::info!("[DEBUG_LOG_17] inner Array 处理, arr_len={}", arr.len());
            Value::Array(arr.iter().map(normalize_tool_arguments_inner).collect())
        }
        Value::String(s) => {
            // 检查字符串是否是 JSON 数组格式
            // ============================================================
            // [DEBUG_LOG_18] inner String 处理
            // ============================================================
            log::info!("[DEBUG_LOG_18] inner String: s={}", s);
            let trimmed = s.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                let inner = &trimmed[1..trimmed.len()-1];
                let elements = parse_json_array_elements(inner);
                log::info!("[DEBUG_LOG_18] inner String 检测为数组格式, elements count={}", elements.len());
                if !elements.is_empty() {
                    return Value::Array(elements);
                }
            }
            Value::String(s.clone())
        }
        other => {
            // ============================================================
            // [DEBUG_LOG_19] inner 其他类型
            // ============================================================
            log::info!("[DEBUG_LOG_19] inner 其他类型: type={:?}, value={}", other, other);
            other.clone()
        }
    }
}

/// 将函数输出转换为文本
fn function_output_to_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Array(arr) => {
            // Try to flatten array of output items
            let mut parts = Vec::new();
            for item in arr {
                if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                    parts.push(text.to_string());
                } else {
                    parts.push(item.to_string());
                }
            }
            parts.join("\n")
        }
        other => other.to_string(),
    }
}

// ============================================================
// Responses API → Chat Completions 响应转换
// ============================================================

/// OpenAI Responses API 响应 → OpenAI Chat Completions 响应
///
/// 将 Responses API 格式的响应转换为 Chat Completions 格式，
/// 以便客户端（如使用 Chat Completions 格式的 SDK）能够正确解析。
pub fn responses_to_chat_completions(body: Value) -> Result<Value, ProxyError> {
    let mut result = json!({});

    // id
    if let Some(id) = body.get("id").and_then(|i| i.as_str()) {
        result["id"] = json!(id);
    } else {
        result["id"] = json!("chatcmpl-default");
    }

    // model
    if let Some(model) = body.get("model").and_then(|m| m.as_str()) {
        result["model"] = json!(model);
    }

    // created timestamp
    if let Some(ts) = body.get("created").and_then(|c| c.as_u64()) {
        result["created"] = json!(ts);
    } else {
        result["created"] = json!(chrono::Utc::now().timestamp() as u64);
    }

    // system_fingerprint
    if let Some(fp) = body.get("system_fingerprint").and_then(|f| f.as_str()) {
        result["system_fingerprint"] = json!(fp);
    }

    // output items → choices
    let output = body.get("output").and_then(|o| o.as_array());
    let choices = convert_responses_output_to_choices(output.map(|v| &**v), &body)?;
    result["choices"] = json!(choices);

    // usage
    let usage = build_chat_completions_usage(body.get("usage"));
    result["usage"] = json!(usage);

    // Responses API doesn't have a direct equivalent to chat completions' object field
    result["object"] = json!("chat.completion");

    Ok(result)
}

/// 将 Responses API 的 output 数组转换为 Chat Completions 的 choices 数组
fn convert_responses_output_to_choices(
    output: Option<&[Value]>,
    _original: &Value,
) -> Result<Vec<Value>, ProxyError> {
    let mut choices = Vec::new();
    let mut chat_message = json!({
        "role": "assistant",
        "content": ""
    });
    let mut finish_reason: Option<String> = None;
    let mut tool_calls: Vec<Value> = Vec::new();

    if let Some(items) = output {
        for item in items {
            let item_type = item.get("type").and_then(|t| t.as_str()).unwrap_or("");

            match item_type {
                "message" => {
                    if let Some(content) = item.get("content").and_then(|c| c.as_array()) {
                        let mut text_parts = Vec::new();
                        for block in content {
                            if let Some(text) = block.get("text").and_then(|t| t.as_str()) {
                                text_parts.push(text.to_string());
                            }
                        }
                        if !text_parts.is_empty() {
                            chat_message["content"] = json!(text_parts.join(""));
                        }
                    }
                    if let Some(status) = item.get("status").and_then(|s| s.as_str()) {
                        finish_reason = Some(status.to_string());
                    }
                }

                "output_text" => {
                    if let Some(text) = item.get("text").and_then(|t| t.as_str()) {
                        chat_message["content"] = json!(text);
                    }
                }

                "function_call" => {
                    let call_id = item.get("call_id").and_then(|c| c.as_str()).unwrap_or("");
                    let name = item.get("name").and_then(|n| n.as_str()).unwrap_or("");
                    let raw_arguments = item.get("arguments").cloned().unwrap_or(json!("{}"));
                    let normalized_arguments = normalize_tool_arguments(&name, &raw_arguments);

                    tool_calls.push(json!({
                        "id": call_id,
                        "type": "function",
                        "function": {
                            "name": name,
                            "arguments": normalized_arguments
                        }
                    }));
                }

                "function_call_output" => {
                    // This is a tool result, typically doesn't affect the assistant message
                }

                "reasoning" => {
                    // Reasoning summary - can be added to content if needed
                }

                _ => {}
            }
        }
    }

    // Determine finish_reason
    let final_finish_reason = if !tool_calls.is_empty() {
        "tool_calls"
    } else {
        finish_reason.as_deref().unwrap_or("stop")
    };

    // Build the choice
    let mut choice = json!({
        "message": chat_message,
        "finish_reason": final_finish_reason,
        "index": 0
    });

    if !tool_calls.is_empty() {
        choice["message"]["tool_calls"] = json!(tool_calls);
    }

    choices.push(choice);

    Ok(choices)
}

/// 从 Responses API usage 构建 Chat Completions 格式的 usage
fn build_chat_completions_usage(usage: Option<&Value>) -> Value {
    let u = match usage {
        Some(v) if !v.is_null() => v,
        _ => {
            return json!({
                "prompt_tokens": 0,
                "completion_tokens": 0,
                "total_tokens": 0
            })
        }
    };

    let input_tokens = u.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
    let output_tokens = u.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0);

    let mut result = json!({
        "prompt_tokens": input_tokens,
        "completion_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens
    });

    // Map cache tokens if present
    if let Some(cached) = u
        .pointer("/input_tokens_details/cached_tokens")
        .and_then(|v| v.as_u64())
    {
        result["prompt_tokens_details"] = json!({
            "cached_tokens": cached
        });
    }

    result
}

// ============================================================
// Chat Completions 响应 → Responses API 响应
// ============================================================

/// OpenAI Chat Completions 响应 → OpenAI Responses API 响应
///
/// 将 Chat Completions 格式的响应转换为 Responses API 格式。
pub fn chat_completions_to_responses_response(body: Value) -> Result<Value, ProxyError> {
    log::debug!("[transform] MiniMax -> Codex response: {}", body);
    let mut result = json!({});

    // id
    if let Some(id) = body.get("id").and_then(|i| i.as_str()) {
        result["id"] = json!(id);
    } else {
        result["id"] = json!("resp-default");
    }

    // model
    if let Some(model) = body.get("model").and_then(|m| m.as_str()) {
        result["model"] = json!(model);
    }

    // created
    if let Some(ts) = body.get("created").and_then(|c| c.as_u64()) {
        result["created"] = json!(ts);
    }

    // system_fingerprint
    if let Some(fp) = body.get("system_fingerprint").and_then(|f| f.as_str()) {
        result["system_fingerprint"] = json!(fp);
    }

    // choices → output
    if let Some(choices) = body.get("choices").and_then(|c| c.as_array()) {
        let mut output = Vec::new();

        for choice in choices {
            let message = choice.get("message");
            let finish_reason = choice.get("finish_reason").and_then(|f| f.as_str());

            if let Some(msg) = message {
                let content = msg.get("content").and_then(|c| c.as_str());

                // Text content
                if let Some(text) = content {
                    output.push(json!({
                        "type": "output_text",
                        "text": text
                    }));
                }

                // Tool calls
                if let Some(tool_calls) = msg.get("tool_calls").and_then(|t| t.as_array()) {
                    for tc in tool_calls {
                        let call_id = tc.get("id").and_then(|i| i.as_str()).unwrap_or("");
                        let func = tc.get("function");
                        let name = func.and_then(|f| f.get("name")).and_then(|n| n.as_str()).unwrap_or("");
                        let arguments = func.and_then(|f| f.get("arguments")).cloned().unwrap_or(json!("{}"));

                        output.push(json!({
                            "type": "function_call",
                            "call_id": call_id,
                            "name": name,
                            "arguments": arguments
                        }));
                    }
                }
            }

            // Track finish_reason for status
            if let Some(_reason) = finish_reason {
                // Status will be set at the end
            }
        }

        result["output"] = json!(output);
    }

    // usage
    if let Some(usage) = body.get("usage") {
        let input_tokens = usage.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
        let output_tokens = usage.get("completion_tokens").and_then(|v| v.as_u64()).unwrap_or(0);

        result["usage"] = json!({
            "input_tokens": input_tokens,
            "output_tokens": output_tokens
        });

        // Map cached tokens
        if let Some(details) = usage.get("prompt_tokens_details") {
            if let Some(cached) = details.get("cached_tokens").and_then(|v| v.as_u64()) {
                if let Some(usage_obj) = result.get_mut("usage") {
                    usage_obj["input_tokens_details"] = json!({
                        "cached_tokens": cached
                    });
                }
            }
        }
    }

    result["status"] = json!("completed");

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chat_completions_to_responses_simple() {
        let input = json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "user", "content": "Hello"}
            ]
        });

        let result = chat_completions_to_responses(input).unwrap();
        assert_eq!(result["model"], "gpt-4o");
        assert_eq!(result["instructions"], "");
        let input_arr = result["input"].as_array().unwrap();
        assert_eq!(input_arr.len(), 1);
    }

    #[test]
    fn test_chat_completions_to_responses_with_system() {
        let input = json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hello"}
            ]
        });

        let result = chat_completions_to_responses(input).unwrap();
        assert_eq!(result["model"], "gpt-4o");
        assert_eq!(result["instructions"], "You are a helpful assistant.");
        let input_arr = result["input"].as_array().unwrap();
        assert_eq!(input_arr.len(), 1);
        assert_eq!(input_arr[0]["role"], "user");
    }

    #[test]
    fn test_chat_completions_to_responses_with_tools() {
        let input = json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "user", "content": "What's the weather?"}
            ],
            "tools": [
                {
                    "type": "function",
                    "name": "get_weather",
                    "description": "Get weather for a location",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "location": {"type": "string"}
                        },
                        "required": ["location"]
                    }
                }
            ]
        });

        let result = chat_completions_to_responses(input).unwrap();
        let tools = result["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "get_weather");
        assert_eq!(tools[0]["type"], "function");
    }

    #[test]
    fn test_responses_to_chat_completions_simple() {
        let input = json!({
            "id": "resp_123",
            "model": "gpt-4o",
            "output": [
                {
                    "type": "message",
                    "role": "assistant",
                    "content": [
                        {"type": "output_text", "text": "Hello!"}
                    ]
                }
            ],
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5
            }
        });

        let result = responses_to_chat_completions(input).unwrap();
        assert_eq!(result["id"], "resp_123");
        assert_eq!(result["model"], "gpt-4o");
        let choices = result["choices"].as_array().unwrap();
        assert_eq!(choices.len(), 1);
        assert_eq!(choices[0]["message"]["content"], "Hello!");
    }

    #[test]
    fn test_responses_to_chat_completions_with_tool_calls() {
        let input = json!({
            "id": "resp_123",
            "model": "gpt-4o",
            "output": [
                {
                    "type": "function_call",
                    "call_id": "call_abc",
                    "name": "get_weather",
                    "arguments": json!({"location": "Beijing"})
                }
            ],
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5
            }
        });

        let result = responses_to_chat_completions(input).unwrap();
        let choices = result["choices"].as_array().unwrap();
        assert_eq!(choices[0]["finish_reason"], "tool_calls");
        let tool_calls = choices[0]["message"]["tool_calls"].as_array().unwrap();
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0]["id"], "call_abc");
        assert_eq!(tool_calls[0]["function"]["name"], "get_weather");
    }

    #[test]
    fn test_chat_completions_to_responses_response_simple() {
        let input = json!({
            "id": "chatcmpl_123",
            "model": "gpt-4o",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "Hello!"
                    },
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5
            }
        });

        let result = chat_completions_to_responses_response(input).unwrap();
        assert_eq!(result["id"], "chatcmpl_123");
        let output = result["output"].as_array().unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0]["text"], "Hello!");
    }
}