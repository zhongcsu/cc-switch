//! OpenAI Responses API SSE → Chat Completions SSE 转换模块
//!
//! 实现 Responses API 流式响应到 Chat Completions 流式格式的转换。
//!
//! Responses API SSE 使用命名事件 (named events):
//! - response.created
//! - response.output_item.added
//! - response.output_text.delta
//! - response.function_call_arguments.delta
//! - response.content_part.done
//! - response.output_item.done
//! - response.completed
//!
//! Chat Completions SSE 使用不同的格式:
//! - data: {"choices":[{"delta":{"content": "..."}}]}
//! - data: {"choices":[{"delta":{"tool_calls":[{"id": "...", "function": {"name": "...", "arguments": "..."}}]}}]}

use crate::proxy::sse::{strip_sse_field, take_sse_block};
use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;

/// 创建 Chat Completions SSE 流，从 Responses API SSE 流转换
///
/// 状态机跟踪:
/// - message_id, model
/// - content_index counter
/// - tool_index_by_item_id mapping
pub fn create_chat_sse_stream_from_responses<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    async_stream::stream! {
        let mut buffer = String::new();
        let mut utf8_remainder: Vec<u8> = Vec::new();

        // 状态跟踪
        let mut message_id: Option<String> = None;
        let mut model: Option<String> = None;
        let mut created: u64 = 0;
        let mut conversation_id: Option<String> = None;

        // 内容状态
        let mut content_index: u32 = 0;
        let mut current_text: String = String::new();
        let mut has_sent_first_chunk = false;

        // 工具调用状态
        let mut tool_index_by_item_id: HashMap<String, u32> = HashMap::new();
        let mut tool_call_buffer: HashMap<String, ToolCallBuffer> = HashMap::new();
        let mut tool_index_counter: u32 = 0;

        // Custom tool_call 状态 (用于 custom_tool_call 事件)
        let mut custom_tool_buffer: HashMap<String, ToolCallBuffer> = HashMap::new();

        // 完成状态
        let mut is_complete = false;
        let mut final_finish_reason: Option<String> = None;

        tokio::pin!(stream);

        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    crate::proxy::sse::append_utf8_safe(&mut buffer, &mut utf8_remainder, &bytes);

                    while let Some(block) = take_sse_block(&mut buffer) {
                        if block.trim().is_empty() {
                            continue;
                        }

                        // 解析 SSE 块
                        let mut event_type: Option<String> = None;
                        let mut data_parts: Vec<String> = Vec::new();

                        for line in block.lines() {
                            if let Some(evt) = strip_sse_field(line, "event") {
                                event_type = Some(evt.trim().to_string());
                            } else if let Some(d) = strip_sse_field(line, "data") {
                                data_parts.push(d.to_string());
                            }
                        }

                        if data_parts.is_empty() {
                            continue;
                        }

                        let data_str = data_parts.join("\n");
                        let event_name = event_type.as_deref().unwrap_or("");

                        let data: Value = match serde_json::from_str(&data_str) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        match event_name {
                            "response.created" => {
                                if let Some(resp) = data.get("response").or(data.get("output")) {
                                    if message_id.is_none() {
                                        message_id = resp.get("id")
                                            .and_then(|v| v.as_str())
                                            .map(|s| s.to_string());
                                    }
                                    if model.is_none() {
                                        model = resp.get("model")
                                            .and_then(|v| v.as_str())
                                            .map(|s| s.to_string());
                                    }
                                    if created == 0 {
                                        created = resp.get("created")
                                            .and_then(|v| v.as_u64())
                                            .unwrap_or_else(|| {
                                                chrono::Utc::now().timestamp() as u64
                                            });
                                    }
                                    conversation_id = resp
                                        .get("conversation_id")
                                        .and_then(|v| v.as_str())
                                        .map(|s| s.to_string());
                                }

                                // 发送 Chat Completions 格式的头部
                                let id = message_id.clone().unwrap_or_else(|| format!("chatcmpl-{}", uuid::Uuid::new_v4().to_string().replace("-", "")[..12].to_string()));
                                let model_str = model.clone().unwrap_or_else(|| "unknown".to_string());

                                yield Ok(Bytes::from(format!(
                                    "data: {{\"id\":\"chatcmpl-{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":null}}]}}\n\n",
                                    id, created, model_str
                                )));
                            }

                            "response.output_item.added" => {
                                // DEBUG: Log function_call items from Codex
                                if let Some(item) = data.get("item") {
                                    let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                                    let item_id = item.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                                    let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                    let call_id = item.get("call_id").and_then(|v| v.as_str()).unwrap_or("");
                                    log::info!("[CODEX->MINIMAX] response.output_item.added: type={}, item_id={}, name={}, call_id={}",
                                        item_type, item_id, name, call_id);

                                    if item_type == "function_call" {
                                        let idx = tool_index_counter;
                                        tool_index_counter += 1;
                                        tool_index_by_item_id.insert(item_id.clone(), idx);

                                        let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                        let call_id = item.get("call_id").and_then(|v| v.as_str()).unwrap_or(&item_id);

                                        tool_call_buffer.insert(item_id.clone(), ToolCallBuffer {
                                            id: call_id.to_string(),
                                            name: name.to_string(),
                                            arguments: String::new(),
                                            index: idx,
                                        });
                                    } else if item_type == "custom_tool_call" {
                                        // custom_tool_call 事件处理
                                        let idx = tool_index_counter;
                                        tool_index_counter += 1;
                                        tool_index_by_item_id.insert(item_id.clone(), idx);

                                        let name = item.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                        let call_id = item.get("call_id").and_then(|v| v.as_str()).unwrap_or(&item_id);

                                        custom_tool_buffer.insert(item_id.clone(), ToolCallBuffer {
                                            id: call_id.to_string(),
                                            name: name.to_string(),
                                            arguments: String::new(),
                                            index: idx,
                                        });
                                        log::info!("[CODEX->MINIMAX] custom_tool_call buffered: item_id={}, name={}, idx={}",
                                            item_id, name, idx);
                                    } else if item_type == "message" {
                                        // Assistant message - will have content
                                    }
                                }
                            }

                            "response.output_text.delta" | "output_text.delta" => {
                                if let Some(delta) = data.get("delta").or(data.get("text")) {
                                    if let Some(text) = delta.get("text").and_then(|v| v.as_str()) {
                                        current_text.push_str(text);

                                        // 发送文本增量
                                        let id = message_id.clone().unwrap_or_else(|| "chatcmpl-local".to_string());
                                        let model_str = model.clone().unwrap_or_else(|| "unknown".to_string());

                                        // 转义 text 中的特殊字符
                                        let escaped_text = text
                                            .replace('\\', "\\\\")
                                            .replace('"', "\\\"")
                                            .replace('\n', "\\n")
                                            .replace('\r', "\\r")
                                            .replace('\t', "\\t");

                                        yield Ok(Bytes::from(format!(
                                            "data: {{\"id\":\"chatcmpl-{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":{},\"delta\":{{\"content\":\"{}\"}},\"finish_reason\":null}}]}}\n\n",
                                            id, created, model_str, content_index, escaped_text
                                        )));

                                        has_sent_first_chunk = true;
                                    }
                                }
                            }

                            "response.function_call_arguments.delta" | "function_call_arguments.delta" => {
                                if let Some(args_delta) = data.get("delta") {
                                    let item_id = data.get("item_id")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();

                                    if let Some(buffer) = tool_call_buffer.get_mut(&item_id) {
                                        if let Some(text) = args_delta.get("arguments")
                                            .and_then(|v| v.as_str())
                                        {
                                            buffer.arguments.push_str(text);
                                        }
                                    }
                                }
                            }

                            "response.function_call_arguments.done" | "function_call_arguments.done" => {
                                // DEBUG: Log when function_call_arguments done
                                let item_id = data.get("item_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                log::info!("[CODEX->MINIMAX] function_call_arguments.done: item_id={}", item_id);

                                if let Some(buffer) = tool_call_buffer.remove(&item_id) {
                                    let escaped_name = buffer.name
                                        .replace('\\', "\\\\")
                                        .replace('"', "\\\"")
                                        .replace('\n', "\\n");

                                    let escaped_args = buffer.arguments
                                        .replace('\\', "\\\\")
                                        .replace('"', "\\\"")
                                        .replace('\n', "\\n")
                                        .replace('\r', "\\r")
                                        .replace('\t', "\\t");

                                    let id = message_id.clone().unwrap_or_else(|| "chatcmpl-local".to_string());
                                    let model_str = model.clone().unwrap_or_else(|| "unknown".to_string());

                                    log::info!("[CODEX->MINIMAX] Sending tool_call: index={}, id=tool_{}, name={}",
                                        buffer.index, buffer.id, buffer.name);

                                    yield Ok(Bytes::from(format!(
                                        "data: {{\"id\":\"chatcmpl-{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":{},\"delta\":{{\"role\":\"assistant\",\"tool_calls\":[{{\"index\":{},\"id\":\"tool_{}\",\"type\":\"function\",\"function\":{{\"name\":\"{}\",\"arguments\":\"{}\"}}}}]}},\"finish_reason\":null}}]}}\n\n",
                                        id, created, model_str, buffer.index, buffer.index, buffer.id, escaped_name, escaped_args
                                    )));

                                    // Note: Do NOT increment tool_index_counter here - use buffer.index which is the actual tool index from Codex
                                    // tool_index_counter += 1;  // REMOVED - this was causing index mismatch with MiniMax
                                }
                            }

                            "response.function_call_output" | "function_call_output" => {
                                // Tool result from Codex - emit as Chat Completions tool message
                                let call_id = data.get("item_id")
                                    .or(data.get("call_id"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let output = data.get("output")
                                    .or(data.get("result"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");

                                // Look up the tool index for this call_id
                                let tool_idx = tool_index_by_item_id.get(call_id).copied().unwrap_or(0);

                                // Escape output content for JSON
                                let escaped_output = output
                                    .replace('\\', "\\\\")
                                    .replace('"', "\\\"")
                                    .replace('\n', "\\n")
                                    .replace('\r', "\\r")
                                    .replace('\t', "\\t");

                                let id = message_id.clone().unwrap_or_else(|| "chatcmpl-local".to_string());
                                let model_str = model.clone().unwrap_or_else(|| "unknown".to_string());

                                log::info!("[CODEX->MINIMAX] function_call_output: call_id={}, tool_idx={}, output_len={}",
                                    call_id, tool_idx, output.len());

                                yield Ok(Bytes::from(format!(
                                    "data: {{\"id\":\"chatcmpl-{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":{},\"delta\":{{\"role\":\"tool\",\"tool_call_id\":\"{}\",\"content\":\"{}\"}},\"finish_reason\":null}}]}}\n\n",
                                    id, created, model_str, tool_idx, call_id, escaped_output
                                )));
                            }

                            // custom_tool_call.delta - arguments增量 (来自 input 字段)
                            "response.custom_tool_call.delta" | "custom_tool_call.delta" => {
                                if let Some(delta) = data.get("delta") {
                                    let item_id = data.get("item_id")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();

                                    if let Some(buffer) = custom_tool_buffer.get_mut(&item_id) {
                                        // 对于 custom_tool_call，delta 可能包含 input 字段的增量
                                        if let Some(input_delta) = delta.get("input")
                                            .and_then(|v| v.as_str())
                                        {
                                            buffer.arguments.push_str(input_delta);
                                        } else if let Some(input_delta) = delta.get("input") {
                                            // 如果是对象，序列化为字符串
                                            if let Ok(input_str) = serde_json::to_string(&input_delta) {
                                                buffer.arguments.push_str(&input_str);
                                            }
                                        }
                                    }
                                }
                            }

                            // custom_tool_call.done - 输出完整的 tool_call
                            "response.custom_tool_call.done" | "custom_tool_call.done" => {
                                let item_id = data.get("item_id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("")
                                    .to_string();
                                log::info!("[CODEX->MINIMAX] custom_tool_call.done: item_id={}", item_id);

                                if let Some(buffer) = custom_tool_buffer.remove(&item_id) {
                                    let escaped_name = buffer.name
                                        .replace('\\', "\\\\")
                                        .replace('"', "\\\"")
                                        .replace('\n', "\\n");

                                    let escaped_args = buffer.arguments
                                        .replace('\\', "\\\\")
                                        .replace('"', "\\\"")
                                        .replace('\n', "\\n")
                                        .replace('\r', "\\r")
                                        .replace('\t', "\\t");

                                    let id = message_id.clone().unwrap_or_else(|| "chatcmpl-local".to_string());
                                    let model_str = model.clone().unwrap_or_else(|| "unknown".to_string());

                                    log::info!("[CODEX->MINIMAX] Sending custom_tool_call as tool_call: index={}, id=tool_{}, name={}",
                                        buffer.index, buffer.id, buffer.name);

                                    yield Ok(Bytes::from(format!(
                                        "data: {{\"id\":\"chatcmpl-{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":{},\"delta\":{{\"role\":\"assistant\",\"tool_calls\":[{{\"index\":{},\"id\":\"tool_{}\",\"type\":\"function\",\"function\":{{\"name\":\"{}\",\"arguments\":\"{}\"}}}}]}},\"finish_reason\":null}}]}}\n\n",
                                        id, created, model_str, buffer.index, buffer.index, buffer.id, escaped_name, escaped_args
                                    )));
                                }
                            }

                            "response.custom_tool_call_output" | "custom_tool_call_output" => {
                                // Tool result from custom_tool_call - emit as Chat Completions tool message
                                let call_id = data.get("item_id")
                                    .or(data.get("call_id"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let output = data.get("output")
                                    .or(data.get("result"))
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");

                                // Look up the tool index for this call_id
                                let tool_idx = tool_index_by_item_id.get(call_id).copied().unwrap_or(0);

                                // Escape output content for JSON
                                let escaped_output = output
                                    .replace('\\', "\\\\")
                                    .replace('"', "\\\"")
                                    .replace('\n', "\\n")
                                    .replace('\r', "\\r")
                                    .replace('\t', "\\t");

                                let id = message_id.clone().unwrap_or_else(|| "chatcmpl-local".to_string());
                                let model_str = model.clone().unwrap_or_else(|| "unknown".to_string());

                                log::info!("[CODEX->MINIMAX] custom_tool_call_output: call_id={}, tool_idx={}, output_len={}",
                                    call_id, tool_idx, output.len());

                                yield Ok(Bytes::from(format!(
                                    "data: {{\"id\":\"chatcmpl-{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":{},\"delta\":{{\"role\":\"tool\",\"tool_call_id\":\"{}\",\"content\":\"{}\"}},\"finish_reason\":null}}]}}\n\n",
                                    id, created, model_str, tool_idx, call_id, escaped_output
                                )));
                            }

                            "response.output_item.done" | "output_item.done" => {
                                if let Some(item) = data.get("item") {
                                    let item_type = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
                                    if item_type == "function_call" {
                                        has_sent_first_chunk = true;
                                    }
                                }
                            }

                            "response.completed" | "response.done" => {
                                is_complete = true;
                                final_finish_reason = data.get("status")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string());

                                // 发送完成块
                                let id = message_id.clone().unwrap_or_else(|| "chatcmpl-local".to_string());
                                let model_str = model.clone().unwrap_or_else(|| "unknown".to_string());
                                let finish_reason = if !tool_call_buffer.is_empty() {
                                    "tool_calls"
                                } else {
                                    final_finish_reason.as_deref().unwrap_or("stop")
                                };

                                yield Ok(Bytes::from(format!(
                                    "data: {{\"id\":\"chatcmpl-{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"{}\"}}]}}\n\n",
                                    id, created, model_str, finish_reason
                                )));

                                // 发送 [DONE]
                                yield Ok(Bytes::from("data: [DONE]\n\n".to_string()));
                                break;
                            }

                            _ => {
                                // 忽略其他事件
                            }
                        }
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }

        // 如果流提前结束，发送最后的完成块
        if !is_complete {
            let id = message_id.clone().unwrap_or_else(|| "chatcmpl-local".to_string());
            let model_str = model.clone().unwrap_or_else(|| "unknown".to_string());
            let finish_reason = if !tool_call_buffer.is_empty() {
                "tool_calls"
            } else {
                "stop"
            };

            yield Ok(Bytes::from(format!(
                "data: {{\"id\":\"chatcmpl-{}\",\"object\":\"chat.completion.chunk\",\"created\":{},\"model\":\"{}\",\"choices\":[{{\"index\":0,\"delta\":{{}},\"finish_reason\":\"{}\"}}]}}\n\n",
                id, created, model_str, finish_reason
            )));

            yield Ok(Bytes::from("data: [DONE]\n\n".to_string()));
        }
    }
}

/// 工具调用缓冲区
#[derive(Debug, Clone)]
struct ToolCallBuffer {
    id: String,
    name: String,
    arguments: String,
    index: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;

    #[tokio::test]
    async fn test_simple_text_stream() {
        // 模拟 Responses API SSE 流
        let sse_data = vec![
            "event: response.created\ndata: {\"response\":{\"id\":\"resp_123\",\"model\":\"gpt-4o\",\"created\":1234567890}}\n\n".into(),
            "event: response.output_text.delta\ndata: {\"delta\":{\"text\":\"Hello\"}}\n\n".into(),
            "event: response.output_text.delta\ndata: {\"delta\":{\"text\":\" World\"}}\n\n".into(),
            "event: response.completed\ndata: {\"status\":\"completed\"}\n\n".into(),
        ];

        let stream = stream::iter(sse_data.into_iter().map(Ok::<Bytes, std::convert::Infallible>));
        let mut output = create_chat_sse_stream_from_responses(stream);

        let mut chunks = Vec::new();
        while let Some(result) = output.next().await {
            if let Ok(chunk) = result {
                chunks.push(String::from_utf8_lossy(&chunk).to_string());
            }
        }

        // 验证输出包含 Chat Completions 格式
        assert!(chunks.iter().any(|c| c.contains("chat.completion.chunk")));
        assert!(chunks.iter().any(|c| c.contains("Hello")));
        assert!(chunks.iter().any(|c| c.contains("[DONE]")));
    }

    #[tokio::test]
    async fn test_stream_with_tool_call() {
        // 模拟带有工具调用的 Responses API SSE 流
        let sse_data = vec![
            "event: response.created\ndata: {\"response\":{\"id\":\"resp_123\",\"model\":\"gpt-4o\"}}\n\n".into(),
            "event: response.output_item.added\ndata: {\"item\":{\"type\":\"function_call\",\"id\":\"item_1\",\"name\":\"get_weather\",\"call_id\":\"call_abc\"}}\n\n".into(),
            "event: response.function_call_arguments.delta\ndata: {\"item_id\":\"item_1\",\"delta\":{\"arguments\":\"{\\\"location\\\": \\\"Beijing\\\"}\"}}\n\n".into(),
            "event: response.completed\ndata: {\"status\":\"completed\"}\n\n".into(),
        ];

        let stream = stream::iter(sse_data.into_iter().map(Ok::<Bytes, std::convert::Infallible>));
        let mut output = create_chat_sse_stream_from_responses(stream);

        let mut chunks = Vec::new();
        while let Some(result) = output.next().await {
            if let Ok(chunk) = result {
                chunks.push(String::from_utf8_lossy(&chunk).to_string());
            }
        }

        // 验证输出包含工具调用
        assert!(chunks.iter().any(|c| c.contains("tool_calls")));
        assert!(chunks.iter().any(|c| c.contains("get_weather")));
    }
}