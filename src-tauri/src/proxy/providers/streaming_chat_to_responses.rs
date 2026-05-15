//! OpenAI Chat Completions SSE → Responses API SSE 格式转换模块
//!
//! 实现 Chat Completions 流式 SSE 格式到 Responses API 流式 SSE 格式的转换。
//!
//! 参考: codex-chat-bridge 的 translate_chat_stream 函数

use crate::proxy::sse::{append_utf8_safe, strip_sse_field, take_sse_block};
use bytes::Bytes;
use futures::stream::{Stream, StreamExt};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// 将 SSE 事件 payload 序列化为正确的 SSE 格式（参考 codex-chat-bridge 的 sse_event 函数）
/// 使用 serde_json::to_string 确保正确的 JSON 序列化，避免手动转义导致的格式错误
fn sse_event(event_name: &str, payload: &Value) -> Bytes {
    let json_payload = serde_json::to_string(payload).unwrap_or_else(|_| {
        r#"{"type":"response.failed","response":{"error":{"message":"internal serialization error"}}}"#.to_string()
    });
    Bytes::from(format!("event: {event_name}\ndata: {json_payload}\n\n"))
}

/// 将 Chat Completions SSE 流转换为 Responses API SSE 流（使用 reasoning_split 时）
///
/// 当 MiniMax 启用 reasoning_split=true 时，思考内容通过 reasoning_content 和
/// reasoning_details 字段单独传输，不再使用<think>/</think> 标签嵌入内容中。
pub fn create_responses_sse_stream_from_chat<E: std::error::Error + Send + 'static>(
    stream: impl Stream<Item = Result<Bytes, E>> + Send + 'static,
) -> impl Stream<Item = Result<Bytes, std::io::Error>> + Send {
    async_stream::stream! {
        let mut buffer = String::new();
        let mut utf8_remainder: Vec<u8> = Vec::new();

        let mut response_id: Option<String> = None;
        let mut model: Option<String> = None;
        let mut created: u64 = 0;
        let mut has_sent_created_event = false;
        let mut has_sent_in_progress_event = false;
        let mut content_index: u32 = 0;
        let mut tool_call_buffer: BTreeMap<u32, ToolCallBuffer> = BTreeMap::new();
        let mut saw_done_marker = false;
        let mut saw_terminal_finish_reason = false;
        let mut final_finish_reason: Option<String> = None;
        // Usage tracking from MiniMax response chunks
                let mut usage: Option<UsageInfo> = None;

        // Track text output item state
        let mut text_output_item_added = false;
        let mut text_output_index: u32 = 0;
        let mut accumulated_text = String::new();

        // Track reasoning output item state (for thinking content)
        let mut reasoning_output_item_added = false;
        let mut reasoning_output_index: u32 = 0;
        let mut accumulated_reasoning = String::new();

        // Track if we've seen any reasoning content (to avoid output_index conflict)
        let mut saw_reasoning_content = false;

        #[derive(Debug, Clone)]
        struct UsageInfo {
            prompt_tokens: u32,
            completion_tokens: u32,
            total_tokens: u32,
        }

        tokio::pin!(stream);

        // 首先生成 response_id（参考实现也是先生成 id）
        let resp_id = response_id.clone()
            .unwrap_or_else(|| format!("resp_{}", uuid::Uuid::new_v4().to_string().replace("-", "")[..12].to_string()));

        // 立即发送 response.created 事件
        yield Ok(sse_event(
            "response.created",
            &json!({
                "type": "response.created",
                "response": { "id": resp_id }
            }),
        ));
        has_sent_created_event = true;

        // 发送 response.in_progress 事件（Codex CLI 期望此事件）
        yield Ok(sse_event(
            "response.in_progress",
            &json!({
                "type": "response.in_progress",
                "response": { "id": resp_id }
            }),
        ));
        has_sent_in_progress_event = true;

        let mut chunk_count = 0u64;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    chunk_count += 1;
                    append_utf8_safe(&mut buffer, &mut utf8_remainder, &bytes);

                    while let Some(block) = take_sse_block(&mut buffer) {
                        if block.trim().is_empty() {
                            continue;
                        }

                        let mut data_content: Option<String> = None;

                        for line in block.lines() {
                            if let Some(d) = strip_sse_field(line, "data") {
                                data_content = Some(d.to_string());
                            }
                        }

                        let data_str = match data_content {
                            Some(content) => content,
                            None => continue,
                        };

                        // DEBUG: Log raw MiniMax SSE data
                        log::debug!("[streaming_chat_to_responses] MiniMax SSE raw: id={} model={} chunk={} data={}",
                            response_id.clone().unwrap_or_default(),
                            model.clone().unwrap_or_default(),
                            chunk_count,
                            data_str);

                        // Check for [DONE]
                        if data_str.trim() == "[DONE]" {
                            saw_done_marker = true;
                            log::debug!("[streaming_chat_to_responses] Received [DONE] marker");
                            continue;
                        }

                        let data: Value = match serde_json::from_str(&data_str) {
                            Ok(v) => v,
                            Err(e) => {
                                log::warn!("[streaming_chat_to_responses] JSON parse error: {} data={}", e, data_str);
                                continue;
                            }
                        };

                        // Extract Chat Completions fields
                        if let Some(id) = data.get("id").and_then(|v| v.as_str()) {
                            if response_id.is_none() {
                                response_id = Some(id.to_string());
                            }
                        }
                        if let Some(m) = data.get("model").and_then(|v| v.as_str()) {
                            if model.is_none() {
                                model = Some(m.to_string());
                            }
                        }
                        if let Some(ts) = data.get("created").and_then(|v| v.as_u64()) {
                            if created == 0 {
                                created = ts;
                            }
                        }

                        // Extract usage from MiniMax response (MiniMax sends usage in each chunk)
                        if usage.is_none() {
                            if let Some(usage_data) = data.get("usage") {
                                if !usage_data.is_null() {
                                    let prompt_tokens = usage_data.get("prompt_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                                    let completion_tokens = usage_data.get("completion_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                                    let total_tokens = usage_data.get("total_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                                    usage = Some(UsageInfo { prompt_tokens, completion_tokens, total_tokens });
                                }
                            }
                        }

                        let choices = match data.get("choices").and_then(|c| c.as_array()) {
                            Some(c) => {
                                log::debug!("[streaming_chat_to_responses] Found {} choices", c.len());
                                c
                            }
                            None => {
                                log::debug!("[streaming_chat_to_responses] No choices in data: {}", data_str);
                                continue;
                            }
                        };

                        for choice in choices {
                            let delta = match choice.get("delta") {
                                Some(d) => d,
                                None => {
                                    log::debug!("[streaming_chat_to_responses] Choice has no delta");
                                    continue;
                                }
                            };

                            let _index = choice.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                            let finish_reason = choice.get("finish_reason").and_then(|v| v.as_str());

                            // Track terminal finish reason
                            if let Some(reason) = finish_reason {
                                if !reason.trim().is_empty() {
                                    saw_terminal_finish_reason = true;
                                    final_finish_reason = Some(reason.to_string());
                                }
                            }

                            // Handle text content (with reasoning_split, content is plain text, no more <think> tags)
                            let content_opt = delta.get("content");
                            if let Some(content) = content_opt.and_then(|v| v.as_str()) {
                                if !content.is_empty() {
                                    log::debug!("[streaming_chat_to_responses] Processing content: {}", content);

                                    // Ensure output_item.added is sent BEFORE first delta
                                    if !text_output_item_added {
                                        yield Ok(sse_event(
                                            "response.output_item.added",
                                            &json!({
                                                "type": "response.output_item.added",
                                                "output_index": text_output_index,
                                                "item": {
                                                    "type": "message",
                                                    "role": "assistant",
                                                    "content": [
                                                        { "type": "output_text", "text": "" }
                                                    ]
                                                }
                                            }),
                                        ));
                                        text_output_item_added = true;
                                    }

                                    // Accumulate text for response.output_item.done
                                    accumulated_text.push_str(content);

                                    log::debug!("[streaming_chat_to_responses] Sending output_text.delta: {}", content);
                                    yield Ok(sse_event(
                                        "response.output_text.delta",
                                        &json!({
                                            "type": "response.output_text.delta",
                                            "delta": content
                                        }),
                                    ));
                                }
                            }

                            // Handle reasoning_content field (when reasoning_split is enabled)
                            if let Some(reasoning) = delta.get("reasoning_content").and_then(|v| v.as_str()) {
                                if !reasoning.is_empty() {
                                    accumulated_reasoning.push_str(reasoning);
                                    saw_reasoning_content = true;

                                    // Ensure reasoning output_item.added is sent if not already
                                    if !reasoning_output_item_added {
                                        let reason_idx = if text_output_item_added { 1 } else { 0 };
                                        yield Ok(sse_event(
                                            "response.output_item.added",
                                            &json!({
                                                "type": "response.output_item.added",
                                                "output_index": reason_idx,
                                                "item": {
                                                    "type": "reasoning",
                                                    "id": format!("{}_reasoning", reason_idx),
                                                    "summary": [
                                                        { "type": "summary_text", "text": "" }
                                                    ]
                                                }
                                            }),
                                        ));
                                        reasoning_output_item_added = true;
                                        reasoning_output_index = reason_idx;
                                    }

                                    yield Ok(sse_event(
                                        "response.reasoning_summary_text.delta",
                                        &json!({
                                            "type": "response.reasoning_summary_text.delta",
                                            "item_id": format!("{}_reasoning", reasoning_output_index),
                                            "output_index": reasoning_output_index,
                                            "summary_index": 0,
                                            "delta": reasoning
                                        }),
                                    ));
                                }
                            }

                            // Handle reasoning_details array (structured reasoning from MiniMax)
                            if let Some(details) = delta.get("reasoning_details").and_then(|v| v.as_array()) {
                                for detail in details {
                                    if let Some(text) = detail.get("text").and_then(|t| t.as_str()) {
                                        if !text.is_empty() {
                                            accumulated_reasoning.push_str(text);

                                            if !reasoning_output_item_added {
                                                let reason_idx = if text_output_item_added { 1 } else { 0 };
                                                yield Ok(sse_event(
                                                    "response.output_item.added",
                                                    &json!({
                                                        "type": "response.output_item.added",
                                                        "output_index": reason_idx,
                                                        "item": {
                                                            "type": "reasoning",
                                                            "id": format!("{}_reasoning", reason_idx),
                                                            "summary": [
                                                                { "type": "summary_text", "text": "" }
                                                            ]
                                                        }
                                                    }),
                                                ));
                                                reasoning_output_item_added = true;
                                                reasoning_output_index = reason_idx;
                                            }

                                            yield Ok(sse_event(
                                                "response.reasoning_summary_text.delta",
                                                &json!({
                                                    "type": "response.reasoning_summary_text.delta",
                                                    "item_id": format!("{}_reasoning", reasoning_output_index),
                                                    "output_index": reasoning_output_index,
                                                    "summary_index": 0,
                                                    "delta": text
                                                }),
                                            ));
                                        }
                                    }
                                }
                            }

                            // Handle tool calls - 累积策略（参考 codex-chat-bridge）
                            // 不立即发送 output_item.added，先累积所有数据
                            if let Some(tool_calls) = delta.get("tool_calls").and_then(|t| t.as_array()) {
                                log::info!("[streaming_chat_to_responses] === TOOL_CALL DELTA ===");
                                log::info!("[streaming_chat_to_responses] Raw tool_calls count: {}", tool_calls.len());
                                for tc in tool_calls {
                                    let call_id = tc.get("id").and_then(|v| v.as_str()).unwrap_or("");
                                    let index = tc.get("index").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
                                    let func = tc.get("function");
                                    let name = func.and_then(|f| f.get("name")).and_then(|n| n.as_str()).unwrap_or("");
                                    let args_delta = func.and_then(|f| f.get("arguments")).and_then(|a| a.as_str()).unwrap_or("");

                                    log::info!("[streaming_chat_to_responses] tool_call chunk: index={}, call_id='{}', name='{}', args_delta='{}'",
                                        index, call_id, name, args_delta);

                                    // 累积数据：使用 index 作为 key（参考 codex-chat-bridge 使用 BTreeMap 的逻辑）
                                    let entry = tool_call_buffer.entry(index).or_insert(ToolCallBuffer {
                                        id: String::new(),
                                        name: String::new(),
                                        arguments: String::new(),
                                        index,
                                    });

                                    // 如果有 call_id，更新它
                                    if !call_id.is_empty() {
                                        entry.id = call_id.to_string();
                                        log::info!("[streaming_chat_to_responses] Set entry[{}].id = '{}'", index, entry.id);
                                    }

                                    // 如果有 name，更新它
                                    if !name.is_empty() {
                                        entry.name = name.to_string();
                                        log::info!("[streaming_chat_to_responses] Set entry[{}].name = '{}'", index, entry.name);
                                    }

                                    // 如果有 arguments 增量，追加它
                                    if !args_delta.is_empty() {
                                        log::info!("[streaming_chat_to_responses] Appending args to entry[{}]: '{}'", index, args_delta);
                                        entry.arguments.push_str(&args_delta);
                                        log::info!("[streaming_chat_to_responses] entry[{}].arguments now = '{}' (len={})", index, entry.arguments, entry.arguments.len());
                                    }
                                }
                                log::info!("[streaming_chat_to_responses] === END TOOL_CALL DELTA ===");
                            }
                        }
                    }
                }
                Err(e) => {
                    break;
                }
            }
        }

        // 检查是否正常完成
        log::debug!("[streaming_chat_to_responses] Stream end: saw_done_marker={} saw_terminal_finish_reason={} final_finish_reason={:?}",
            saw_done_marker, saw_terminal_finish_reason, final_finish_reason);
        if !saw_done_marker && !saw_terminal_finish_reason {
            // 流异常结束，发送失败事件
            log::warn!("[streaming_chat_to_responses] Stream ended abnormally - sending response.failed");
            yield Ok(sse_event(
                "response.failed",
                &json!({
                    "type": "response.failed",
                    "response": {
                        "id": resp_id,
                        "error": {
                            "code": "upstream_stream_incomplete",
                            "message": "upstream stream ended before terminal marker"
                        }
                    }
                }),
            ));
            // 发送 [DONE] 标记以正常终止流
            yield Ok(Bytes::from("data: [DONE]\n\n".to_string()));
            return;
        }

        // 发送 response.output_item.done 标记消息完成（Codex CLI 需要此事件来保存会话）
        if text_output_item_added {
            // 参考 codex-chat-bridge: item 中包含完整的 assistant 回复文本，不包含 output_index
            yield Ok(sse_event(
                "response.output_item.done",
                &json!({
                    "type": "response.output_item.done",
                    "item": {
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            { "type": "output_text", "text": accumulated_text }
                        ]
                    }
                }),
            ));
        }

        // 发送 response.reasoning_summary_text.done 标记推理完成
        if reasoning_output_item_added {
            yield Ok(sse_event(
                "response.reasoning_summary_text.done",
                &json!({
                    "type": "response.reasoning_summary_text.done",
                    "item_id": format!("{}_reasoning", reasoning_output_index)
                }),
            ));
        }

        // 发送 response.output_item.added, function_call_arguments.done, output_item.done 标记工具调用完成
        // 按 index 顺序发送（参考 codex-chat-bridge）
        // output_index = base_index + relative_position_in_tool_calls
        log::info!("[streaming_chat_to_responses] === FLUSHING TOOL_CALL BUFFER ===");
        log::info!("[streaming_chat_to_responses] tool_call_buffer size: {}, text_added: {}, reasoning_added: {}",
            tool_call_buffer.len(), text_output_item_added, reasoning_output_item_added);
        let tool_base_index = if text_output_item_added || reasoning_output_item_added { 1 } else { 0 };
        let mut tool_rel_index: u32 = 0;
        for (_, buf) in tool_call_buffer.iter() {
            let output_idx = tool_base_index + tool_rel_index;

            log::info!("[streaming_chat_to_responses] FLUSH entry: index={}, call_id='{}', name='{}', arguments='{}' (len={})",
                buf.index, buf.id, buf.name, buf.arguments, buf.arguments.len());

            // MiniMax 返回的 arguments 可能是序列化后的 JSON 字符串
            // 如果为空则使用 "{}"
            let arguments = if buf.arguments.is_empty() {
                "{}".to_string()
            } else {
                buf.arguments.clone()
            };

            log::info!("[streaming_chat_to_responses] FINAL arguments to send: {}", arguments);

            // 1. 先发送 output_item.added（带完整参数，参考 codex-chat-bridge）
            yield Ok(sse_event(
                "response.output_item.added",
                &json!({
                    "type": "response.output_item.added",
                    "output_index": output_idx,
                    "item": {
                        "type": "function_call",
                        "name": buf.name,
                        "arguments": arguments,
                        "call_id": buf.id
                    }
                }),
            ));

            // 2. 再发送 function_call_arguments.done
            yield Ok(sse_event(
                "response.function_call_arguments.done",
                &json!({
                    "type": "response.function_call_arguments.done",
                    "item_id": buf.id,
                    "output_index": output_idx,
                    "arguments": arguments
                }),
            ));

            // 3. 最后发送 output_item.done（带完整参数）
            yield Ok(sse_event(
                "response.output_item.done",
                &json!({
                    "type": "response.output_item.done",
                    "output_index": output_idx,
                    "item": {
                        "type": "function_call",
                        "name": buf.name,
                        "arguments": arguments,
                        "call_id": buf.id
                    }
                }),
            ));

            tool_rel_index += 1;
        }
        log::info!("[streaming_chat_to_responses] === END FLUSH TOOL_CALL BUFFER ===");

        // 构建 usage JSON（参考实现的格式）
        let usage_value = usage.map(|u| {
            json!({
                "input_tokens": u.prompt_tokens,
                "input_tokens_details": Value::Null,
                "output_tokens": u.completion_tokens,
                "output_tokens_details": Value::Null,
                "total_tokens": u.total_tokens
            })
        });

        log::debug!("[streaming_chat_to_responses] Stream completed normally - sending response.completed");
        yield Ok(sse_event(
            "response.completed",
            &json!({
                "type": "response.completed",
                "response": {
                    "id": resp_id,
                    "usage": usage_value
                }
            }),
        ));
    }
}

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
    async fn test_chat_to_responses_stream() {
        let sse_data = vec![
            "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1234567890,\"model\":\"codex-MiniMax-M2.7\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n".into(),
            "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1234567890,\"model\":\"codex-MiniMax-M2.7\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" World\"},\"finish_reason\":null}]}\n\n".into(),
            "data: {\"id\":\"chatcmpl-123\",\"object\":\"chat.completion.chunk\",\"created\":1234567890,\"model\":\"codex-MiniMax-M2.7\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n".into(),
            "data: [DONE]\n\n".into(),
        ];

        let stream = stream::iter(sse_data.into_iter().map(Ok::<Bytes, std::convert::Infallible>));
        let mut output = create_responses_sse_stream_from_chat(stream);

        let mut events = Vec::new();
        while let Some(result) = output.next().await {
            if let Ok(chunk) = result {
                events.push(String::from_utf8_lossy(&chunk).to_string());
            }
        }

        assert!(events.iter().any(|e| e.contains("response.created")));
        assert!(events.iter().any(|e| e.contains("response.output_text.delta")));
        assert!(events.iter().any(|e| e.contains("Hello")));
        assert!(events.iter().any(|e| e.contains("response.completed")));
    }
}