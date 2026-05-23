use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;

use fish_core::error::Result;
use fish_core::message::MessageSegment;

/// Encode a MessageSegment to fish protocol JSON Value, returning (payload, content_type).
/// Matches Python adapters/fish/message.py auto_encode / FishPayloadNode.encode.
pub fn encode_message(msg: &MessageSegment) -> Result<(Value, i64)> {
    match msg {
        MessageSegment::Text { text } => Ok((
            serde_json::json!({"contentType": 1, "text": {"text": text}}),
            1,
        )),
        MessageSegment::Image { image_url, width, height } => Ok((
            serde_json::json!({
                "contentType": 2,
                "image": { "pics": [{"type": 0, "url": image_url, "width": width, "height": height}] }
            }),
            2,
        )),
        MessageSegment::Audio { audio_url, duration_ms } => Ok((
            serde_json::json!({
                "contentType": 3,
                "audio": { "url": audio_url, "duration": duration_ms }
            }),
            2,
        )),
        MessageSegment::CustomNode { content, .. } => {
            let segments: Vec<Value> = serde_json::from_value(content.clone()).unwrap_or_default();
            let encoded = STANDARD.encode(serde_json::to_string(&segments)?.as_bytes());
            Ok((
                serde_json::json!({
                    "contentType": 101,
                    "custom": { "type": 2, "data": encoded }
                }),
                2,
            ))
        }
    }
}

/// Encode a whole MessageChain for sending (wraps segments in CustomNode if multiple).
pub fn encode_chain(chain: &[MessageSegment]) -> Result<(Value, i64)> {
    if chain.len() == 1 {
        return encode_message(&chain[0]);
    }
    // Multiple segments: encode as Custom
    let items: Vec<Value> = chain
        .iter()
        .map(|seg| match seg {
            MessageSegment::Text { text } => serde_json::json!({"type":"text","text":text}),
            MessageSegment::Image { image_url, .. } => {
                serde_json::json!({"type":"image","image_url":image_url})
            }
            MessageSegment::Audio { audio_url, .. } => {
                serde_json::json!({"type":"audio","audio_url":audio_url})
            }
            MessageSegment::CustomNode { desc, content } => {
                serde_json::json!({"type":"node","desc":desc,"content":content})
            }
        })
        .collect();

    let data = STANDARD.encode(serde_json::to_string(&items)?.as_bytes());
    Ok((
        serde_json::json!({
            "contentType": 101,
            "custom": { "type": 2, "data": data }
        }),
        2,
    ))
}

/// Decode a fish protocol JSON payload into a MessageSegment.
/// Matches Python adapters/fish/message.py Content.to_message_chain / FishPayloadNode.decode.
pub fn decode_message(payload: &Value) -> Result<MessageSegment> {
    let ct = payload.get("contentType").and_then(|v| v.as_i64()).unwrap_or(0);
    match ct {
        1 => {
            let text = payload["text"]["text"].as_str().unwrap_or("").to_string();
            Ok(MessageSegment::Text { text })
        }
        2 => {
            let pic = &payload["image"]["pics"][0];
            Ok(MessageSegment::Image {
                image_url: pic["url"].as_str().unwrap_or("").to_string(),
                width: pic["width"].as_u64().unwrap_or(0) as u32,
                height: pic["height"].as_u64().unwrap_or(0) as u32,
            })
        }
        3 => {
            let audio = &payload["audio"];
            Ok(MessageSegment::Audio {
                audio_url: audio["url"].as_str().unwrap_or("").to_string(),
                duration_ms: audio["duration"].as_u64().unwrap_or(0),
            })
        }
        7 => {
            // ItemCard → encode as CustomNode for core compatibility
            let item = &payload["itemCard"]["item"];
            let card_content = serde_json::json!({
                "fish_type": "item_card",
                "item_id": item["itemId"].as_str().unwrap_or(""),
                "title": item["title"].as_str().unwrap_or(""),
                "price": item["price"].as_str().unwrap_or(""),
                "main_pic": item["mainPic"].as_str().unwrap_or(""),
                "url": payload["itemCard"]["action"]["page"]["url"].as_str().unwrap_or(""),
            });
            Ok(MessageSegment::CustomNode {
                desc: "商品卡片".into(),
                content: card_content,
            })
        }
        14 => {
            let tip_text = payload["tip"]["tip"].as_str().unwrap_or("").to_string();
            let tip_content = serde_json::json!({
                "fish_type": "system_tip",
                "tip_text": tip_text,
            });
            Ok(MessageSegment::CustomNode {
                desc: "系统提示".into(),
                content: tip_content,
            })
        }
        26 => {
            let main = &payload["dxCard"]["item"]["main"];
            let ex = &main["exContent"];
            let button = &payload["dxCard"]["button"];
            let target_url = button["targetUrl"].as_str().unwrap_or("");
            let order_id = target_url.split("id=").nth(1).unwrap_or("").to_string();
            let card_content = serde_json::json!({
                "fish_type": "fish_trade_card",
                "title": ex["title"].as_str().unwrap_or(""),
                "content": ex["desc"].as_str().unwrap_or(""),
                "order_id": order_id,
                "button_text": button["text"].as_str().unwrap_or(""),
                "task_id": main["clickParam"]["args"]["task_id"].as_str().unwrap_or(""),
            });
            Ok(MessageSegment::CustomNode {
                desc: "交易卡片".into(),
                content: card_content,
            })
        }
        101 => {
            // Custom/rich media message
            let data_b64 = payload["custom"]["data"].as_str().unwrap_or("");
            let decoded = STANDARD.decode(data_b64)?;
            let segments: Value = serde_json::from_slice(&decoded)?;
            Ok(MessageSegment::CustomNode {
                desc: String::new(),
                content: segments,
            })
        }
        _ => Ok(MessageSegment::CustomNode {
            desc: "未知消息".into(),
            content: serde_json::json!({}),
        }),
    }
}

/// Parse raw fish protocol body into a Vec<MessageSegment>.
pub fn decode_content(payload: &Value) -> Result<Vec<MessageSegment>> {
    // Check if this is a structured content object with contentType
    if payload.get("contentType").is_some() {
        return Ok(vec![decode_message(payload)?]);
    }
    // Check if array
    if let Some(arr) = payload.as_array() {
        return arr.iter().map(|v| decode_message(v)).collect();
    }
    Ok(vec![decode_message(payload)?])
}
