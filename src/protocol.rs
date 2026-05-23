use base64::{Engine as _, engine::general_purpose::STANDARD};
use crate::error::{AppError, Result};
use crate::model::Message;
use serde_json::Value;

/// Encode a Message to fish protocol JSON Value
pub fn encode_message(msg: &Message) -> Result<Value> {
    match msg {
        Message::Text { text } => Ok(serde_json::json!({
            "contentType": 1,
            "text": { "text": text }
        })),
        Message::Image { url, width, height } => Ok(serde_json::json!({
            "contentType": 2,
            "image": {
                "pics": [{"type": 0, "url": url, "width": width, "height": height}]
            }
        })),
        Message::Audio { url, duration_ms } => Ok(serde_json::json!({
            "contentType": 3,
            "audio": { "url": url, "duration": duration_ms }
        })),
        Message::SystemTip { tip_text } => Ok(serde_json::json!({
            "contentType": 14,
            "tip": { "argInfo": {}, "tip": tip_text }
        })),
        Message::FishTradeCard { title, content, order_id, button_text, task_id } => Ok(serde_json::json!({
            "contentType": 26,
            "dxCard": {
                "item": {
                    "main": {
                        "exContent": { "title": title, "desc": content },
                        "clickParam": { "args": { "task_id": task_id } }
                    },
                    "targetUrl": format!("fleamarket://orderDetail?id={}", order_id)
                },
                "button": { "text": button_text, "targetUrl": format!("fleamarket://orderDetail?id={}", order_id) }
            }
        })),
        Message::ItemCard { item_id, title, price, url, main_pic } => Ok(serde_json::json!({
            "contentType": 7,
            "itemCard": {
                "item": { "itemId": item_id, "title": title, "price": price, "mainPic": main_pic },
                "action": { "page": { "url": url } }
            }
        })),
        Message::Custom { segments } => {
            let data = STANDARD.encode(serde_json::to_string(&segments.iter().map(|s| {
                match s {
                    Message::Text { text } => serde_json::json!({"type":"text","text":text}),
                    Message::Image { url, .. } => serde_json::json!({"type":"image","image_url":url}),
                    _ => serde_json::json!({"type":"unknown"})
                }
            }).collect::<Vec<_>>())?);
            Ok(serde_json::json!({
                "contentType": 101,
                "custom": { "type": 2, "data": data }
            }))
        },
        Message::Unknown => Err(AppError::Protocol("Cannot encode unknown message type".into())),
    }
}

/// Decode a fish protocol JSON Value to a Message
pub fn decode_message(payload: &Value) -> Result<Message> {
    let ct = payload.get("contentType").and_then(|v| v.as_i64()).unwrap_or(0);
    match ct {
        1 => Ok(Message::Text { text: payload["text"]["text"].as_str().unwrap_or("").to_string() }),
        2 => {
            let pic = &payload["image"]["pics"][0];
            Ok(Message::Image {
                url: pic["url"].as_str().unwrap_or("").to_string(),
                width: pic["width"].as_u64().unwrap_or(0) as u32,
                height: pic["height"].as_u64().unwrap_or(0) as u32
            })
        },
        3 => {
            let audio = &payload["audio"];
            Ok(Message::Audio {
                url: audio["url"].as_str().unwrap_or("").to_string(),
                duration_ms: audio["duration"].as_u64().unwrap_or(0)
            })
        },
        7 => {
            let item = &payload["itemCard"]["item"];
            Ok(Message::ItemCard {
                item_id: item["itemId"].as_str().unwrap_or("").to_string(),
                title: item["title"].as_str().unwrap_or("").to_string(),
                price: item["price"].as_str().unwrap_or("").to_string(),
                main_pic: item["mainPic"].as_str().unwrap_or("").to_string(),
                url: payload["itemCard"]["action"]["page"]["url"].as_str().unwrap_or("").to_string(),
            })
        },
        14 => Ok(Message::SystemTip { tip_text: payload["tip"]["tip"].as_str().unwrap_or("").to_string() }),
        26 => {
            let main = &payload["dxCard"]["item"]["main"];
            let ex = &main["exContent"];
            let url = main["clickParam"]["args"]["task_id"].as_str().unwrap_or("");
            let order_id = url.split("id=").nth(1).unwrap_or("").to_string();
            Ok(Message::FishTradeCard {
                title: ex["title"].as_str().unwrap_or("").to_string(),
                content: ex["desc"].as_str().unwrap_or("").to_string(),
                order_id: order_id.clone(),
                button_text: payload["dxCard"]["button"]["text"].as_str().unwrap_or("").to_string(),
                task_id: main["clickParam"]["args"]["task_id"].as_str().unwrap_or("").to_string(),
            })
        },
        101 => {
            let data_b64 = payload["custom"]["data"].as_str().unwrap_or("");
            let decoded = STANDARD.decode(data_b64)?;
            let segments: Vec<Value> = serde_json::from_slice(&decoded)?;
            let messages = segments.into_iter().map(|v| {
                match v["type"].as_str().unwrap_or("") {
                    "text" => Message::Text { text: v["text"].as_str().unwrap_or("").to_string() },
                    "image" => Message::Image { url: v["image_url"].as_str().unwrap_or("").to_string(), width: 0, height: 0 },
                    _ => Message::Unknown,
                }
            }).collect();
            Ok(Message::Custom { segments: messages })
        },
        _ => Ok(Message::Unknown),
    }
}
