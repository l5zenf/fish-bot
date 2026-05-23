use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;

use fish_core::error::Result;
use fish_core::message::MessageSegment;

/// Encode a MessageSegment to fish protocol JSON Value, returning (payload, content_type).
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
        return arr.iter().map(decode_message).collect();
    }
    Ok(vec![decode_message(payload)?])
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! seg_text {
        ($t:expr) => { MessageSegment::Text { text: $t.into() } };
    }

    fn make_ct_payload(ct: i64, extra: serde_json::Value) -> Value {
        let mut map = serde_json::Map::new();
        map.insert("contentType".into(), serde_json::json!(ct));
        if let Some(obj) = extra.as_object() {
            for (k, v) in obj {
                map.insert(k.clone(), v.clone());
            }
        }
        Value::Object(map)
    }

    #[test]
    fn t3_3_encode_text() -> anyhow::Result<()> {
        let msg = seg_text!("hello");
        let (payload, ct) = encode_message(&msg)?;
        assert_eq!(ct, 1);
        assert_eq!(payload["contentType"], 1);
        assert_eq!(payload["text"]["text"], "hello");
        Ok(())
    }

    #[test]
    fn t3_4_encode_image() -> anyhow::Result<()> {
        let msg = MessageSegment::Image {
            image_url: "https://example.com/pic.jpg".into(),
            width: 800,
            height: 600,
        };
        let (payload, ct) = encode_message(&msg)?;
        assert_eq!(ct, 2);
        assert_eq!(payload["contentType"], 2);
        assert_eq!(payload["image"]["pics"][0]["url"], "https://example.com/pic.jpg");
        assert_eq!(payload["image"]["pics"][0]["width"], 800);
        assert_eq!(payload["image"]["pics"][0]["height"], 600);
        Ok(())
    }

    #[test]
    fn t3_5_encode_audio() -> anyhow::Result<()> {
        let msg = MessageSegment::Audio {
            audio_url: "https://example.com/a.mp3".into(),
            duration_ms: 5000,
        };
        let (payload, ct) = encode_message(&msg)?;
        assert_eq!(ct, 2);
        assert_eq!(payload["contentType"], 3);
        assert_eq!(payload["audio"]["url"], "https://example.com/a.mp3");
        assert_eq!(payload["audio"]["duration"], 5000);
        Ok(())
    }

    #[test]
    fn t3_6_encode_custom_node() -> anyhow::Result<()> {
        let content = serde_json::json!([{"type": "text", "text": "hello"}]);
        let msg = MessageSegment::CustomNode {
            desc: "test".into(),
            content: content.clone(),
        };
        let (payload, ct) = encode_message(&msg)?;
        assert_eq!(ct, 2);
        assert_eq!(payload["contentType"], 101);
        let data = payload["custom"]["data"].as_str().ok_or_else(|| anyhow::anyhow!("custom.data should be a string"))?;
        let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data)?;
        let parsed: Value = serde_json::from_slice(&decoded)?;
        assert_eq!(parsed[0]["text"], "hello");
        Ok(())
    }

    #[test]
    fn t3_7_encode_chain_single() -> anyhow::Result<()> {
        let chain = [seg_text!("hello")];
        let (payload, ct) = encode_chain(&chain)?;
        assert_eq!(ct, 1);
        assert_eq!(payload["text"]["text"], "hello");
        Ok(())
    }

    #[test]
    fn t3_8_encode_chain_multi() -> anyhow::Result<()> {
        let chain = [seg_text!("a"), seg_text!("b")];
        let (payload, ct) = encode_chain(&chain)?;
        assert_eq!(ct, 2);
        assert_eq!(payload["contentType"], 101);
        let data = payload["custom"]["data"].as_str().ok_or_else(|| anyhow::anyhow!("custom.data should be a string"))?;
        let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data)?;
        let parsed: Value = serde_json::from_slice(&decoded)?;
        assert_eq!(parsed[0]["text"], "a");
        assert_eq!(parsed[1]["text"], "b");
        Ok(())
    }

    #[test]
    fn t3_9_decode_text() -> anyhow::Result<()> {
        let payload = make_ct_payload(1, serde_json::json!({"text": {"text": "hello"}}));
        let decoded = decode_message(&payload)?;
        assert!(matches!(decoded, MessageSegment::Text { ref text } if text == "hello"));
        Ok(())
    }

    #[test]
    fn t3_10_decode_image() -> anyhow::Result<()> {
        let payload = make_ct_payload(2, serde_json::json!({
            "image": {"pics": [{"url": "pic.jpg", "width": 800, "height": 600}]}
        }));
        let decoded = decode_message(&payload)?;
        assert!(matches!(decoded, MessageSegment::Image { ref image_url, width: 800, height: 600 }
            if image_url == "pic.jpg"));
        Ok(())
    }

    #[test]
    fn t3_11_decode_audio() -> anyhow::Result<()> {
        let payload = make_ct_payload(3, serde_json::json!({
            "audio": {"url": "a.mp3", "duration": 5000}
        }));
        let decoded = decode_message(&payload)?;
        assert!(matches!(decoded, MessageSegment::Audio { ref audio_url, duration_ms: 5000 }
            if audio_url == "a.mp3"));
        Ok(())
    }

    #[test]
    fn t3_12_decode_item_card() -> anyhow::Result<()> {
        let payload = make_ct_payload(7, serde_json::json!({
            "itemCard": {
                "item": {"itemId": "123", "title": "test item", "price": "100", "mainPic": "pic.jpg"},
                "action": {"page": {"url": "https://item.page"}}
            }
        }));
        let decoded = decode_message(&payload)?;
        assert!(matches!(&decoded, MessageSegment::CustomNode { desc, .. } if desc == "商品卡片"));
        if let MessageSegment::CustomNode { content, .. } = &decoded {
            assert_eq!(content["fish_type"], "item_card");
            assert_eq!(content["item_id"], "123");
        }
        Ok(())
    }

    #[test]
    fn t3_13_decode_system_tip() -> anyhow::Result<()> {
        let payload = make_ct_payload(14, serde_json::json!({
            "tip": {"tip": "system message"}
        }));
        let decoded = decode_message(&payload)?;
        assert!(matches!(&decoded, MessageSegment::CustomNode { desc, .. } if desc == "系统提示"));
        Ok(())
    }

    #[test]
    fn t3_14_decode_fish_trade_card() -> anyhow::Result<()> {
        let payload = make_ct_payload(26, serde_json::json!({
            "dxCard": {
                "item": {
                    "main": {
                        "exContent": {"title": "order title", "desc": "order desc"},
                        "clickParam": {"args": {"task_id": "task123"}}
                    }
                },
                "button": {"text": "查看", "targetUrl": "https://example.com?id=order456"}
            }
        }));
        let decoded = decode_message(&payload)?;
        assert!(matches!(&decoded, MessageSegment::CustomNode { desc, .. } if desc == "交易卡片"));
        if let MessageSegment::CustomNode { content, .. } = &decoded {
            assert_eq!(content["order_id"], "order456");
            assert_eq!(content["task_id"], "task123");
        }
        Ok(())
    }

    #[test]
    fn t3_15_decode_custom_101() -> anyhow::Result<()> {
        let inner = serde_json::json!([{"type":"text","text":"hello"}]);
        let inner_str = serde_json::to_string(&inner)?;
        let data = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            inner_str,
        );
        let payload = make_ct_payload(101, serde_json::json!({
            "custom": {"type": 2, "data": data}
        }));
        let decoded = decode_message(&payload)?;
        assert!(matches!(&decoded, MessageSegment::CustomNode { .. }));
        if let MessageSegment::CustomNode { content, .. } = &decoded {
            assert_eq!(content[0]["text"], "hello");
        }
        Ok(())
    }

    #[test]
    fn t3_16_decode_unknown_ct() -> anyhow::Result<()> {
        let payload = make_ct_payload(999, serde_json::json!({}));
        let decoded = decode_message(&payload)?;
        assert!(matches!(&decoded, MessageSegment::CustomNode { desc, .. } if desc == "未知消息"));
        Ok(())
    }

    #[test]
    fn t3_17_decode_content_variants() -> anyhow::Result<()> {
        let payload = make_ct_payload(1, serde_json::json!({"text": {"text": "hi"}}));
        let segs = decode_content(&payload)?;
        assert_eq!(segs.len(), 1);

        let arr = serde_json::json!([
            {"contentType": 1, "text": {"text": "a"}},
            {"contentType": 1, "text": {"text": "b"}},
        ]);
        let segs = decode_content(&arr)?;
        assert_eq!(segs.len(), 2);

        let empty = serde_json::json!({});
        let segs = decode_content(&empty)?;
        assert_eq!(segs.len(), 1);
        assert!(matches!(&segs[0], MessageSegment::CustomNode { desc, .. } if desc == "未知消息"));
        Ok(())
    }

    #[test]
    fn t3_18_encode_decode_roundtrip() -> anyhow::Result<()> {
        let cases = vec![
            seg_text!("hello"),
            MessageSegment::Image {
                image_url: "https://pic.jpg".into(),
                width: 100,
                height: 200,
            },
            MessageSegment::Audio {
                audio_url: "https://a.mp3".into(),
                duration_ms: 3000,
            },
        ];

        for original in cases {
            let (payload, _) = encode_message(&original)?;
            let decoded = decode_message(&payload)?;
            assert_eq!(original.desc(), decoded.desc());
            assert_eq!(original.summary(), decoded.summary());
        }
        Ok(())
    }

    #[test]
    fn t3_40_decode_empty_custom_data() -> anyhow::Result<()> {
        let payload = make_ct_payload(101, serde_json::json!({
            "custom": {"type": 2, "data": ""}
        }));
        let result = decode_message(&payload);
        assert!(result.is_err(), "empty base64 custom data should produce an error");
        Ok(())
    }

    #[test]
    fn t3_41_decode_content_empty_object() -> anyhow::Result<()> {
        let payload = serde_json::json!({});
        let segs = decode_content(&payload)?;
        assert_eq!(segs.len(), 1);
        assert!(matches!(&segs[0], MessageSegment::CustomNode { desc, .. } if desc == "未知消息"));
        Ok(())
    }

    #[test]
    fn t3_42_encode_chain_mixed_types() -> anyhow::Result<()> {
        let chain = [
            MessageSegment::text("hello"),
            MessageSegment::Image { image_url: "pic.jpg".into(), width: 100, height: 200 },
            MessageSegment::Audio { audio_url: "a.mp3".into(), duration_ms: 5000 },
        ];
        let (payload, ct) = encode_chain(&chain)?;
        assert_eq!(ct, 2, "multi-segment should use custom type");
        assert_eq!(payload["contentType"], 101);
        let data = payload["custom"]["data"].as_str().ok_or_else(|| anyhow::anyhow!("missing data"))?;
        let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data)?;
        let parsed: Value = serde_json::from_slice(&decoded)?;
        assert_eq!(parsed[0]["type"], "text");
        assert_eq!(parsed[1]["type"], "image");
        assert_eq!(parsed[2]["type"], "audio");
        Ok(())
    }

    #[test]
    fn t3_43_decode_content_array_mixed() -> anyhow::Result<()> {
        let arr = serde_json::json!([
            {"contentType": 1, "text": {"text": "hi"}},
            {"contentType": 1, "text": {"text": "there"}},
        ]);
        let segs = decode_content(&arr)?;
        assert_eq!(segs.len(), 2);
        Ok(())
    }

    #[test]
    fn t3_44_decode_system_tip_content() -> anyhow::Result<()> {
        let payload = make_ct_payload(14, serde_json::json!({
            "tip": {"tip": "test tip message"}
        }));
        let decoded = decode_message(&payload)?;
        if let MessageSegment::CustomNode { desc, content } = &decoded {
            assert_eq!(desc, "系统提示");
            assert_eq!(content["tip_text"], "test tip message");
            assert_eq!(content["fish_type"], "system_tip");
        }
        Ok(())
    }

    #[test]
    fn t3_45_decode_fish_trade_card_content() -> anyhow::Result<()> {
        let payload = make_ct_payload(26, serde_json::json!({
            "dxCard": {
                "item": {
                    "main": {
                        "exContent": {"title": "订单", "desc": "描述"},
                        "clickParam": {"args": {"task_id": "task999"}}
                    }
                },
                "button": {"text": "去查看", "targetUrl": "https://example.com?id=order123"}
            }
        }));
        let decoded = decode_message(&payload)?;
        if let MessageSegment::CustomNode { desc, content } = &decoded {
            assert_eq!(desc, "交易卡片");
            assert_eq!(content["fish_type"], "fish_trade_card");
            assert_eq!(content["order_id"], "order123");
            assert_eq!(content["task_id"], "task999");
        }
        Ok(())
    }

    #[test]
    fn t3_46_decode_item_card_content() -> anyhow::Result<()> {
        let payload = make_ct_payload(7, serde_json::json!({
            "itemCard": {
                "item": {"itemId": "456", "title": "商品", "price": "99", "mainPic": "img.jpg"},
                "action": {"page": {"url": "https://item.example.com"}}
            }
        }));
        let decoded = decode_message(&payload)?;
        if let MessageSegment::CustomNode { desc, content } = &decoded {
            assert_eq!(desc, "商品卡片");
            assert_eq!(content["fish_type"], "item_card");
            assert_eq!(content["item_id"], "456");
        }
        Ok(())
    }

    #[test]
    fn t3_47_decode_image_missing_fields() -> anyhow::Result<()> {
        // Image with minimal fields
        let payload = make_ct_payload(2, serde_json::json!({
            "image": {"pics": [{"url": "minimal.jpg"}]}
        }));
        let decoded = decode_message(&payload)?;
        if let MessageSegment::Image { image_url, width, height } = &decoded {
            assert_eq!(image_url, "minimal.jpg");
            assert_eq!(*width, 0);
            assert_eq!(*height, 0);
        }
        Ok(())
    }

    #[test]
    fn t3_48_decode_audio_missing_fields() -> anyhow::Result<()> {
        let payload = make_ct_payload(3, serde_json::json!({
            "audio": {"url": "sound.mp3"}
        }));
        let decoded = decode_message(&payload)?;
        if let MessageSegment::Audio { audio_url, duration_ms } = &decoded {
            assert_eq!(audio_url, "sound.mp3");
            assert_eq!(*duration_ms, 0);
        }
        Ok(())
    }

    #[test]
    fn t3_49_decode_fish_trade_card_minimal() -> anyhow::Result<()> {
        let payload = make_ct_payload(26, serde_json::json!({
            "dxCard": {
                "item": {
                    "main": {
                        "exContent": {},
                        "clickParam": {"args": {}}
                    }
                },
                "button": {}
            }
        }));
        let decoded = decode_message(&payload)?;
        if let MessageSegment::CustomNode { desc, content } = &decoded {
            assert_eq!(desc, "交易卡片");
            assert_eq!(content["order_id"], "");
            assert_eq!(content["task_id"], "");
        }
        Ok(())
    }
}
