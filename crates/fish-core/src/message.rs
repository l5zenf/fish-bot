use serde::{Deserialize, Serialize};

/// All message segment types (core), matching Python message.py
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MessageSegment {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "image")]
    Image {
        image_url: String,
        #[serde(default)]
        width: u32,
        #[serde(default)]
        height: u32,
    },
    #[serde(rename = "audio")]
    Audio {
        audio_url: String,
        #[serde(default)]
        duration_ms: u64,
    },
    #[serde(rename = "node")]
    CustomNode {
        #[serde(default)]
        desc: String,
        #[serde(default)]
        content: serde_json::Value,
    },
}

impl MessageSegment {
    pub fn text(text: impl Into<String>) -> Self {
        MessageSegment::Text {
            text: text.into(),
        }
    }

    pub fn image(image_url: impl Into<String>) -> Self {
        MessageSegment::Image {
            image_url: image_url.into(),
            width: 0,
            height: 0,
        }
    }

    pub fn desc(&self) -> &str {
        match self {
            MessageSegment::Text { .. } => "文本",
            MessageSegment::Image { .. } => "图片",
            MessageSegment::Audio { .. } => "音频",
            MessageSegment::CustomNode { desc, .. } => {
                if desc.is_empty() {
                    "节点消息"
                } else {
                    desc.as_str()
                }
            }
        }
    }

    pub fn summary(&self) -> String {
        match self {
            MessageSegment::Text { text } => text.clone(),
            other => format!("[{}]", other.desc()),
        }
    }
}

/// Core message chain, matching Python MessageChain
#[derive(Debug, Clone, Default)]
pub struct MessageChain {
    segments: Vec<MessageSegment>,
}

impl MessageChain {
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
        }
    }

    pub fn from_segment(seg: MessageSegment) -> Self {
        Self {
            segments: vec![seg],
        }
    }

    pub fn append(&mut self, item: impl Into<MessageChainItem>) {
        match item.into() {
            MessageChainItem::Segment(seg) => self.segments.push(seg),
            MessageChainItem::Chain(chain) => self.segments.extend(chain.segments),
        }
    }

    pub fn extend(&mut self, items: impl IntoIterator<Item = impl Into<MessageChainItem>>) {
        for item in items {
            self.append(item);
        }
    }

    pub fn segments(&self) -> &[MessageSegment] {
        &self.segments
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn plain_text(&self) -> String {
        self.segments
            .iter()
            .filter_map(|seg| match seg {
                MessageSegment::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("")
    }

    pub fn summary(&self) -> String {
        if self.segments.is_empty() {
            return "(空消息)".to_string();
        }
        let mut result = String::new();
        for seg in &self.segments {
            match seg {
                MessageSegment::Text { text } => {
                    result.push_str(text);
                    result.push(' ');
                }
                other => {
                    result.push_str(&format!("[{}] ", other.desc()));
                }
            }
        }
        result.trim().to_string()
    }

    pub fn has_image(&self) -> bool {
        self.segments
            .iter()
            .any(|seg| matches!(seg, MessageSegment::Image { .. }))
    }
}

impl From<MessageSegment> for MessageChain {
    fn from(seg: MessageSegment) -> Self {
        Self::from_segment(seg)
    }
}

impl From<Vec<MessageSegment>> for MessageChain {
    fn from(segments: Vec<MessageSegment>) -> Self {
        Self { segments }
    }
}

impl From<String> for MessageChain {
    fn from(text: String) -> Self {
        Self::from_segment(MessageSegment::Text { text })
    }
}

impl From<&str> for MessageChain {
    fn from(text: &str) -> Self {
        Self::from_segment(MessageSegment::Text {
            text: text.to_string(),
        })
    }
}

/// Helper for type-safe append operations.
pub enum MessageChainItem {
    Segment(MessageSegment),
    Chain(MessageChain),
}

impl From<MessageSegment> for MessageChainItem {
    fn from(seg: MessageSegment) -> Self {
        MessageChainItem::Segment(seg)
    }
}

impl From<MessageChain> for MessageChainItem {
    fn from(chain: MessageChain) -> Self {
        MessageChainItem::Chain(chain)
    }
}

impl From<String> for MessageChainItem {
    fn from(text: String) -> Self {
        MessageChainItem::Segment(MessageSegment::Text { text })
    }
}

impl From<&str> for MessageChainItem {
    fn from(text: &str) -> Self {
        MessageChainItem::Segment(MessageSegment::Text {
            text: text.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- MessageSegment tests ----

    #[test]
    fn t1_1_text_construction_and_serde() -> anyhow::Result<()> {
        let seg = MessageSegment::text("hello");
        assert!(matches!(seg, MessageSegment::Text { ref text } if text == "hello"));

        let json = serde_json::to_string(&seg)?;
        let deser: MessageSegment = serde_json::from_str(&json)?;
        assert!(matches!(deser, MessageSegment::Text { ref text } if text == "hello"));
        Ok(())
    }

    #[test]
    fn t1_2_image_construction_and_serde() -> anyhow::Result<()> {
        let seg = MessageSegment::image("https://example.com/pic.jpg");
        assert!(matches!(seg, MessageSegment::Image { ref image_url, width: 0, height: 0 }
            if image_url == "https://example.com/pic.jpg"));

        let json = serde_json::to_string(&seg)?;
        let deser: MessageSegment = serde_json::from_str(&json)?;
        assert!(matches!(deser, MessageSegment::Image { ref image_url, width: 0, height: 0 }
            if image_url == "https://example.com/pic.jpg"));
        Ok(())
    }

    #[test]
    fn t1_3_audio_and_customnode_construction() -> anyhow::Result<()> {
        let audio = MessageSegment::Audio {
            audio_url: "https://example.com/audio.mp3".into(),
            duration_ms: 5000,
        };
        assert!(matches!(&audio, MessageSegment::Audio { audio_url, duration_ms: 5000 }
            if audio_url == "https://example.com/audio.mp3"));

        let custom = MessageSegment::CustomNode {
            desc: "test".into(),
            content: serde_json::json!({"key": "value"}),
        };
        assert!(matches!(&custom, MessageSegment::CustomNode { desc, .. } if desc == "test"));

        for seg in [audio, custom] {
            let json = serde_json::to_string(&seg)?;
            let deser: MessageSegment = serde_json::from_str(&json)?;
            assert_eq!(seg.desc(), deser.desc());
        }
        Ok(())
    }

    #[test]
    fn t1_4_desc_labels() {
        assert_eq!(MessageSegment::text("hi").desc(), "文本");
        assert_eq!(MessageSegment::image("url").desc(), "图片");
        assert_eq!(
            MessageSegment::Audio { audio_url: "url".into(), duration_ms: 0 }.desc(),
            "音频"
        );
        assert_eq!(
            MessageSegment::CustomNode { desc: "".into(), content: serde_json::json!({}) }.desc(),
            "节点消息"
        );
        assert_eq!(
            MessageSegment::CustomNode { desc: "卡片".into(), content: serde_json::json!({}) }.desc(),
            "卡片"
        );
    }

    #[test]
    fn t1_5_summary() {
        assert_eq!(MessageSegment::text("hello").summary(), "hello");
        assert_eq!(MessageSegment::image("url").summary(), "[图片]");
        assert_eq!(
            MessageSegment::Audio { audio_url: "url".into(), duration_ms: 0 }.summary(),
            "[音频]"
        );
        assert_eq!(
            MessageSegment::CustomNode { desc: "test".into(), content: serde_json::json!({}) }.summary(),
            "[test]"
        );
    }

    // ---- MessageChain tests ----

    #[test]
    fn t1_6_chain_new_from_segment_is_empty() {
        let chain = MessageChain::new();
        assert!(chain.is_empty());

        let chain = MessageChain::from_segment(MessageSegment::text("hi"));
        assert!(!chain.is_empty());
    }

    #[test]
    fn t1_7_chain_append() {
        let mut chain = MessageChain::new();
        chain.append(MessageSegment::text("a"));
        chain.append("b");
        chain.append(String::from("c"));

        assert_eq!(chain.plain_text(), "abc");
        assert_eq!(chain.segments().len(), 3);
    }

    #[test]
    fn t1_8_chain_extend() {
        let mut chain = MessageChain::new();
        chain.extend(vec![
            MessageSegment::text("a"),
            MessageSegment::text("b"),
            MessageSegment::text("c"),
        ]);
        assert_eq!(chain.plain_text(), "abc");
        assert_eq!(chain.segments().len(), 3);
    }

    #[test]
    fn t1_9_chain_plain_text_only_text() {
        let mut chain = MessageChain::new();
        chain.append(MessageSegment::text("hello"));
        chain.append(MessageSegment::image("pic.jpg"));
        chain.append(MessageSegment::text(" world"));

        assert_eq!(chain.plain_text(), "hello world");
    }

    #[test]
    fn t1_10_chain_summary() {
        // Empty
        let chain = MessageChain::new();
        assert_eq!(chain.summary(), "(空消息)");

        // Plain text only
        let mut chain = MessageChain::new();
        chain.append("hello");
        assert_eq!(chain.summary(), "hello");

        // Mixed
        let mut chain = MessageChain::new();
        chain.append(MessageSegment::text("check this"));
        chain.append(MessageSegment::image("pic.jpg"));
        assert_eq!(chain.summary(), "check this [图片]");
    }

    #[test]
    fn t1_11_has_image() {
        let mut chain = MessageChain::new();
        assert!(!chain.has_image());

        chain.append(MessageSegment::text("no pic"));
        assert!(!chain.has_image());

        chain.append(MessageSegment::image("pic.jpg"));
        assert!(chain.has_image());
    }

    #[test]
    fn t1_12_from_impls_message_chain() {
        // String -> MessageChain
        let chain: MessageChain = String::from("hello").into();
        assert_eq!(chain.plain_text(), "hello");

        // &str -> MessageChain
        let chain: MessageChain = "world".into();
        assert_eq!(chain.plain_text(), "world");

        // MessageSegment -> MessageChain
        let chain: MessageChain = MessageSegment::image("pic.jpg").into();
        assert!(chain.has_image());

        // Vec<MessageSegment> -> MessageChain
        let chain: MessageChain = vec![
            MessageSegment::text("a"),
            MessageSegment::text("b"),
        ].into();
        assert_eq!(chain.plain_text(), "ab");
    }

    #[test]
    fn t1_13_message_chain_item_from() {
        // All From impls for MessageChainItem
        let _: MessageChainItem = MessageSegment::text("x").into();
        let _: MessageChainItem = MessageChain::new().into();
        let _: MessageChainItem = String::from("x").into();
        let _: MessageChainItem = "x".into();
    }
}
