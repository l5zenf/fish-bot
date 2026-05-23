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
