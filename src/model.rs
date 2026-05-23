use serde_json::Value;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum Message {
    Text { text: String },
    Image { url: String, width: u32, height: u32 },
    Audio { url: String, duration_ms: u64 },
    ItemCard {
        item_id: String,
        title: String,
        price: String,
        url: String,
        main_pic: String,
    },
    SystemTip { tip_text: String },
    FishTradeCard {
        title: String,
        content: String,
        order_id: String,
        button_text: String,
        task_id: String,
    },
    Custom { segments: Vec<Message> },
    Unknown,
}

#[derive(Debug, Clone)]
pub struct MessageEvent {
    pub cid: String,
    pub sender_id: String,
    pub sender_name: String,
    pub messages: Vec<Message>,
    pub raw_payload: Value,
}

impl MessageEvent {
    pub fn new(cid: String, sender_id: String, sender_name: String, messages: Vec<Message>, raw_payload: Value) -> Self {
        Self { cid, sender_id, sender_name, messages, raw_payload }
    }

    pub fn plain_text(&self) -> String {
        self.messages.iter().filter_map(|m| match m {
            Message::Text { text } => Some(text.as_str()),
            _ => None,
        }).collect::<Vec<_>>().join(" ")
    }
}
