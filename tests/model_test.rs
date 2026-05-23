use fish_bot::model::{Message, MessageEvent};

#[test]
fn test_message_text() {
    let msg = Message::Text { text: "hello".to_string() };
    assert!(matches!(msg, Message::Text { .. }));
}

#[test]
fn test_event_summary() {
    let event = MessageEvent::new(
        "cid123".to_string(),
        "user456".to_string(),
        "Alice".to_string(),
        vec![Message::Text { text: "hi".to_string() }],
        serde_json::json!({}),
    );
    assert_eq!(event.sender_name, "Alice");
    assert_eq!(event.plain_text(), "hi");
}
