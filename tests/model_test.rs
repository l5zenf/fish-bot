use fish_core::event::MessageEvent;
use fish_core::message::{MessageChain, MessageSegment};

#[test]
fn test_message_text() {
    let msg = MessageSegment::text("hello");
    assert!(matches!(msg, MessageSegment::Text { .. }));
}

#[test]
fn test_event_fields() {
    let chain = MessageChain::from(MessageSegment::text("hi"));
    let event = MessageEvent::new(
        "cid123".to_string(),
        "user456".to_string(),
        "Alice".to_string(),
        chain,
        serde_json::json!({}),
    );
    assert_eq!(event.sender_name, "Alice");
    assert_eq!(event.plain_text(), "hi");
    assert_eq!(event.cid, "cid123");
    assert_eq!(event.sender_id, "user456");
}
