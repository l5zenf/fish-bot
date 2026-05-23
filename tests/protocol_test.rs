use fish_bot::protocol::{encode_message, decode_message};
use fish_bot::model::Message;

#[test]
fn test_text_encode_decode() {
    let msg = Message::Text { text: "hello".to_string() };
    let encoded = encode_message(&msg).unwrap();
    let decoded = decode_message(&encoded).unwrap();
    assert!(matches!(decoded, Message::Text { text } if text == "hello"));
}
