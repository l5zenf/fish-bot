use fish_core::message::MessageSegment;
use fish_runtime::fish::protocol::{decode_message, encode_message};

#[test]
fn test_text_encode_decode() -> anyhow::Result<()> {
    let msg = MessageSegment::Text {
        text: "hello".to_string(),
    };
    let (encoded, _) = encode_message(&msg)?;
    let decoded = decode_message(&encoded)?;
    assert!(matches!(decoded, MessageSegment::Text { text } if text == "hello"));
    Ok(())
}
