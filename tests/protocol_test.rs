use fish_runtime::{MessageChain, MessageSegment};

#[test]
fn test_message_chain_summary() {
    let chain = MessageChain::from(MessageSegment::text("hello"));
    assert_eq!(chain.summary(), "hello");
}
