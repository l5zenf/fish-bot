use std::sync::Arc;

use fish_core::error::{AppError, Result};
use futures::stream::SplitStream;
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::interval;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use super::sign::generate_mid;

type WsWriter = futures::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    WsMessage,
>;
type WsReader = SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Raw WebSocket transport layer for the fish protocol.
/// Handles connection, sending, handshake, and heartbeat.
pub(crate) struct FishConnection {
    ws_writer: RwLock<Option<WsWriter>>,
}

impl FishConnection {
    pub(crate) fn new() -> Self {
        Self {
            ws_writer: RwLock::new(None),
        }
    }

    /// Send a serialized JSON value over the active WebSocket.
    pub(crate) async fn send(&self, msg: &Value) -> Result<()> {
        let mut writer = self.ws_writer.write().await;
        match writer.as_mut() {
            Some(w) => {
                let text = serde_json::to_string(msg)?;
                w.send(WsMessage::Text(text))
                    .await
                    .map_err(|e| AppError::ws(e.to_string()))?;
                Ok(())
            }
            None => Err(AppError::protocol("WebSocket not connected")),
        }
    }

    /// Open a WebSocket connection, split the stream, store the writer, return the reader.
    pub(crate) async fn connect(&self, url: &str) -> Result<WsReader> {
        use tokio_tungstenite::connect_async;
        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| AppError::ws(e.to_string()))?;
        let (writer, reader) = ws_stream.split();
        let mut guard = self.ws_writer.write().await;
        *guard = Some(writer);
        Ok(reader)
    }

    /// Perform the fish-specific WS handshake: /reg + sync ackDiff.
    pub(crate) async fn handshake(&self, token: &str, device_id: &str) -> Result<()> {
        let reg_msg = build_registration_message(token, device_id);
        self.send(&reg_msg).await?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let sync_msg = build_sync_ack_message(now);
        self.send(&sync_msg).await?;
        Ok(())
    }

    /// Spawn a background task that sends heartbeat frames every 15 seconds.
    /// Requires `self: &Arc<Self>` so the spawned task can hold its own reference.
    pub(crate) fn spawn_heartbeat(self: &Arc<Self>) {
        let hb_self = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = interval(std::time::Duration::from_secs(15));
            loop {
                ticker.tick().await;
                let hb = serde_json::json!({
                    "lwp": "/!",
                    "headers": { "mid": generate_mid() }
                });
                if hb_self.send(&hb).await.is_err() {
                    break;
                }
                tracing::debug!("Heartbeat sent");
            }
        });
    }
}

fn build_registration_message(token: &str, device_id: &str) -> Value {
    serde_json::json!({
        "lwp": "/reg",
        "headers": {
            "cache-header": "app-key token ua wv",
            "app-key": "444e9908a51d1cb236a27862abc769c9",
            "token": token,
            "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 DingTalk(2.1.5) OS(Windows/10) Browser(Chrome/133.0.0.0) DingWeb/2.1.5 IMPaaS DingWeb/2.1.5",
            "dt": "j",
            "wv": "im:3,au:3,sy:6",
            "sync": "0,0;0;0;",
            "did": device_id,
            "mid": generate_mid(),
        }
    })
}

fn build_sync_ack_message(now_ms: u64) -> Value {
    serde_json::json!({
        "lwp": "/r/SyncStatus/ackDiff",
        "headers": { "mid": generate_mid() },
        "body": [
            {
                "pipeline": "sync",
                "tooLong2Tag": "PNM,1",
                "channel": "sync",
                "topic": "sync",
                "highPts": 0,
                "pts": now_ms * 1000,
                "seq": 0,
                "timestamp": now_ms,
            }
        ]
    })
}

impl Default for FishConnection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn t_connection_new() {
        let conn = FishConnection::new();
        assert!(conn.ws_writer.read().await.is_none());
    }

    #[tokio::test]
    async fn t_connection_default() {
        let conn = FishConnection::default();
        assert!(conn.ws_writer.read().await.is_none());
    }

    #[test]
    fn t_connection_registration_message_matches_reference_shape() {
        let msg = build_registration_message("token-123", "device-456");
        assert_eq!(msg["lwp"], "/reg");
        assert_eq!(msg["headers"]["token"], "token-123");
        assert_eq!(msg["headers"]["did"], "device-456");
        assert_eq!(msg["headers"]["sync"], "0,0;0;0;");
    }

    #[test]
    fn t_connection_sync_ack_message_matches_reference_shape() {
        let msg = build_sync_ack_message(1_700_000_000_000);
        assert_eq!(msg["lwp"], "/r/SyncStatus/ackDiff");
        assert_eq!(msg["body"][0]["pipeline"], "sync");
        assert_eq!(msg["body"][0]["pts"], 1_700_000_000_000u64 * 1000);
        assert_eq!(msg["body"][0]["timestamp"], 1_700_000_000_000u64);
    }
}
