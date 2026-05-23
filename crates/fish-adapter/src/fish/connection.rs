use std::sync::Arc;

use fish_core::error::{AppError, Result};
use futures::stream::SplitStream;
use futures::{SinkExt, StreamExt};
use tokio::sync::RwLock;
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::time::interval;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::MaybeTlsStream;

use super::sign::generate_mid;

type WsWriter = futures::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>,
    WsMessage,
>;
type WsReader = SplitStream<tokio_tungstenite::WebSocketStream<MaybeTlsStream<TcpStream>>>;

/// Raw WebSocket transport layer for the fish protocol.
/// Handles connection, sending, handshake, and heartbeat.
pub struct FishConnection {
    ws_writer: RwLock<Option<WsWriter>>,
}

impl FishConnection {
    pub fn new() -> Self {
        Self {
            ws_writer: RwLock::new(None),
        }
    }

    /// Send a serialized JSON value over the active WebSocket.
    pub async fn send(&self, msg: &Value) -> Result<()> {
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
    pub async fn connect(&self, url: &str) -> Result<WsReader> {
        use tokio_tungstenite::connect_async;
        let (ws_stream, _) = connect_async(url).await.map_err(|e| AppError::ws(e.to_string()))?;
        let (writer, reader) = ws_stream.split();
        let mut guard = self.ws_writer.write().await;
        *guard = Some(writer);
        Ok(reader)
    }

    /// Perform the fish-specific WS handshake: /reg + sync ackDiff.
    pub async fn handshake(&self, token: &str, device_id: &str) -> Result<()> {
        let reg_msg = serde_json::json!({
            "lwp": "/reg",
            "headers": {
                "cache-header": "app-key token ua wv",
                "app-key": "444e9908a51d1cb236a27862abc769c9",
                "token": token,
                "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 DingTalk(2.1.5) OS(Windows/10) Browser(Chrome/133.0.0.0) DingWeb/2.1.5 IMPaaS DingWeb/2.1.5",
                "dt": "j",
                "wv": "im:3,au:3,sy:6",
                "did": device_id,
                "mid": generate_mid(),
            }
        });
        self.send(&reg_msg).await?;

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let sync_msg = serde_json::json!({
            "lwp": "/r/SyncStatus/ackDiff",
            "headers": { "mid": generate_mid() },
            "body": [
                {
                    "pipeline": "sync",
                    "tooLong2Tag": "PNM,1",
                    "channel": "sync",
                    "topic": "sync",
                    "highPts": 0,
                    "pts": now * 1000,
                    "seq": 0,
                    "timestamp": now,
                }
            ]
        });
        self.send(&sync_msg).await?;
        Ok(())
    }

    /// Spawn a background task that sends heartbeat frames every 15 seconds.
    /// Requires `self: &Arc<Self>` so the spawned task can hold its own reference.
    pub fn spawn_heartbeat(self: &Arc<Self>) {
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
}
