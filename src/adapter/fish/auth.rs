use crate::error::Result;
use std::collections::HashMap;

pub struct AuthManager {
    client: reqwest::Client,
    cookies: HashMap<String, String>,
}

impl AuthManager {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            cookies: HashMap::new(),
        }
    }

    pub async fn from_local_or_qr_login() -> Result<Self> {
        todo!()
    }

    pub async fn qrcode_login(&mut self) -> Result<()> {
        todo!()
    }

    pub async fn get_access_token(&self) -> Result<String> {
        todo!()
    }

    pub async fn refresh_if_needed(&mut self) -> Result<()> {
        todo!()
    }
}
