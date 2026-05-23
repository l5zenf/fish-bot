use crate::error::Result;
use crate::adapter::fish::auth::AuthManager;
use serde_json::Value;

pub struct FishAPI {
    client: reqwest::Client,
    auth: AuthManager,
}

impl FishAPI {
    pub fn new(auth: AuthManager) -> Self {
        Self {
            client: reqwest::Client::new(),
            auth,
        }
    }

    pub async fn get_token(&self) -> Result<Value> {
        todo!()
    }

    pub async fn get_access_token(&self) -> Result<String> {
        todo!()
    }

    pub async fn get_user_info(&self, user_id: &str) -> Result<Value> {
        todo!()
    }

    pub async fn get_item_list(&self, user_id: &str, page: u64, page_size: u64) -> Result<Value> {
        todo!()
    }

    pub async fn publish_item(&self, images_path: Vec<String>, goods_desc: String, price: Option<std::collections::HashMap<String, f64>>, ds: std::collections::HashMap<String, String>) -> Result<Value> {
        todo!()
    }
}
