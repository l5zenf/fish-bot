#[derive(Clone)]
pub struct FishHttpClient(reqwest::Client);

pub trait ClientProvider: Send + Sync {
    type Client: Clone + Send + Sync + 'static;

    fn client(&self) -> Self::Client;
    fn client_ref(&self) -> &Self::Client;
}

impl FishHttpClient {
    pub(crate) fn new(inner: reqwest::Client) -> Self {
        Self(inner)
    }
}

impl ClientProvider for FishHttpClient {
    type Client = reqwest::Client;

    fn client(&self) -> Self::Client {
        let Self(cli) = self;
        Self::Client::clone(cli)
    }

    fn client_ref(&self) -> &Self::Client {
        let Self(cli) = self;
        cli
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fish_http_client_exposes_borrowed_client_ref() {
        let client = FishHttpClient::new(reqwest::Client::new());
        let request = client.client_ref().get("https://example.com");
        let _request = request.build().expect("request should build");
    }
}
