use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum AppError {
    #[snafu(display("HTTP request failed: {message}"))]
    Http { message: String },

    #[snafu(display("WebSocket error: {message}"))]
    Ws { message: String },

    #[snafu(display("JSON error: {source}"))]
    Json { source: serde_json::Error },

    #[snafu(display("Base64 decode error: {source}"))]
    Base64 { source: base64::DecodeError },

    #[snafu(display("Authentication failed: {details}"))]
    Auth { details: String },

    #[snafu(display("Protocol error: {details}"))]
    Protocol { details: String },

    #[snafu(display("Internal error: {details}"))]
    Internal { details: String },
}

pub type Result<T> = std::result::Result<T, AppError>;

// `?` compatibility: serde_json and base64 errors auto-convert (they are core concerns)
impl From<serde_json::Error> for AppError {
    fn from(source: serde_json::Error) -> Self {
        AppError::Json { source }
    }
}

impl From<base64::DecodeError> for AppError {
    fn from(source: base64::DecodeError) -> Self {
        AppError::Base64 { source }
    }
}

impl AppError {
    pub fn auth(details: impl Into<String>) -> Self {
        AppError::Auth { details: details.into() }
    }

    pub fn protocol(details: impl Into<String>) -> Self {
        AppError::Protocol { details: details.into() }
    }

    pub fn http(message: impl Into<String>) -> Self {
        AppError::Http { message: message.into() }
    }

    pub fn ws(message: impl Into<String>) -> Self {
        AppError::Ws { message: message.into() }
    }
}
