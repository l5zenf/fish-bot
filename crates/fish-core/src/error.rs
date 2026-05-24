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

    pub fn internal(details: impl Into<String>) -> Self {
        AppError::Internal { details: details.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t1_34_error_constructors() {
        let e = AppError::auth("login failed");
        assert!(matches!(e, AppError::Auth { .. }));

        let e = AppError::protocol("bad message");
        assert!(matches!(e, AppError::Protocol { .. }));

        let e = AppError::http("timeout");
        assert!(matches!(e, AppError::Http { .. }));

        let e = AppError::ws("disconnected");
        assert!(matches!(e, AppError::Ws { .. }));
    }

    #[test]
    fn t1_35_display_all_variants() {
        let cases: Vec<(AppError, &str)> = vec![
            (AppError::Http { message: "timeout".into() }, "HTTP request failed: timeout"),
            (AppError::Ws { message: "closed".into() }, "WebSocket error: closed"),
            (AppError::Json { source: serde_json::from_str::<()>("invalid").unwrap_err() },
             "JSON error:"),
            (AppError::Base64 { source: base64::Engine::decode(&base64::engine::general_purpose::STANDARD, "!!!").unwrap_err() },
             "Base64 decode error:"),
            (AppError::Auth { details: "bad token".into() }, "Authentication failed: bad token"),
            (AppError::Protocol { details: "bad msg".into() }, "Protocol error: bad msg"),
            (AppError::Internal { details: "oops".into() }, "Internal error: oops"),
        ];

        for (err, expected_prefix) in cases {
            let display = err.to_string();
            assert!(display.starts_with(expected_prefix),
                "expected '{display}' to start with '{expected_prefix}'");
        }
    }

    #[test]
    fn t1_36_from_serde_json_error() {
        let result: std::result::Result<(), AppError> =
            Err(Into::into(serde_json::from_str::<()>("invalid").unwrap_err()));
        assert!(matches!(result, Err(AppError::Json { .. })));
    }

    #[test]
    fn t1_37_from_base64_error() {
        let result: std::result::Result<(), AppError> =
            Err(Into::into(base64::Engine::decode(&base64::engine::general_purpose::STANDARD, "!!!").unwrap_err()));
        assert!(matches!(result, Err(AppError::Base64 { .. })));
    }

    #[test]
    fn t1_38_result_type_alias() -> anyhow::Result<()> {
        fn foo() -> Result<i32> {
            Ok(42)
        }
        fn bar() -> std::result::Result<i32, AppError> {
            Ok(42)
        }
        assert_eq!(foo()?, bar()?);
        Ok(())
    }
}
