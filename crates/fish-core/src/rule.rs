use regex::Regex;
use std::sync::Arc;

use crate::event::MessageEvent;

/// Rule system matching Python rule.py Rule class.
/// A Rule is a composable predicate over MessageEvent.
#[derive(Clone)]
pub struct Rule {
    checker: Arc<dyn Fn(&MessageEvent) -> bool + Send + Sync>,
}

impl Rule {
    pub fn new(checker: impl Fn(&MessageEvent) -> bool + Send + Sync + 'static) -> Self {
        Self {
            checker: Arc::new(checker),
        }
    }

    /// Create a Rule from a plain function (matching Python Rule(callable)).
    pub fn from_fn(f: impl Fn(&MessageEvent) -> bool + Send + Sync + 'static) -> Self {
        Self::new(f)
    }

    pub fn check(&self, event: &MessageEvent) -> bool {
        (self.checker)(event)
    }

    /// Combine two rules with logical AND, matching Python Rule.__and__.
    pub fn and(&self, other: &Rule) -> Rule {
        let a = self.checker.clone();
        let b = other.checker.clone();
        Rule::new(move |event| a(event) && b(event))
    }

    /// Combine two rules with logical OR, matching Python Rule.__or__.
    pub fn or(&self, other: &Rule) -> Rule {
        let a = self.checker.clone();
        let b = other.checker.clone();
        Rule::new(move |event| a(event) || b(event))
    }
}

// ---- Rule constructors, matching Python rule.py helpers ----
// Python accepts Union[str, Tuple[str, ...]]; Rust accepts single &str or Vec<&str> via MatchList.

/// A list of match patterns — supports single string or multiple strings.
/// Matching Python rule.py's Union[str, Tuple[str, ...]] parameter convention.
pub struct MatchList(Vec<String>);

impl From<&str> for MatchList {
    fn from(s: &str) -> Self {
        MatchList(vec![s.to_string()])
    }
}

impl From<String> for MatchList {
    fn from(s: String) -> Self {
        MatchList(vec![s])
    }
}

impl From<Vec<&str>> for MatchList {
    fn from(v: Vec<&str>) -> Self {
        MatchList(v.into_iter().map(|s| s.to_string()).collect())
    }
}

impl<const N: usize> From<[&str; N]> for MatchList {
    fn from(arr: [&str; N]) -> Self {
        MatchList(arr.into_iter().map(|s| s.to_string()).collect())
    }
}

/// Match messages that start with any of the given strings.
pub fn is_startswith(msg: impl Into<MatchList>) -> Rule {
    let prefixes = msg.into().0;
    Rule::new(move |event| {
        let text = event.plain_text();
        prefixes.iter().any(|p| text.starts_with(p))
    })
}

/// Match messages that exactly equal one of the given strings.
pub fn is_fullmatch(msg: impl Into<MatchList>) -> Rule {
    let candidates = msg.into().0;
    Rule::new(move |event| {
        let text = event.plain_text().trim().to_string();
        candidates.iter().any(|c| text == *c)
    })
}

/// Match messages that contain any of the given keywords.
pub fn is_keywords(keyword: impl Into<MatchList>) -> Rule {
    let keywords = keyword.into().0;
    Rule::new(move |event| {
        let text = event.plain_text();
        keywords.iter().any(|kw| text.contains(kw))
    })
}

/// Match messages that match the given regex pattern.
/// For flags, embed them in the pattern string using Rust regex syntax, e.g. "(?i)pattern".
/// Returns a Rule that never matches if the pattern is invalid.
pub fn is_regex(pattern: &str) -> Rule {
    match Regex::new(pattern) {
        Ok(compiled) => Rule::new(move |event| compiled.is_match(&event.plain_text())),
        Err(_) => Rule::new(|_| false),
    }
}

impl std::fmt::Debug for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rule").finish()
    }
}
