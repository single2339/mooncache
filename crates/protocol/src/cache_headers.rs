use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheControl {
    Default,
    Bypass,
    ReadOnly,
    WriteOnly,
    CacheOnly,
    ForceReplay,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HeaderError {
    #[error("invalid cache control: {0}")]
    InvalidCacheControl(String),
}

impl CacheControl {
    pub fn parse(value: &str) -> Result<Self, HeaderError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "default" => Ok(Self::Default),
            "bypass" => Ok(Self::Bypass),
            "read-only" => Ok(Self::ReadOnly),
            "write-only" => Ok(Self::WriteOnly),
            "cache-only" => Ok(Self::CacheOnly),
            "force-replay" => Ok(Self::ForceReplay),
            other => Err(HeaderError::InvalidCacheControl(other.to_owned())),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheStatus {
    Hit,
    Miss,
    Bypass,
    Ineligible,
    CacheOnlyMiss,
    Degraded,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cache_control_modes() {
        assert_eq!(CacheControl::parse("bypass").unwrap(), CacheControl::Bypass);
        assert_eq!(
            CacheControl::parse("cache-only").unwrap(),
            CacheControl::CacheOnly
        );
        assert_eq!(CacheControl::parse("").unwrap(), CacheControl::Default);
    }

    #[test]
    fn rejects_unknown_cache_control_mode() {
        let err = CacheControl::parse("semantic").unwrap_err();
        assert!(err.to_string().contains("invalid cache control"));
    }
}
