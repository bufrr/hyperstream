//! Shared utility functions for parsers
//!
//! This module contains helper functions used across multiple parsers:
//! - Line extraction and processing
//! - Timestamp parsing (ISO8601)
//! - Transaction hash normalization
//! - Flexible deserialization helpers
//! - File path utilities
//! - System timestamp utilities

use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Default limit for line preview in error messages (256 characters)
pub const LINE_PREVIEW_LIMIT: usize = 256;

/// Extracts starting block number from file path (e.g., "808750000" from path).
///
/// Returns None if the filename cannot be parsed as a u64.
pub(crate) fn extract_starting_block(file_path: &Path) -> Option<u64> {
    file_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.parse::<u64>().ok())
}

/// Flexible deserializer for Option<String> that handles various input types.
///
/// Returns None for null/empty values, Some(String) otherwise.
/// Handles strings, numbers, and booleans.
pub(crate) fn deserialize_option_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::String(s)) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(trimmed.to_string()))
            }
        }
        Some(serde_json::Value::Number(n)) => Ok(Some(n.to_string())),
        Some(serde_json::Value::Bool(b)) => Ok(Some(b.to_string())),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected string or null, got {other:?}"
        ))),
    }
}

/// Extracts complete lines (ending with `\n`) from a buffer.
///
/// Lines are removed from the buffer and returned as a Vec. Incomplete data
/// (after the last `\n`) remains in the buffer for the next call.
pub(crate) fn drain_complete_lines(buffer: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut lines = Vec::new();
    let mut start = 0usize;

    for (idx, byte) in buffer.iter().enumerate() {
        if *byte == b'\n' {
            let line = buffer[start..idx].to_vec();
            lines.push(line);
            start = idx + 1;
        }
    }

    if start > 0 {
        buffer.drain(0..start);
    }

    lines
}

/// Trims trailing whitespace and carriage returns from a line.
pub(crate) fn trim_line_bytes(mut line: Vec<u8>) -> Vec<u8> {
    while line.last().map(|b| *b == b'\r' || *b == b' ') == Some(true) {
        line.pop();
    }
    line
}

/// Creates a preview string from a byte slice, truncating at the specified limit with ellipsis.
///
/// Used for logging parsed line content without overwhelming the logs.
pub(crate) fn line_preview(line: &[u8], limit: usize) -> String {
    let text = String::from_utf8_lossy(line);
    let mut preview = String::new();
    for (idx, ch) in text.chars().enumerate() {
        if idx >= limit {
            preview.push('…');
            return preview;
        }
        preview.push(ch);
    }
    preview
}

/// Flexible deserializer that accepts both strings and numbers.
///
/// Converts numbers to strings. Useful for handling Hyperliquid's inconsistent field types.
pub(crate) fn deserialize_string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(String::new()),
        Some(serde_json::Value::String(s)) => Ok(s),
        Some(serde_json::Value::Number(n)) => Ok(n.to_string()),
        Some(serde_json::Value::Bool(b)) => Ok(b.to_string()),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected string or number, got {other:?}"
        ))),
    }
}

/// Flexible deserializer for u64 that accepts strings, numbers, or booleans.
///
/// Returns 0 for null values. Booleans are converted to 1/0.
pub(crate) fn deserialize_u64_from_any<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(0),
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("expected u64-compatible number")),
        Some(serde_json::Value::String(s)) => s
            .parse::<u64>()
            .map_err(|_| serde::de::Error::custom(format!("failed to parse u64 from {s:?}"))),
        Some(serde_json::Value::Bool(true)) => Ok(1),
        Some(serde_json::Value::Bool(false)) => Ok(0),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected number, got {other:?}"
        ))),
    }
}

/// Flexible deserializer for Option<u64> that accepts strings, numbers, or booleans.
///
/// Returns None for null values or empty strings.
pub(crate) fn deserialize_option_u64_from_any<'de, D>(
    deserializer: D,
) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom("expected u64-compatible number"))
            .map(Some),
        Some(serde_json::Value::String(s)) => {
            if s.trim().is_empty() {
                Ok(None)
            } else {
                s.parse::<u64>().map(Some).map_err(|_| {
                    serde::de::Error::custom(format!("failed to parse u64 from {s:?}"))
                })
            }
        }
        Some(serde_json::Value::Bool(true)) => Ok(Some(1)),
        Some(serde_json::Value::Bool(false)) => Ok(Some(0)),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected number, got {other:?}"
        ))),
    }
}

/// Parses an ISO8601 timestamp string to milliseconds since Unix epoch.
///
/// Supports:
/// - Zulu time (Z suffix)
/// - Timezone offsets (+HH:MM, -HH:MM, +HHMM, -HHMM)
/// - Fractional seconds (arbitrary precision, converted to milliseconds)
///
/// Returns None for invalid timestamps or dates before Unix epoch.
pub(crate) fn parse_iso8601_to_millis(input: &str) -> Option<u64> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut tz_minutes = 0i32;
    let mut body = trimmed;

    if let Some(stripped) = body.strip_suffix('Z').or_else(|| body.strip_suffix('z')) {
        body = stripped;
    } else if let Some(idx) = body.rfind(['+', '-']) {
        if let Some(t_pos) = body.find('T') {
            if idx > t_pos {
                let tz_part = &body[idx..];
                if let Some(offset) = parse_timezone_offset_minutes(tz_part) {
                    tz_minutes = offset;
                    body = &body[..idx];
                }
            }
        }
    }

    let (date_part, time_part) = body.split_once('T')?;
    let mut date_segments = date_part.split('-');
    let year = date_segments.next()?.parse::<i32>().ok()?;
    let month = date_segments.next()?.parse::<u32>().ok()?;
    let day = date_segments.next()?.parse::<u32>().ok()?;

    let mut time_segments = time_part.split(':');
    let hour = time_segments.next()?.parse::<u32>().ok()?;
    let minute = time_segments.next()?.parse::<u32>().ok()?;
    let second_with_frac = time_segments.next()?;

    let (second_str, nanos) = match second_with_frac.split_once('.') {
        Some((sec, frac)) => {
            let nanos = parse_fractional_nanos(frac)?;
            (sec, nanos)
        }
        None => (second_with_frac, 0),
    };
    let second = second_str.parse::<u32>().ok()?;

    let days = days_from_civil(year, month, day)?;
    let mut total_seconds =
        days as i64 * 86_400 + hour as i64 * 3_600 + minute as i64 * 60 + second as i64;
    total_seconds -= tz_minutes as i64 * 60;

    if total_seconds < 0 {
        return None;
    }

    let millis = total_seconds
        .checked_mul(1_000)?
        .checked_add((nanos / 1_000_000) as i64)?;

    if millis < 0 {
        None
    } else {
        Some(millis as u64)
    }
}

/// Normalizes a transaction hash, filtering out empty or all-zero hashes.
///
/// Returns None for:
/// - Empty strings
/// - All-zero hashes (0x000...000)
///
/// Preserves the `0x` prefix if present.
pub(crate) fn normalize_tx_hash(hash: &str) -> Option<String> {
    let trimmed = hash.trim();
    if trimmed.is_empty() {
        return None;
    }

    let without_prefix = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);

    if without_prefix.chars().all(|c| c == '0') {
        return None;
    }

    Some(trimmed.to_string())
}

/// Returns the current Unix timestamp in seconds.
///
/// Used for checkpoint tracking and hash store caching.
pub fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

// ===== Private helper functions =====

fn parse_timezone_offset_minutes(part: &str) -> Option<i32> {
    if part.len() < 2 {
        return None;
    }
    let sign = if part.starts_with('-') {
        -1
    } else if part.starts_with('+') {
        1
    } else {
        return None;
    };

    let rest = &part[1..];
    let (hours_str, minutes_str) = if let Some((h, m)) = rest.split_once(':') {
        (h, m)
    } else if rest.len() >= 2 {
        rest.split_at(2)
    } else {
        (rest, "0")
    };

    let hours = hours_str.parse::<i32>().ok()?;
    let minutes = minutes_str.parse::<i32>().unwrap_or(0);

    Some(sign * (hours * 60 + minutes))
}

fn parse_fractional_nanos(fraction: &str) -> Option<u32> {
    let mut nanos = 0u32;
    let mut digits = 0u32;
    for ch in fraction.chars() {
        if !ch.is_ascii_digit() {
            break;
        }
        if digits < 9 {
            nanos = nanos * 10 + (ch as u32 - '0' as u32);
            digits += 1;
        }
    }
    while digits < 9 {
        nanos *= 10;
        digits += 1;
    }
    Some(nanos)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let era_adjusted_year = year - (month <= 2) as i32;
    let era = if era_adjusted_year >= 0 {
        era_adjusted_year / 400
    } else {
        (era_adjusted_year - 399) / 400
    };
    let year_of_era = era_adjusted_year - era * 400;
    let month_adjusted = month as i32 + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_adjusted + 2) / 5 + day as i32 - 1;
    let day_of_era = year_of_era as i64 * 365 + year_of_era as i64 / 4 - year_of_era as i64 / 100
        + day_of_year as i64;
    Some(era as i64 * 146_097 + day_of_era - 719468)
}

/// Generic deserializer for event lists that may be null, an array, or an object-map.
///
/// Handles flexible input formats commonly seen in Hyperliquid node data:
/// - `null` → empty Vec
/// - `[...]` → Vec from array items
/// - `{key1: ..., key2: ...}` → Vec from object values (keys ignored)
///
/// The `context` parameter is used for error messages to identify the event type.
pub(crate) fn deserialize_flexible_events<'de, D, T>(
    deserializer: D,
    context: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: DeserializeOwned,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    match value {
        None | Some(serde_json::Value::Null) => Ok(Vec::new()),
        Some(serde_json::Value::Array(arr)) => arr
            .into_iter()
            .map(|item| serde_json::from_value(item).map_err(serde::de::Error::custom))
            .collect(),
        Some(serde_json::Value::Object(map)) => map
            .into_iter()
            .map(|(_, item)| serde_json::from_value(item).map_err(serde::de::Error::custom))
            .collect(),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected {context} list or object, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, FixedOffset};

    fn chrono_millis(reference: &str) -> u64 {
        DateTime::<FixedOffset>::parse_from_rfc3339(reference)
            .expect("reference timestamp should parse")
            .timestamp_millis() as u64
    }

    fn assert_timestamp_matches(input: &str, reference: &str) {
        let expected = chrono_millis(reference);
        assert_eq!(
            parse_iso8601_to_millis(input),
            Some(expected),
            "{input} should equal {reference}"
        );
    }

    #[test]
    fn parses_leap_year_zulu_timestamp() {
        assert_timestamp_matches("2024-02-29T23:59:59.999Z", "2024-02-29T23:59:59.999Z");
    }

    #[test]
    fn parses_lowercase_z_and_long_fractional_precision() {
        assert_timestamp_matches(
            "2023-05-10T08:15:30.123456789z",
            "2023-05-10T08:15:30.123456789Z",
        );
    }

    #[test]
    fn applies_positive_timezone_offsets_with_minutes() {
        assert_timestamp_matches(
            "2023-05-10T08:15:30.250+05:45",
            "2023-05-10T08:15:30.250+05:45",
        );
    }

    #[test]
    fn applies_negative_timezone_without_colon() {
        assert_timestamp_matches("2019-12-31T18:00:00-0530", "2019-12-31T18:00:00-05:30");
    }

    #[test]
    fn rejects_pre_epoch_timestamps() {
        assert_eq!(parse_iso8601_to_millis("1969-12-31T23:59:59Z"), None);
    }

    #[test]
    fn rejects_impossible_dates_and_missing_time() {
        assert_eq!(parse_iso8601_to_millis("2024-13-01T00:00:00Z"), None);
        assert_eq!(parse_iso8601_to_millis("2024-01-01"), None);
    }

    #[test]
    fn rejects_invalid_strings_and_empty_input() {
        assert_eq!(parse_iso8601_to_millis(""), None);
        assert_eq!(parse_iso8601_to_millis("invalid"), None);
    }

    #[test]
    fn normalizes_valid_hashes() {
        assert_eq!(normalize_tx_hash("0xabc123"), Some("0xabc123".to_string()));
        assert_eq!(normalize_tx_hash("abc123"), Some("abc123".to_string()));
    }

    #[test]
    fn rejects_empty_and_zero_hashes() {
        assert_eq!(normalize_tx_hash(""), None);
        assert_eq!(normalize_tx_hash("0x0000000000000000"), None);
        assert_eq!(normalize_tx_hash("0000000000000000"), None);
    }
}
