use serde::de::{DeserializeOwned, MapAccess, SeqAccess, Visitor};
use sonic_rs::{JsonValueTrait, Value};
use std::{
    fmt,
    marker::PhantomData,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

pub const LINE_PREVIEW_LIMIT: usize = 256;

pub(crate) fn extract_starting_block(file_path: &Path) -> Option<u64> {
    file_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.parse::<u64>().ok())
}

pub(crate) fn deserialize_option_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(val) if val.is_null() => Ok(None),
        Some(val) => {
            if let Some(s) = val.as_str() {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(trimmed.to_string()))
                }
            } else if let Some(n) = val.as_number() {
                Ok(Some(n.to_string()))
            } else if let Some(b) = val.as_bool() {
                Ok(Some(b.to_string()))
            } else {
                Err(serde::de::Error::custom(format!(
                    "expected string or null, got {val:?}"
                )))
            }
        }
    }
}

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

pub(crate) fn trim_line_bytes(mut line: Vec<u8>) -> Vec<u8> {
    while line.last().map(|b| *b == b'\r' || *b == b' ') == Some(true) {
        line.pop();
    }
    line
}

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

pub(crate) fn deserialize_string_or_number<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None => Ok(String::new()),
        Some(val) if val.is_null() => Ok(String::new()),
        Some(val) => {
            if let Some(s) = val.as_str() {
                Ok(s.to_string())
            } else if let Some(n) = val.as_number() {
                Ok(n.to_string())
            } else if let Some(b) = val.as_bool() {
                Ok(b.to_string())
            } else {
                Err(serde::de::Error::custom(format!(
                    "expected string or number, got {val:?}"
                )))
            }
        }
    }
}

pub(crate) fn deserialize_u64_from_any<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None => Ok(0),
        Some(val) if val.is_null() => Ok(0),
        Some(val) => {
            if let Some(num) = val.as_u64() {
                Ok(num)
            } else if let Some(s) = val.as_str() {
                s.parse::<u64>().map_err(|_| {
                    serde::de::Error::custom(format!("failed to parse u64 from {s:?}"))
                })
            } else if let Some(true) = val.as_bool() {
                Ok(1)
            } else if let Some(false) = val.as_bool() {
                Ok(0)
            } else {
                Err(serde::de::Error::custom(format!(
                    "expected number, got {val:?}"
                )))
            }
        }
    }
}

pub(crate) fn deserialize_option_u64_from_any<'de, D>(
    deserializer: D,
) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None => Ok(None),
        Some(val) if val.is_null() => Ok(None),
        Some(val) => {
            if let Some(num) = val.as_u64() {
                Ok(Some(num))
            } else if let Some(s) = val.as_str() {
                if s.trim().is_empty() {
                    Ok(None)
                } else {
                    s.parse::<u64>().map(Some).map_err(|_| {
                        serde::de::Error::custom(format!("failed to parse u64 from {s:?}"))
                    })
                }
            } else if let Some(true) = val.as_bool() {
                Ok(Some(1))
            } else if let Some(false) = val.as_bool() {
                Ok(Some(0))
            } else {
                Err(serde::de::Error::custom(format!(
                    "expected number, got {val:?}"
                )))
            }
        }
    }
}

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

pub(crate) fn deserialize_flexible_events<'de, D, T>(
    deserializer: D,
    context: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: DeserializeOwned,
{
    struct FlexibleEventsVisitor<T> {
        marker: PhantomData<T>,
        context: &'static str,
    }

    impl<'de, T: DeserializeOwned> Visitor<'de> for FlexibleEventsVisitor<T> {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(formatter, "{} list or object", self.context)
        }

        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut values = Vec::new();
            while let Some(value) = seq.next_element()? {
                values.push(value);
            }
            Ok(values)
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut values = Vec::new();
            while let Some((_key, value)) = map.next_entry::<serde::de::IgnoredAny, T>()? {
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_any(FlexibleEventsVisitor {
        marker: PhantomData,
        context,
    })
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
