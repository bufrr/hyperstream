use crate::sorter_client::proto::DataRecord;
use anyhow::{anyhow, Result};
use std::path::Path;

pub mod blocks;
pub mod fills;
pub mod misc_events;
pub mod orders;
pub mod trades;
pub mod transactions;

pub use blocks::BlocksParser;
pub use fills::FillsParser;
pub use misc_events::MiscEventsParser;
pub use orders::OrdersParser;
pub use trades::TradesParser;
pub use transactions::TransactionsParser;

pub trait Parser: Send {
    fn parse(&mut self, file_path: &Path, data: &[u8]) -> Result<Vec<DataRecord>>;
    fn backlog_len(&self) -> usize;
}

pub fn route_parser(file_path: &Path) -> Result<Box<dyn Parser>> {
    if path_contains(file_path, "periodic_abci_states") {
        return Ok(Box::new(BlocksParser::default()));
    }
    if path_contains(file_path, "replica_cmds") {
        return Ok(Box::new(TransactionsParser::default()));
    }
    if path_contains(file_path, "node_trades") {
        return Ok(Box::new(TradesParser::default()));
    }
    if path_contains(file_path, "node_order_statuses")
        || path_contains(file_path, "node_fills_by_block")
        || path_contains(file_path, "node_order_statuses_by_block")
    {
        return Ok(Box::new(FillsParser::default()));
    }
    if path_contains(file_path, "node_raw_book_diffs")
        || path_contains(file_path, "node_raw_book_diffs_by_block")
    {
        return Ok(Box::new(OrdersParser::default()));
    }
    if path_contains(file_path, "misc_events") || path_contains(file_path, "misc_events_by_block") {
        return Ok(Box::new(MiscEventsParser::default()));
    }

    Err(anyhow!(
        "no parser registered for path {}",
        file_path.display()
    ))
}

fn path_contains(path: &Path, needle: &str) -> bool {
    path.iter()
        .filter_map(|c| c.to_str())
        .any(|component| component == needle)
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

/// Creates a preview string from a byte slice, truncating at 256 characters with ellipsis.
/// Used for logging parsed line content without overwhelming the logs.
pub(crate) fn line_preview(line: &[u8], limit: usize) -> String {
    let text = String::from_utf8_lossy(line);
    let mut preview = String::new();
    let mut consumed = 0usize;
    for ch in text.chars() {
        if consumed >= limit {
            preview.push('…');
            return preview;
        }
        preview.push(ch);
        consumed += 1;
    }
    preview
}

/// Returns "unknown" if the input string is empty, otherwise returns the input as-is.
/// Used for partition keys to ensure valid kafka-style routing even when data is missing.
pub(crate) fn partition_key_or_unknown(value: &str) -> String {
    if value.is_empty() {
        "unknown".to_string()
    } else {
        value.to_string()
    }
}

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

pub(crate) fn parse_iso8601_to_millis(input: &str) -> Option<u64> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut tz_minutes = 0i32;
    let mut body = trimmed;

    if let Some(stripped) = body.strip_suffix('Z').or_else(|| body.strip_suffix('z')) {
        body = stripped;
    } else if let Some(idx) = body.rfind(|c| c == '+' || c == '-') {
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
    if month < 1 || month > 12 || day < 1 || day > 31 {
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
    let day_of_era = year_of_era as i64 * 365
        + year_of_era as i64 / 4
        - year_of_era as i64 / 100
        + day_of_year as i64;
    Some(era as i64 * 146_097 + day_of_era - 719468)
}
