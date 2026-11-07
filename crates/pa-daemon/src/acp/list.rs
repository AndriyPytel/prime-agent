//! ACP `session/list`: the supervisor's session roster as ACP `SessionInfo` rows.

use std::path::Path;
use std::sync::Arc;

use pa_types::daemon::DaemonCommand;
use serde_json::{json, Value};

use super::daemon::{spawn_event_consumer, DaemonLink};
use super::jsonrpc;
use super::producer::FrameSink;

const PAGE_SIZE: usize = 100;
const SUBAGENT_TITLE_PREFIX: &str = "\u{21b3} ";
const NO_MESSAGES_PLACEHOLDER: &str = "(no messages)";

/// Answer `session/list` from the supervisor's merged roster.
pub(super) async fn handle_session_list(
    id: &Value,
    params: &Value,
    socket_path: &Path,
    tx: &FrameSink,
) {
    let roster = async {
        let link = Arc::new(DaemonLink::connect(socket_path).await?);
        let consumer = spawn_event_consumer(Arc::clone(&link), Arc::default());
        let response = link
            .request(DaemonCommand::List {
                id: None,
                all: Some(true),
                cwd: None,
                session_dir: None,
                include_client_owned: None,
                rest: serde_json::Map::default(),
            })
            .await;
        consumer.abort();
        let response = response?;
        if !response.success {
            anyhow::bail!(response
                .error
                .unwrap_or_else(|| "unknown error".to_string()));
        }
        Ok::<_, anyhow::Error>(response.data.unwrap_or_default())
    }
    .await;
    let frame = match roster {
        Ok(data) => {
            let summaries = data["sessions"].as_array().map_or(&[][..], Vec::as_slice);
            match session_list_result(summaries, params) {
                Ok(result) => jsonrpc::response(id, &result),
                Err(reason) => jsonrpc::error_response(
                    id,
                    jsonrpc::INVALID_PARAMS,
                    "Invalid params",
                    Some(&json!({ "reason": reason })),
                ),
            }
        }
        Err(error) => super::internal_error(id, &format!("{error:#}")),
    };
    let _ = tx.send(frame);
}

/// One page of `SessionInfo` rows, newest first. Archived sessions and
/// sessions without a message (drafts) are left out; `cursor` is the offset
/// of the page's first row.
fn session_list_result(summaries: &[Value], params: &Value) -> Result<Value, String> {
    let cwd = params["cwd"].as_str();
    let offset = match params["cursor"].as_str() {
        None => 0,
        Some(cursor) => cursor
            .parse::<usize>()
            .map_err(|_| format!("invalid cursor: {cursor}"))?,
    };
    let mut rows: Vec<&Value> = summaries
        .iter()
        .filter(|summary| {
            summary["lifecycle"] != "archived"
                && summary["messageCount"].as_u64() != Some(0)
                && summary["sessionId"].is_string()
                && cwd.is_none_or(|cwd| summary["cwd"] == cwd)
        })
        .collect();
    rows.sort_by(|a, b| b["modified"].as_str().cmp(&a["modified"].as_str()));
    let sessions: Vec<Value> = rows
        .iter()
        .skip(offset)
        .take(PAGE_SIZE)
        .copied()
        .map(session_info)
        .collect();
    let mut result = json!({ "sessions": sessions });
    if rows.len() > offset + PAGE_SIZE {
        result["nextCursor"] = json!((offset + PAGE_SIZE).to_string());
    }
    Ok(result)
}

fn session_info(summary: &Value) -> Value {
    let first_message = summary["firstMessage"]
        .as_str()
        .filter(|message| *message != NO_MESSAGES_PLACEHOLDER);
    let prefix = if summary["runtimeKind"] == "subagent" {
        SUBAGENT_TITLE_PREFIX
    } else {
        ""
    };
    let mut info = json!({
        "sessionId": summary["sessionId"],
        "cwd": summary["cwd"],
        "updatedAt": summary["modified"],
    });
    if let Some(title) = summary["sessionName"].as_str().or(first_message) {
        info["title"] = json!(format!("{prefix}{title}"));
    }
    info
}

#[cfg(test)]
mod tests {
    use super::*;

    fn summary(session_id: &str, cwd: &str, modified: &str) -> Value {
        json!({
            "sessionId": session_id,
            "lifecycle": "live",
            "cwd": cwd,
            "modified": modified,
            "messageCount": 2,
            "firstMessage": format!("hello {session_id}"),
            "runtimeKind": "top-level",
        })
    }

    #[test]
    fn lists_newest_first_with_subagents_and_without_drafts_or_archived() {
        let mut subagent = summary("sub", "/w", "2026-01-03T00:00:00.000Z");
        subagent["runtimeKind"] = json!("subagent");
        subagent["sessionName"] = json!("kid");
        subagent["firstMessage"] = json!("(no messages)");
        let mut draft = summary("draft", "/w", "2026-01-05T00:00:00.000Z");
        draft["lifecycle"] = json!("draft");
        draft["messageCount"] = json!(0);
        let mut archived = summary("old", "/w", "2026-01-06T00:00:00.000Z");
        archived["lifecycle"] = json!("archived");
        let summaries = [
            summary("a", "/w", "2026-01-01T00:00:00.000Z"),
            subagent,
            draft,
            archived,
            summary("b", "/w", "2026-01-04T00:00:00.000Z"),
        ];
        assert_eq!(
            session_list_result(&summaries, &json!({})).unwrap(),
            json!({ "sessions": [
                { "sessionId": "b", "cwd": "/w", "title": "hello b", "updatedAt": "2026-01-04T00:00:00.000Z" },
                { "sessionId": "sub", "cwd": "/w", "title": "\u{21b3} kid", "updatedAt": "2026-01-03T00:00:00.000Z" },
                { "sessionId": "a", "cwd": "/w", "title": "hello a", "updatedAt": "2026-01-01T00:00:00.000Z" },
            ] })
        );
    }

    #[test]
    fn filters_by_cwd() {
        let summaries = [
            summary("a", "/w", "2026-01-01T00:00:00.000Z"),
            summary("b", "/other", "2026-01-02T00:00:00.000Z"),
        ];
        assert_eq!(
            session_list_result(&summaries, &json!({ "cwd": "/other" })).unwrap(),
            json!({ "sessions": [
                { "sessionId": "b", "cwd": "/other", "title": "hello b", "updatedAt": "2026-01-02T00:00:00.000Z" },
            ] })
        );
    }

    #[test]
    fn pages_through_the_roster_with_a_cursor() {
        let summaries: Vec<Value> = (0..250)
            .map(|n| summary(&format!("s{n:03}"), "/w", &format!("2026-01-01T{n:04}Z")))
            .collect();
        let first = session_list_result(&summaries, &json!({})).unwrap();
        assert_eq!(first["sessions"].as_array().unwrap().len(), 100);
        assert_eq!(first["nextCursor"], "100");
        let last = session_list_result(&summaries, &json!({ "cursor": "200" })).unwrap();
        assert_eq!(last["sessions"].as_array().unwrap().len(), 50);
        assert_eq!(last.get("nextCursor"), None);
    }
}
