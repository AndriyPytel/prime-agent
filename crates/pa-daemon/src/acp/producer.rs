//! The sole producer of ACP `session/update` notifications for one session:
//! serializes publication and stamps the delivered order (a strictly
//! increasing `eventSequence`, the `promptTurnId` allocated at prompt
//! admission, and a phase). Updates published before the `session/new`
//! response is queued are held and released after it.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

use super::jsonrpc;
use super::meta::{
    prime_agent_meta, PrimeAgentEventPhase, PrimeAgentOutcome, PrimeAgentSessionMeta,
    PRIME_AGENT_META_NAMESPACE,
};
use super::types::AcpSessionUpdate;
use serde_json::{json, Value};

/// How updates leave the producer: an ordered write queue shared with the
/// response writer, so responses and notifications interleave in
/// publication order.
pub type FrameSink = tokio::sync::mpsc::UnboundedSender<Value>;

/// Quiet time after the last published update before held telemetry is
/// released without waiting for the message to end.
const DEFERRED_TELEMETRY_FLUSH: Duration = Duration::from_millis(500);

pub struct UpdateProducer {
    session_id: String,
    sink: FrameSink,
    state: Mutex<ProducerState>,
}

#[derive(Debug, Default)]
struct ProducerState {
    event_sequence: u64,
    next_prompt_turn_id: u64,
    active_prompt_turn_id: u64,
    admission: AdmissionState,
    /// Telemetry held while a message streams, with its own turn ids.
    deferred: Vec<(AcpSessionUpdate, u64)>,
    flush_timer: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Debug, Default)]
struct AdmissionState {
    mode: AdmissionMode,
    /// Updates held while `session/new` has not yet replied.
    held: Vec<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum AdmissionMode {
    #[default]
    Buffering,
    Open,
    Closed,
}

impl UpdateProducer {
    pub fn new(session_id: impl Into<String>, sink: FrameSink) -> Arc<Self> {
        Arc::new(UpdateProducer {
            session_id: session_id.into(),
            sink,
            state: Mutex::new(ProducerState::default()),
        })
    }

    /// Allocate the causal turn for an accepted prompt. Called before the
    /// first await of the prompt handler so late events cannot relabel an
    /// update onto the next turn.
    pub async fn begin_prompt(&self) -> u64 {
        let mut state = self.state.lock().await;
        state.next_prompt_turn_id += 1;
        state.active_prompt_turn_id = state.next_prompt_turn_id;
        state.active_prompt_turn_id
    }

    /// The turn every non-child update currently belongs to.
    pub async fn active_prompt_turn(&self) -> u64 {
        self.state.lock().await.active_prompt_turn_id
    }

    /// End the prompt turn: after this, connection-scoped updates resume
    /// turn id `0`.
    pub async fn finish_prompt(&self, turn_id: u64) {
        let mut state = self.state.lock().await;
        if state.active_prompt_turn_id == turn_id {
            state.active_prompt_turn_id = 0;
        }
    }

    /// Open admission: the `session/new` response has been queued on the
    /// sink, so held updates may now flow behind it. The flush holds the
    /// lock so a concurrent publish cannot interleave.
    pub async fn commit_session_new_response(&self) {
        let mut state = self.state.lock().await;
        if state.admission.mode != AdmissionMode::Buffering {
            return;
        }
        state.admission.mode = AdmissionMode::Open;
        let held = std::mem::take(&mut state.admission.held);
        for frame in held {
            self.send(frame);
        }
    }

    /// Send a replayed history update at once, ahead of the held updates,
    /// so it precedes the `session/load` response.
    pub fn replay(&self, update: &AcpSessionUpdate) {
        self.send(jsonrpc::notification(
            "session/update",
            &json!({ "sessionId": self.session_id, "update": update.to_bare_value() }),
        ));
    }

    /// Fence the producer: no further updates are admitted, matching a
    /// closed session until a replacement `session/new` is admitted.
    pub async fn close(&self) {
        let mut state = self.state.lock().await;
        state.admission.mode = AdmissionMode::Closed;
        state.admission.held.clear();
        state.deferred.clear();
        if let Some(timer) = state.flush_timer.take() {
            timer.abort();
        }
    }

    /// Publish an event-driven update, holding telemetry (session info and
    /// usage) while a message streams so it cannot split the answer.
    pub async fn publish_event(
        self: &Arc<Self>,
        update: &AcpSessionUpdate,
        turn_id: u64,
        streaming: bool,
    ) {
        if streaming
            && matches!(
                update,
                AcpSessionUpdate::SessionInfoUpdate { .. } | AcpSessionUpdate::UsageUpdate { .. }
            )
        {
            let mut state = self.state.lock().await;
            state.deferred.push((update.clone(), turn_id));
            self.arm_flush_timer(&mut state);
            return;
        }
        self.publish(update, turn_id, PrimeAgentEventPhase::Event, None)
            .await;
        let mut state = self.state.lock().await;
        if state.flush_timer.is_some() {
            self.arm_flush_timer(&mut state);
        }
    }

    /// Release held telemetry in arrival order once no message streams.
    pub async fn flush_deferred(self: &Arc<Self>, streaming: bool) {
        if !streaming {
            self.release_deferred(true).await;
        }
    }

    async fn release_deferred(&self, abort_timer: bool) {
        let held = {
            let mut state = self.state.lock().await;
            if let Some(timer) = state.flush_timer.take() {
                if abort_timer {
                    timer.abort();
                }
            }
            std::mem::take(&mut state.deferred)
        };
        for (update, turn_id) in held {
            self.publish(&update, turn_id, PrimeAgentEventPhase::Event, None)
                .await;
        }
    }

    fn arm_flush_timer(self: &Arc<Self>, state: &mut ProducerState) {
        if let Some(timer) = state.flush_timer.take() {
            timer.abort();
        }
        let producer = Arc::clone(self);
        state.flush_timer = Some(tokio::spawn(async move {
            tokio::time::sleep(DEFERRED_TELEMETRY_FLUSH).await;
            producer.release_deferred(false).await;
        }));
    }

    /// Publish one update with its correlation fields. Returns `false` when
    /// the producer is fenced or the sink is gone; a false boundary
    /// publication fails the prompt.
    pub async fn publish(
        &self,
        update: &AcpSessionUpdate,
        turn_id: u64,
        phase: PrimeAgentEventPhase,
        outcome: Option<PrimeAgentOutcome>,
    ) -> bool {
        // Stamp and send under one lock hold so `eventSequence` order is
        // delivery order.
        let mut state = self.state.lock().await;
        state.event_sequence += 1;
        let frame = self.correlate(update, turn_id, phase, outcome, state.event_sequence);
        match state.admission.mode {
            AdmissionMode::Buffering => {
                state.admission.held.push(frame);
                true
            }
            AdmissionMode::Open => {
                self.send(frame);
                true
            }
            AdmissionMode::Closed => false,
        }
    }

    /// Stamp the update with its `eventSequence` and namespace payload.
    fn correlate(
        &self,
        update: &AcpSessionUpdate,
        turn_id: u64,
        phase: PrimeAgentEventPhase,
        outcome: Option<PrimeAgentOutcome>,
        event_sequence: u64,
    ) -> Value {
        let mut value = update.to_bare_value();
        let correlation = PrimeAgentSessionMeta {
            prompt_turn_id: Some(turn_id),
            event_sequence: Some(event_sequence),
            phase: Some(phase),
            outcome,
            ..Default::default()
        };
        // Merge with any namespace payload the update already carries
        // (e.g. ipython rich output): correlation fields are stamped onto
        // the delivered frame, never dropped.
        let meta = value
            .as_object_mut()
            .expect("session updates are objects")
            .entry("_meta")
            .or_insert_with(|| prime_agent_meta(&PrimeAgentSessionMeta::default()));
        let namespace = meta
            .as_object_mut()
            .expect("_meta is a namespaced object")
            .entry(PRIME_AGENT_META_NAMESPACE.to_string())
            .or_insert_with(|| json!({}));
        let mut payload =
            serde_json::from_value::<PrimeAgentSessionMeta>(namespace.clone()).unwrap_or_default();
        payload.prompt_turn_id = correlation.prompt_turn_id;
        payload.event_sequence = correlation.event_sequence;
        payload.phase = correlation.phase;
        payload.outcome = correlation.outcome;
        *namespace = serde_json::to_value(&payload).expect("meta payload serializes");
        jsonrpc::notification(
            "session/update",
            &json!({ "sessionId": self.session_id, "update": value }),
        )
    }

    fn send(&self, frame: Value) {
        let _ = self.sink.send(frame);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn chunk() -> AcpSessionUpdate {
        AcpSessionUpdate::AgentMessageChunk {
            message_id: "m".into(),
            content: super::super::types::TextBlock::new("x"),
            meta: None,
        }
    }

    fn usage() -> AcpSessionUpdate {
        AcpSessionUpdate::UsageUpdate {
            used: 1,
            size: 2,
            cost: None,
        }
    }

    fn info() -> AcpSessionUpdate {
        AcpSessionUpdate::SessionInfoUpdate {
            meta: json!({ "k": 1 }),
        }
    }

    async fn open_producer() -> (
        Arc<UpdateProducer>,
        tokio::sync::mpsc::UnboundedReceiver<Value>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let producer = UpdateProducer::new("s", tx);
        producer.commit_session_new_response().await;
        (producer, rx)
    }

    fn kinds(rx: &mut tokio::sync::mpsc::UnboundedReceiver<Value>) -> Vec<String> {
        let mut kinds = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            kinds.push(
                frame["params"]["update"]["sessionUpdate"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
        kinds
    }

    #[tokio::test(start_paused = true)]
    async fn telemetry_is_held_while_streaming_and_released_in_order() {
        let (producer, mut rx) = open_producer().await;
        producer.publish_event(&info(), 1, true).await;
        producer.publish_event(&chunk(), 1, true).await;
        producer.publish_event(&usage(), 1, true).await;
        assert_eq!(kinds(&mut rx), ["agent_message_chunk"]);
        producer.flush_deferred(true).await;
        assert_eq!(kinds(&mut rx).len(), 0);
        producer.flush_deferred(false).await;
        assert_eq!(kinds(&mut rx), ["session_info_update", "usage_update"]);
    }

    #[tokio::test(start_paused = true)]
    async fn telemetry_is_not_held_when_not_streaming() {
        let (producer, mut rx) = open_producer().await;
        producer.publish_event(&usage(), 1, false).await;
        assert_eq!(kinds(&mut rx), ["usage_update"]);
    }

    #[tokio::test(start_paused = true)]
    async fn held_telemetry_is_released_after_silence_not_total_hold_time() {
        let (producer, mut rx) = open_producer().await;
        producer.publish_event(&usage(), 1, true).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        producer.publish_event(&chunk(), 1, true).await;
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(kinds(&mut rx), ["agent_message_chunk"]);
        tokio::time::sleep(Duration::from_millis(101)).await;
        assert_eq!(kinds(&mut rx), ["usage_update"]);
    }
}
