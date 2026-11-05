//! Timer and rate-tracking state for keep-alive, frame read-rate, and write
//! timeout handling.
use ntex_io::{IoBoxed, cfg::IoConfig};
use ntex_util::time::Seconds;

/// Dispatcher timer and frame read state.
///
/// The transport has a single dispatcher timer, `active` records what it is
/// currently armed for.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct Timers {
    /// Purpose of the transport timer; updated whenever the dispatcher arms or stops it.
    pub(super) active: Timer,
    /// Partial-frame progress retained across decode attempts and streamed payload chunks.
    pub(super) read: ReadPhase,
}

/// Progress of frame decoding on the connection.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum ReadPhase {
    /// No partial frame is buffered.
    Idle,
    /// A frame has started but is not complete, including a publish
    /// whose payload chunks are still being received.
    ReadingFrame(ReadProgress),
}

/// The purpose of the armed dispatcher timer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) enum Timer {
    /// No dispatcher deadline is armed.
    Stopped,
    /// Deadline for receiving the next complete control packet while idle or partially reading.
    KeepAlive,
    /// Rate-check period for an incomplete frame, subject to its cumulative time budget.
    FrameRead,
    /// Write timeout, from enabling write backpressure until it is disabled.
    Write,
}

/// Byte accounting and remaining timeout budget for the current incomplete frame.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(super) struct ReadProgress {
    /// Application read buffer length at the last decode attempt.
    pub(super) remains: u32,
    /// Bytes received during the current rate period, including bytes the
    /// codec consumed without producing a frame.
    pub(super) consumed: u32,
    /// Remaining cumulative frame-read budget.
    pub(super) max_timeout: Seconds,
}

impl Timers {
    /// Creates idle timer state.
    ///
    /// The dispatcher starts after the MQTT handshake, so the connection is
    /// idle; the dispatcher selects a timer after its first decode attempt. A timer left
    /// on the transport by the handshake does not apply to the dispatcher.
    pub(super) fn new(io: &IoBoxed) -> Self {
        io.stop_timer();
        Timers {
            active: Timer::Stopped,
            read: ReadPhase::Idle,
        }
    }

    /// Updates the read phase after a decode attempt.
    ///
    /// `item` is `true` when a complete frame was decoded. A partial item,
    /// a part of a streamed publish, keeps the frame in progress.
    /// `remains` is the buffered input left by the decoder and `consumed` the
    /// input it took.
    pub(super) fn update_read(&mut self, cfg: &IoConfig, item: bool, remains: u32, consumed: u32) {
        if item {
            self.read = ReadPhase::Idle;
            return;
        }
        let partial = remains != 0 || consumed != 0;

        let Some(params) = cfg.frame_read_rate() else {
            self.read = if partial {
                ReadPhase::ReadingFrame(ReadProgress::EMPTY)
            } else {
                ReadPhase::Idle
            };
            return;
        };

        if self.read == ReadPhase::Idle {
            if !partial {
                return;
            }
            self.read = ReadPhase::ReadingFrame(ReadProgress {
                max_timeout: params.max_timeout,
                ..ReadProgress::EMPTY
            });
        }
        if let Some(p) = self.read.progress() {
            let received = remains.saturating_add(consumed).saturating_sub(p.remains);
            p.consumed = p.consumed.saturating_add(received);
            p.remains = remains;
        }
    }

    /// Restarts rate tracking of a partial frame with a fresh period and
    /// `max_timeout` budget, used when the service is not ready.
    pub(super) fn reset_read(&mut self, cfg: &IoConfig) {
        if let (Some(params), Some(p)) = (cfg.frame_read_rate(), self.read.progress()) {
            p.consumed = 0;
            p.max_timeout = params.max_timeout;
        }
    }

    /// Selects the read-side timer.
    ///
    /// A frame being read is bounded by the frame read rate, when one is
    /// configured. Otherwise keep-alive applies; MQTT keep-alive bounds the
    /// time between complete control packets.
    pub(super) fn select(&self, cfg: &IoConfig, keepalive: bool, handling: bool) -> Timer {
        match self.read {
            ReadPhase::ReadingFrame(_) if cfg.frame_read_rate().is_some() => Timer::FrameRead,
            _ if keepalive && !handling => Timer::KeepAlive,
            _ => Timer::Stopped,
        }
    }
}

impl ReadPhase {
    /// Returns mutable rate-tracking state only while a frame is incomplete.
    pub(super) fn progress(&mut self) -> Option<&mut ReadProgress> {
        match self {
            ReadPhase::Idle => None,
            ReadPhase::ReadingFrame(p) => Some(p),
        }
    }
}

impl ReadProgress {
    /// Initial byte counters with no cumulative read-time budget.
    pub(super) const EMPTY: ReadProgress = ReadProgress {
        remains: 0,
        consumed: 0,
        max_timeout: Seconds::ZERO,
    };
}
