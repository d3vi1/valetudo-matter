use std::{
    cell::{Cell, RefCell},
    time,
};

use anyhow::{Context as _, bail};
use enumset::EnumSet;
use log::{debug, error, warn};
use rand::thread_rng;
use rs_matter::dm::AttrChangeNotifier;
use smol::{Timer, stream::StreamExt};

mod capabilities;
mod state;

pub(crate) use capabilities::*;
pub(crate) use state::*;

use crate::{handlers::VersionedCell, http::ValetudoClient};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct DeviceState {
    pub(crate) value: StatusValue,
    pub(crate) flag: StatusFlag,
}

impl Default for DeviceState {
    fn default() -> Self {
        Self {
            value: StatusValue::Idle,
            flag: StatusFlag::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub(crate) struct MapSegment {
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) name: Option<String>,
}

pub(crate) struct Device {
    pub(crate) client: ValetudoClient,
    pub(crate) capabilities: EnumSet<Capability>,
    pub(crate) cleaning_presets: EnumSet<Preset>,
    pub(crate) current_preset: VersionedCell<Option<Preset>>,
    pub(crate) current_state: VersionedCell<DeviceState>,
    pub(crate) identify_time: VersionedCell<u16>,
    pub(crate) segments: Vec<MapSegment>,
    pub(crate) battery: VersionedCell<BatteryState>,
    pub(crate) cycle_mode: VersionedCell<CycleMode>,
    pub(crate) cycle_known: Cell<bool>,
    pub(crate) robot_error: RefCell<Option<RobotError>>,
    pending_events: RefCell<std::collections::VecDeque<OperationalEvent>>,
    cycle_observed_active: Cell<bool>,
    start_pending_since: Cell<Option<std::time::Instant>>,
    pub(crate) paused_activity: Cell<Option<DeviceState>>,
    /// Only fresh observations in this connected session may authorize Resume.
    /// Saved history has no operation ID and cannot establish this trust.
    pub(crate) resume_context_trusted: Cell<bool>,
    pub(crate) selected_areas: RefCell<Vec<u32>>,
    command_in_progress: Cell<bool>,
    completion_generation: Cell<u64>,
    observation_epoch: Cell<u64>,
}

impl Device {
    pub(crate) async fn init(client: ValetudoClient) -> anyhow::Result<Self> {
        debug!("fetching capabilities");
        let mut capabilities: EnumSet<Capability> = client
            .get("/api/v2/robot/capabilities")
            .await
            .context("Failed to fetch capabilities")?;

        if capabilities.remove(Capability::Unknown) {
            warn!("Ignoring unsupported Valetudo capabilities");
        }
        debug!("capabilities: {capabilities:?}");
        if !capabilities.contains(Capability::BasicControlCapability) {
            bail!("Device requires at least BasicControlCapability");
        }

        let cleaning_presets = if capabilities.contains(Capability::OperationModeControlCapability)
        {
            match client
                .get::<Vec<String>>(
                    "/api/v2/robot/capabilities/OperationModeControlCapability/presets",
                )
                .await
            {
                Ok(presets) => known_presets(presets),
                Err(error) => {
                    warn!("Optional cleaning presets unavailable: {error:#}");
                    EnumSet::new()
                }
            }
        } else {
            EnumSet::new()
        };

        if cleaning_presets.is_empty() {
            warn!("No recognized presets; RVC Clean Mode will be omitted");
        }
        debug!("fetching status");
        let attributes: Vec<state::StateAttribute> = client
            .get("/api/v2/robot/state/attributes")
            .await
            .context("Failed to fetch initial status")?;
        let Some(state::StateAttribute::StatusStateAttribute { value, flag, .. }) = attributes
            .iter()
            .find(|attr| matches!(attr, state::StateAttribute::StatusStateAttribute { .. }))
        else {
            bail!("No status attribute in api response");
        };

        debug!("current state: {value:?}/{flag:?}");
        let current_state = VersionedCell::new(
            DeviceState {
                value: *value,
                flag: *flag,
            },
            &mut thread_rng(),
        );

        let current_preset_value = attributes.iter().find_map(|attr| match attr {
            state::StateAttribute::PresetSelectionStateAttribute { r#type, value }
                if r#type == "operation_mode" =>
            {
                Some(value.as_str())
            }
            _ => None,
        });

        let segments: Vec<MapSegment> =
            if capabilities.contains(Capability::MapSegmentationCapability) {
                client
                    .get("/api/v2/robot/capabilities/MapSegmentationCapability")
                    .await
                    .unwrap_or_else(|error| {
                        warn!("Optional map segments unavailable: {error:#}");
                        Vec::new()
                    })
            } else {
                Vec::new()
            };

        let default_preset = current_preset_value
            .and_then(preset_from_str)
            .filter(|preset| cleaning_presets.contains(*preset));
        debug!("current preset: {default_preset:?}");

        Ok(Self {
            client: client.clone(),
            capabilities,
            cleaning_presets,
            current_preset: VersionedCell::new(default_preset, &mut thread_rng()),
            current_state,
            identify_time: VersionedCell::new(0, &mut thread_rng()),
            segments,
            battery: VersionedCell::new(
                attributes
                    .iter()
                    .find_map(|attr| match attr {
                        StateAttribute::BatteryStateAttribute { level, flag } => {
                            Some(BatteryState::from_api(*level, *flag))
                        }
                        _ => None,
                    })
                    .unwrap_or_default(),
                &mut thread_rng(),
            ),
            resume_context_trusted: Cell::new(
                matches!(*value, StatusValue::Cleaning | StatusValue::Returning)
                    || (*value == StatusValue::Moving && *flag == StatusFlag::Mapping),
            ),
            cycle_known: Cell::new(cycle_is_unambiguous(DeviceState {
                value: *value,
                flag: *flag,
            })),
            cycle_mode: VersionedCell::new(
                CycleMode::Idle.observe(DeviceState {
                    value: *value,
                    flag: *flag,
                }),
                &mut thread_rng(),
            ),
            robot_error: RefCell::new(attributes.iter().find_map(|attr| match attr {
                StateAttribute::StatusStateAttribute { error, .. } => error.clone(),
                _ => None,
            })),
            pending_events: RefCell::new(std::collections::VecDeque::new()),
            start_pending_since: Cell::new(None),
            paused_activity: Cell::new(None),
            cycle_observed_active: Cell::new(
                matches!(*value, StatusValue::Cleaning)
                    || (*value == StatusValue::Moving && *flag == StatusFlag::Mapping),
            ),
            selected_areas: RefCell::new(Vec::new()),
            command_in_progress: Cell::new(false),
            completion_generation: Cell::new(0),
            observation_epoch: Cell::new(0),
        })
    }

    /// A background worker updating the device state in response to SSE
    /// events. Notifies Matter subscriptions when state changes.
    pub(crate) async fn monitor_status(
        &self,
        notify: &impl AttrChangeNotifier,
        run_mode_cluster: u32,
        clean_mode_cluster: u32,
        operational_state_cluster: u32,
    ) {
        loop {
            match self
                .monitor_status_once(
                    notify,
                    run_mode_cluster,
                    clean_mode_cluster,
                    operational_state_cluster,
                )
                .await
                .context("Stream error")
            {
                Ok(_) => debug!("GET /api/v2/robot/state/attributes/sse: stream exited"),
                Err(e) => error!("GET /api/v2/robot/state/attributes/sse: {e:#}"),
            }

            // A dead/malformed stream must not leave the last known state actionable.
            self.apply_snapshot(
                &[],
                notify,
                run_mode_cluster,
                clean_mode_cluster,
                operational_state_cluster,
            );
            Timer::after(time::Duration::from_secs(1)).await;
        }
    }

    async fn monitor_status_once(
        &self,
        notify: &impl AttrChangeNotifier,
        run_mode_cluster: u32,
        clean_mode_cluster: u32,
        operational_state_cluster: u32,
    ) -> anyhow::Result<()> {
        // Poll the state once before streaming. This helps fix issues if the
        // SSE stream dies.
        let attributes: Vec<state::StateAttribute> = self
            .client
            .get("/api/v2/robot/state/attributes")
            .await
            .context("Failed to fetch initial status")?;
        self.apply_snapshot(
            &attributes,
            notify,
            run_mode_cluster,
            clean_mode_cluster,
            operational_state_cluster,
        );

        let mut stream = self
            .client
            .sse("/api/v2/robot/state/attributes/sse")
            .await?;
        let mut refresh_at = std::time::Instant::now() + time::Duration::from_secs(15);
        loop {
            // Keep the deadline independent of event traffic. A missed status event
            // must be repaired even while unrelated battery/preset events continue.
            let next = smol::future::or(async { Ok(stream.next().await) }, async {
                Timer::at(refresh_at).await;
                Err(())
            })
            .await;
            let ev: Vec<state::StateAttribute> = match next {
                Ok(Some(event)) => serde_json::from_str(&event?).context("Invalid event")?,
                Ok(None) => break,
                Err(()) => {
                    let snapshot: Vec<StateAttribute> =
                        self.client.get("/api/v2/robot/state/attributes").await?;
                    refresh_at = std::time::Instant::now() + time::Duration::from_secs(15);
                    self.apply_snapshot(
                        &snapshot,
                        notify,
                        run_mode_cluster,
                        clean_mode_cluster,
                        operational_state_cluster,
                    );
                    continue;
                }
            };
            self.apply_attributes(
                &ev,
                notify,
                run_mode_cluster,
                clean_mode_cluster,
                operational_state_cluster,
            );
        }

        Ok(())
    }

    /// Full snapshots authoritatively remove absent values. SSE updates may be
    /// partial, so only fields present in those updates are applied.
    fn apply_snapshot(
        &self,
        attrs: &[StateAttribute],
        notify: &impl AttrChangeNotifier,
        run_mode_cluster: u32,
        clean_mode_cluster: u32,
        operational_state_cluster: u32,
    ) {
        let mut missing = Vec::new();
        if !attrs
            .iter()
            .any(|attr| matches!(attr, StateAttribute::StatusStateAttribute { .. }))
        {
            missing.push(StateAttribute::StatusStateAttribute {
                value: StatusValue::Unknown,
                flag: StatusFlag::Unknown,
                error: None,
            });
        }
        if !attrs
            .iter()
            .any(|attr| matches!(attr, StateAttribute::BatteryStateAttribute { .. }))
        {
            missing.push(StateAttribute::BatteryStateAttribute {
                level: None,
                flag: BatteryFlag::Unknown,
            });
        }
        let preset_type = "operation_mode";
        if !attrs.iter().any(|attr| matches!(attr, StateAttribute::PresetSelectionStateAttribute { r#type, .. } if r#type == preset_type)) {
            if self.current_preset.replace(None).is_some() { notify.notify_cluster_changed(1, clean_mode_cluster); }
        }
        self.apply_attributes(
            &missing,
            notify,
            run_mode_cluster,
            clean_mode_cluster,
            operational_state_cluster,
        );
        self.apply_attributes(
            attrs,
            notify,
            run_mode_cluster,
            clean_mode_cluster,
            operational_state_cluster,
        );
    }

    fn apply_attributes(
        &self,
        attrs: &[state::StateAttribute],
        notify: &impl AttrChangeNotifier,
        run_mode_cluster: u32,
        clean_mode_cluster: u32,
        operational_state_cluster: u32,
    ) {
        for attr in attrs {
            match attr {
                state::StateAttribute::BatteryStateAttribute { level, flag } => {
                    let next = BatteryState::from_api(*level, *flag);
                    if next != self.battery.get() {
                        self.battery.set(next);
                        self.current_state.dataver_changed();
                        notify.notify_cluster_changed(1, operational_state_cluster);
                    }
                }
                state::StateAttribute::StatusStateAttribute { value, flag, error } => {
                    let new_state = DeviceState {
                        value: *value,
                        flag: *flag,
                    };
                    debug!("setting state: {:?}/{:?}", value, flag);
                    let old = self.current_state.get();
                    if *value == StatusValue::Unknown {
                        self.resume_context_trusted.set(false);
                        self.cycle_known.set(false);
                        self.cycle_observed_active.set(false);
                        self.start_pending_since.set(None);
                        self.paused_activity.set(None);
                        self.cycle_mode.set(CycleMode::Idle);
                        self.observation_epoch
                            .set(self.observation_epoch.get().wrapping_add(1));
                    } else if matches!(*value, StatusValue::Cleaning | StatusValue::Returning)
                        || (*value == StatusValue::Moving && *flag == StatusFlag::Mapping)
                    {
                        self.resume_context_trusted.set(true);
                    }

                    if *value == StatusValue::Paused && old.value != StatusValue::Paused {
                        self.paused_activity
                            .set(self.resume_context_trusted.get().then_some(old));
                    } else if *value != StatusValue::Paused && *value != StatusValue::Unknown {
                        self.paused_activity.set(None);
                    }
                    let old_cycle = self.cycle_mode.get();
                    if cycle_is_unambiguous(new_state) {
                        self.cycle_known.set(true);
                    }
                    let observed_active = *value == StatusValue::Cleaning
                        || (*value == StatusValue::Moving && *flag == StatusFlag::Mapping)
                        || (old_cycle != CycleMode::Idle && *value == StatusValue::Returning);
                    if observed_active
                        || (*value == StatusValue::Error && old_cycle != CycleMode::Idle)
                    {
                        self.cycle_observed_active.set(true);
                        self.start_pending_since.set(None);
                    }
                    let mut next_cycle = old_cycle.observe(new_state);
                    // A start HTTP acknowledgement sets CurrentMode immediately,
                    // but an old idle/docked snapshot cannot prove completion.
                    if old_cycle != CycleMode::Idle
                        && next_cycle == CycleMode::Idle
                        && !self.cycle_observed_active.get()
                    {
                        if self
                            .start_pending_since
                            .get()
                            .is_some_and(|since| since.elapsed() >= time::Duration::from_secs(30))
                        {
                            // The API accepted start, but no activity appeared within
                            // two reconciliation intervals. Do not hold CurrentMode
                            // active forever or manufacture a hardware error state.
                            self.queue_completion(1);
                            self.start_pending_since.set(None);
                        } else {
                            next_cycle = old_cycle;
                        }
                    }
                    self.cycle_mode.set(next_cycle);
                    let error_changed = *self.robot_error.borrow() != *error;
                    *self.robot_error.borrow_mut() = error.clone();
                    if *value == StatusValue::Error
                        && (old.value != StatusValue::Error || error_changed)
                    {
                        self.queue_event(OperationalEvent::Error(error.clone()));
                    }
                    if old_cycle != CycleMode::Idle
                        && next_cycle == CycleMode::Idle
                        && self.cycle_observed_active.get()
                    {
                        self.queue_completion(if old.value == StatusValue::Error {
                            2
                        } else {
                            0
                        });
                        self.cycle_observed_active.set(false);
                    }
                    self.current_state.set(new_state);
                    if error_changed {
                        self.current_state.dataver_changed();
                        notify.notify_cluster_changed(1, operational_state_cluster);
                    }
                    if new_state != old || next_cycle != old_cycle {
                        log::info!(
                            "Robot state update via Valetudo API: {:?}/{:?} -> {:?}/{:?}",
                            old.value,
                            old.flag,
                            value,
                            flag
                        );
                        notify.notify_cluster_changed(1, run_mode_cluster);
                        notify.notify_cluster_changed(1, operational_state_cluster);
                    }
                }
                state::StateAttribute::PresetSelectionStateAttribute { r#type, value }
                    if r#type == "operation_mode" =>
                {
                    let preset =
                        preset_from_str(value).filter(|p| self.cleaning_presets.contains(*p));
                    let old = self.current_preset.get();
                    self.current_preset.set(preset);
                    if preset != old {
                        notify.notify_cluster_changed(1, clean_mode_cluster);
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn ensure_known_status(&self) -> anyhow::Result<()> {
        if self.current_state.get().value == StatusValue::Unknown {
            bail!("Robot state is not understood; refusing control until it recovers");
        }
        Ok(())
    }

    pub(crate) fn set_cycle_mode(
        &self,
        mode: CycleMode,
        ctx: &impl rs_matter::dm::OperationContext,
    ) {
        if self.cycle_mode.get() != CycleMode::Idle && mode == CycleMode::Idle {
            self.queue_completion(if self.current_state.get().value == StatusValue::Error {
                2
            } else {
                0
            });
        }
        let already_observed = mode != CycleMode::Idle
            && self.cycle_mode.get() == mode
            && self.cycle_observed_active.get();
        self.cycle_observed_active.set(already_observed);
        self.start_pending_since
            .set((mode != CycleMode::Idle && !already_observed).then(std::time::Instant::now));
        self.cycle_known.set(true);
        self.cycle_mode.set_notify(mode, ctx);
    }

    /// A late HTTP acknowledgement cannot reopen a cycle whose observed
    /// completion was already published while that request was in flight.
    pub(crate) fn start_receipt(&self) -> (u64, u64) {
        (
            self.completion_generation.get(),
            self.observation_epoch.get(),
        )
    }

    pub(crate) fn acknowledge_start(
        &self,
        mode: CycleMode,
        receipt: (u64, u64),
        ctx: &impl rs_matter::dm::OperationContext,
    ) -> bool {
        if self.completion_generation.get() != receipt.0 {
            true // The operation was already observed completing.
        } else if self.observation_epoch.get() != receipt.1 {
            false // An observation gap makes the physical outcome uncertain.
        } else {
            self.set_cycle_mode(mode, ctx);
            true
        }
    }

    fn queue_completion(&self, code: u8) {
        self.completion_generation
            .set(self.completion_generation.get().wrapping_add(1));
        self.queue_event(OperationalEvent::Completed(code));
    }

    /// Serialize command validation, physical dispatch and acknowledged state
    /// publication across fabrics. Status observations continue while HTTP waits.
    pub(crate) fn try_command(&self) -> Option<CommandGuard<'_>> {
        if self.command_in_progress.replace(true) {
            None
        } else {
            Some(CommandGuard(&self.command_in_progress))
        }
    }

    pub(crate) fn supports_clean_modes(&self) -> bool {
        self.cleaning_presets.len() >= 2
            && self
                .cleaning_presets
                .iter()
                .any(|preset| matches!(preset, Preset::Vacuum | Preset::Mop | Preset::VacuumAndMop))
    }

    pub(crate) fn cycle_active(&self) -> bool {
        !self.cycle_known.get() || self.cycle_mode.get() != CycleMode::Idle
    }

    pub(crate) async fn resume(&self) -> anyhow::Result<()> {
        // BasicControl owns vendor-specific resume semantics (including segments).
        self.ensure_known_status()?;
        if self
            .paused_activity
            .get()
            .is_some_and(|state| state.value == StatusValue::Returning)
        {
            self.go_home().await
        } else {
            self.client
                .put(
                    "/api/v2/robot/capabilities/BasicControlCapability",
                    r#"{"action":"start"}"#.into(),
                )
                .await
        }
    }

    pub(crate) async fn start_cleaning(&self) -> anyhow::Result<()> {
        self.ensure_known_status()?;
        let selected = self.selected_areas.borrow().clone();
        if selected.is_empty() {
            debug!("starting full clean");
            self.client
                .put(
                    "/api/v2/robot/capabilities/BasicControlCapability",
                    r#"{"action": "start"}"#.to_owned(),
                )
                .await
        } else {
            let segment_ids: Vec<String> = selected
                .iter()
                .filter_map(|area_id| {
                    self.segments
                        .iter()
                        .find(|s| segment_area_id(s) == Some(*area_id))
                        .map(|s| format!(r#""{}""#, s.id))
                })
                .collect();

            debug!("starting segment clean for segments: {segment_ids:?}");
            let body = format!(
                r#"{{"action": "start_segment_action", "segment_ids": [{}], "iterations": 1}}"#,
                segment_ids.join(", "),
            );
            self.client
                .put("/api/v2/robot/capabilities/MapSegmentationCapability", body)
                .await
        }
    }

    pub(crate) async fn pause(&self) -> anyhow::Result<()> {
        self.ensure_known_status()?;
        debug!("starting pause command to robot");
        self.client
            .put(
                "/api/v2/robot/capabilities/BasicControlCapability",
                r#"{"action": "pause"}"#.to_owned(),
            )
            .await
    }

    pub(crate) async fn go_home(&self) -> anyhow::Result<()> {
        self.ensure_known_status()?;
        debug!("starting home command to robot");
        self.client
            .put(
                "/api/v2/robot/capabilities/BasicControlCapability",
                r#"{"action": "home"}"#.to_owned(),
            )
            .await
    }

    pub(crate) async fn set_preset(&self, preset: Preset) -> anyhow::Result<()> {
        self.ensure_known_status()?;
        let name = match preset {
            Preset::Vacuum => "vacuum",
            Preset::Mop => "mop",
            Preset::VacuumAndMop => "vacuum_and_mop",
            Preset::VacuumThenMop => "vacuum_then_mop",
        };

        debug!("setting operation mode preset to {name}");
        self.client
            .put(
                "/api/v2/robot/capabilities/OperationModeControlCapability/preset",
                format!(r#"{{"name": "{name}"}}"#),
            )
            .await
    }

    pub(crate) async fn stop(&self) -> anyhow::Result<()> {
        self.ensure_known_status()?;
        debug!("starting stop command to robot");
        self.client
            .put(
                "/api/v2/robot/capabilities/BasicControlCapability",
                r#"{"action": "stop"}"#.to_owned(),
            )
            .await
    }
}

/// Map a Valetudo segment to a Matter area ID by parsing the string ID.
pub(crate) fn segment_area_id(segment: &MapSegment) -> Option<u32> {
    segment.id.parse().ok()
}

fn preset_from_str(s: &str) -> Option<Preset> {
    match s {
        "vacuum" => Some(Preset::Vacuum),
        "mop" => Some(Preset::Mop),
        "vacuum_and_mop" => Some(Preset::VacuumAndMop),
        "vacuum_then_mop" => Some(Preset::VacuumThenMop),
        _ => None,
    }
}

fn known_presets(values: Vec<String>) -> EnumSet<Preset> {
    values.iter().filter_map(|v| preset_from_str(v)).collect()
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;
    #[test]
    fn future_capabilities_and_attributes_are_ignored() {
        let caps: EnumSet<Capability> =
            serde_json::from_str(r#"["BasicControlCapability","FutureCapability"]"#).unwrap();
        assert!(caps.contains(Capability::BasicControlCapability));
        assert!(caps.contains(Capability::Unknown));
        let attrs: Vec<StateAttribute> = serde_json::from_str(r#"[{"__class":"FutureAttribute","value":{"anything":42}},{"__class":"StatusStateAttribute","value":"future_state","flag":"future_flag"}]"#).unwrap();
        assert!(matches!(attrs[0], StateAttribute::Unknown));
        assert!(matches!(
            attrs[1],
            StateAttribute::StatusStateAttribute {
                value: StatusValue::Unknown,
                flag: StatusFlag::Unknown,
                error: None
            }
        ));
    }
    #[test]
    fn unknown_presets_are_not_advertised_as_known_modes() {
        let presets = known_presets(vec!["vacuum".into(), "future".into(), "mop".into()]);
        assert_eq!(presets.len(), 2);
        assert!(presets.contains(Preset::Vacuum));
        assert!(known_presets(vec!["future".into()]).is_empty());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OperationalEvent {
    Error(Option<RobotError>),
    Completed(u8),
}

impl Device {
    fn queue_event(&self, event: OperationalEvent) {
        const MAX_PENDING_EVENTS: usize = 32;
        let mut pending = self.pending_events.borrow_mut();
        if pending.len() == MAX_PENDING_EVENTS {
            log::warn!("RVC event delivery backlog full; evicting oldest event");
            pending.pop_front();
        }
        pending.push_back(event);
    }

    /// Drain transition events into the Matter event store. Keep an event queued
    /// on storage failure rather than silently acknowledging its delivery.
    pub(crate) fn flush_events(&self, emitter: &impl rs_matter::dm::EventEmitter) {
        use rs_matter::im::EventPriority;
        loop {
            let Some(event) = self.pending_events.borrow().front().cloned() else {
                break;
            };
            let (id, priority) = match event {
                OperationalEvent::Error(_) => (0, EventPriority::Critical),
                OperationalEvent::Completed(_) => (1, EventPriority::Info),
            };
            let result =
                emitter.emit_event(1, 0x61, id, priority, |mut writer| event.write(&mut writer));
            if let Err(error) = result {
                warn!("Cannot store RVC event: {error}");
                break;
            }
            self.pending_events.borrow_mut().pop_front();
        }
    }
}

pub(crate) fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

impl OperationalEvent {
    fn write(
        &self,
        writer: &mut impl rs_matter::tlv::TLVWrite,
    ) -> Result<(), rs_matter::error::Error> {
        use rs_matter::{dm::events::EVENT_DATA_TAG, tlv::TLVTag};
        writer.start_struct(&EVENT_DATA_TAG)?;
        match self {
            Self::Error(error) => {
                writer.start_struct(&TLVTag::Context(0))?;
                writer.u8(&TLVTag::Context(0), 2)?;
                let label = error
                    .as_ref()
                    .filter(|e| !e.vendor_error_code.is_empty())
                    .map(|e| truncate_utf8(&e.vendor_error_code, 64))
                    .unwrap_or("Valetudo error");
                writer.utf8(&TLVTag::Context(1), label)?;
                if let Some(error) = error {
                    writer.utf8(&TLVTag::Context(2), truncate_utf8(&error.message, 64))?;
                }
                writer.end_container()?;
            }
            Self::Completed(code) => writer.u8(&TLVTag::Context(0), *code)?,
        }
        writer.end_container()
    }
}

#[cfg(test)]
mod cycle_tests {
    use super::*;

    pub(super) fn device() -> Device {
        let executor = Box::leak(Box::new(smol::LocalExecutor::new()));
        Device {
            client: ValetudoClient::new(executor, "http://127.0.0.1:1".parse().unwrap()),
            capabilities: EnumSet::new(),
            cleaning_presets: EnumSet::new(),
            current_preset: VersionedCell::new(None, &mut thread_rng()),
            current_state: VersionedCell::new(DeviceState::default(), &mut thread_rng()),
            identify_time: VersionedCell::new(0, &mut thread_rng()),
            segments: Vec::new(),
            battery: VersionedCell::new(BatteryState::default(), &mut thread_rng()),
            cycle_known: Cell::new(true),
            resume_context_trusted: Cell::new(false),
            cycle_mode: VersionedCell::new(CycleMode::Idle, &mut thread_rng()),
            robot_error: RefCell::new(None),
            pending_events: RefCell::new(Default::default()),
            cycle_observed_active: Cell::new(false),
            start_pending_since: Cell::new(None),
            paused_activity: Cell::new(None),
            selected_areas: RefCell::new(Vec::new()),
            command_in_progress: Cell::new(false),
            completion_generation: Cell::new(0),
            observation_epoch: Cell::new(0),
        }
    }
    fn status(device: &Device, value: StatusValue, flag: StatusFlag) {
        device.apply_attributes(
            &[StateAttribute::StatusStateAttribute {
                value,
                flag,
                error: None,
            }],
            &(),
            0x54,
            0x55,
            0x61,
        );
    }
    #[test]
    fn completion_requires_observed_operation_and_excludes_resumable_docking() {
        let device = device();
        // Accepted start is reflected in CurrentMode before firmware activity.
        device.cycle_mode.set(CycleMode::Cleaning);
        status(&device, StatusValue::Docked, StatusFlag::None);
        assert!(device.cycle_active());
        assert!(device.pending_events.borrow().is_empty());
        status(&device, StatusValue::Cleaning, StatusFlag::Segment);
        status(&device, StatusValue::Paused, StatusFlag::Resumable);
        status(&device, StatusValue::Returning, StatusFlag::Resumable);
        status(&device, StatusValue::Docked, StatusFlag::Resumable);
        assert!(device.cycle_active());
        assert!(device.pending_events.borrow().is_empty());
        status(&device, StatusValue::Cleaning, StatusFlag::Segment);
        status(&device, StatusValue::Returning, StatusFlag::None);
        status(&device, StatusValue::Docked, StatusFlag::None);
        assert!(!device.cycle_active());
        assert_eq!(
            *device.pending_events.borrow(),
            [OperationalEvent::Completed(0)]
        );
        status(&device, StatusValue::Docked, StatusFlag::None);
        assert_eq!(device.pending_events.borrow().len(), 1);
    }
    #[test]
    fn docking_without_a_cycle_does_not_emit_completion() {
        let device = device();
        status(&device, StatusValue::Returning, StatusFlag::None);
        status(&device, StatusValue::Docked, StatusFlag::None);
        assert!(device.pending_events.borrow().is_empty());
    }
    #[test]
    fn identical_error_snapshots_do_not_duplicate_events_and_backlog_is_bounded() {
        let device = device();
        status(&device, StatusValue::Error, StatusFlag::None);
        status(&device, StatusValue::Error, StatusFlag::None);
        assert_eq!(device.pending_events.borrow().len(), 1);
        for _ in 0..100 {
            status(&device, StatusValue::Idle, StatusFlag::None);
            status(&device, StatusValue::Error, StatusFlag::None);
        }
        assert_eq!(device.pending_events.borrow().len(), 32);
        struct FailingEmitter;
        impl rs_matter::dm::EventEmitter for FailingEmitter {
            fn emit_event<F>(
                &self,
                _: u16,
                _: u32,
                _: u32,
                _: rs_matter::im::EventPriority,
                _: F,
            ) -> Result<u64, rs_matter::error::Error>
            where
                F: FnOnce(
                    rs_matter::dm::events::EventTLVWrite<'_>,
                ) -> Result<(), rs_matter::error::Error>,
            {
                Err(rs_matter::error::ErrorCode::Failure.into())
            }
        }
        device.flush_events(&FailingEmitter);
        assert_eq!(device.pending_events.borrow().len(), 32);
    }
    #[test]
    fn partial_events_preserve_battery_but_missing_full_snapshot_and_disconnect_clear_it() {
        let device = device();
        device.apply_attributes(
            &[StateAttribute::BatteryStateAttribute {
                level: Some(100.0),
                flag: BatteryFlag::Charged,
            }],
            &(),
            0x54,
            0x55,
            0x61,
        );
        device.apply_attributes(&[StateAttribute::Unknown], &(), 0x54, 0x55, 0x61);
        assert_eq!(device.battery.get().half_percent, Some(200));
        device.apply_snapshot(
            &[StateAttribute::StatusStateAttribute {
                value: StatusValue::Docked,
                flag: StatusFlag::None,
                error: None,
            }],
            &(),
            0x54,
            0x55,
            0x61,
        );
        assert_eq!(device.battery.get(), BatteryState::default());
        device.apply_attributes(
            &[StateAttribute::BatteryStateAttribute {
                level: Some(50.0),
                flag: BatteryFlag::Charging,
            }],
            &(),
            0x54,
            0x55,
            0x61,
        );
        device.apply_snapshot(&[], &(), 0x54, 0x55, 0x61);
        assert_eq!(device.battery.get(), BatteryState::default());
        assert_eq!(device.current_state.get().value, StatusValue::Unknown);
        assert!(device.ensure_known_status().is_err());
    }
    #[test]
    fn start_failure_and_missing_activity_eventually_end_the_requested_cycle() {
        let device = device();
        device.cycle_mode.set(CycleMode::Cleaning);
        device
            .start_pending_since
            .set(Some(std::time::Instant::now()));
        status(&device, StatusValue::Error, StatusFlag::None);
        status(&device, StatusValue::Idle, StatusFlag::None);
        assert_eq!(device.cycle_mode.get(), CycleMode::Idle);
        assert!(
            device
                .pending_events
                .borrow()
                .contains(&OperationalEvent::Completed(2))
        );
        device.pending_events.borrow_mut().clear();
        device.cycle_mode.set(CycleMode::Cleaning);
        device.start_pending_since.set(Some(
            std::time::Instant::now() - time::Duration::from_secs(31),
        ));
        status(&device, StatusValue::Idle, StatusFlag::None);
        assert_eq!(device.cycle_mode.get(), CycleMode::Idle);
        assert_eq!(
            *device.pending_events.borrow(),
            [OperationalEvent::Completed(1)]
        );
    }
    #[test]
    fn clean_mode_requires_two_options_and_a_vacuum_or_mop_semantic() {
        let mut device = device();
        assert!(!device.supports_clean_modes());
        device.cleaning_presets.insert(Preset::Vacuum);
        assert!(!device.supports_clean_modes());
        device.cleaning_presets.insert(Preset::Mop);
        assert!(device.supports_clean_modes());
        device.cleaning_presets = [Preset::VacuumThenMop].into_iter().collect();
        assert!(!device.supports_clean_modes());
    }

    #[test]
    fn event_payload_is_valid_matter_tlv_with_bounded_utf8_error() {
        use rs_matter::{tlv::TLVElement, utils::storage::WriteBuf};
        let mut buffer = [0; 512];
        let mut writer = WriteBuf::new(&mut buffer);
        OperationalEvent::Error(Some(RobotError {
            message: "ș".repeat(40),
            vendor_error_code: "1".into(),
        }))
        .write(&mut writer)
        .unwrap();
        let data = TLVElement::new(writer.as_slice()).structure().unwrap();
        let error = data.find_ctx(0).unwrap().structure().unwrap();
        assert_eq!(error.find_ctx(0).unwrap().u8().unwrap(), 2);
        assert_eq!(error.find_ctx(1).unwrap().utf8().unwrap(), "1");
        assert_eq!(error.find_ctx(2).unwrap().utf8().unwrap(), "ș".repeat(32));
        let mut writer = WriteBuf::new(&mut buffer);
        OperationalEvent::Completed(0).write(&mut writer).unwrap();
        let data = TLVElement::new(writer.as_slice()).structure().unwrap();
        assert_eq!(data.find_ctx(0).unwrap().u8().unwrap(), 0);
        assert!(data.find_ctx(1).unwrap().is_empty()); // No fabricated timing statistics.
    }
}

#[cfg(test)]
mod tests;

pub(crate) struct CommandGuard<'a>(&'a Cell<bool>);
impl Drop for CommandGuard<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

fn cycle_is_unambiguous(state: DeviceState) -> bool {
    matches!(state.value, StatusValue::Cleaning)
        || (state.value == StatusValue::Moving && state.flag == StatusFlag::Mapping)
        || (matches!(state.value, StatusValue::Docked | StatusValue::Idle)
            && state.flag == StatusFlag::None)
}
