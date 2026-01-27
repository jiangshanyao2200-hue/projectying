use std::borrow::Cow;

use pulldown_cmark::{Alignment, CowStr, Event, Options, Parser, Tag, TagEnd};

use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::block::BorderType;
use ratatui::widgets::{Block, Borders, Clear, Padding, Paragraph, Wrap};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::{
    CommandSpec, ContextLine, Core, MindKind, Mode, PulseDir, Role, SettingsFocus, THINKING_MARKER,
};

#[derive(Debug, Clone, Copy)]
pub struct HeartbeatStyle {
    pub intensity: f32,
    pub visible: bool,
}

pub fn fill_background(f: &mut ratatui::Frame, theme: &Theme, area: Rect) {
    let b = Block::default()
        .borders(Borders::NONE)
        .style(Style::default().bg(theme.bg));
    f.render_widget(b, area);
}

const MAX_CHAT_BYTES: usize = 12_000;
const USER_SUMMARY_LINE_THRESHOLD: usize = 5;
const USER_SUMMARY_CHAR_THRESHOLD: usize = 800;
const TOOL_LABEL_WIDTH: usize = 6;

fn truncate_to_width(text: &str, max_cells: usize) -> String {
    if max_cells == 0 || text.is_empty() {
        return String::new();
    }
    let mut out = String::new();
    let mut used = 0usize;
    for ch in text.chars() {
        let w = UnicodeWidthChar::width(ch).unwrap_or(1);
        if used.saturating_add(w) > max_cells {
            break;
        }
        out.push(ch);
        used = used.saturating_add(w);
        if used >= max_cells {
            break;
        }
    }
    out
}

fn build_settings_tab_lines(
    menu_items: &[String],
    selected: usize,
    focus: SettingsFocus,
    theme: &Theme,
    max_w: usize,
) -> (Vec<Span<'static>>, Vec<Span<'static>>) {
    let mut line1: Vec<Span> = Vec::new();
    let mut line2: Vec<Span> = Vec::new();
    let mut width1 = 0usize;
    let mut width2 = 0usize;
    let mut use_second = false;
    for (idx, item) in menu_items.iter().enumerate() {
        let is_sel = idx == selected;
        let is_focus = matches!(focus, SettingsFocus::Tabs);
        let style = if is_sel && is_focus {
            Style::default()
                .fg(theme.bg)
                .bg(theme.yellow)
                .add_modifier(Modifier::BOLD)
        } else if is_sel {
            Style::default().fg(theme.yellow).bg(theme.bg)
        } else {
            Style::default().fg(theme.dim).bg(theme.bg)
        };
        let label = format!(" {item} ");
        let label_w = UnicodeWidthStr::width(label.as_str());
        let gap_w = 2;
        if !use_second {
            let needed = if width1 == 0 {
                label_w
            } else {
                label_w.saturating_add(gap_w)
            };
            if width1.saturating_add(needed) > max_w && !line1.is_empty() {
                use_second = true;
            }
        }
        if use_second {
            if !line2.is_empty() {
                line2.push(Span::styled("  ", Style::default().bg(theme.bg)));
                width2 = width2.saturating_add(gap_w);
            }
            line2.push(Span::styled(label, style));
            width2 = width2.saturating_add(label_w);
        } else {
            if !line1.is_empty() {
                line1.push(Span::styled("  ", Style::default().bg(theme.bg)));
                width1 = width1.saturating_add(gap_w);
            }
            line1.push(Span::styled(label, style));
            width1 = width1.saturating_add(label_w);
        }
    }
    if line2.is_empty() {
        line2.push(Span::styled("", Style::default().bg(theme.bg)));
    }
    (line1, line2)
}

fn take_last_by_width(text: &str, max_cells: usize) -> String {
    if max_cells == 0 || text.is_empty() {
        return String::new();
    }
    let mut out_rev = String::new();
    let mut used = 0usize;
    for ch in text.chars().rev() {
        let w = UnicodeWidthChar::width(ch).unwrap_or(1);
        if used.saturating_add(w) > max_cells {
            break;
        }
        out_rev.push(ch);
        used = used.saturating_add(w);
        if used >= max_cells {
            break;
        }
    }
    out_rev.chars().rev().collect()
}

fn fit_middle_ellipsis(text: &str, max_cells: usize) -> String {
    if max_cells == 0 || text.is_empty() {
        return String::new();
    }
    let total = UnicodeWidthStr::width(text);
    if total <= max_cells {
        return text.to_string();
    }
    if max_cells <= 3 {
        return truncate_to_width(text, max_cells);
    }
    let usable = max_cells.saturating_sub(3);
    let head_cells = usable.saturating_mul(2) / 3;
    let tail_cells = usable.saturating_sub(head_cells);
    let head = truncate_to_width(text, head_cells);
    let tail = take_last_by_width(text, tail_cells);
    format!("{head}...{tail}")
}
fn slice_by_cells(text: &str, start: usize, max_cells: usize) -> String {
    if max_cells == 0 || text.is_empty() {
        return String::new();
    }
    let end = start.saturating_add(max_cells);
    let mut out = String::new();
    let mut pos = 0usize;
    for ch in text.chars() {
        let w = UnicodeWidthChar::width(ch).unwrap_or(1).max(1);
        let next = pos.saturating_add(w);
        if next <= start {
            pos = next;
            continue;
        }
        if pos >= end {
            break;
        }
        out.push(ch);
        pos = next;
        if pos >= end {
            break;
        }
    }
    out
}

fn wrap_plain_line(text: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return vec![String::new()];
    }
    if text.is_empty() {
        return vec![String::new()];
    }
    let total = UnicodeWidthStr::width(text);
    if total <= width {
        return vec![text.to_string()];
    }
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < total {
        let chunk = slice_by_cells(text, offset, width);
        if chunk.is_empty() {
            break;
        }
        let chunk_w = UnicodeWidthStr::width(chunk.as_str());
        out.push(chunk);
        offset = offset.saturating_add(chunk_w.max(1));
    }
    out
}

fn truncate_for_ui(text: &str) -> Cow<'_, str> {
    if text.len() <= MAX_CHAT_BYTES {
        return Cow::Borrowed(text);
    }
    let mut end = MAX_CHAT_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    let mut out = text[..end].to_string();
    out.push_str(&format!(
        "\n\n[内容过长已截断：{} bytes → {} bytes]",
        text.len(),
        end
    ));
    Cow::Owned(out)
}

fn should_compact_user_message(text: &str) -> bool {
    let lines = text.lines().count().max(1);
    let chars = text.chars().count();
    lines >= USER_SUMMARY_LINE_THRESHOLD || chars >= USER_SUMMARY_CHAR_THRESHOLD
}

fn summarize_user_message(text: &str, max_cells: usize) -> String {
    let lines = text.lines().count().max(1);
    let chars = text.chars().count();
    let tail = format!("...总行数：{lines} 字符数：{chars}");
    let tail_w = UnicodeWidthStr::width(tail.as_str());
    let avail = max_cells.saturating_sub(tail_w);
    let preview = if avail > 0 {
        truncate_to_width(&compact_ws(text), avail)
    } else {
        String::new()
    };
    if preview.is_empty() {
        format!("总行数：{lines} 字符数：{chars}")
    } else {
        format!("{preview}{tail}")
    }
}

fn truncate_by_chars(text: &str, limit: usize) -> String {
    if limit == 0 {
        return String::new();
    }
    let mut out = String::new();
    for (count, ch) in text.chars().enumerate() {
        if count >= limit {
            break;
        }
        out.push(ch);
    }
    out
}

fn lerp_color(a: Color, b: Color, t: f32) -> Color {
    match (a, b) {
        (Color::Rgb(ar, ag, ab), Color::Rgb(br, bg, bb)) => {
            let clamp = |v: f32| v.clamp(0.0, 255.0) as u8;
            let r = ar as f32 + (br as f32 - ar as f32) * t;
            let g = ag as f32 + (bg as f32 - ag as f32) * t;
            let b = ab as f32 + (bb as f32 - ab as f32) * t;
            Color::Rgb(clamp(r), clamp(g), clamp(b))
        }
        _ => b,
    }
}

fn pulse_color(theme: &Theme, accent: Color, tick: usize) -> Color {
    let steps = [0.15, 0.3, 0.5, 0.7, 0.85, 0.7, 0.5, 0.3];
    let t = steps[tick % steps.len()];
    lerp_color(theme.dim, accent, t)
}

fn fast_pulse_color(theme: &Theme, accent: Color, tick: usize) -> Color {
    pulse_color(theme, accent, tick.saturating_mul(2))
}

fn build_header_label_spans(
    label: &str,
    bg: Color,
    fg: Color,
) -> (Vec<Span<'static>>, usize, String) {
    let text = match label {
        "MAIN" => "▒░ 萤 ░▒".to_string(),
        "DOG" => "▒░萤·潜░▒".to_string(),
        _ => format!("▒░{label}░▒"),
    };
    let span = Span::styled(
        text.clone(),
        Style::default().fg(fg).bg(bg).add_modifier(Modifier::BOLD),
    );
    let width = UnicodeWidthStr::width(text.as_str());
    (vec![span], width, text)
}

fn pingpong(step: usize, len: usize) -> usize {
    if len <= 1 {
        return 0;
    }
    let cycle = len.saturating_mul(2).saturating_sub(2).max(1);
    let mut pos = step % cycle;
    if pos >= len {
        pos = cycle - pos;
    }
    pos
}

fn mix64(mut x: u64) -> u64 {
    // SplitMix64: 快速、确定性，用于 UI 动画的“伪随机”。
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}

struct HeaderTrackSpansArgs {
    len: usize,
    active_kind: Option<MindKind>,
    active_all: bool,
    user_active: bool,
    pulse_dir: Option<PulseDir>,
    tick: usize,
    bg: Color,
    fg: Color,
}

fn build_header_track_spans(args: HeaderTrackSpansArgs) -> Vec<Span<'static>> {
    let HeaderTrackSpansArgs {
        len,
        active_kind,
        active_all,
        user_active,
        pulse_dir,
        tick,
        bg,
        fg,
    } = args;
    if len == 0 {
        return Vec::new();
    }

    // 顶栏横杠底纹：▉▉░▪▪···········▪▪░▉▉
    // 乱码动效“局部化”：
    // - MAIN 活跃 -> 只在左侧 dot 区域窜动
    // - DOG 活跃 -> 只在右侧 dot 区域窜动
    // - 传话 pulse -> 一次性快速脉冲，从源侧扫到目标侧
    // 同时尽量用单 Span（整段同样式）降低撕裂感。
    let left_cap: &[char] = &['▉', '▉', '░', '▪', '▪'];
    let right_cap: &[char] = &['▪', '▪', '░', '▉', '▉'];
    let cap_len = left_cap.len() + right_cap.len();
    let dot_start = left_cap.len();
    let dot_len = len.saturating_sub(cap_len);

    let mut out: Vec<char> = Vec::with_capacity(len);
    if len <= cap_len {
        out.extend(std::iter::repeat_n('▪', len));
    } else {
        out.extend_from_slice(left_cap);
        out.extend(std::iter::repeat_n('·', dot_len));
        out.extend_from_slice(right_cap);
    }

    let animate = tick > 0
        && (pulse_dir.is_some() || active_kind.is_some() || active_all || user_active);
    if animate && dot_len > 0 {
        const GLYPHS: &[u8] = b"01ABCDEFabcdef@#$%&*+=?-_";

        let left_zone_len = (dot_len / 2).max(1);
        let right_zone_len = dot_len.saturating_sub(left_zone_len).max(1);
        let left_zone = 0..left_zone_len;
        let right_zone = left_zone_len..dot_len;

        let active_main = active_all || active_kind == Some(MindKind::Main);
        let active_dog = active_all || active_kind == Some(MindKind::Sub);

        let mut place_glitch = |pos: usize, salt: u64| {
            if pos >= dot_len {
                return;
            }
            let seed = (tick as u64)
                .wrapping_mul(0x9E3779B97F4A7C15)
                .wrapping_add((pos as u64).wrapping_mul(0xBF58476D1CE4E5B9))
                .wrapping_add(salt);
            let idx = (seed as usize) % GLYPHS.len();
            out[dot_start + pos] = GLYPHS[idx] as char;
        };

        if let Some(dir) = pulse_dir {
            // 传话：快速扫一条“乱码带”，只持续很短时间（由 main.rs 的 mind_pulse until 控制）。
            let band_w = (dot_len / 6).max(6).min(dot_len);
            let speed = tick.saturating_mul(5);
            let head = speed % (dot_len + band_w);
            let start = head.saturating_sub(band_w).min(dot_len.saturating_sub(1));
            let end = head.min(dot_len);
            let (from, to) = match dir {
                PulseDir::MainToDog => (start, end),
                PulseDir::DogToMain => (dot_len.saturating_sub(end), dot_len.saturating_sub(start)),
            };
            let a = from.min(to);
            let b = from.max(to).min(dot_len);
            for pos in a..b {
                place_glitch(pos, 0xC0DE_F00Du64);
            }
        } else {
            // 局部化乱码：每侧 2 个点位。
            if active_main {
                let p1 = left_zone.start + pingpong(tick.saturating_mul(4), left_zone_len);
                let p2 = left_zone.start
                    + pingpong(tick.saturating_mul(3).saturating_add(left_zone_len / 2), left_zone_len);
                place_glitch(p1, 0xA11CE);
                place_glitch(p2, 0x00A1_1CE2);
            }
            if active_dog {
                let p1 = right_zone.start
                    + pingpong(tick.saturating_mul(4), right_zone_len);
                let p2 = right_zone.start
                    + pingpong(tick.saturating_mul(3).saturating_add(right_zone_len / 2), right_zone_len);
                place_glitch(p1, 0xD06E);
                place_glitch(p2, 0x000D_06E2);
            }
        }
    }

    let text: String = out.into_iter().collect();
    vec![Span::styled(text, Style::default().fg(fg).bg(bg))]
}

struct SeparatorSpansArgs<'a> {
    len: usize,
    mode: Mode,
    active_kind: MindKind,
    user_active: bool,
    tick: usize,
    emergent_tick: usize,
    emergent_seed: u64,
    emergent_exit_step: Option<usize>,
    theme: &'a Theme,
}

fn build_separator_spans(args: SeparatorSpansArgs<'_>) -> Vec<Span<'static>> {
    let SeparatorSpansArgs {
        len,
        mode,
        active_kind,
        user_active,
        tick,
        emergent_tick,
        emergent_seed,
        emergent_exit_step,
        theme,
    } = args;
    if len == 0 {
        return Vec::new();
    }
    // 顶栏“下方横杠”与 line2 的 ● 联动：● 出现位置 -> 横杠同位置变为 ○，并向两侧扩散光效。
    // 其它分隔线调用时传 0/None，等价于关闭该效果。
    const MAX_R: usize = 4;
    const BLINK_FRAMES: usize = 3;
    const HOLD_FRAMES: usize = 2;
    const PULSE_STEPS: usize = 3;
    const POST_FRAMES: usize = 1;
    const FADE_FRAMES: usize = 3;
    const REST_FRAMES: usize = 1;
    const CYCLE_LEN: usize =
        BLINK_FRAMES + HOLD_FRAMES + PULSE_STEPS + POST_FRAMES + FADE_FRAMES + REST_FRAMES;

    let pulse_state: Option<(usize, usize)> = if emergent_seed != 0 || emergent_exit_step.is_some()
    {
        let max_r = MAX_R.min(len.saturating_sub(1) / 2);
        let phase = emergent_tick % CYCLE_LEN;
        let cycle = emergent_tick / CYCLE_LEN;

        let mut visible = true;
        let mut radius = 0usize;
        if let Some(k) = emergent_exit_step {
            if k <= 1 {
                radius = max_r;
            } else if k <= 4 {
                radius = 0;
            } else {
                visible = false;
            }
        } else if phase < BLINK_FRAMES + HOLD_FRAMES {
            radius = 0;
        } else if phase < BLINK_FRAMES + HOLD_FRAMES + PULSE_STEPS {
            let s = phase - (BLINK_FRAMES + HOLD_FRAMES);
            let outer = 2usize.saturating_add(s).min(max_r);
            radius = outer;
        } else if phase
            < BLINK_FRAMES + HOLD_FRAMES + PULSE_STEPS + POST_FRAMES + FADE_FRAMES
        {
            radius = 0;
        } else {
            visible = false;
        }

        if !visible {
            None
        } else {
            let usable = len.saturating_sub(max_r.saturating_mul(2));
            let center = if usable <= 1 || max_r == 0 {
                len / 2
            } else {
                let r =
                    mix64(emergent_seed ^ mix64((cycle as u64).wrapping_mul(0xD1B54A32D192ED03)));
                max_r + ((r as usize) % usable)
            };
            Some((center.min(len.saturating_sub(1)), radius))
        }
    } else {
        None
    };

    let active = matches!(mode, Mode::Generating | Mode::ExecutingTool);
    let use_user = user_active && !active;
    // “光标”更快，且用 3 段 spans（而不是逐格）减少撕裂感。
    // 若启用“○ 扩散”联动，则不叠加光标，避免两套动画打架。
    let sparkle = if pulse_state.is_some() {
        None
    } else if use_user {
        Some(tick.saturating_mul(4) % len)
    } else if active {
        Some(tick.saturating_mul(6) % len)
    } else {
        None
    };
    let accent = if use_user {
        theme.magenta
    } else {
        match active_kind {
            MindKind::Main => theme.cyan,
            MindKind::Sub => theme.magenta,
        }
    };
    let base = pulse_color(theme, accent, tick);
    let base_style = Style::default().fg(theme.border_idle).bg(theme.bg);
    if let Some((center, radius)) = pulse_state {
        let accent = if (emergent_seed & 1) == 0 {
            theme.cyan
        } else {
            theme.magenta
        };
        let glow = pulse_color(theme, accent, tick);
        let mid = lerp_color(theme.border_idle, glow, 0.55);
        let inner = lerp_color(theme.border_idle, glow, 0.85);

        // 视觉上更像“扩散光效”：比 line2 更宽一点
        let glow_r = (radius.saturating_mul(3).max(1)).min(len.saturating_sub(1));
        let inner_r = (glow_r / 2).max(1);
        let left_glow = center.saturating_sub(glow_r);
        let left_inner = center.saturating_sub(inner_r);
        let right_inner = (center + inner_r).min(len.saturating_sub(1));
        let right_glow = (center + glow_r).min(len.saturating_sub(1));

        let mut spans: Vec<Span<'static>> = Vec::new();
        if left_glow > 0 {
            spans.push(Span::styled("─".repeat(left_glow), base_style));
        }
        if left_inner > left_glow {
            spans.push(Span::styled(
                "─".repeat(left_inner - left_glow),
                Style::default().fg(mid).bg(theme.bg),
            ));
        }
        if center > left_inner {
            spans.push(Span::styled(
                "─".repeat(center - left_inner),
                Style::default().fg(inner).bg(theme.bg),
            ));
        }
        spans.push(Span::styled(
            "○".to_string(),
            Style::default()
                .fg(glow)
                .bg(theme.bg)
                .add_modifier(Modifier::BOLD),
        ));
        if right_inner > center {
            spans.push(Span::styled(
                "─".repeat(right_inner - center),
                Style::default().fg(inner).bg(theme.bg),
            ));
        }
        if right_glow > right_inner {
            spans.push(Span::styled(
                "─".repeat(right_glow - right_inner),
                Style::default().fg(mid).bg(theme.bg),
            ));
        }
        let tail = len.saturating_sub(right_glow.saturating_add(1));
        if tail > 0 {
            spans.push(Span::styled("─".repeat(tail), base_style));
        }
        spans
    } else if let Some(p) = sparkle {
        let mut spans = Vec::with_capacity(3);
        if p > 0 {
            spans.push(Span::styled("─".repeat(p), base_style));
        }
        spans.push(Span::styled(
            "─".to_string(),
            Style::default()
                .fg(base)
                .bg(theme.bg)
                .add_modifier(Modifier::BOLD),
        ));
        let rest = len.saturating_sub(p.saturating_add(1));
        if rest > 0 {
            spans.push(Span::styled("─".repeat(rest), base_style));
        }
        spans
    } else {
        vec![Span::styled("─".repeat(len), base_style)]
    }
}

fn compact_preview(text: &str, limit: usize) -> String {
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= limit {
        compact
    } else {
        let mut out = truncate_by_chars(&compact, limit);
        out.push_str("...");
        out
    }
}

fn compact_ws(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn base_name_for_display(path: &str) -> String {
    let trimmed = path.trim().trim_start_matches("./");
    if trimmed.is_empty() {
        return String::new();
    }
    trimmed.rsplit('/').next().unwrap_or(trimmed).to_string()
}

fn extract_tool_detail(text: &str) -> &str {
    let mut pos = 0usize;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("output:") || trimmed.starts_with("meta:") {
            return &text[pos..];
        }
        pos = pos.saturating_add(line.len().saturating_add(1));
    }
    text
}

fn extract_tool_sections(text: &str) -> (Vec<String>, Vec<String>) {
    #[derive(Clone, Copy)]
    enum Section {
        None,
        Output,
        Meta,
    }
    let mut section = Section::None;
    let mut in_fence = false;
    let mut output_lines: Vec<String> = Vec::new();
    let mut meta_lines: Vec<String> = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.eq_ignore_ascii_case("output:") {
            section = Section::Output;
            in_fence = false;
            continue;
        }
        if trimmed.eq_ignore_ascii_case("meta:") {
            section = Section::Meta;
            in_fence = false;
            continue;
        }
        if trimmed == "```text" {
            in_fence = true;
            continue;
        }
        if trimmed == "```" {
            in_fence = false;
            continue;
        }
        if matches!(section, Section::None) {
            continue;
        }
        if !in_fence && trimmed.is_empty() {
            continue;
        }
        match section {
            Section::Output => output_lines.push(line.to_string()),
            Section::Meta => meta_lines.push(line.to_string()),
            Section::None => {}
        }
    }
    (output_lines, meta_lines)
}

fn extract_brief_value(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    for prefix in ["explain:", "brief:"] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let val = rest.trim();
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

fn extract_brief_token(token: &str) -> Option<String> {
    let trimmed = token.trim();
    for prefix in ["explain:", "brief:"] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            let val = rest.trim();
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

fn extract_tool_explain(text: &str) -> Option<String> {
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("output:") || trimmed.starts_with("meta:") {
            break;
        }
        if let Some(val) = extract_brief_value(trimmed) {
            return Some(val);
        }
    }
    None
}

fn extract_tool_status_token(text: &str) -> Option<String> {
    let (output_lines, meta_lines) = extract_tool_sections(text);
    for line in meta_lines.iter() {
        if let Some(pos) = line.find("状态:") {
            let rest = line[pos + "状态:".len()..].trim_start();
            let token = rest
                .split(|c: char| c.is_whitespace() || c == '|' || c == ',')
                .next()
                .unwrap_or("")
                .trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    }
    for line in output_lines.iter() {
        let t = line.trim();
        if t.contains("格式错误") || t.contains("工具执行失败") || t.contains("失败") {
            return Some("fail".to_string());
        }
    }
    None
}

fn tool_status_result_line(text: &str) -> Option<String> {
    let token = extract_tool_status_token(text)?;
    let t = token.to_ascii_lowercase();
    if matches!(t.as_str(), "running" | "in_progress" | "done" | "processing") {
        return None;
    }
    let ok = t == "0"
        || t == "ok"
        || t == "无"
        || t.starts_with("ok_")
        || t.starts_with("status0")
        || t.contains("success");
    if ok { None } else { Some(format!("执行失败：{token}")) }
}

fn parse_mind_msg(text: &str) -> Option<(String, String, String, String)> {
    let raw = text.trim_start();
    if !raw.starts_with("[mind_msg]") {
        return None;
    }
    let mut from = None;
    let mut to = None;
    let mut brief = None;
    let mut content = String::new();
    let mut in_content = false;
    for line in raw.lines().skip(1) {
        if in_content {
            if !content.is_empty() {
                content.push('\n');
            }
            content.push_str(line);
            continue;
        }
        let t = line.trim();
        if let Some(v) = t.strip_prefix("from:") {
            from = Some(v.trim().to_string());
            continue;
        }
        if let Some(v) = t.strip_prefix("to:") {
            to = Some(v.trim().to_string());
            continue;
        }
        if let Some(v) = t.strip_prefix("brief:") {
            brief = Some(v.trim().to_string());
            continue;
        }
        if t.eq_ignore_ascii_case("content:") {
            in_content = true;
            continue;
        }
    }
    Some((
        from.unwrap_or_else(|| "main".to_string()),
        to.unwrap_or_else(|| "dog".to_string()),
        brief.unwrap_or_else(|| "mind_msg".to_string()),
        content.trim_end().to_string(),
    ))
}

fn extract_tool_mind(text: &str) -> Option<&'static str> {
    for line in text.lines() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("output:") || trimmed.starts_with("meta:") {
            break;
        }
        if let Some(rest) = trimmed.strip_prefix("mind:") {
            let t = rest.trim();
            if t.eq_ignore_ascii_case("dog") || t == "潜意识" {
                return Some("dog");
            }
            if t.eq_ignore_ascii_case("main") || t == "萤" || t == "主意识" {
                return Some("main");
            }
        }
    }
    None
}

fn parse_pass_token(text: &str) -> Option<&'static str> {
    let t = text.trim();
    match t {
        "[mainpass]" => Some("main"),
        "[dogpass]" => Some("dog"),
        _ => None,
    }
}

fn extract_search_target(text: &str) -> Option<String> {
    let mut seen_output = false;
    let mut in_code = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if !seen_output {
            if trimmed.starts_with("output:") {
                seen_output = true;
            }
            continue;
        }
        if trimmed.starts_with("```") {
            if in_code {
                break;
            } else {
                in_code = true;
                continue;
            }
        }
        if !in_code {
            continue;
        }
        if trimmed.is_empty() || trimmed == "(no output)" || trimmed == "未找到匹配" {
            return None;
        }
        let mut parts = trimmed.splitn(3, ':');
        let path = parts.next().unwrap_or("").trim();
        let line_no = parts.next().unwrap_or("").trim();
        if path.is_empty() || line_no.is_empty() || !line_no.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let name = base_name_for_display(path);
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

fn pad_to_width(text: &str, width: usize) -> String {
    let cur = UnicodeWidthStr::width(text);
    if cur >= width {
        return text.to_string();
    }
    let pad = width.saturating_sub(cur);
    format!("{text}{}", " ".repeat(pad))
}

fn push_blank_line(out: &mut Vec<Line<'static>>) {
    if out.last().is_some_and(|line| line.width() == 0) {
        return;
    }
    out.push(Line::from(""));
}

fn push_tool_explain_line(
    out: &mut Vec<Line<'static>>,
    theme: &Theme,
    width: usize,
    explain: &str,
    reveal_idx: Option<usize>,
    msg_idx: usize,
    reveal_len: usize,
) {
    let explain_prefix = "   ↳ ";
    let explain_indent_width = UnicodeWidthStr::width(explain_prefix);
    let explain_text = compact_ws(explain);
    let w = width.max(1);
    let mut line = if width <= explain_indent_width {
        truncate_to_width(&format!("{explain_prefix}{explain_text}"), w)
    } else {
        let avail = width.saturating_sub(explain_indent_width).max(1);
        let explain_fit = truncate_to_width(&explain_text, avail);
        format!("{explain_prefix}{explain_fit}")
    };
    if reveal_idx == Some(msg_idx) {
        line = truncate_to_width(&truncate_by_chars(&line, reveal_len), w);
    }
    out.push(Line::from(vec![Span::styled(
        line,
        Style::default().fg(theme.fg).add_modifier(Modifier::ITALIC),
    )]));
}

fn apply_reveal(
    mut text: String,
    width: usize,
    reveal_idx: Option<usize>,
    msg_idx: usize,
    reveal_len: usize,
) -> String {
    if reveal_idx == Some(msg_idx) {
        text = truncate_to_width(&truncate_by_chars(&text, reveal_len), width.max(1));
    }
    text
}

fn strip_thinking_marker(text: &str) -> Option<&str> {
    text.strip_prefix(THINKING_MARKER)
        .map(|rest| rest.trim_start())
}

fn summarize_tool_message(text: &str) -> String {
    let mut tool = "tool";
    let mut input = None;
    let mut raw_input = None;
    let mut brief = None;
    let mut status = None;
    for line in text.lines() {
        let trimmed = line.trim_start();
        if let Some(val) = trimmed.strip_prefix("[tool:") {
            let val = val.trim_end_matches(']').trim();
            if !val.is_empty() {
                tool = val;
            }
        }
        if trimmed.starts_with("操作:") {
            let val = trimmed.trim_start_matches("操作:").trim();
            if !val.is_empty() {
                tool = val;
            }
        }
        if input.is_none() && line.trim_start().starts_with("input:") {
            let val = line.trim_start().trim_start_matches("input:").trim();
            if !val.is_empty() {
                input = Some(val.to_string());
                raw_input = Some(val.to_string());
            }
        }
        if brief.is_none() && let Some(val) = extract_brief_value(trimmed) {
            brief = Some(val);
        }
        if status.is_none() && let Some(pos) = line.find("状态:") {
            let s = line[pos..].trim();
            if !s.is_empty() {
                status = Some(s.to_string());
            }
        }
        if input.is_some() && (brief.is_some() || status.is_some()) {
            break;
        }
    }
    if tool.eq_ignore_ascii_case("search") {
        let pattern = raw_input
            .as_deref()
            .and_then(|s| {
                let s = s.trim();
                for prefix in ["pattern=", "file="] {
                    if let Some(rest) = s.strip_prefix(prefix) {
                        if let Some((left, _right)) = rest.split_once(" in ") {
                            let pat = left.trim();
                            if !pat.is_empty() {
                                return Some(pat.to_string());
                            }
                        } else {
                            let pat = rest.trim();
                            if !pat.is_empty() {
                                return Some(pat.to_string());
                            }
                        }
                    }
                }
                None
            })
            .unwrap_or_else(|| "pattern".to_string());
        let file = extract_search_target(text)
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "results".to_string());
        input = Some(format!("{pattern} in {file}"));
    }
    if tool.eq_ignore_ascii_case("skills")
        && let Some(raw) = raw_input.as_deref().map(str::trim).filter(|s| !s.is_empty())
    {
        input = Some(raw.to_string());
    }
    let mut parts = Vec::new();
    parts.push(format!("tool:{tool}"));
    if let Some(i) = input {
        parts.push(format!("input:{}", compact_preview(&i, 120)));
    }
    if let Some(b) = brief {
        parts.push(format!("brief:{}", compact_preview(&b, 40)));
    } else if let Some(s) = status {
        parts.push(compact_preview(&s, 80));
    }
    parts.join(" | ")
}

fn normalize_memory_target(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let (head, tail) = trimmed
        .split_once("::")
        .map(|(a, b)| (a.trim(), Some(b.trim())))
        .unwrap_or((trimmed, None));
    let mapped = match head {
        "fastmemo" => "fastmemo.jsonl",
        "contextmemo" => "contextmemo.jsonl",
        "memo.db" | "memo" => "memo.db",
        other => other,
    };
    if let Some(t) = tail.filter(|s| !s.is_empty()) {
        format!("{mapped}::{t}")
    } else {
        mapped.to_string()
    }
}

fn extract_delta_inline(text: &str) -> Option<(String, String)> {
    let start = text.find("(+");
    let start = start?;
    let rest = &text[start..];
    let end = rest.find(')')?;
    let inside = rest[1..end].trim(); // drop leading '('
    if inside.starts_with('+') && inside.contains(" -") {
        let clean = inside.to_string();
        let mut without = text.to_string();
        without.replace_range(start..start + end + 1, "");
        Some((compact_ws_inline(&without), clean))
    } else {
        None
    }
}

fn compact_ws_inline(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_tool_display(tool: &str, raw_cmd: &str) -> (String, String) {
    let tool_key = tool.trim();
    let tool_name = match tool_key {
        "MEdit" | "memory_edit" => "编织记忆",
        "MAdd" | "memory_add" => "添加记忆",
        "MRead" | "memory_read" => "读取记忆",
        "MFind" | "memory_check" => "检索记忆",
        "Sys" | "system_config" => "心跳频率",
        other => other,
    }
    .to_string();

    let cmd = compact_ws_inline(raw_cmd);
    if cmd.is_empty() {
        return (tool_name, String::new());
    }
    if tool_key.eq_ignore_ascii_case("list") {
        return (tool_name, cmd);
    }
    if tool_key.eq_ignore_ascii_case("search") {
        // 期望形态：`关键词 in 文件名`
        let mut s = cmd;
        if let Some((left, right)) = s.rsplit_once(" in ") {
            let file = base_name_for_display(right.trim());
            s = format!("{} in {file}", left.trim());
        }
        return (tool_name, s);
    }
    if tool_key.eq_ignore_ascii_case("skills") {
        return (tool_name, cmd);
    }
    if tool_key.eq_ignore_ascii_case("sys") || tool_key.eq_ignore_ascii_case("system_config") {
        let s = cmd
            .strip_prefix("heartbeat:")
            .map(str::trim)
            .unwrap_or(cmd.as_str())
            .to_string();
        return (tool_name, s);
    }
    if matches!(
        tool_key,
        "MEdit" | "MAdd" | "MRead" | "MFind" | "memory_edit" | "memory_add" | "memory_read"
            | "memory_check"
    ) {
        return (tool_name, normalize_memory_target(&cmd));
    }

    if matches!(tool_key, "Run" | "Shell" | "Termux") {
        return (tool_name, cmd);
    }

    let mut first = cmd.split_whitespace().next().unwrap_or("").to_string();
    let mut rest = cmd
        .split_whitespace()
        .skip(1)
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    let file = base_name_for_display(&first);
    if file.is_empty() {
        return (tool_name, cmd);
    }
    first = file;

    // 去掉 write 的 bytes 噪音
    if tool_key.eq_ignore_ascii_case("write")
        && let Some(start) = rest.find('(')
        && rest[start..].contains("bytes")
        && let Some(end) = rest[start..].find(')')
    {
        rest.replace_range(start..start + end + 1, "");
        rest = compact_ws_inline(&rest);
    }

    let (without_delta, delta) = extract_delta_inline(&format!("{first} {rest}"))
        .map(|(w, d)| (w, Some(d)))
        .unwrap_or_else(|| (format!("{first} {rest}"), None));
    let mut base = without_delta.split_whitespace().next().unwrap_or("").to_string();
    if base.is_empty() {
        base = first;
    }

    if tool_key.eq_ignore_ascii_case("read") {
        let mut range = rest.trim().to_string();
        if range.starts_with("tail:") {
            range = range.trim_start_matches("tail:").trim().to_string();
            if !range.is_empty() {
                return (tool_name, format!("{base} tail {range} lines"));
            }
        }
        if range.starts_with("1-") || range.contains('-') {
            let tok = range.split_whitespace().next().unwrap_or("").trim();
            if !tok.is_empty() {
                return (tool_name, format!("{base} {tok} lines"));
            }
        }
        return (tool_name, base);
    }
    if tool_key.eq_ignore_ascii_case("info") {
        return (tool_name, base);
    }
    if matches!(tool_key, "Write" | "Edit" | "Patch") {
        if let Some(delta) = delta {
            let mut delta = delta.trim().trim_matches(|c| c == '(' || c == ')').to_string();
            if !delta.contains(" lines") && !delta.contains(" chars") {
                let unit = if tool_key.eq_ignore_ascii_case("edit") {
                    "chars"
                } else {
                    "lines"
                };
                delta.push(' ');
                delta.push_str(unit);
            }
            if !delta.starts_with('+') {
                delta = format!("+{delta}");
            }
            return (tool_name, format!("{base} {delta}"));
        }
        return (tool_name, base);
    }
    (tool_name, format!("{base} {rest}").trim().to_string())
}

fn parse_tool_summary(text: &str) -> (String, Option<String>, Option<String>) {
    let mut tool = "tool".to_string();
    let mut input = None;
    let mut brief = None;
    let mut status = None;
    for part in text.split('|') {
        let p = part.trim();
        if let Some(rest) = p.strip_prefix("tool:") {
            if !rest.trim().is_empty() {
                tool = rest.trim().to_string();
            }
        } else if let Some(rest) = p.strip_prefix("input:") {
            if !rest.trim().is_empty() {
                input = Some(rest.trim().to_string());
            }
        } else if let Some(val) = extract_brief_token(p) {
            brief = Some(val);
        } else if p.starts_with("状态:") {
            status = Some(p.to_string());
        }
    }
    let summary = if let Some(b) = brief {
        Some(b)
    } else if let Some(s) = status {
        Some(s)
    } else {
        Some("未说明".to_string())
    };
    (tool, input, summary)
}

fn render_thinking_lines(
    theme: &Theme,
    width: usize,
    label: &str,
    text: &str,
    scroll: usize,
) -> Vec<Line<'static>> {
    if width == 0 {
        return Vec::new();
    }
    let body = compact_ws(text);
    if body.is_empty() {
        return Vec::new();
    }
    let prefix = format!("· {label} · ");
    let prefix_w = UnicodeWidthStr::width(prefix.as_str());
    let avail = width.saturating_sub(prefix_w);
    if avail == 0 {
        return Vec::new();
    }
    let body_w = UnicodeWidthStr::width(body.as_str());
    let max_offset = body_w.saturating_sub(avail);
    let offset = scroll.min(max_offset);
    let mut window = slice_by_cells(&body, offset, avail);
    let win_w = UnicodeWidthStr::width(window.as_str());
    if win_w < avail {
        window.push_str(&" ".repeat(avail.saturating_sub(win_w)));
    }
    vec![Line::from(vec![
        Span::styled(
            prefix,
            Style::default()
                .fg(theme.dim)
                .bg(theme.bg)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(window, Style::default().fg(theme.dim).bg(theme.bg)),
    ])]
}

pub fn thinking_scroll_limit(width: usize, label: &str, text: &str) -> usize {
    if width == 0 {
        return 0;
    }
    let body = compact_ws(text);
    if body.is_empty() {
        return 0;
    }
    let prefix = format!("· {label} · ");
    let prefix_w = UnicodeWidthStr::width(prefix.as_str());
    let avail = width.saturating_sub(prefix_w);
    if avail == 0 {
        return 0;
    }
    let body_w = UnicodeWidthStr::width(body.as_str());
    body_w.saturating_sub(avail)
}

fn build_activity_dots_line(theme: &Theme, width: usize, tick: usize) -> Option<Line<'static>> {
    if width == 0 {
        return None;
    }
    let dots = 6usize;
    let total_w = dots.saturating_add(dots.saturating_sub(1));
    if total_w == 0 {
        return None;
    }
    let pad_left = width.saturating_sub(total_w) / 2;
    let palette = [
        Color::Rgb(80, 90, 110),
        Color::Rgb(115, 125, 150),
        Color::Rgb(150, 165, 190),
        Color::Rgb(185, 200, 225),
        Color::Rgb(225, 232, 245),
    ];
    let cycle = dots.saturating_mul(2).saturating_sub(2).max(1);
    let mut glow = tick % cycle;
    if glow >= dots {
        glow = cycle - glow;
    }
    let mut spans = Vec::new();
    if pad_left > 0 {
        spans.push(Span::styled(
            " ".repeat(pad_left),
            Style::default().bg(theme.bg),
        ));
    }
    for i in 0..dots {
        let dist = i.abs_diff(glow);
        let level = match dist {
            0 => 4,
            1 => 3,
            2 => 2,
            3 => 1,
            _ => 0,
        };
        spans.push(Span::styled(
            "•".to_string(),
            Style::default().fg(palette[level]).bg(theme.bg),
        ));
        if i + 1 < dots {
            spans.push(Span::styled(" ".to_string(), Style::default().bg(theme.bg)));
        }
    }
    Some(Line::from(spans))
}

fn build_emergent_intel_spans(
    len: usize,
    tick: usize,
    seed: u64,
    exit_step: Option<usize>,
    theme: &Theme,
) -> Vec<Span<'static>> {
    if len == 0 {
        return Vec::new();
    }
    // 顶栏第 2 行：“翅膀脉冲”——随机位置 ● 闪烁 → 停顿 → 左右扩散 ▪ → 只剩 ● → 渐隐。
    // idle 不会调用到这里（外层已纯黑）。
    let bg = theme.bg;
    let dim_style = Style::default().fg(theme.dim).bg(bg);

    const MAX_R: usize = 4; // 最终视觉接近：··▪▪●▪▪··
    const BLINK_FRAMES: usize = 3;
    const HOLD_FRAMES: usize = 2;
    const PULSE_STEPS: usize = 3; // 2 -> 3 -> 4（避免 clamp 卡住）
    const POST_FRAMES: usize = 1;
    const FADE_FRAMES: usize = 3;
    const REST_FRAMES: usize = 1;
    const CYCLE_LEN: usize =
        BLINK_FRAMES + HOLD_FRAMES + PULSE_STEPS + POST_FRAMES + FADE_FRAMES + REST_FRAMES;

    let phase = tick % CYCLE_LEN;
    let cycle = tick / CYCLE_LEN;

    let accent = if (seed & 1) == 0 {
        theme.cyan
    } else {
        theme.magenta
    };
    let bright_style = Style::default()
        .fg(accent)
        .bg(bg)
        .add_modifier(Modifier::BOLD);
    let wing_style = Style::default()
        .fg(lerp_color(theme.dim, accent, 0.7))
        .bg(bg)
        .add_modifier(Modifier::BOLD);
    let soft_center_style = Style::default()
        .fg(lerp_color(theme.dim, accent, 0.5))
        .bg(bg)
        .add_modifier(Modifier::BOLD);

    let max_r = MAX_R.min(len.saturating_sub(1) / 2);

    // 本 cycle 的中心位置：随机，但 cycle 内固定（避免抖动），cycle 之间跳位。
    let usable = len.saturating_sub(max_r.saturating_mul(2));
    let center = if usable <= 1 || max_r == 0 {
        len / 2
    } else {
        let r = mix64(seed ^ mix64((cycle as u64).wrapping_mul(0xD1B54A32D192ED03)));
        max_r + ((r as usize) % usable)
    };

    // kind: 0=dim, 1=wing, 2=center bright, 3=center soft
    let mut chars: Vec<char> = vec![' '; len];
    let mut kinds: Vec<u8> = vec![0u8; len];

    let mut visible = true;
    let mut center_on = true;
    let mut center_kind: u8 = 2;
    let mut pulse_outer: Option<usize> = None;

    if let Some(k) = exit_step {
        if k <= 1 {
            pulse_outer = Some(max_r);
            center_kind = 2;
        } else if k <= 3 {
            center_kind = 2;
        } else if k == 4 {
            center_kind = 3;
        } else {
            visible = false;
        }
    } else if phase < BLINK_FRAMES {
        // 亮/暗闪烁（只改中心风格）
        center_kind = if phase.is_multiple_of(2) { 2 } else { 3 };
    } else if phase < BLINK_FRAMES + HOLD_FRAMES {
        center_kind = 2;
    } else if phase < BLINK_FRAMES + HOLD_FRAMES + PULSE_STEPS {
        let s = phase - (BLINK_FRAMES + HOLD_FRAMES);
        let outer = 2usize.saturating_add(s).min(max_r);
        pulse_outer = Some(outer);
        center_kind = 2;
    } else if phase < BLINK_FRAMES + HOLD_FRAMES + PULSE_STEPS + POST_FRAMES {
        center_kind = 2;
    } else if phase < BLINK_FRAMES + HOLD_FRAMES + PULSE_STEPS + POST_FRAMES + FADE_FRAMES {
        let s = phase - (BLINK_FRAMES + HOLD_FRAMES + PULSE_STEPS + POST_FRAMES);
        center_kind = 3;
        center_on = s.is_multiple_of(2);
    } else {
        visible = false;
    }

    if !visible || len == 0 {
        return vec![Span::styled(" ".repeat(len), dim_style)];
    }

    if center < len && center_on {
        chars[center] = '●';
        kinds[center] = center_kind;
    }

    if let Some(outer) = pulse_outer
        && max_r > 0
        && outer > 0
        && center >= outer
        && center + outer < len
    {
        let inner = outer.saturating_sub(1).max(1).min(max_r);
        // trail：中心到 inner-1 变成 ·；外侧空余也变成 ·（形成“刷一下”的扩散）
        for r in 1..=max_r {
            let ch = if r < inner || r > outer { '·' } else { ' ' };
            if ch != ' ' {
                let li = center - r;
                let ri = center + r;
                chars[li] = ch;
                chars[ri] = ch;
                kinds[li] = 0;
                kinds[ri] = 0;
            }
        }
        // wings：inner..=outer 用 ▪
        for r in inner..=outer.min(max_r) {
            let li = center - r;
            let ri = center + r;
            chars[li] = '▪';
            chars[ri] = '▪';
            kinds[li] = 1;
            kinds[ri] = 1;
        }
    }

    // 拼 spans：按 kind 合并，减少撕裂概率。
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut i = 0usize;
    while i < len {
        let k = kinds[i];
        let style = match k {
            1 => wing_style,
            2 => bright_style,
            3 => soft_center_style,
            _ => dim_style,
        };
        let mut j = i + 1;
        while j < len && kinds[j] == k {
            j += 1;
        }
        let s: String = chars[i..j].iter().collect();
        spans.push(Span::styled(s, style));
        i = j;
    }
    spans
}

fn render_tool_detail_lines(theme: &Theme, text: &str, width: usize) -> Vec<Line<'static>> {
    if width == 0 {
        return Vec::new();
    }
    let (output_lines, meta_lines) = extract_tool_sections(text);
    if output_lines.is_empty() && meta_lines.is_empty() {
        let detail = extract_tool_detail(text);
        if detail.trim().is_empty() {
            return Vec::new();
        }
        let display = truncate_for_ui(detail);
        let base_style = Style::default().fg(theme.dim).bg(theme.bg);
        let rendered =
            render_markdown_to_lines(display.as_ref(), width.saturating_sub(2).max(1), base_style);
        if rendered.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::new();
        for line in rendered {
            let mut spans = Vec::new();
            spans.push(Span::styled(
                "  ",
                Style::default().fg(theme.dim).bg(theme.bg),
            ));
            spans.extend(line.spans);
            out.push(Line::from(spans));
        }
        return out;
    }

    const OUTPUT_MAX_LINES: usize = 18;
    const META_MAX_LINES: usize = 8;
    let mut out = Vec::new();
    let heading_style = Style::default().fg(theme.dim).bg(theme.bg);
    let pipe_style = Style::default().fg(theme.dim).bg(theme.bg);
    let content_style = Style::default().fg(theme.fg).bg(theme.bg);

    let mut push_section = |title: &str, lines: &[String], max_lines: usize| {
        if lines.is_empty() {
            return;
        }
        let header = format!("  {title}:");
        out.push(Line::from(vec![Span::styled(header, heading_style)]));
        let mut truncated = false;
        let mut shown = lines;
        if lines.len() > max_lines {
            shown = &lines[..max_lines];
            truncated = true;
        }
        let prefix = "  ┆ ";
        let prefix_w = UnicodeWidthStr::width(prefix);
        let avail = width.saturating_sub(prefix_w).max(1);
        for line in shown {
            for chunk in wrap_plain_line(line, avail) {
                out.push(Line::from(vec![
                    Span::styled(prefix.to_string(), pipe_style),
                    Span::styled(chunk, content_style),
                ]));
            }
        }
        if truncated {
            let note = format!("... [截断 {} 行]", lines.len().saturating_sub(max_lines));
            for chunk in wrap_plain_line(&note, avail) {
                out.push(Line::from(vec![
                    Span::styled(prefix.to_string(), pipe_style),
                    Span::styled(chunk, heading_style),
                ]));
            }
        }
    };

    push_section("output", &output_lines, OUTPUT_MAX_LINES);
    push_section("meta", &meta_lines, META_MAX_LINES);
    out
}

fn render_think_output_lines_italic(theme: &Theme, text: &str, width: usize) -> Vec<Line<'static>> {
    if width == 0 {
        return Vec::new();
    }
    let (output_lines, _meta_lines) = extract_tool_sections(text);
    if output_lines.is_empty() {
        return Vec::new();
    }
    let avail = width.max(1);
    let style = Style::default()
        .fg(theme.dim)
        .bg(theme.bg)
        .add_modifier(Modifier::ITALIC);
    let mut out = Vec::new();
    for line in output_lines {
        for chunk in wrap_plain_line(&line, avail) {
            out.push(Line::from(vec![Span::styled(chunk, style)]));
        }
    }
    out
}

pub fn draw_command_menu(
    f: &mut ratatui::Frame,
    theme: &Theme,
    area: Rect,
    items: &[CommandSpec],
    selected: usize,
) {
    if items.is_empty() || area.width == 0 || area.height == 0 {
        return;
    }
    // 菜单不加边框，避免“/”下拉被框住；同时保持窄屏安全裁剪。
    f.render_widget(Clear, area);
    let bg_block = Block::default().style(Style::default().bg(theme.bg));
    f.render_widget(bg_block, area);

    let inner_w = area.width.max(1) as usize;
    let mut lines: Vec<Line> = Vec::new();
    let max_items = area.height.max(1) as usize;
    for (i, it) in items.iter().take(max_items).enumerate() {
        let is_sel = i == selected;
        let style = if is_sel {
            Style::default()
                .fg(theme.bg)
                .bg(theme.yellow)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(theme.fg).bg(theme.bg)
        };
        let prefix = if is_sel { "› " } else { "  " };
        let raw = format!("{prefix}{}  {}", it.cmd, it.desc);
        let text = truncate_to_width(&raw, inner_w);
        lines.push(Line::from(vec![Span::styled(text, style)]));
    }

    let p = Paragraph::new(Text::from(lines))
        .block(Block::default().borders(Borders::NONE))
        .wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

#[derive(Default)]
pub struct SettingsDrawResult {
    pub editor_rect: Option<Rect>,
    pub cursor: Option<(u16, u16)>,
}

pub struct DrawSettingsArgs<'a> {
    pub theme: &'a Theme,
    pub area: Rect,
    pub title: &'a str,
    pub menu_items: &'a [String],
    pub menu_selected: usize,
    pub fields: &'a [(String, String)],
    pub field_selected: usize,
    pub focus: SettingsFocus,
    pub prompt_editor: Option<(&'a str, usize)>,
    pub tick: usize,
}

pub fn draw_settings(f: &mut ratatui::Frame, args: DrawSettingsArgs<'_>) -> SettingsDrawResult {
    let DrawSettingsArgs {
        theme,
        area,
        title,
        menu_items,
        menu_selected,
        fields,
        field_selected,
        focus,
        prompt_editor,
        tick,
    } = args;
    let mut result = SettingsDrawResult::default();
    if area.width == 0 || area.height == 0 {
        return result;
    }
    f.render_widget(Clear, area);
    let bg = Block::default().style(Style::default().bg(theme.bg));
    f.render_widget(bg, area);

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(1)])
        .split(area);
    let tabs_area = layout[0];
    let body_area = layout[1];

    if tabs_area.height > 0 {
        let tab_line_area = Rect {
            x: tabs_area.x,
            y: tabs_area.y,
            width: tabs_area.width,
            height: 1,
        };
        let max_w = tabs_area.width.max(1) as usize;
        let (line1, line2) =
            build_settings_tab_lines(menu_items, menu_selected, focus, theme, max_w);
        let tab_line = Line::from(line1);
        let tab_p = Paragraph::new(Text::from(tab_line))
            .block(Block::default().borders(Borders::NONE))
            .wrap(Wrap { trim: true });
        f.render_widget(tab_p, tab_line_area);
        if tabs_area.height > 1 {
            let tab2_area = Rect {
                x: tabs_area.x,
                y: tabs_area.y.saturating_add(1),
                width: tabs_area.width,
                height: 1,
            };
            let tab2_line = Line::from(line2);
            let tab2_p = Paragraph::new(Text::from(tab2_line))
                .block(Block::default().borders(Borders::NONE))
                .wrap(Wrap { trim: true });
            f.render_widget(tab2_p, tab2_area);
        }
    }

    if tabs_area.height > 2 {
        let line_area = Rect {
            x: tabs_area.x,
            y: tabs_area.y.saturating_add(2),
            width: tabs_area.width,
            height: 1,
        };
        let line = Line::from(Span::styled(
            "─".repeat(tabs_area.width.max(1) as usize),
            Style::default().fg(theme.dim).bg(theme.bg),
        ));
        let p = Paragraph::new(Text::from(line))
            .block(Block::default().borders(Borders::NONE))
            .wrap(Wrap { trim: true });
        f.render_widget(p, line_area);
    }

    let panel_border = if matches!(focus, SettingsFocus::Prompt) {
        pulse_color(theme, theme.magenta, tick)
    } else if matches!(focus, SettingsFocus::Fields | SettingsFocus::Input) {
        pulse_color(theme, theme.cyan, tick)
    } else {
        theme.border_idle
    };
    let panel_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Double)
        .border_style(Style::default().fg(panel_border))
        .title(Span::styled(
            format!(" {title} "),
            Style::default()
                .fg(theme.fg)
                .bg(theme.bg)
                .add_modifier(Modifier::BOLD),
        ))
        .style(Style::default().bg(theme.bg));
    let panel_inner = panel_block.inner(body_area);
    f.render_widget(panel_block, body_area);

    if let Some((text, cursor)) = prompt_editor {
        let width = panel_inner.width.max(1) as usize;
        let height = panel_inner.height.max(1) as usize;
        let (cx, cy) = cursor_xy(width, text, cursor.min(text.len()));
        let scroll_y = cy.saturating_sub(height.saturating_sub(1));
        let scroll_y_u16 = scroll_y.min(u16::MAX as usize) as u16;
        let visible_y = cy.saturating_sub(scroll_y);
        let wrapped = wrap_text_fixed(width, text);
        let line = Paragraph::new(wrapped)
            .style(Style::default().fg(theme.fg).bg(theme.bg))
            .block(Block::default().borders(Borders::NONE))
            .scroll((scroll_y_u16, 0));
        f.render_widget(line, panel_inner);
        let cx_u16 = panel_inner
            .x
            .saturating_add(cx.min(u16::MAX as usize) as u16);
        let cy_u16 = panel_inner
            .y
            .saturating_add(visible_y.min(u16::MAX as usize) as u16);
        result.cursor = Some((cx_u16, cy_u16));
        result.editor_rect = Some(panel_inner);
        return result;
    }

    if panel_inner.height == 0 || panel_inner.width == 0 {
        return result;
    }
    if fields.is_empty() {
        let hint = "Enter 编辑提示词";
        let line = Line::from(Span::styled(
            truncate_to_width(hint, panel_inner.width.max(1) as usize),
            Style::default()
                .fg(theme.dim)
                .bg(theme.bg)
                .add_modifier(Modifier::ITALIC),
        ));
        let p = Paragraph::new(Text::from(line))
            .block(Block::default().borders(Borders::NONE))
            .style(Style::default().bg(theme.bg))
            .wrap(Wrap { trim: true });
        f.render_widget(p, panel_inner);
        return result;
    }
    let label_w = fields
        .iter()
        .map(|(label, _)| UnicodeWidthStr::width(label.as_str()))
        .max()
        .unwrap_or(6)
        .clamp(4, 14);
    let mut field_lines: Vec<Line> = Vec::new();
    let max_fields = panel_inner.height.max(1) as usize;
    let field_w = panel_inner.width.max(1) as usize;
    for (idx, (label, value)) in fields.iter().take(max_fields).enumerate() {
        let is_sel = idx == field_selected;
        let is_focus = matches!(focus, SettingsFocus::Fields | SettingsFocus::Input);
        let label_pad = pad_to_width(label, label_w);
        let base = format!("{label_pad} : {value}");
        let text = truncate_to_width(&base, field_w);
        let style = if is_sel && is_focus {
            Style::default()
                .fg(theme.bg)
                .bg(theme.yellow)
                .add_modifier(Modifier::BOLD)
        } else if is_sel {
            Style::default().fg(theme.yellow).bg(theme.bg)
        } else {
            Style::default().fg(theme.dim).bg(theme.bg)
        };
        field_lines.push(Line::from(Span::styled(text, style)));
    }
    let field_p = Paragraph::new(Text::from(field_lines))
        .block(Block::default().borders(Borders::NONE))
        .wrap(Wrap { trim: true });
    f.render_widget(field_p, panel_inner);

    result
}

pub fn draw_settings_status(
    f: &mut ratatui::Frame,
    theme: &Theme,
    area: Rect,
    focus: SettingsFocus,
    tick: usize,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let bg = Block::default()
        .borders(Borders::NONE)
        .style(Style::default().bg(theme.bg));
    f.render_widget(bg, area);
    let label = match focus {
        SettingsFocus::Tabs => "设置导航",
        SettingsFocus::Fields => "配置浏览",
        SettingsFocus::Input => "输入编辑",
        SettingsFocus::Prompt => "提示词编辑",
    };
    let glow = pulse_color(theme, theme.cyan, tick);
    let max_w = area.width.max(1) as usize;
    let body = format!("█▒░ {label} ░▒█");
    let text = fit_middle_ellipsis(&body, max_w);
    let line = Line::from(Span::styled(
        text,
        Style::default()
            .fg(glow)
            .bg(theme.bg)
            .add_modifier(Modifier::BOLD),
    ));
    let p = Paragraph::new(Text::from(line))
        .block(Block::default().borders(Borders::NONE))
        .style(Style::default().bg(theme.bg))
        .wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

pub fn draw_header(
    f: &mut ratatui::Frame,
    args: DrawHeaderArgs<'_>,
) {
    let DrawHeaderArgs {
        theme,
        area,
        main_mode,
        dog_mode,
        user_active,
        pulse_dir,
        tick,
        emergent_tick,
        emergent_seed,
        emergent_exit_step,
    } = args;
    // 顶栏两行：MAIN/DOG 状态 + 静态 SYS 状态。
    let block = Block::default()
        .borders(Borders::NONE)
        .style(Style::default().bg(theme.bg));
    let inner = block.inner(area);
    f.render_widget(block, area);
    let max_w = inner.width.max(1) as usize;
    if inner.height == 0 {
        return;
    }

    let line1_area = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: 1,
    };
    let line2_area = Rect {
        x: inner.x,
        y: inner.y.saturating_add(1),
        width: inner.width,
        height: 1,
    };
    let sep_area = Rect {
        x: inner.x,
        y: inner.y.saturating_add(2),
        width: inner.width,
        height: inner.height.saturating_sub(2),
    };

    let left_active = matches!(main_mode, Mode::Generating | Mode::ExecutingTool);
    let right_active = matches!(dog_mode, Mode::Generating | Mode::ExecutingTool);
    let header_bg = Color::Rgb(255, 255, 255);
    let header_fg = Color::Black;
    let (left_spans, left_w, left_text) = build_header_label_spans("MAIN", header_bg, header_fg);
    let (right_spans, right_w, right_text) = build_header_label_spans("DOG", header_bg, header_fg);

    let mut header_spans = Vec::new();
    let full_w = left_w.saturating_add(right_w);
    if full_w.saturating_add(1) <= max_w {
        let gap = max_w.saturating_sub(full_w);
        header_spans.extend(left_spans);
        let active_kind = if left_active && !right_active {
            Some(MindKind::Main)
        } else if right_active && !left_active {
            Some(MindKind::Sub)
        } else {
            None
        };
        let active_all = left_active && right_active;
        header_spans.extend(build_header_track_spans(HeaderTrackSpansArgs {
            len: gap.max(1),
            active_kind,
            active_all,
            user_active,
            pulse_dir,
            tick,
            bg: header_bg,
            fg: header_fg,
        }));
        header_spans.extend(right_spans);
    } else {
        let left_max = (max_w / 2).max(1);
        let right_max = max_w.saturating_sub(left_max).max(1);
        let left_fit = fit_middle_ellipsis(&left_text, left_max);
        let right_fit = fit_middle_ellipsis(&right_text, right_max);
        let left_fit_w = UnicodeWidthStr::width(left_fit.as_str());
        let right_fit_w = UnicodeWidthStr::width(right_fit.as_str());
        let gap = max_w.saturating_sub(left_fit_w + right_fit_w);
        header_spans.push(Span::styled(
            left_fit,
            Style::default().fg(header_fg).bg(header_bg),
        ));
        header_spans.push(Span::styled(
            " ".repeat(gap.max(1)),
            Style::default().bg(header_bg),
        ));
        header_spans.push(Span::styled(
            right_fit,
            Style::default().fg(header_fg).bg(header_bg),
        ));
    }

    let header_line = Line::from(header_spans);
    let header_p = Paragraph::new(Text::from(header_line))
        .block(Block::default().borders(Borders::NONE))
        .style(Style::default().fg(header_fg).bg(header_bg))
        .wrap(Wrap { trim: true });
    f.render_widget(header_p, line1_area);

    if inner.height < 2 {
        return;
    }

    // 顶栏第二行：黑暗中涌现出智能（低频）。
    // 规则：
    // - idle：纯黑（不显示任何字符）
    // - 活跃：播放“涌现智能”循环
    // - 结束：即使没播完也快速收尾并消失
    let header_active = left_active || right_active || pulse_dir.is_some();
    if header_active || emergent_exit_step.is_some() {
        let line2 = Line::from(build_emergent_intel_spans(
            max_w,
            emergent_tick,
            emergent_seed,
            emergent_exit_step,
            theme,
        ));
        let p2 = Paragraph::new(Text::from(line2))
            .block(Block::default().borders(Borders::NONE))
            // 不 wrap：必须保留行首空白，否则随机位置会被 trim 吃掉导致“挤在开头”。
            .style(Style::default().bg(theme.bg));
        f.render_widget(p2, line2_area);
    } else {
        let blank = Paragraph::new(Text::from(Line::from(Span::styled(
            " ".repeat(max_w),
            Style::default().bg(theme.bg),
        ))))
        .block(Block::default().borders(Borders::NONE))
        .style(Style::default().bg(theme.bg));
        f.render_widget(blank, line2_area);
    }

    if inner.height < 3 {
        return;
    }
    if sep_area.height > 0 {
        let active_mode = if matches!(main_mode, Mode::Idle) {
            dog_mode
        } else {
            main_mode
        };
        let active_kind = if matches!(main_mode, Mode::Idle) {
            MindKind::Sub
        } else {
            MindKind::Main
        };
        let sep_line = Line::from(build_separator_spans(SeparatorSpansArgs {
            len: max_w,
            mode: active_mode,
            active_kind,
            user_active: false,
            tick,
            emergent_tick,
            emergent_seed,
            emergent_exit_step,
            theme,
        }));
        let sep_p = Paragraph::new(Text::from(sep_line))
            .block(Block::default().borders(Borders::NONE))
            .style(Style::default().bg(theme.bg))
            .wrap(Wrap { trim: true });
        f.render_widget(sep_p, sep_area);
    }
}

pub struct DrawHeaderArgs<'a> {
    pub theme: &'a Theme,
    pub area: Rect,
    pub main_mode: Mode,
    pub dog_mode: Mode,
    pub user_active: bool,
    pub pulse_dir: Option<PulseDir>,
    pub tick: usize,
    pub emergent_tick: usize,
    pub emergent_seed: u64,
    pub emergent_exit_step: Option<usize>,
}

pub fn draw_chat(
    f: &mut ratatui::Frame,
    theme: &Theme,
    area: Rect,
    lines: Vec<Line<'static>>,
    scroll: u16,
) {
    let p = Paragraph::new(Text::from(lines))
        .style(Style::default().bg(theme.bg))
        .block(Block::default().borders(Borders::NONE))
        .scroll((scroll, 0));
    f.render_widget(p, area);
}

pub fn draw_input(
    f: &mut ratatui::Frame,
    args: DrawInputArgs<'_>,
) {
    let DrawInputArgs {
        theme,
        area,
        input,
        cursor,
        mode,
        tick,
        system_line,
        status_anim_tick,
        status_anim_seed,
        system_center,
    } = args;
    let border = if matches!(mode, Mode::Generating | Mode::ExecutingTool) {
        // 呼吸渐变：仅在 API 活跃态才变（tick 只在活跃态递增）
        let c = pulse_color(theme, theme.border_active, tick);
        Style::default().fg(c)
    } else if mode == Mode::ApprovingTool {
        let c = pulse_color(theme, theme.border_warn, tick);
        Style::default().fg(c)
    } else {
        Style::default().fg(theme.border_idle)
    };

    let inner = Rect {
        x: area.x.saturating_add(2),
        y: area.y.saturating_add(2),
        width: area.width.saturating_sub(4).max(1),
        height: area.height.saturating_sub(4).max(1),
    };
    let (cursor_x, cursor_y) =
        cursor_xy(inner.width.max(1) as usize, input, cursor.min(input.len()));
    let scroll_y = cursor_y.saturating_sub(inner.height.saturating_sub(1) as usize);
    let scroll_y_u16 = scroll_y.min(u16::MAX as usize) as u16;
    let wrapped = wrap_text_fixed(inner.width.max(1) as usize, input);

    let max_title_w = area.width.saturating_sub(2).max(1) as usize; // borders
    let left_mark = "▒";
    let right_mark = "▒";
    let dot = "●";
    let left_fixed_w = UnicodeWidthStr::width(left_mark).saturating_add(1);
    let right_fixed_w = 1usize.saturating_add(UnicodeWidthStr::width(right_mark));
    let rest_w = max_title_w.saturating_sub(left_fixed_w.saturating_add(right_fixed_w));
    let sys_body = if system_line.trim().is_empty() {
        "就绪".to_string()
    } else {
        compact_ws(system_line)
    };
    // 标题栏内部不显示“♡/♥”前缀（例如 "♡ 你在输入呢…" -> "你在输入呢…"）。
    let sys_body = {
        let t = sys_body.trim_start();
        if let Some(rest) = t.strip_prefix('♡') {
            rest.trim_start().to_string()
        } else if let Some(rest) = t.strip_prefix('♥') {
            rest.trim_start().to_string()
        } else {
            t.to_string()
        }
    };
    let center_line = system_center;
    let dot_w = UnicodeWidthStr::width(dot);
    // 新标题格式：`● 内容`（不再左右各一个点），因此仅扣掉一个点与一个空格。
    let body_avail = rest_w.saturating_sub(dot_w.saturating_add(1));
    let sys_text = if body_avail == 0 {
        String::new()
    } else {
        truncate_to_width(&sys_body, body_avail)
    };
    let has_sys = !sys_text.is_empty();
    let content_w = dot_w
        .saturating_add(1)
        .saturating_add(UnicodeWidthStr::width(sys_text.as_str()));
    let pad_w = rest_w.saturating_sub(content_w);
    let (pad_left, pad_right) = if center_line {
        let left = pad_w / 2;
        (left, pad_w.saturating_sub(left))
    } else {
        (0, pad_w)
    };
    // 空闲态（就绪）必须完全静止：不做任何 pulse，避免“就绪一直闪烁”。
    let (sys_style, dot_color) = if sys_body == "就绪" {
        (
            Style::default().fg(theme.dim).add_modifier(Modifier::BOLD),
            theme.dim,
        )
    } else {
        (
            Style::default()
                .fg(pulse_color(theme, theme.fg, tick))
                .add_modifier(Modifier::BOLD),
            fast_pulse_color(theme, theme.fg, tick),
        )
    };

    // 状态栏动画：● 闪烁三次 → 乱码展开内容
    const BLINK_FRAMES: usize = 6; // 3 次（亮/灭）
    const HOLD_FRAMES: usize = 1;
    let total_cells = UnicodeWidthStr::width(sys_text.as_str()).max(1);
    // 加速：更快从“乱码”收敛到正文
    let reveal_frames = ((total_cells + 2) / 4).clamp(4, 12);
    let reveal_start = BLINK_FRAMES + HOLD_FRAMES;
    let reveal_end = reveal_start + reveal_frames;
    let anim = status_anim_seed != 0;
    let phase = status_anim_tick;
    let dot_visible = if !anim {
        true
    } else if phase < BLINK_FRAMES {
        phase.is_multiple_of(2)
    } else {
        true
    };
    let (render_text, scrambling) = if !has_sys {
        (String::new(), false)
    } else if !anim {
        (sys_text.clone(), false)
    } else if phase < reveal_start {
        (String::new(), false)
    } else if phase < reveal_end {
        let p = phase - reveal_start + 1;
        let reveal = (p.saturating_mul(total_cells))
            .saturating_div(reveal_frames)
            .min(total_cells);
        const GLYPHS: &[char] = &[
            '░', '▒', '▓', '█', '▌', '▐', '▀', '▄', '▖', '▗', '▘', '▙', '▚', '▛', '▜', '▝', '▞',
            '▟',
        ];
        let mut out = String::new();
        let mut used = 0usize;
        for (i, ch) in sys_text.chars().enumerate() {
            let w = UnicodeWidthChar::width(ch).unwrap_or(1).max(1);
            let m = mix64(
                status_anim_seed
                    ^ ((i as u64).wrapping_mul(0x9E3779B97F4A7C15))
                    ^ ((phase as u64).wrapping_mul(0xD1B54A32D192ED03)),
            );
            let glyph = GLYPHS[(m as usize) % GLYPHS.len()];
            if used + w <= reveal {
                out.push(ch);
            } else {
                for _ in 0..w {
                    out.push(glyph);
                }
            }
            used = used.saturating_add(w);
        }
        (out, true)
    } else {
        (sys_text.clone(), false)
    };
    let sys_style = if scrambling {
        Style::default()
            .fg(pulse_color(theme, theme.fg, tick))
            .bg(theme.bg)
            .add_modifier(Modifier::BOLD)
    } else {
        sys_style.bg(theme.bg)
    };

    let mut title_spans = vec![
        Span::styled(left_mark, Style::default().fg(theme.magenta)),
        Span::styled(" ", Style::default().bg(theme.bg)),
    ];
    if pad_left > 0 {
        title_spans.push(Span::styled(
            " ".repeat(pad_left),
            Style::default().bg(theme.bg),
        ));
    }
    if has_sys {
        let dot_char = if dot_visible { dot } else { " " };
        title_spans.push(Span::styled(
            dot_char,
            Style::default().fg(dot_color).bg(theme.bg).add_modifier(Modifier::BOLD),
        ));
        title_spans.push(Span::styled(" ", Style::default().bg(theme.bg)));
        if render_text.is_empty() {
            // 关键：动画前几帧正文为空时也要占位，否则标题宽度变化会导致“边框/角标跟着伸缩”。
            let w = UnicodeWidthStr::width(sys_text.as_str());
            if w > 0 {
                title_spans.push(Span::styled(" ".repeat(w), sys_style));
            }
        } else {
            title_spans.push(Span::styled(render_text, sys_style));
        }
    }
    if pad_right > 0 {
        title_spans.push(Span::styled(
            " ".repeat(pad_right),
            Style::default().bg(theme.bg),
        ));
    }
    title_spans.push(Span::styled(" ", Style::default().bg(theme.bg)));
    title_spans.push(Span::styled(right_mark, Style::default().fg(theme.magenta)));

    let p = Paragraph::new(wrapped)
        .style(Style::default().fg(theme.fg).bg(theme.bg))
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Double)
                .border_style(border)
                .padding(Padding::new(1, 1, 1, 1))
                .style(Style::default().bg(theme.bg))
                .title(Line::from(title_spans)),
        )
        .scroll((scroll_y_u16, 0));
    f.render_widget(p, area);

    let visible_y = cursor_y.saturating_sub(scroll_y);
    let cx = inner
        .x
        .saturating_add(cursor_x.min(u16::MAX as usize) as u16);
    let cy = inner
        .y
        .saturating_add(visible_y.min(u16::MAX as usize) as u16);
    f.set_cursor_position((cx, cy));
}

pub struct DrawInputArgs<'a> {
    pub theme: &'a Theme,
    pub area: Rect,
    pub input: &'a str,
    pub cursor: usize,
    pub mode: Mode,
    pub tick: usize,
    pub system_line: &'a str,
    pub status_anim_tick: usize,
    pub status_anim_seed: u64,
    pub system_center: bool,
}

pub fn draw_separator(
    f: &mut ratatui::Frame,
    args: DrawSeparatorArgs<'_>,
) {
    let DrawSeparatorArgs {
        theme,
        area,
        mode,
        active_kind,
        user_active,
        tick,
        emergent_tick,
        emergent_seed,
        emergent_exit_step,
    } = args;
    if area.height == 0 {
        return;
    }
    let max_w = area.width.max(1) as usize;
    let line = Line::from(build_separator_spans(SeparatorSpansArgs {
        len: max_w,
        mode,
        active_kind,
        user_active,
        tick,
        emergent_tick,
        emergent_seed,
        emergent_exit_step,
        theme,
    }));
    let p = Paragraph::new(Text::from(line))
        .block(Block::default().borders(Borders::NONE))
        .style(Style::default().bg(theme.bg))
        .wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

pub struct DrawSeparatorArgs<'a> {
    pub theme: &'a Theme,
    pub area: Rect,
    pub mode: Mode,
    pub active_kind: MindKind,
    pub user_active: bool,
    pub tick: usize,
    pub emergent_tick: usize,
    pub emergent_seed: u64,
    pub emergent_exit_step: Option<usize>,
}

pub fn draw_status_panel(
    f: &mut ratatui::Frame,
    args: DrawStatusPanelArgs<'_>,
) {
    let DrawStatusPanelArgs {
        theme,
        area,
        label,
        thinking_text,
        thinking_scroll,
        show_dots,
        tick,
    } = args;
    if area.height == 0 || area.width == 0 {
        return;
    }
    let bg = Block::default()
        .borders(Borders::NONE)
        .style(Style::default().bg(theme.bg));
    f.render_widget(bg, area);

    let width = area.width.max(1) as usize;
    let mut lines: Vec<Line<'static>> = Vec::new();
    if !thinking_text.trim().is_empty() {
        lines = render_thinking_lines(theme, width, label, thinking_text, thinking_scroll);
    } else if show_dots && let Some(line) = build_activity_dots_line(theme, width, tick) {
        lines.push(line);
    }
    if lines.is_empty() {
        return;
    }
    let p = Paragraph::new(Text::from(lines))
        .block(Block::default().borders(Borders::NONE))
        // 该栏强制单行裁切：不允许自动换行，避免“串到下一条横杠/区域”的错觉与撕裂。
        .style(Style::default().bg(theme.bg));
    f.render_widget(p, area);
}

pub struct DrawStatusPanelArgs<'a> {
    pub theme: &'a Theme,
    pub area: Rect,
    pub label: &'a str,
    pub thinking_text: &'a str,
    pub thinking_scroll: usize,
    pub show_dots: bool,
    pub tick: usize,
}

pub fn draw_context_bar(f: &mut ratatui::Frame, theme: &Theme, area: Rect, line: ContextLine) {
    fn format_tokens_m(total: u64) -> String {
        let m = total as f64 / 1_000_000.0;
        format!("{m:.2}M")
    }

    let max_cells = area.width.max(1) as usize;
    let run = {
        let secs = line.run_secs;
        let total_h = (secs / 3600).min(999);
        let m = (secs % 3600) / 60;
        format!("{total_h}H{m:02}M")
    };
    let text = format!(
        "R：{} CTX {}% ⇧{} ⇩{} T↑{} T↓{} ♡{} ♥{} {}",
        line.rounds,
        line.ctx_pct,
        format_tokens_m(line.run_in_tokens),
        format_tokens_m(line.run_out_tokens),
        format_tokens_m(line.total_in_tokens),
        format_tokens_m(line.total_out_tokens),
        line.heartbeat_count,
        line.response_count,
        run,
    );
    let clipped = truncate_to_width(&text, max_cells);
    let line = Line::from(Span::styled(
        clipped,
        Style::default().fg(theme.dim).bg(theme.bg),
    ));
    let p = Paragraph::new(Text::from(line))
        .block(Block::default().borders(Borders::NONE))
        .style(Style::default().bg(theme.bg))
        .wrap(Wrap { trim: true });
    f.render_widget(p, area);
}

fn cursor_xy(width: usize, input: &str, cursor_end: usize) -> (usize, usize) {
    let width = width.max(1);
    let mut x: usize = 0;
    let mut y: usize = 0;
    let end = cursor_end.min(input.len());
    let slice = &input[..end];
    for ch in slice.chars() {
        if ch == '\n' {
            y = y.saturating_add(1);
            x = 0;
            continue;
        }
        let w = UnicodeWidthChar::width(ch).unwrap_or(1).max(1);
        if x + w > width {
            y = y.saturating_add(1);
            x = 0;
        }
        x = x.saturating_add(w);
        if x >= width {
            y = y.saturating_add(1);
            x = 0;
        }
    }
    (x, y)
}

fn wrap_text_fixed(width: usize, input: &str) -> String {
    let width = width.max(1);
    let mut out = String::new();
    let mut x: usize = 0;
    for ch in input.chars() {
        if ch == '\n' {
            out.push('\n');
            x = 0;
            continue;
        }
        let w = UnicodeWidthChar::width(ch).unwrap_or(1).max(1);
        if x + w > width {
            out.push('\n');
            x = 0;
        }
        out.push(ch);
        x = x.saturating_add(w);
        if x >= width {
            out.push('\n');
            x = 0;
        }
    }
    out
}

pub struct ChatRenderCache {
    width: usize,
    per_msg: Vec<Option<Vec<Line<'static>>>>,
    scramble: Vec<Option<ScrambleState>>,
}

#[derive(Clone, Debug)]
struct ScrambleSegment {
    start: usize,
    end: usize,
    born_tick: usize,
}

#[derive(Clone, Debug, Default)]
struct ScrambleState {
    last_len: usize,
    segments: Vec<ScrambleSegment>,
}

impl ChatRenderCache {
    pub fn new() -> Self {
        Self {
            width: 0,
            per_msg: Vec::new(),
            scramble: Vec::new(),
        }
    }

    pub fn prepare(&mut self, width: usize, msg_len: usize) {
        if self.width != width {
            self.width = width;
            self.per_msg.clear();
            self.scramble.clear();
        }
        if self.per_msg.len() > msg_len {
            self.per_msg.clear();
            self.scramble.clear();
        }
        if self.per_msg.len() < msg_len {
            self.per_msg.resize_with(msg_len, || None);
        }
        if self.scramble.len() < msg_len {
            self.scramble.resize_with(msg_len, || None);
        }
    }

    pub fn invalidate(&mut self, idx: usize) {
        if let Some(slot) = self.per_msg.get_mut(idx) {
            *slot = None;
        }
    }

    pub fn has_scramble_pending(&self) -> bool {
        self.scramble
            .iter()
            .any(|s| s.as_ref().is_some_and(|st| !st.segments.is_empty()))
    }

    fn update_scramble_state(&mut self, idx: usize, text: &str, tick: usize, active: bool) {
        const MIN_DELAY_FRAMES: usize = 1;
        const CHARS_PER_FRAME: usize = 4;
        const NOISE_FRAMES: usize = 2;
        let new_len = text.chars().count();

        let slot = self.scramble.get_mut(idx);
        let Some(slot) = slot else {
            return;
        };

        let state = slot.get_or_insert_with(Default::default);
        // 初次遇到历史消息时不要触发“乱码→清晰”，避免打开/重绘时整屏闪烁。
        // 只有当该消息处于“当前活跃输出”时才允许从 0 开始触发。
        if state.last_len == 0 && state.segments.is_empty() && !active {
            state.last_len = new_len;
            return;
        }
        if state.last_len == 0 && state.segments.is_empty() && active && new_len > 0 {
            state.segments.push(ScrambleSegment {
                start: 0,
                end: new_len,
                born_tick: tick,
            });
            state.last_len = new_len;
        } else if new_len > state.last_len {
            state.segments.push(ScrambleSegment {
                start: state.last_len,
                end: new_len,
                born_tick: tick,
            });
            state.last_len = new_len;
        } else if new_len < state.last_len {
            state.last_len = new_len;
            state.segments.clear();
        }

        while let Some(seg) = state.segments.first() {
            let len = seg.end.saturating_sub(seg.start);
            let settle_frames = MIN_DELAY_FRAMES
                .saturating_add(len.saturating_sub(1) / CHARS_PER_FRAME)
                .saturating_add(NOISE_FRAMES)
                .saturating_add(1);
            if tick.saturating_sub(seg.born_tick) >= settle_frames {
                state.segments.remove(0);
            } else {
                break;
            }
        }
    }

    fn scramble_text(&self, idx: usize, text: &str, tick: usize, active: bool) -> Option<String> {
        const MIN_DELAY_FRAMES: usize = 1;
        const CHARS_PER_FRAME: usize = 4;
        const NOISE_FRAMES: usize = 2;

        let state = self.scramble.get(idx).and_then(|s| s.as_ref())?;
        if state.segments.is_empty() {
            return None;
        }

        let mut chars: Vec<char> = text.chars().collect();
        for seg in &state.segments {
            let seg_len = seg.end.saturating_sub(seg.start);
            for (offset, idx) in (seg.start..seg.end).enumerate() {
                let Some(&ch) = chars.get(idx) else {
                    continue;
                };
                if ch == '\n' {
                    continue;
                }
                let age = tick.saturating_sub(seg.born_tick);
                let per_char_delay = MIN_DELAY_FRAMES.saturating_add(offset / CHARS_PER_FRAME);
                if age < per_char_delay {
                    chars[idx] = scramble_noise(ch, tick, seg.born_tick, idx);
                    continue;
                }
                if age < per_char_delay.saturating_add(NOISE_FRAMES) {
                    let jitter = (tick as u64)
                        .wrapping_mul(0x9E3779B97F4A7C15)
                        .wrapping_add((idx as u64).wrapping_mul(0xBF58476D1CE4E5B9))
                        ^ (seg.born_tick as u64);
                    if (jitter & 0b1) == 0 {
                        chars[idx] = scramble_noise(ch, tick, seg.born_tick, idx);
                    }
                }
                let _ = seg_len;
            }
        }

        let mut out: String = chars.into_iter().collect();
        if active {
            out.push_str(&scramble_tail(tick));
        }
        Some(out)
    }
}

fn scramble_noise(ch: char, tick: usize, born_tick: usize, idx: usize) -> char {
    let w = UnicodeWidthChar::width(ch).unwrap_or(1).max(1);
    let seed = (tick as u64)
        .wrapping_mul(0x9E3779B97F4A7C15)
        .wrapping_add((born_tick as u64).wrapping_mul(0xBF58476D1CE4E5B9))
        .wrapping_add(idx as u64);
    if w >= 2 {
        const NOISE2: [char; 8] = ['Ｘ', 'Ｚ', '＠', '＃', '％', '＆', '口', '田'];
        NOISE2[(seed as usize) % NOISE2.len()]
    } else {
        const NOISE1: [char; 12] = ['@', '%', '&', '$', 'X', 'Z', '0', '1', '7', '9', 'Λ', 'Ω'];
        NOISE1[(seed as usize) % NOISE1.len()]
    }
}

fn scramble_tail(tick: usize) -> String {
    const TAIL: [char; 10] = ['░', '▒', '▓', '⟡', '⟠', '·', '•', '⋯', '⫶', '⫷'];
    let a = TAIL[(tick.wrapping_mul(3)) % TAIL.len()];
    let b = TAIL[(tick.wrapping_mul(5).wrapping_add(1)) % TAIL.len()];
    let c = TAIL[(tick.wrapping_mul(7).wrapping_add(2)) % TAIL.len()];
    format!(" {a}{b}{c}")
}

fn render_message_lines(
    theme: &Theme,
    msg: &crate::Message,
    width: usize,
    dot: &str,
    color: Color,
    base_style: Style,
    reveal_len: Option<usize>,
) -> Vec<Line<'static>> {
    let prefix = format!("{dot} ");
    let prefix_width = UnicodeWidthStr::width(prefix.as_str());
    let prefix_style = if msg.role == Role::User {
        Style::default()
            .fg(color)
            .bg(theme.bg)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(color).bg(theme.bg)
    };
    if msg.role == Role::User && should_compact_user_message(&msg.text) {
        let summary = summarize_user_message(&msg.text, width.saturating_sub(prefix_width));
        let summary = if let Some(reveal_len) = reveal_len {
            truncate_by_chars(&summary, reveal_len)
        } else {
            summary
        };
        return vec![Line::from(vec![
            Span::styled(prefix, prefix_style),
            Span::styled(summary, base_style),
        ])];
    }
    let display = if let Some(reveal_len) = reveal_len {
        Cow::Owned(truncate_by_chars(&msg.text, reveal_len))
    } else {
        truncate_for_ui(&msg.text)
    };
    let rendered =
        render_markdown_to_lines(&display, width.saturating_sub(prefix_width), base_style);
    if rendered.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut it = rendered.into_iter();
    if let Some(first_line) = it.next() {
        let mut spans = Vec::new();
        spans.push(Span::styled(prefix, prefix_style));
        spans.extend(first_line.spans);
        out.push(Line::from(spans));
    }
    for line in it {
        let mut spans = Vec::new();
        spans.push(Span::styled(
            " ".repeat(prefix_width),
            Style::default().bg(theme.bg),
        ));
        spans.extend(line.spans);
        out.push(Line::from(spans));
    }
    out
}

pub fn build_chat_lines(
    args: BuildChatLinesArgs<'_>,
) -> Vec<Line<'static>> {
    let BuildChatLinesArgs {
        theme,
        core,
        render_cache,
        width,
        streaming_idx,
        reveal_idx,
        reveal_len,
        tick,
        expanded_tool_idx,
        expand_all_tools,
        show_think,
        cute_anim,
        pulse_idx,
        pulse_style,
    } = args;
    let mut out: Vec<Line<'static>> = Vec::new();
    let tool_label_width = TOOL_LABEL_WIDTH;
    render_cache.prepare(width, core.history.len());
    // 只用真实的 streaming_idx 作为“当前正在输出”的消息索引，避免在请求尚未开始（还没 push 占位）
    // 时误把“上一条 assistant”当作 active，导致旧消息触发乱码闪烁。
    let active_stream_idx = streaming_idx.or_else(|| {
        reveal_idx.and_then(|idx| {
            core.history
                .get(idx)
                .is_some_and(|m| m.role == Role::Assistant)
                .then_some(idx)
        })
    });

    for (msg_idx, msg) in core.history.iter().enumerate() {
        // assistant 占位（空内容）不渲染：避免在“思考/工具阶段”插入任何聊天区提示造成错位。
        if msg.role == Role::Assistant && msg.text.trim().is_empty() {
            continue;
        }

        let is_heartbeat_banner =
            msg.role == Role::System && msg.text.trim_end() == crate::HEARTBEAT_BANNER;
        let pulse = if pulse_idx == Some(msg_idx) {
            pulse_style
        } else {
            None
        };
        let pulse_color = pulse.map(|p| {
            if p.visible {
                lerp_color(theme.dim, theme.magenta, p.intensity.clamp(0.0, 1.0))
            } else {
                theme.bg
            }
        });
        let scramble_active = active_stream_idx == Some(msg_idx);
        let scramble_pending = render_cache
            .scramble
            .get(msg_idx)
            .and_then(|s| s.as_ref())
            .is_some_and(|st| !st.segments.is_empty());
        let scramble_enabled = msg.role == Role::Assistant
            && (msg.mind == Some(MindKind::Sub) || scramble_active || scramble_pending);
        if scramble_enabled {
            render_cache.update_scramble_state(msg_idx, &msg.text, tick, scramble_active);
        }
        if msg.role == Role::Tool {
            let summary = summarize_tool_message(&msg.text);
            let (tool_raw, input_raw, _note) = parse_tool_summary(&summary);
            let is_think = tool_raw.eq_ignore_ascii_case("think");
            let is_mind = tool_raw.eq_ignore_ascii_case("mind");
            let show_details = expand_all_tools || expanded_tool_idx == Some(msg_idx);

            if is_think {
                let think_running = msg.text.contains("状态:running") || msg.text.contains("状态:in_progress");
                // 思考详情的展开/折叠必须与“工具详情”完全解耦：
                // - `/Show Think` 才展开思考
                // - `/Show MCP Detail` 只影响工具，不影响思考
                let think_expanded = show_think;
                let _ = think_running;
                if !think_expanded {
                    continue;
                }
                // 消息块之间保持 1 行间隔：由“上一块末尾的空行”保证，这里不额外插空。
                let mind = extract_tool_mind(&msg.text);
                let dot = if mind == Some("dog") { "○" } else { "●" };
                let dot_color = theme.cyan;
                let base_style = Style::default()
                    .fg(theme.dim)
                    .bg(theme.bg)
                    .add_modifier(Modifier::ITALIC);
                let thinking_body = render_think_output_lines_italic(theme, &msg.text, width)
                    .into_iter()
                    .map(|l| l.to_string())
                    .collect::<Vec<_>>()
                    .join("\n");
                let think_msg = crate::Message {
                    role: Role::Assistant,
                    text: thinking_body,
                    mind: Some(if mind == Some("dog") {
                        MindKind::Sub
                    } else {
                        MindKind::Main
                    }),
                };
                let rendered =
                    render_message_lines(theme, &think_msg, width, dot, dot_color, base_style, None);
                out.extend(rendered);
                push_blank_line(&mut out);
                continue;
            }

            // mind_msg 工具本体默认隐藏：需要展开工具详情才显示
            if is_mind && !show_details {
                continue;
            }

            // 消息块间隔：上一块末尾已插入空行，这里不额外插空。

            let mind = extract_tool_mind(&msg.text);
            let dot = if mind == Some("dog") { "○" } else { "●" };
            let (tool_disp, cmd_disp) = normalize_tool_display(
                &tool_raw,
                input_raw.as_deref().unwrap_or(""),
            );
            let tool_pad = if tool_label_width > 0 {
                pad_to_width(&tool_disp, tool_label_width)
            } else {
                tool_disp.clone()
            };
            let label = format!("{dot} · {tool_pad} · ");
            let _label_width = UnicodeWidthStr::width(label.as_str());
            let prefix = label.clone();
            let prefix_width = UnicodeWidthStr::width(prefix.as_str());
            let cmd_compact = compact_ws(&cmd_disp);
            let mut text = if width <= prefix_width || cmd_compact.is_empty() {
                truncate_to_width(&prefix, width.max(1))
            } else {
                let avail = width.saturating_sub(prefix_width).max(1);
                let cmd_fit = fit_middle_ellipsis(&cmd_compact, avail);
                format!("{prefix}{cmd_fit}")
            };
            text = apply_reveal(text, width, reveal_idx, msg_idx, reveal_len);
            let tool_color = theme.yellow;
            let prefix_style = Style::default()
                .fg(tool_color)
                .bg(theme.bg)
                .add_modifier(Modifier::BOLD);
            let rest_style = Style::default().fg(tool_color).bg(theme.bg);
            let (prefix_part, rest_part) = if text.len() >= prefix.len() {
                text.split_at(prefix.len())
            } else {
                (text.as_str(), "")
            };
            out.push(Line::from(vec![
                Span::styled(prefix_part.to_string(), prefix_style),
                Span::styled(rest_part.to_string(), rest_style),
            ]));

            if !show_details {
                if let Some(explain) = extract_tool_explain(&msg.text) {
                    push_tool_explain_line(
                        &mut out, theme, width, &explain, reveal_idx, msg_idx, reveal_len,
                    );
                }
                if !is_mind && let Some(result) = tool_status_result_line(&msg.text) {
                    push_tool_explain_line(
                        &mut out, theme, width, &result, reveal_idx, msg_idx, reveal_len,
                    );
                }
            } else {
                let details = render_tool_detail_lines(theme, &msg.text, width);
                out.extend(details);
            }
            push_blank_line(&mut out);
            continue;
        }

        if msg.role == Role::System && strip_thinking_marker(&msg.text).is_some() {
            continue;
        }
        if msg.text.trim().is_empty() {
            continue;
        }

        if msg.role == Role::Assistant {
            if let Some((from, to, brief, content)) = parse_mind_msg(&msg.text) {
                // 消息块间隔：上一块末尾已插入空行，这里不额外插空。
                let from_is_dog = from.eq_ignore_ascii_case("dog");
                let dot = if from_is_dog { "○" } else { "●" };
                let from_label = if from_is_dog { "Dog" } else { "Main" };
                let to_label = if to.eq_ignore_ascii_case("dog") { "Dog" } else { "Main" };
                let header_spans = vec![
                    Span::styled(
                        format!("{dot} {from_label} "),
                        Style::default().fg(theme.cyan).bg(theme.bg),
                    ),
                    Span::styled(
                        "➠".to_string(),
                        Style::default()
                            .fg(theme.fg)
                            .bg(theme.bg)
                            .add_modifier(Modifier::ITALIC),
                    ),
                    Span::styled(
                        format!(" {to_label}：{brief}"),
                        Style::default().fg(theme.fg).bg(theme.bg),
                    ),
                ];
                out.push(Line::from(header_spans));
                if !content.trim().is_empty() {
                    let prefix = "   ↳ ";
                    let line = truncate_to_width(
                        &format!("{prefix}{}", compact_ws(&content)),
                        width.max(1),
                    );
                    out.push(Line::from(vec![Span::styled(
                        line,
                        Style::default().fg(theme.fg).bg(theme.bg).add_modifier(Modifier::ITALIC),
                    )]));
                }
                push_blank_line(&mut out);
                continue;
            }
            if let Some(pass_kind) = parse_pass_token(&msg.text) {
                // 消息块间隔：上一块末尾已插入空行，这里不额外插空。
                let dot = if pass_kind == "main" { "●" } else { "○" };
                let line = format!("{dot} ♥");
                out.push(Line::from(vec![Span::styled(
                    line,
                    Style::default()
                        .fg(theme.magenta)
                        .bg(theme.bg)
                        .add_modifier(Modifier::BOLD),
                )]));
                push_blank_line(&mut out);
                continue;
            }
        }

        // 消息块之间保持 1 行间隔：每个已渲染块末尾都会插入空行，这里不额外插空。
        let (dot, color) = match msg.role {
            Role::User => ("☯", Color::White),
            Role::Assistant => {
                let dot = match msg.mind {
                    Some(MindKind::Sub) => "○",
                    _ => "●",
                };
                (dot, theme.cyan)
            }
            Role::System => {
                let _ = (is_heartbeat_banner, cute_anim);
                ("·", theme.dim)
            }
            Role::Tool => ("◇", theme.yellow),
        };
        let dot_color = pulse_color.unwrap_or(color);
        let fg_color = pulse_color.unwrap_or(match msg.role {
            Role::User => Color::White,
            Role::Assistant => theme.fg,
            Role::System => theme.dim,
            Role::Tool => theme.yellow,
        });
        let base_style = if msg.role == Role::User {
            Style::default()
                .fg(fg_color)
                .bg(theme.bg)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(fg_color).bg(theme.bg)
        };
        let mut animated_assistant = false;
        let decorated: Option<crate::Message> = if scramble_enabled {
            if let Some(scrambled) = render_cache.scramble_text(
                msg_idx,
                &msg.text,
                tick,
                scramble_active,
            ) {
                let mut next = msg.clone();
                next.text = scrambled;
                animated_assistant = true;
                Some(next)
            } else {
                None
            }
        } else {
            None
        };
        let msg_ref: &crate::Message = decorated.as_ref().unwrap_or(msg);

        let rendered = if reveal_idx == Some(msg_idx) {
            render_message_lines(
                theme,
                msg_ref,
                width,
                dot,
                dot_color,
                base_style,
                Some(reveal_len),
            )
        } else if let Some(lines) = render_cache
            .per_msg
            .get(msg_idx)
            .and_then(|entry| entry.as_ref())
            && pulse.is_none()
            && !animated_assistant
        {
            lines.clone()
        } else {
            let lines =
                render_message_lines(theme, msg_ref, width, dot, dot_color, base_style, None);
            if !animated_assistant && let Some(slot) = render_cache.per_msg.get_mut(msg_idx) {
                *slot = Some(lines.clone());
            }
            lines
        };
        if rendered.is_empty() {
            continue;
        }
        out.extend(rendered);
        push_blank_line(&mut out);
    }

    out
}

pub struct BuildChatLinesArgs<'a> {
    pub theme: &'a Theme,
    pub core: &'a Core,
    pub render_cache: &'a mut ChatRenderCache,
    pub width: usize,
    pub streaming_idx: Option<usize>,
    pub reveal_idx: Option<usize>,
    pub reveal_len: usize,
    pub tick: usize,
    pub expanded_tool_idx: Option<usize>,
    pub expand_all_tools: bool,
    pub show_think: bool,
    pub cute_anim: bool,
    pub pulse_idx: Option<usize>,
    pub pulse_style: Option<HeartbeatStyle>,
}
//（已移除 system 文案“柔化”逻辑：system 必须原样展示，避免误导与不一致。）

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Core, Message, MindKind, Role};

    #[test]
    fn think_tool_is_directly_above_assistant() {
        let theme = Theme::cyberpunk();
        let mut core = Core::new();
        core.push_tool(
            "操作: Think\ninput: \nmind: main\noutput:\n```text\nTHINK_DETAIL\n```\nmeta:\n```text\n状态:done\n```\n"
                .to_string(),
        );
        core.history.push(Message {
            role: Role::Assistant,
            text: "hello".to_string(),
            mind: Some(MindKind::Main),
        });

        let mut cache = ChatRenderCache::new();
        let lines = build_chat_lines(BuildChatLinesArgs {
            theme: &theme,
            core: &core,
            render_cache: &mut cache,
            width: 80,
            streaming_idx: None,
            reveal_idx: None,
            reveal_len: 0,
            tick: 0,
            expanded_tool_idx: None,
            expand_all_tools: false,
            show_think: true,
            cute_anim: false,
            pulse_idx: None,
            pulse_style: None,
        });

        // 展开思考时：Think 正文应出现在 assistant 正文之前，且不带“操作/Think”等标签。
        let joined = lines
            .iter()
            .map(|l| l.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(joined.contains("THINK_DETAIL"));
        assert!(joined.contains("● hello"));
    }

    #[test]
    fn scramble_does_not_flash_on_inactive_first_seen_message() {
        let mut cache = ChatRenderCache::new();
        cache.prepare(80, 1);

        cache.update_scramble_state(0, "hello", 0, false);
        assert!(cache.scramble_text(0, "hello", 0, false).is_none());

        cache.update_scramble_state(0, "hello!", 1, true);
        let scrambled = cache.scramble_text(0, "hello!", 1, true);
        assert!(scrambled.is_some());
        assert_ne!(scrambled.unwrap(), "hello!");
    }

    #[test]
    fn scramble_starts_immediately_for_active_first_seen_message() {
        let mut cache = ChatRenderCache::new();
        cache.prepare(80, 1);

        cache.update_scramble_state(0, "hello", 0, true);
        let scrambled = cache.scramble_text(0, "hello", 0, true);
        assert!(scrambled.is_some());
        assert_ne!(scrambled.unwrap(), "hello");
    }
}

#[derive(Debug, Clone)]
pub struct Theme {
    pub bg: Color,
    pub fg: Color,
    pub dim: Color,
    pub cyan: Color,
    pub magenta: Color,
    pub yellow: Color,
    pub border_idle: Color,
    pub border_active: Color,
    pub border_warn: Color,
}

impl Theme {
    pub fn cyberpunk() -> Self {
        Self {
            bg: Color::Rgb(0, 0, 0),
            fg: Color::Rgb(232, 238, 255),
            dim: Color::Rgb(170, 185, 210),
            cyan: Color::Rgb(0, 240, 255),
            magenta: Color::Rgb(255, 80, 220),
            yellow: Color::Rgb(255, 210, 80),
            border_idle: Color::Rgb(120, 140, 170),
            border_active: Color::Rgb(0, 240, 255),
            border_warn: Color::Rgb(255, 210, 80),
        }
    }
}

#[derive(Clone)]
struct StyledChunk {
    text: String,
    style: Style,
}

#[derive(Clone)]
struct StyledLogicalLine {
    head: Vec<StyledChunk>,
    cont_prefix: Vec<StyledChunk>,
}

fn flush_line(out: &mut Vec<StyledLogicalLine>, line: &mut StyledLogicalLine) {
    if line.head.is_empty() {
        return;
    }
    out.push(line.clone());
    line.head.clear();
    line.cont_prefix.clear();
}

fn push_empty_line(out: &mut Vec<StyledLogicalLine>, base_style: Style) {
    out.push(StyledLogicalLine {
        head: vec![StyledChunk {
            text: String::new(),
            style: base_style,
        }],
        cont_prefix: vec![],
    });
}

fn ensure_prefix(
    line: &mut StyledLogicalLine,
    base_style: Style,
    in_code_block: bool,
    quote_depth: usize,
    in_item: bool,
    item_prefix: &[StyledChunk],
    item_cont_prefix: &[StyledChunk],
) {
    if !line.head.is_empty() {
        return;
    }
    if in_code_block {
        return;
    }
    let mut head_prefix: Vec<StyledChunk> = Vec::new();
    let mut cont_prefix: Vec<StyledChunk> = Vec::new();

    if quote_depth > 0 {
        let q = "│ ".repeat(quote_depth);
        head_prefix.push(StyledChunk {
            text: q.clone(),
            style: base_style.fg(Color::DarkGray),
        });
        cont_prefix.push(StyledChunk {
            text: q,
            style: base_style.fg(Color::DarkGray),
        });
    }

    if in_item {
        head_prefix.extend(item_prefix.iter().cloned());
        cont_prefix.extend(item_cont_prefix.iter().cloned());
    }

    if !head_prefix.is_empty() {
        line.head.extend(head_prefix);
        line.cont_prefix = cont_prefix;
    }
}

fn push_text(line: &mut StyledLogicalLine, s: &str, style: Style) {
    if s.is_empty() {
        return;
    }
    if let Some(last) = line.head.last_mut() && last.style == style {
        last.text.push_str(s);
        return;
    }
    line.head.push(StyledChunk {
        text: s.to_string(),
        style,
    });
}

#[derive(Default)]
struct InlineStyle {
    strong: usize,
    emph: usize,
    strike: usize,
    link_href: Option<String>,
}

#[derive(Clone)]
enum ListKind {
    Unordered,
    Ordered { next: u64 },
}

#[derive(Clone)]
struct ListState {
    kind: ListKind,
}

pub fn render_markdown_to_lines(text: &str, width: usize, base_style: Style) -> Vec<Line<'static>> {
    let width = width.max(1);
    let logical = markdown_to_logical_lines(text, base_style);
    let mut out: Vec<Line<'static>> = Vec::new();
    for line in logical {
        out.extend(wrap_styled_line(&line, width));
    }
    out
}

fn markdown_to_logical_lines(text: &str, base_style: Style) -> Vec<StyledLogicalLine> {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    opts.insert(Options::ENABLE_TABLES);
    let parser = Parser::new_ext(text, opts);

    let mut out: Vec<StyledLogicalLine> = Vec::new();
    let mut current = StyledLogicalLine {
        head: vec![],
        cont_prefix: vec![],
    };

    let mut inline = InlineStyle::default();
    let mut quote_depth: usize = 0;
    let mut lists: Vec<ListState> = Vec::new();
    let mut in_code_block: bool = false;
    let mut heading_level: Option<u8> = None;
    let mut in_item: bool = false;
    let mut item_prefix: Vec<StyledChunk> = Vec::new();
    let mut item_cont_prefix: Vec<StyledChunk> = Vec::new();
    let mut table: Option<TableState> = None;

    for ev in parser {
        match ev {
            Event::Start(tag) => match tag {
                Tag::Paragraph => {
                    flush_line(&mut out, &mut current);
                }
                Tag::Heading { level, .. } => {
                    flush_line(&mut out, &mut current);
                    heading_level = Some(level as u8);
                }
                Tag::BlockQuote(_) => {
                    flush_line(&mut out, &mut current);
                    quote_depth = quote_depth.saturating_add(1);
                }
                Tag::List(start) => {
                    flush_line(&mut out, &mut current);
                    let kind = if let Some(n) = start {
                        ListKind::Ordered { next: n }
                    } else {
                        ListKind::Unordered
                    };
                    lists.push(ListState { kind });
                }
                Tag::Item => {
                    flush_line(&mut out, &mut current);
                    in_item = true;

                    let mut prefix = String::new();
                    let mut cont = String::new();

                    if !lists.is_empty() {
                        let indent = "  ".repeat(lists.len().saturating_sub(1));
                        prefix.push_str(&indent);
                        cont.push_str(&indent);

                        if let Some(last) = lists.last_mut() {
                            match &mut last.kind {
                                ListKind::Unordered => {
                                    prefix.push_str("• ");
                                    cont.push_str("  ");
                                }
                                ListKind::Ordered { next } => {
                                    let marker = format!("{next}.");
                                    *next = next.saturating_add(1);
                                    prefix.push_str(&format!("{marker} "));
                                    cont.push_str(&" ".repeat(marker.chars().count() + 1));
                                }
                            }
                        }
                    } else {
                        prefix.push_str("• ");
                        cont.push_str("  ");
                    }

                    item_prefix = vec![StyledChunk {
                        text: prefix,
                        style: base_style.fg(Color::LightMagenta),
                    }];
                    item_cont_prefix = vec![StyledChunk {
                        text: cont,
                        style: base_style,
                    }];
                }
                Tag::Emphasis => inline.emph += 1,
                Tag::Strong => inline.strong += 1,
                Tag::Strikethrough => inline.strike += 1,
                Tag::Link { dest_url, .. } => {
                    inline.link_href = Some(dest_url.to_string());
                }
                Tag::Image { dest_url, .. } => {
                    // 以“图片块”降级显示：把 alt 文本按普通文本渲染，并在结尾追加 URL。
                    inline.link_href = Some(dest_url.to_string());
                    ensure_prefix(
                        &mut current,
                        base_style,
                        in_code_block,
                        quote_depth,
                        in_item,
                        &item_prefix,
                        &item_cont_prefix,
                    );
                    push_text(&mut current, "[img:", base_style.fg(Color::DarkGray));
                }
                Tag::CodeBlock(_) => {
                    flush_line(&mut out, &mut current);
                    in_code_block = true;
                }
                Tag::Table(aligns) => {
                    flush_line(&mut out, &mut current);
                    table = Some(TableState::new(aligns.into_iter().collect()));
                }
                Tag::TableHead => {
                    if let Some(t) = table.as_mut() {
                        t.in_head = true;
                    }
                }
                Tag::TableRow => {
                    if let Some(t) = table.as_mut() {
                        t.start_row();
                    }
                }
                Tag::TableCell => {
                    if let Some(t) = table.as_mut() {
                        t.start_cell();
                    }
                }
                _ => {}
            },
            Event::End(end) => match end {
                TagEnd::Paragraph => {
                    flush_line(&mut out, &mut current);
                    push_empty_line(&mut out, base_style);
                }
                TagEnd::Heading(_) => {
                    flush_line(&mut out, &mut current);
                    push_empty_line(&mut out, base_style);
                    heading_level = None;
                }
                TagEnd::BlockQuote(_) => {
                    flush_line(&mut out, &mut current);
                    quote_depth = quote_depth.saturating_sub(1);
                    push_empty_line(&mut out, base_style);
                }
                TagEnd::List(_) => {
                    flush_line(&mut out, &mut current);
                    lists.pop();
                    push_empty_line(&mut out, base_style);
                }
                TagEnd::Item => {
                    flush_line(&mut out, &mut current);
                    in_item = false;
                    item_prefix.clear();
                    item_cont_prefix.clear();
                }
                TagEnd::Emphasis => inline.emph = inline.emph.saturating_sub(1),
                TagEnd::Strong => inline.strong = inline.strong.saturating_sub(1),
                TagEnd::Strikethrough => inline.strike = inline.strike.saturating_sub(1),
                TagEnd::Link => {
                    if let Some(href) = inline.link_href.take() {
                        ensure_prefix(
                            &mut current,
                            base_style,
                            in_code_block,
                            quote_depth,
                            in_item,
                            &item_prefix,
                            &item_cont_prefix,
                        );
                        push_text(
                            &mut current,
                            &format!(" ({href})"),
                            base_style.fg(Color::DarkGray),
                        );
                    }
                }
                TagEnd::Image => {
                    if let Some(href) = inline.link_href.take() {
                        ensure_prefix(
                            &mut current,
                            base_style,
                            in_code_block,
                            quote_depth,
                            in_item,
                            &item_prefix,
                            &item_cont_prefix,
                        );
                        push_text(
                            &mut current,
                            &format!("] ({href})"),
                            base_style.fg(Color::DarkGray),
                        );
                    } else {
                        ensure_prefix(
                            &mut current,
                            base_style,
                            in_code_block,
                            quote_depth,
                            in_item,
                            &item_prefix,
                            &item_cont_prefix,
                        );
                        push_text(&mut current, "]", base_style.fg(Color::DarkGray));
                    }
                }
                TagEnd::CodeBlock => {
                    in_code_block = false;
                    flush_line(&mut out, &mut current);
                    push_empty_line(&mut out, base_style);
                }
                TagEnd::TableCell => {
                    if let Some(t) = table.as_mut() {
                        t.end_cell();
                    }
                }
                TagEnd::TableRow => {
                    if let Some(t) = table.as_mut() {
                        t.end_row();
                    }
                }
                TagEnd::TableHead => {
                    if let Some(t) = table.as_mut() {
                        t.in_head = false;
                    }
                }
                TagEnd::Table => {
                    flush_line(&mut out, &mut current);
                    if let Some(t) = table.take() {
                        for line_text in t.render() {
                            current.head.clear();
                            current.cont_prefix.clear();
                            ensure_prefix(
                                &mut current,
                                base_style,
                                in_code_block,
                                quote_depth,
                                in_item,
                                &item_prefix,
                                &item_cont_prefix,
                            );
                            push_text(&mut current, &line_text, base_style.fg(Color::Gray));
                            flush_line(&mut out, &mut current);
                        }
                        push_empty_line(&mut out, base_style);
                    }
                }
                _ => {}
            },
            Event::Text(t) => {
                if let Some(tb) = table.as_mut() {
                    tb.push_text(&t);
                    continue;
                }
                if in_code_block {
                    for (i, line) in split_lines_preserve_empty(&t).into_iter().enumerate() {
                        if i > 0 {
                            flush_line(&mut out, &mut current);
                        }
                        let head = vec![
                            StyledChunk {
                                text: "┆ ".to_string(),
                                style: base_style.fg(Color::DarkGray),
                            },
                            StyledChunk {
                                text: line,
                                style: base_style.fg(Color::LightGreen),
                            },
                        ];
                        out.push(StyledLogicalLine {
                            head,
                            cont_prefix: vec![StyledChunk {
                                text: "┆ ".to_string(),
                                style: base_style.fg(Color::DarkGray),
                            }],
                        });
                        current.head.clear();
                        current.cont_prefix.clear();
                    }
                } else {
                    ensure_prefix(
                        &mut current,
                        base_style,
                        in_code_block,
                        quote_depth,
                        in_item,
                        &item_prefix,
                        &item_cont_prefix,
                    );
                    let style = inline_style(base_style, &inline, heading_level);
                    push_text(&mut current, t.as_ref(), style);
                }
            }
            Event::Code(code) => {
                if let Some(tb) = table.as_mut() {
                    tb.push_code(&code);
                    continue;
                }
                ensure_prefix(
                    &mut current,
                    base_style,
                    in_code_block,
                    quote_depth,
                    in_item,
                    &item_prefix,
                    &item_cont_prefix,
                );
                let style = base_style
                    .fg(Color::Yellow)
                    .bg(Color::Black)
                    .add_modifier(Modifier::BOLD);
                push_text(&mut current, code.as_ref(), style);
            }
            Event::SoftBreak => {
                if let Some(tb) = table.as_mut() {
                    tb.push_soft_break();
                    continue;
                }
                ensure_prefix(
                    &mut current,
                    base_style,
                    in_code_block,
                    quote_depth,
                    in_item,
                    &item_prefix,
                    &item_cont_prefix,
                );
                push_text(&mut current, " ", base_style);
            }
            Event::HardBreak => {
                if let Some(tb) = table.as_mut() {
                    tb.push_soft_break();
                    continue;
                }
                ensure_prefix(
                    &mut current,
                    base_style,
                    in_code_block,
                    quote_depth,
                    in_item,
                    &item_prefix,
                    &item_cont_prefix,
                );
                push_text(&mut current, "\n", base_style);
            }
            Event::Rule => {
                flush_line(&mut out, &mut current);
                out.push(StyledLogicalLine {
                    head: vec![StyledChunk {
                        text: "────────────────────────".to_string(),
                        style: base_style.fg(Color::DarkGray),
                    }],
                    cont_prefix: vec![],
                });
                push_empty_line(&mut out, base_style);
            }
            Event::Html(html) => {
                if let Some(tb) = table.as_mut() {
                    tb.push_html(&html);
                    continue;
                }
                // 终端不渲染 HTML，降级为原文（淡色）。
                ensure_prefix(
                    &mut current,
                    base_style,
                    in_code_block,
                    quote_depth,
                    in_item,
                    &item_prefix,
                    &item_cont_prefix,
                );
                push_text(&mut current, html.as_ref(), base_style.fg(Color::DarkGray));
            }
            Event::InlineHtml(html) => {
                if let Some(tb) = table.as_mut() {
                    tb.push_html(&html);
                    continue;
                }
                ensure_prefix(
                    &mut current,
                    base_style,
                    in_code_block,
                    quote_depth,
                    in_item,
                    &item_prefix,
                    &item_cont_prefix,
                );
                push_text(&mut current, html.as_ref(), base_style.fg(Color::DarkGray));
            }
            Event::FootnoteReference(name) => {
                if let Some(tb) = table.as_mut() {
                    tb.push_footnote(&name);
                    continue;
                }
                ensure_prefix(
                    &mut current,
                    base_style,
                    in_code_block,
                    quote_depth,
                    in_item,
                    &item_prefix,
                    &item_cont_prefix,
                );
                push_text(
                    &mut current,
                    &format!("[^{name}]"),
                    base_style
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::UNDERLINED),
                );
            }
            Event::TaskListMarker(checked) => {
                if let Some(tb) = table.as_mut() {
                    tb.push_task_marker(checked);
                    continue;
                }
                ensure_prefix(
                    &mut current,
                    base_style,
                    in_code_block,
                    quote_depth,
                    in_item,
                    &item_prefix,
                    &item_cont_prefix,
                );
                let mark = if checked { "[x] " } else { "[ ] " };
                push_text(&mut current, mark, base_style.fg(Color::LightMagenta));
            }
            Event::InlineMath(m) | Event::DisplayMath(m) => {
                if let Some(tb) = table.as_mut() {
                    tb.push_text(&m);
                    continue;
                }
                ensure_prefix(
                    &mut current,
                    base_style,
                    in_code_block,
                    quote_depth,
                    in_item,
                    &item_prefix,
                    &item_cont_prefix,
                );
                let style = base_style.fg(Color::Cyan).add_modifier(Modifier::ITALIC);
                push_text(&mut current, m.as_ref(), style);
            }
        }
    }

    flush_line(&mut out, &mut current);

    // 收尾：去掉末尾多余空行
    while out.last().is_some_and(is_empty_visual_line) {
        out.pop();
    }

    // 链接：把 href 追加为 (url)
    // （pulldown-cmark 事件不直接给文本与链接的绑定，这里只做最简：在遇到 Link 标签时不处理，保持可读性）
    out
}

struct TableState {
    aligns: Vec<Alignment>,
    rows: Vec<(bool, Vec<String>)>,
    cur_row: Vec<String>,
    cur_cell: String,
    in_head: bool,
    in_row: bool,
    in_cell: bool,
}

impl TableState {
    fn new(aligns: Vec<Alignment>) -> Self {
        Self {
            aligns,
            rows: Vec::new(),
            cur_row: Vec::new(),
            cur_cell: String::new(),
            in_head: false,
            in_row: false,
            in_cell: false,
        }
    }

    fn start_row(&mut self) {
        self.cur_row.clear();
        self.in_row = true;
    }

    fn end_row(&mut self) {
        if !self.in_row {
            return;
        }
        // 若最后一个 cell 没闭合，强制收尾。
        if self.in_cell {
            self.end_cell();
        }
        self.rows.push((self.in_head, self.cur_row.clone()));
        self.cur_row.clear();
        self.in_row = false;
    }

    fn start_cell(&mut self) {
        self.cur_cell.clear();
        self.in_cell = true;
    }

    fn end_cell(&mut self) {
        if !self.in_cell {
            return;
        }
        self.cur_row.push(self.cur_cell.trim().to_string());
        self.cur_cell.clear();
        self.in_cell = false;
    }

    fn push_text(&mut self, t: &CowStr<'_>) {
        self.cur_cell.push_str(t.as_ref());
    }

    fn push_code(&mut self, code: &CowStr<'_>) {
        self.cur_cell.push_str(code.as_ref());
    }

    fn push_soft_break(&mut self) {
        if !self.cur_cell.ends_with(' ') {
            self.cur_cell.push(' ');
        }
    }

    fn push_html(&mut self, html: &CowStr<'_>) {
        self.cur_cell.push_str(html.as_ref());
    }

    fn push_footnote(&mut self, name: &CowStr<'_>) {
        self.cur_cell.push_str(&format!("[^{name}]"));
    }

    fn push_task_marker(&mut self, checked: bool) {
        if checked {
            self.cur_cell.push_str("[x] ");
        } else {
            self.cur_cell.push_str("[ ] ");
        }
    }

    fn render(mut self) -> Vec<String> {
        // 收尾：若表格结束时仍在 row/cell，补齐。
        if self.in_cell {
            self.end_cell();
        }
        if self.in_row {
            self.end_row();
        }
        if self.rows.is_empty() {
            return vec![];
        }

        let cols = self.rows.iter().map(|(_, r)| r.len()).max().unwrap_or(0);
        let mut widths = vec![0usize; cols];
        for (_, row) in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                widths[i] = widths[i].max(text_width(cell));
            }
        }

        let mut out = Vec::new();
        let mut header_done = false;
        for (is_head, row) in &self.rows {
            out.push(render_table_row(row, &widths, &self.aligns));
            if *is_head && !header_done {
                out.push(render_table_sep(&widths));
                header_done = true;
            }
        }
        out
    }
}

fn render_table_row(row: &[String], widths: &[usize], aligns: &[Alignment]) -> String {
    let mut out = String::new();
    out.push('|');
    for (i, w) in widths.iter().enumerate() {
        out.push(' ');
        let cell = row.get(i).map(|s| s.as_str()).unwrap_or("");
        let aligned = align_text(cell, *w, aligns.get(i).copied().unwrap_or(Alignment::Left));
        out.push_str(&aligned);
        out.push(' ');
        out.push('|');
    }
    out
}

fn render_table_sep(widths: &[usize]) -> String {
    let mut out = String::new();
    out.push('|');
    for w in widths {
        out.push_str(&"-".repeat(w.saturating_add(2)));
        out.push('|');
    }
    out
}

fn align_text(s: &str, width: usize, align: Alignment) -> String {
    let w = text_width(s);
    if w >= width {
        return s.to_string();
    }
    let pad = width - w;
    match align {
        Alignment::Right => format!("{}{}", " ".repeat(pad), s),
        Alignment::Center => {
            let left = pad / 2;
            let right = pad - left;
            format!("{}{}{}", " ".repeat(left), s, " ".repeat(right))
        }
        _ => format!("{}{}", s, " ".repeat(pad)),
    }
}

fn text_width(s: &str) -> usize {
    s.chars()
        .map(|ch| UnicodeWidthChar::width(ch).unwrap_or(1).max(1))
        .sum()
}

fn inline_style(base: Style, inline: &InlineStyle, heading_level: Option<u8>) -> Style {
    let mut style = base;
    if inline.emph > 0 {
        style = style.add_modifier(Modifier::ITALIC);
    }
    if inline.strong > 0 {
        style = style.add_modifier(Modifier::BOLD);
    }
    if inline.strike > 0 {
        style = style.add_modifier(Modifier::CROSSED_OUT);
    }
    if inline.link_href.is_some() {
        style = style.fg(Color::Cyan).add_modifier(Modifier::UNDERLINED);
    }
    if let Some(level) = heading_level {
        style = style.add_modifier(Modifier::BOLD);
        style = match level {
            1 => style.fg(Color::Magenta),
            2 => style.fg(Color::LightMagenta),
            3 => style.fg(Color::Cyan),
            _ => style.fg(Color::Gray),
        };
    }
    style
}

fn wrap_styled_line(line: &StyledLogicalLine, width: usize) -> Vec<Line<'static>> {
    let width = width.max(1);

    // “空行”仍要占 1 行，避免 Markdown 段落断层丢失。
    if line.head.len() == 1 && line.head[0].text.is_empty() {
        return vec![Line::from(Span::styled(String::new(), line.head[0].style))];
    }

    let cont_prefix = line.cont_prefix.clone();
    let cont_prefix_w = styled_chunks_width(&cont_prefix);

    let mut out_chunks: Vec<Vec<StyledChunk>> = Vec::new();
    let mut cur: Vec<StyledChunk> = Vec::new();
    let mut cur_w: usize = 0;
    let mut is_cont: bool = false;

    let start_line =
        |cur: &mut Vec<StyledChunk>, cur_w: &mut usize, is_cont_ref: &mut bool, cont: bool| {
            cur.clear();
            *cur_w = 0;
            *is_cont_ref = cont;
            if cont && !cont_prefix.is_empty() {
                cur.extend(cont_prefix.clone());
                *cur_w = cont_prefix_w.min(width);
            }
        };

    let flush_line = |out: &mut Vec<Vec<StyledChunk>>,
                      cur: &mut Vec<StyledChunk>,
                      cur_w: &mut usize,
                      is_cont_ref: &mut bool| {
        out.push(cur.clone());
        start_line(cur, cur_w, is_cont_ref, true);
    };

    start_line(&mut cur, &mut cur_w, &mut is_cont, false);

    for chunk in &line.head {
        let style = chunk.style;
        let mut buf = String::new();
        let mut buf_w: usize = 0;

        for ch in chunk.text.chars() {
            if ch == '\n' {
                if !buf.is_empty() {
                    push_chunk_merged(&mut cur, &buf, style);
                    cur_w = cur_w.saturating_add(buf_w);
                    buf.clear();
                    buf_w = 0;
                }
                out_chunks.push(cur.clone());
                start_line(&mut cur, &mut cur_w, &mut is_cont, true);
                continue;
            }

            let cw = UnicodeWidthChar::width(ch).unwrap_or(1).max(1);
            if cur_w + buf_w + cw > width && cur_w + buf_w > if is_cont { cont_prefix_w } else { 0 }
            {
                if !buf.is_empty() {
                    push_chunk_merged(&mut cur, &buf, style);
                    cur_w = cur_w.saturating_add(buf_w);
                    buf.clear();
                    buf_w = 0;
                }
                flush_line(&mut out_chunks, &mut cur, &mut cur_w, &mut is_cont);
            }

            buf.push(ch);
            buf_w = buf_w.saturating_add(cw);
        }

        if !buf.is_empty() {
            push_chunk_merged(&mut cur, &buf, style);
            cur_w = cur_w.saturating_add(buf_w);
        }
    }

    if !cur.is_empty() {
        out_chunks.push(cur);
    }

    out_chunks
        .into_iter()
        .map(|chunks| {
            let spans = chunks
                .into_iter()
                .map(|c| Span::styled(c.text, c.style))
                .collect::<Vec<_>>();
            Line::from(spans)
        })
        .collect()
}

fn push_chunk_merged(dst: &mut Vec<StyledChunk>, text: &str, style: Style) {
    if text.is_empty() {
        return;
    }
    if let Some(last) = dst.last_mut() && last.style == style {
        last.text.push_str(text);
        return;
    }
    dst.push(StyledChunk {
        text: text.to_string(),
        style,
    });
}

fn styled_chunks_width(chunks: &[StyledChunk]) -> usize {
    let mut w = 0usize;
    for c in chunks {
        for ch in c.text.chars() {
            if ch == '\n' {
                continue;
            }
            w = w.saturating_add(UnicodeWidthChar::width(ch).unwrap_or(1).max(1));
        }
    }
    w
}

fn split_lines_preserve_empty(s: &CowStr<'_>) -> Vec<String> {
    let raw = s.as_ref();
    if raw.is_empty() {
        return vec![String::new()];
    }
    raw.split('\n').map(|x| x.to_string()).collect()
}

fn is_empty_visual_line(line: &StyledLogicalLine) -> bool {
    if line.head.len() != 1 {
        return false;
    }
    line.head[0].text.trim().is_empty()
}
