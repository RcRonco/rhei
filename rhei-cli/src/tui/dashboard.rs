//! Metrics dashboard rendering.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use rhei_runtime::metrics_snapshot::MetricsSnapshot;

/// Format a `Duration` as `HH:MM:SS`.
fn format_uptime(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

/// Format a number with thousands separators.
fn fmt_num(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// Render the metrics dashboard into the given area.
#[allow(clippy::too_many_lines)]
pub fn render_dashboard(frame: &mut Frame<'_>, area: Rect, snap: &MetricsSnapshot) {
    let block = Block::default()
        .title(Line::from(vec![
            Span::styled(
                " Rhei Pipeline ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("─── "),
            Span::styled("[q]", Style::default().fg(Color::DarkGray)),
            Span::styled("uit ", Style::default().fg(Color::DarkGray)),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Status line
            Constraint::Length(1), // Separator
            Constraint::Length(1), // Throughput
            Constraint::Length(1), // Elements / Batches
            Constraint::Length(1), // Separator
            Constraint::Length(1), // Cache / Latency
            Constraint::Length(1), // State / Checkpoint
            Constraint::Length(1), // Backpressure
        ])
        .split(inner);

    // Row 0: Status + Workers + Uptime
    let workers_str = if snap.workers > 0 {
        format!("{}", snap.workers)
    } else {
        "—".to_string()
    };
    let status_line = Line::from(vec![
        Span::styled(" Status: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            "Running",
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("    "),
        Span::styled("Workers: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            workers_str,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("    "),
        Span::styled("Uptime: ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format_uptime(snap.uptime),
            Style::default().fg(Color::White),
        ),
    ]);
    frame.render_widget(Paragraph::new(status_line), rows[0]);

    // Row 1: separator
    let sep = Line::from(Span::styled(
        " ─".to_string() + &"─".repeat(inner.width.saturating_sub(3) as usize),
        Style::default().fg(Color::DarkGray),
    ));
    frame.render_widget(Paragraph::new(sep.clone()), rows[1]);

    // Row 2: Throughput
    let throughput = Line::from(vec![
        Span::styled(" Throughput   ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.0} elem/s", snap.elements_per_second),
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    ]);
    frame.render_widget(Paragraph::new(throughput), rows[2]);

    // Row 3: Elements / Batches
    let counts = Line::from(vec![
        Span::styled(" Elements  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            fmt_num(snap.elements_total),
            Style::default().fg(Color::White),
        ),
        Span::raw("      "),
        Span::styled("Batches  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            fmt_num(snap.batches_total),
            Style::default().fg(Color::White),
        ),
    ]);
    frame.render_widget(Paragraph::new(counts), rows[3]);

    // Row 4: separator
    frame.render_widget(Paragraph::new(sep), rows[4]);

    // Row 5-6: Cache + Latency in two columns
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[5]);

    let cache_row1 = Line::from(vec![
        Span::styled(" L1 Hit ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.0}%", snap.l1_hit_rate),
            Style::default().fg(Color::Green),
        ),
        Span::raw("  "),
        Span::styled("L2 Hit ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.0}%", snap.l2_hit_rate),
            Style::default().fg(Color::Yellow),
        ),
        Span::raw("  "),
        Span::styled("L3 Hit ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.0}%", snap.l3_hit_rate),
            Style::default().fg(Color::Red),
        ),
    ]);
    frame.render_widget(Paragraph::new(cache_row1), cols[0]);

    let latency_row1 = Line::from(vec![
        Span::styled("p50 ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.2}ms", snap.element_duration_p50 * 1000.0),
            Style::default().fg(Color::White),
        ),
        Span::raw("  "),
        Span::styled("p99 ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.2}ms", snap.element_duration_p99 * 1000.0),
            Style::default().fg(Color::White),
        ),
    ]);
    frame.render_widget(Paragraph::new(latency_row1), cols[1]);

    // Row 6: State / Checkpoint
    let cols2 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[6]);

    let state_ops = Line::from(vec![
        Span::styled(" State  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!(
                "get={} put={}",
                fmt_num(snap.state_get_total),
                fmt_num(snap.state_put_total)
            ),
            Style::default().fg(Color::White),
        ),
    ]);
    frame.render_widget(Paragraph::new(state_ops), cols2[0]);

    let ckpt = Line::from(vec![
        Span::styled("Checkpoint ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.2}s", snap.checkpoint_duration_secs),
            Style::default().fg(Color::White),
        ),
    ]);
    frame.render_widget(Paragraph::new(ckpt), cols2[1]);

    // Row 7: Backpressure
    let backpressure = Line::from(vec![
        Span::styled(" Stash  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.0}", snap.stash_depth),
            Style::default().fg(if snap.stash_depth > 0.0 {
                Color::Yellow
            } else {
                Color::White
            }),
        ),
        Span::raw("      "),
        Span::styled("Pending  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            format!("{:.0}", snap.pending_futures),
            Style::default().fg(if snap.pending_futures > 0.0 {
                Color::Yellow
            } else {
                Color::White
            }),
        ),
    ]);
    frame.render_widget(Paragraph::new(backpressure), rows[7]);
}
