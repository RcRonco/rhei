//! Scrollable log viewer panel.

use std::collections::VecDeque;

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, List, ListItem};
use tracing::Level;

use rhei_runtime::tracing_capture::LogEntry;

fn level_color(level: Level) -> Color {
    match level {
        Level::ERROR => Color::Red,
        Level::WARN => Color::Yellow,
        Level::INFO => Color::Green,
        Level::DEBUG => Color::Blue,
        Level::TRACE => Color::DarkGray,
    }
}

fn format_time(ts: std::time::SystemTime) -> String {
    let since_epoch = ts.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let total_secs = since_epoch.as_secs();
    let hours = (total_secs / 3600) % 24;
    let minutes = (total_secs / 60) % 60;
    let seconds = total_secs % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

/// Render the log viewer into the given area.
pub fn render_log_viewer(
    frame: &mut Frame<'_>,
    area: Rect,
    logs: &VecDeque<LogEntry>,
    scroll_offset: usize,
) {
    let block = Block::default()
        .title(Line::from(vec![
            Span::styled(
                " Logs ",
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled("[", Style::default().fg(Color::DarkGray)),
            Span::styled("\u{2191}\u{2193}", Style::default().fg(Color::White)),
            Span::styled("] ", Style::default().fg(Color::DarkGray)),
        ]))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let inner_height = block.inner(area).height as usize;

    let items: Vec<ListItem<'_>> = logs
        .iter()
        .rev()
        .skip(scroll_offset)
        .take(inner_height)
        .map(|entry| {
            let time = format_time(entry.timestamp);
            let level_str = format!("{:5}", entry.level);
            let color = level_color(entry.level);

            let mut spans = vec![
                Span::styled(time, Style::default().fg(Color::DarkGray)),
                Span::raw(" "),
                Span::styled(
                    level_str,
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
                Span::raw(" "),
            ];
            if let Some(w) = entry.worker {
                spans.push(Span::styled(
                    format!("Worker={w} "),
                    Style::default().fg(Color::Magenta),
                ));
            }
            spans.push(Span::styled(
                &entry.message,
                Style::default().fg(Color::White),
            ));

            ListItem::new(Line::from(spans))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();

    let list = List::new(items).block(block);
    frame.render_widget(list, area);
}
