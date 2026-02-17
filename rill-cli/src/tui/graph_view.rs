//! Pipeline graph visualization panel.

use ratatui::Frame;
use ratatui::layout::{Alignment, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph};

use rill_core::graph::{LogicalPlan, NodeKind};

/// Returns the display color for a given node kind.
fn node_color(kind: &NodeKind) -> Color {
    match kind {
        NodeKind::Source(_) => Color::Green,
        NodeKind::Map(_) | NodeKind::Operator(_) => Color::Cyan,
        NodeKind::Filter(_) => Color::Yellow,
        NodeKind::KeyBy(_) => Color::Blue,
        NodeKind::Sink(_) => Color::Magenta,
    }
}

/// Render the pipeline graph into the given area.
pub fn render_graph(frame: &mut Frame<'_>, area: Rect, plan: &LogicalPlan) {
    let block = Block::default()
        .title(Span::styled(
            " Pipeline ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray));

    let order = plan.topological_order();

    let mut spans: Vec<Span<'_>> = Vec::new();
    let mut prev_is_exchange = false;
    for (i, &idx) in order.iter().enumerate() {
        let node = &plan.nodes[idx];
        let color = node_color(&node.kind);
        let name = node.kind.name().to_string();
        let is_exchange = matches!(node.kind, NodeKind::KeyBy(_));

        // Draw connector between nodes.
        if i > 0 {
            let exchange_style = Style::default().fg(Color::Blue);
            let normal_style = Style::default().fg(Color::DarkGray);
            if is_exchange {
                // Dashed line leading into exchange
                spans.push(Span::styled(" \u{254C}\u{254C} ", exchange_style));
            } else if prev_is_exchange {
                // Dashed line leading out of exchange
                spans.push(Span::styled(" \u{254C}\u{254C}\u{25B6} ", exchange_style));
            } else {
                spans.push(Span::styled(" \u{2500}\u{2500}\u{25B6} ", normal_style));
            }
        }

        // Draw the node itself.
        if is_exchange {
            let style = Style::default().fg(color).add_modifier(Modifier::BOLD);
            spans.push(Span::styled("\u{25C6} ", style));
            spans.push(Span::styled(name, style));
            spans.push(Span::styled(" \u{25C6}", style));
        } else {
            spans.push(Span::styled(
                format!("[{name}]"),
                Style::default().fg(color).add_modifier(Modifier::BOLD),
            ));
        }

        prev_is_exchange = is_exchange;
    }

    let line = Line::from(spans);
    let paragraph = Paragraph::new(line)
        .alignment(Alignment::Center)
        .block(block);
    frame.render_widget(paragraph, area);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_colors() {
        assert_eq!(node_color(&NodeKind::Source("s".into())), Color::Green);
        assert_eq!(node_color(&NodeKind::Map("m".into())), Color::Cyan);
        assert_eq!(node_color(&NodeKind::Operator("o".into())), Color::Cyan);
        assert_eq!(node_color(&NodeKind::Filter("f".into())), Color::Yellow);
        assert_eq!(node_color(&NodeKind::KeyBy("k".into())), Color::Blue);
        assert_eq!(node_color(&NodeKind::Sink("sk".into())), Color::Magenta);
    }
}
