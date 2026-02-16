//! TUI dashboard for monitoring Rill pipeline execution.
//!
//! Uses ratatui + crossterm for terminal rendering. Consumes metrics and logs
//! from the decoupled data layer in `rill-runtime` (no UI deps there).

mod dashboard;
mod graph_view;
mod log_viewer;

use std::collections::VecDeque;
use std::io;
use std::time::Duration;

use crossterm::event::{Event, EventStream, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures::StreamExt;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};

use rill_core::graph::LogicalPlan;
use rill_runtime::metrics_snapshot::MetricsSnapshot;
use rill_runtime::tracing_capture::LogEntry;

use self::dashboard::render_dashboard;
use self::graph_view::render_graph;
use self::log_viewer::render_log_viewer;

const MAX_LOG_ENTRIES: usize = 500;

/// The TUI application state.
#[derive(Debug)]
pub struct TuiApp {
    metrics_rx: tokio::sync::watch::Receiver<MetricsSnapshot>,
    log_rx: tokio::sync::mpsc::Receiver<LogEntry>,
    logs: VecDeque<LogEntry>,
    snapshot: MetricsSnapshot,
    graph: Option<LogicalPlan>,
    log_scroll: usize,
    auto_scroll: bool,
    quit: bool,
    workers: usize,
}

impl TuiApp {
    /// Create a new TUI app from telemetry handles.
    pub fn new(
        metrics_rx: tokio::sync::watch::Receiver<MetricsSnapshot>,
        log_rx: tokio::sync::mpsc::Receiver<LogEntry>,
        graph: Option<LogicalPlan>,
        workers: usize,
    ) -> Self {
        Self {
            metrics_rx,
            log_rx,
            logs: VecDeque::with_capacity(MAX_LOG_ENTRIES),
            snapshot: MetricsSnapshot::default(),
            graph,
            log_scroll: 0,
            auto_scroll: true,
            quit: false,
            workers,
        }
    }

    /// Run the TUI event loop. Returns when the user presses 'q' or Ctrl-C.
    pub async fn run(mut self) -> anyhow::Result<()> {
        // Set up terminal
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        // Install panic hook to restore terminal on panic
        let original_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let _ = disable_raw_mode();
            let _ = execute!(io::stdout(), LeaveAlternateScreen);
            original_hook(info);
        }));

        let mut event_stream = EventStream::new();
        let mut tick = tokio::time::interval(Duration::from_millis(250));

        // Initial render
        terminal.draw(|frame| self.render(frame))?;

        loop {
            if self.quit {
                break;
            }

            tokio::select! {
                // Keyboard / mouse events
                maybe_event = event_stream.next() => {
                    if let Some(Ok(event)) = maybe_event {
                        self.handle_event(event);
                    }
                }
                // New metrics snapshot
                Ok(()) = self.metrics_rx.changed() => {
                    self.snapshot = self.metrics_rx.borrow_and_update().clone();
                    self.snapshot.workers = self.workers;
                }
                // New log entry
                Some(entry) = self.log_rx.recv() => {
                    self.push_log(entry);
                }
                // Periodic re-render
                _ = tick.tick() => {}
            }

            terminal.draw(|frame| self.render(frame))?;
        }

        // Restore terminal
        disable_raw_mode()?;
        execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
        terminal.show_cursor()?;

        Ok(())
    }

    fn handle_event(&mut self, event: Event) {
        if let Event::Key(key) = event {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => self.quit = true,
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    self.quit = true;
                }
                KeyCode::Up => {
                    self.auto_scroll = false;
                    self.log_scroll = self.log_scroll.saturating_add(1);
                }
                KeyCode::Down => {
                    if self.log_scroll > 0 {
                        self.log_scroll -= 1;
                    } else {
                        self.auto_scroll = true;
                    }
                }
                _ => {}
            }
        }
    }

    fn push_log(&mut self, entry: LogEntry) {
        if self.logs.len() >= MAX_LOG_ENTRIES {
            self.logs.pop_front();
        }
        self.logs.push_back(entry);
        if self.auto_scroll {
            self.log_scroll = 0;
        }
    }

    fn render(&self, frame: &mut ratatui::Frame<'_>) {
        if let Some(plan) = &self.graph {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(3),  // Graph
                    Constraint::Length(10), // Dashboard
                    Constraint::Min(5),     // Log viewer
                ])
                .split(frame.area());

            render_graph(frame, chunks[0], plan);
            render_dashboard(frame, chunks[1], &self.snapshot);
            render_log_viewer(frame, chunks[2], &self.logs, self.log_scroll);
        } else {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(10), // Dashboard
                    Constraint::Min(5),     // Log viewer
                ])
                .split(frame.area());

            render_dashboard(frame, chunks[0], &self.snapshot);
            render_log_viewer(frame, chunks[1], &self.logs, self.log_scroll);
        }
    }
}
