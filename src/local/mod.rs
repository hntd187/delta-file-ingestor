use std::path::PathBuf;

use anyhow::Result;
use notify::event::CreateKind;
use notify::RecursiveMode::Recursive;
use notify::{Config, Event, EventHandler, EventKind, RecommendedWatcher, Watcher};
use object_store::path::Path;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::FileEvents;

struct EventCallback {
    sender: Sender<Event>,
}

impl EventHandler for EventCallback {
    fn handle_event(&mut self, event: notify::Result<Event>) {
        if let Ok(evt) = event {
            match evt.kind {
                EventKind::Create(CreateKind::File) => {
                    self.sender.blocking_send(evt).expect("Don't error plz :'(");
                }
                _ => {}
            }
        }
    }
}

pub struct LocalFileEvents {
    events: Receiver<Event>,
    watcher: RecommendedWatcher,
}

impl LocalFileEvents {
    pub fn new(location: PathBuf) -> Result<Self> {
        let (sender, events) = channel::<Event>(100);
        let callback = EventCallback { sender };
        let mut watcher = RecommendedWatcher::new(callback, Config::default())?;
        watcher.watch(location.as_path(), Recursive)?;
        Ok(Self { events, watcher })
    }
}

impl FileEvents for LocalFileEvents {
    async fn next_file(&mut self) -> Result<Vec<Path>> {
        if let Some(evt) = self.events.recv().await {
            Ok(evt
                .paths
                .into_iter()
                .flat_map(Path::from_filesystem_path)
                .collect::<Vec<_>>())
        } else {
            Ok(vec![])
        }
    }
}
