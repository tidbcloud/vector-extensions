use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use std::{fs, io};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vector_lib::event::Event;

const TMP_FILE_NAME: &str = "checkpoints.new.json";
const CHECKPOINT_FILE_NAME: &str = "checkpoints.json";

pub struct Checkpointer {
    tmp_file_path: PathBuf,
    stable_file_path: PathBuf,
    checkpoints: CheckPointsView,
    last: State,
}

impl Checkpointer {
    pub fn new(data_dir: PathBuf) -> Checkpointer {
        let tmp_file_path = data_dir.join(TMP_FILE_NAME);
        let stable_file_path = data_dir.join(CHECKPOINT_FILE_NAME);
        Checkpointer {
            tmp_file_path,
            stable_file_path,
            checkpoints: CheckPointsView::default(),
            last: State::V1 {
                checkpoints: BTreeSet::default(),
            },
        }
    }

    pub fn contains(&self, key: &UploadKey, upload_time_after: SystemTime) -> bool {
        self.checkpoints.contains(key, upload_time_after)
    }

    pub fn update(&mut self, key: UploadKey, upload_time: SystemTime, expire_after: Duration) {
        self.checkpoints
            .update(key, upload_time.into(), (upload_time + expire_after).into());
    }

    /// Read persisted checkpoints from disk, preferring the new JSON file format.
    pub fn read_checkpoints(&mut self) {
        // First try reading from the tmp file location. If this works, it means
        // that the previous process was interrupted in the process of
        // checkpointing and the tmp file should contain more recent data that
        // should be preferred.
        match self.read_checkpoints_file(&self.tmp_file_path) {
            Ok(state) => {
                warn!(message = "Recovered checkpoint data from interrupted process.");
                self.checkpoints.set_state(&state);
                self.last = state;

                // Try to move this tmp file to the stable location so we don't
                // immediately overwrite it when we next persist checkpoints.
                if let Err(error) = fs::rename(&self.tmp_file_path, &self.stable_file_path) {
                    warn!(message = "Error persisting recovered checkpoint file.", %error);
                }
                return;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // This is expected, so no warning needed
            }
            Err(error) => {
                error!(message = "Unable to recover checkpoint data from interrupted process.", %error);
            }
        }

        // Next, attempt to read checkpoints from the stable file location. This
        // is the expected location, so warn more aggressively if something goes
        // wrong.
        match self.read_checkpoints_file(&self.stable_file_path) {
            Ok(state) => {
                info!(message = "Loaded checkpoint data.");
                self.checkpoints.set_state(&state);
                self.last = state;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // This is expected, so no warning needed
            }
            Err(error) => {
                warn!(message = "Unable to load checkpoint data.", %error);
            }
        }
    }

    /// Persist the current checkpoints state to disk, making our best effort to
    /// do so in an atomic way that allow for recovering the previous state in
    /// the event of a crash.
    pub fn write_checkpoints(&mut self) -> Result<usize, io::Error> {
        self.checkpoints.remove_expired();
        let state = self.checkpoints.get_state();

        if self.last == state {
            return Ok(self.checkpoints.len());
        }

        // Write the new checkpoints to a tmp file and flush it fully to
        // disk. If vector dies anywhere during this section, the existing
        // stable file will still be in its current valid state and we'll be
        // able to recover.
        let mut f = io::BufWriter::new(fs::File::create(&self.tmp_file_path)?);
        serde_json::to_writer(&mut f, &state)?;
        f.into_inner()?.sync_all()?;

        // Once the temp file is fully flushed, rename the tmp file to replace
        // the previous stable file. This is an atomic operation on POSIX
        // systems (and the stdlib claims to provide equivalent behavior on
        // Windows), which should prevent scenarios where we don't have at least
        // one full valid file to recover from.
        fs::rename(&self.tmp_file_path, &self.stable_file_path)?;

        self.last = state;
        Ok(self.checkpoints.len())
    }

    fn read_checkpoints_file(&self, path: &Path) -> Result<State, io::Error> {
        let reader = io::BufReader::new(fs::File::open(path)?);
        serde_json::from_reader(reader).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub struct UploadKey {
    pub filename: String,
    pub bucket: String,
    pub object_key: String,
}

impl UploadKey {
    pub fn from_event(event: &Event, bucket: &str) -> Option<Self> {
        let log = event.maybe_as_log()?;
        let filename_val = log.get("message")?;
        let filename = String::from_utf8_lossy(filename_val.as_bytes()?);

        let object_key_val = log.get("key")?;
        let object_key = String::from_utf8_lossy(object_key_val.as_bytes()?);

        Some(UploadKey {
            bucket: bucket.to_owned(),
            object_key: object_key.to_string(),
            filename: filename.to_string(),
        })
    }
}

#[derive(Default)]
struct CheckPointsView {
    upload_times: HashMap<UploadKey, DateTime<Utc>>,
    expire_times: HashMap<UploadKey, DateTime<Utc>>,
}

impl CheckPointsView {
    pub fn get_state(&self) -> State {
        State::V1 {
            checkpoints: self
                .expire_times
                .iter()
                .map(|(key, time)| Checkpoint {
                    upload_key: key.clone(),
                    expire_at: *time,
                    upload_at: self.upload_times.get(key).copied().unwrap_or_else(Utc::now),
                })
                .collect(),
        }
    }

    pub fn set_state(&mut self, state: &State) {
        match state {
            State::V1 { checkpoints } => {
                for checkpoint in checkpoints {
                    self.expire_times
                        .insert(checkpoint.upload_key.clone(), checkpoint.expire_at);
                    self.upload_times
                        .insert(checkpoint.upload_key.clone(), checkpoint.upload_at);
                }
            }
        }
    }

    pub fn contains(&self, key: &UploadKey, upload_time_after: SystemTime) -> bool {
        let upload_time_after = DateTime::<Utc>::from(upload_time_after);
        self.upload_times
            .get(key)
            .map(|time| time >= &upload_time_after)
            .unwrap_or_default()
    }

    pub fn update(&mut self, key: UploadKey, upload_at: DateTime<Utc>, expire_at: DateTime<Utc>) {
        self.upload_times.insert(key.clone(), upload_at);
        self.expire_times.insert(key, expire_at);
    }

    pub fn remove_expired(&mut self) {
        let now = Utc::now();
        let mut expired = Vec::new();
        for (key, expire_time) in &self.expire_times {
            if expire_time < &now {
                expired.push(key.clone());
            }
        }
        for key in expired {
            self.upload_times.remove(&key);
            self.expire_times.remove(&key);
        }
    }

    pub fn len(&self) -> usize {
        self.upload_times.len()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "version", rename_all = "snake_case")]
enum State {
    #[serde(rename = "1")]
    V1 { checkpoints: BTreeSet<Checkpoint> },
}

/// A simple JSON-friendly struct of the fingerprint/position pair, since
/// fingerprints as objects cannot be keys in a plain JSON map.
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
#[serde(rename_all = "snake_case")]
struct Checkpoint {
    upload_key: UploadKey,
    upload_at: DateTime<Utc>,
    expire_at: DateTime<Utc>,
}
