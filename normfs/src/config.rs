use globset::{Glob, GlobMatcher};
use normfs_types::{CompressionType, EncryptionType};

/// Which page arena a queue draws from.
///
/// The pool decides the queue's page size, and with it two things: the 2-page
/// floor an idle queue holds forever, and the widest record the queue accepts.
/// Passive is the default so a queue nobody thought about costs a tiny floor;
/// the queues that need wide records are the ones somebody names in a rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PoolKind {
    Active,
    #[default]
    Passive,
}

#[derive(Debug, Clone, Copy)]
pub struct QueueConfig {
    pub compression_type: CompressionType,
    pub enable_fsync: bool,
    pub encryption_type: EncryptionType,
    pub pool: PoolKind,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            compression_type: CompressionType::Zstd,
            enable_fsync: true,
            encryption_type: EncryptionType::Aes,
            pool: PoolKind::default(),
        }
    }
}

impl QueueConfig {
    pub fn active() -> Self {
        Self {
            pool: PoolKind::Active,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueueSettings {
    rules: Vec<(GlobMatcher, QueueConfig)>,
    pub default_config: QueueConfig,
}

impl QueueSettings {
    pub fn new(
        patterns: Vec<(String, QueueConfig)>,
        default_config: QueueConfig,
    ) -> Result<Self, globset::Error> {
        let rules = patterns
            .into_iter()
            .map(|(pat, config)| {
                let glob = Glob::new(&pat)?;
                Ok((glob.compile_matcher(), config))
            })
            .collect::<Result<Vec<_>, globset::Error>>()?;
        Ok(Self {
            rules,
            default_config,
        })
    }

    pub fn get_config(&self, queue_path: &str) -> QueueConfig {
        for (matcher, config) in &self.rules {
            if matcher.is_match(queue_path) {
                return *config;
            }
        }
        self.default_config
    }

    pub fn all_active() -> Self {
        Self {
            rules: Vec::new(),
            default_config: QueueConfig::active(),
        }
    }
}

#[derive(Default)]
pub struct QueueMode {
    pub readonly: bool,
}
