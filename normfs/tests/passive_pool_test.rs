//! Two arenas: active pages for the queues somebody names in a rule, small
//! passive pages for everything else. The same instance must refuse a wide
//! record on a passive queue, accept it on an active one, and a passive
//! queue must still work end to end.

use bytes::Bytes;
use normfs::{
    NormFS, NormFsSettings, QueueConfig, QueueSettings, ReadPosition, DEFAULT_PASSIVE_PAGE_SIZE,
};
use tokio::sync::mpsc;
use uintn::UintN;

fn settings_with_cam_rule() -> NormFsSettings {
    NormFsSettings {
        // Rules match the queue's absolute path (`/instance/cam0`), so a
        // name-shaped rule needs the `**/` prefix.
        queue_settings: QueueSettings::new(
            vec![("**/cam*".to_string(), QueueConfig::active())],
            QueueConfig::default(),
        )
        .expect("the glob is valid"),
        ..NormFsSettings::default()
    }
}

#[tokio::test]
async fn a_rule_routes_a_queue_to_the_active_arena_and_the_default_is_passive() {
    let temp = tempfile::TempDir::new().unwrap();
    let fs = NormFS::new(temp.path().to_path_buf(), settings_with_cam_rule())
        .await
        .unwrap();

    // Wider than a passive page, well within an active one.
    let wide = Bytes::from(vec![7u8; DEFAULT_PASSIVE_PAGE_SIZE]);

    let cam = fs.resolve("cam0");
    fs.ensure_queue_exists_for_write(&cam).await.unwrap();
    fs.enqueue(&cam, wide.clone())
        .await
        .expect("the cam* rule puts this queue on the active arena");

    let startup = fs.resolve("startup");
    fs.ensure_queue_exists_for_write(&startup).await.unwrap();
    let refused = fs.enqueue(&startup, wide).await;
    assert!(
        refused.is_err(),
        "a record wider than a passive page must be refused on a queue no \
         rule made active"
    );

    // The refusal happened before an id was taken: the next record is id 0.
    let small = Bytes::from_static(b"one line a month");
    let id = fs.enqueue(&startup, small).await.unwrap();
    assert_eq!(id, UintN::zero(), "a refused record must not consume an id");

    fs.close().await.unwrap();
}

#[tokio::test]
async fn a_passive_queue_survives_a_restart() {
    let temp = tempfile::TempDir::new().unwrap();
    let records: Vec<Bytes> = (0..50)
        .map(|i| Bytes::from(format!("startup-record-{i}")))
        .collect();

    {
        let fs = NormFS::new(temp.path().to_path_buf(), settings_with_cam_rule())
            .await
            .unwrap();
        let queue = fs.resolve("startup");
        fs.ensure_queue_exists_for_write(&queue).await.unwrap();
        for r in &records {
            fs.enqueue(&queue, r.clone()).await.unwrap();
        }
        fs.close().await.unwrap();
    }

    let fs = NormFS::new(temp.path().to_path_buf(), settings_with_cam_rule())
        .await
        .unwrap();
    let queue = fs.resolve("startup");
    fs.ensure_queue_exists_for_write(&queue).await.unwrap();

    let (tx, mut rx) = mpsc::channel(64);
    fs.read(&queue, ReadPosition::Absolute(UintN::zero()), 50, 1, tx)
        .await
        .unwrap();
    for (i, expected) in records.iter().enumerate() {
        let entry = rx.recv().await.unwrap_or_else(|| {
            panic!("record {i} must come back after a restart of a passive queue")
        });
        assert_eq!(&entry.data, expected, "record {i} must round-trip intact");
    }

    fs.close().await.unwrap();
}
